import asyncio
import logging
import os
import urllib.parse
import httpx
from fastapi import FastAPI, HTTPException, Response, Query, Request
from fastapi.responses import StreamingResponse, HTMLResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("StreamSanitizarr")

app = FastAPI(title="FAST Stream Sanitizer Proxy")

VIDEO_CODEC = os.getenv("VIDEO_CODEC", "libx264")
VIDEO_BITRATE = os.getenv("VIDEO_BITRATE", "3000k")
AUDIO_BITRATE = os.getenv("AUDIO_BITRATE", "128k")
STREAM_USER_AGENT = os.getenv(
    "STREAM_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
)
PROBE_TIMEOUT = float(os.getenv("PROBE_TIMEOUT", "10"))

# Maximum seconds of silence on ffmpeg's stdout before we treat the worker
# as stalled and force a respawn. This covers the case where the upstream
# (e.g. a Pluto TV ad-break stitching gap, or Masqueradarr's internal
# engine pausing) goes quiet WITHOUT actually closing the TCP connection.
# In that case ffmpeg stays alive and "connected" indefinitely, so none of
# the -reconnect* flags or the existing "if not chunk: break" disconnect
# detection ever fire - the read() call just blocks waiting for data that
# isn't coming. Left unhandled, this produces the freeze-then-burst-catchup
# pattern (a long visible freeze, then a jerky dump of buffered frames once
# the source resumes) instead of a clean, fast respawn.
STALL_TIMEOUT = float(os.getenv("STALL_TIMEOUT", "8"))

# How many recent chunks to keep in memory per channel so that a newly
# connecting client (or Dispatcharr's buffer-fill phase) gets a small
# burst of recent data immediately rather than waiting for the next chunk
# from ffmpeg. 4 chunks at 65536 bytes each = ~256 KB, roughly 0.5-1s of
# video at 2-3 Mbps — enough to satisfy Dispatcharr's buffer-threshold
# check without holding too much in memory.
SUBSCRIBER_QUEUE_DEPTH = int(os.getenv("SUBSCRIBER_QUEUE_DEPTH", "32"))


# ---------------------------------------------------------------------------
# Channel broker — one ffmpeg worker per unique source URL
# ---------------------------------------------------------------------------
#
# Previously, every GET /stream request spawned its own independent ffmpeg
# process pulling from the same upstream URL. Two simultaneous connections
# (e.g. Dispatcharr reconnecting while the old connection is still alive)
# would produce two ffmpeg workers both hitting Masqueradarr at once. That
# upstream contention was itself the primary cause of stalls and H.264
# decode errors after respawn (Masqueradarr's engine can't cleanly serve
# two simultaneous reads of the same channel, so one or both get garbage
# data). The broker ensures only ONE ffmpeg worker ever runs per URL, and
# all clients subscribe to its output via per-client asyncio queues.

class ChannelBroker:
    def __init__(self):
        # url -> {"task": asyncio.Task, "queues": set[asyncio.Queue], "lock": asyncio.Lock}
        self._channels: dict[str, dict] = {}
        self._global_lock = asyncio.Lock()

    async def subscribe(self, url: str) -> asyncio.Queue:
        """Return a queue that will receive chunks for this URL, starting the
        worker task if one is not already running."""
        async with self._global_lock:
            if url not in self._channels:
                self._channels[url] = {
                    "task": None,
                    "queues": set(),
                    "lock": asyncio.Lock(),
                }
            ch = self._channels[url]

        async with ch["lock"]:
            q: asyncio.Queue = asyncio.Queue(maxsize=SUBSCRIBER_QUEUE_DEPTH)
            ch["queues"].add(q)
            if ch["task"] is None or ch["task"].done():
                logger.info(f"[broker] Starting worker for {url} (clients: {len(ch['queues'])})")
                ch["task"] = asyncio.create_task(self._worker(url))
            else:
                logger.info(f"[broker] Joining existing worker for {url} (clients: {len(ch['queues'])})")
            return q

    async def unsubscribe(self, url: str, q: asyncio.Queue):
        """Remove a client queue. If no clients remain, cancel the worker."""
        if url not in self._channels:
            return
        ch = self._channels[url]
        async with ch["lock"]:
            ch["queues"].discard(q)
            remaining = len(ch["queues"])
            logger.info(f"[broker] Client left {url} (remaining: {remaining})")
            if remaining == 0:
                if ch["task"] and not ch["task"].done():
                    logger.info(f"[broker] No clients left for {url}, stopping worker.")
                    ch["task"].cancel()
                async with self._global_lock:
                    self._channels.pop(url, None)

    async def _broadcast(self, url: str, chunk: bytes):
        """Push a chunk to every subscriber queue for this URL. If a queue is
        full (slow client), drop the oldest chunk to make room rather than
        blocking the worker for every other client."""
        if url not in self._channels:
            return
        ch = self._channels[url]
        for q in list(ch["queues"]):
            if q.full():
                try:
                    q.get_nowait()  # drop oldest
                except asyncio.QueueEmpty:
                    pass
            try:
                q.put_nowait(chunk)
            except asyncio.QueueFull:
                pass  # already handled above; just skip if still full

    async def _worker(self, url: str):
        """Single ffmpeg worker for one source URL. Runs until cancelled
        (all clients disconnected) or the outer task is explicitly cancelled."""
        probe = await probe_stream(url)

        while True:
            ffmpeg_cmd = build_ffmpeg_cmd(url, probe)
            logger.info(f"[broker] Spawning FFmpeg worker for stream: {url}")
            process = await asyncio.create_subprocess_exec(
                *ffmpeg_cmd,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            async def _drain_stderr(proc):
                try:
                    async for line in proc.stderr:
                        decoded = line.decode(errors="ignore").strip()
                        if decoded:
                            logger.warning(f"[ffmpeg] {decoded}")
                except Exception:
                    pass

            stderr_task = asyncio.create_task(_drain_stderr(process))
            stalled = False

            try:
                while True:
                    try:
                        chunk = await asyncio.wait_for(
                            process.stdout.read(65536), timeout=STALL_TIMEOUT
                        )
                    except asyncio.TimeoutError:
                        logger.warning(
                            f"[broker] No data from FFmpeg stdout for {STALL_TIMEOUT}s "
                            f"(upstream stall, connection still open). Forcing respawn."
                        )
                        stalled = True
                        break
                    if not chunk:
                        break
                    await self._broadcast(url, chunk)
            except asyncio.CancelledError:
                logger.info(f"[broker] Worker cancelled for {url} (no clients remain).")
                try:
                    process.terminate()
                    await process.wait()
                except ProcessLookupError:
                    pass
                stderr_task.cancel()
                raise
            except Exception as e:
                logger.error(f"[broker] Error reading stream chunks for {url}: {e}")
            finally:
                if process.returncode is None:
                    try:
                        process.terminate()
                        await process.wait()
                    except ProcessLookupError:
                        pass
                stderr_task.cancel()

            if stalled:
                logger.warning(f"[broker] Worker stalled (silent upstream) for {url}. Re-spawning...")
            else:
                logger.warning(f"[broker] Upstream disconnected for {url}. Re-spawning...")

            # Some upstreams (e.g. masqueradarr's LocalNow handler) run their
            # own internal capture engine (cvlc) that takes a few seconds to
            # spin up and has its own idle-timeout/teardown logic. Retrying
            # after only 1 second can land right in that engine's cold-start
            # or teardown window, producing a flapping cycle of 502s/empty
            # reads. A few seconds of breathing room avoids hammering a slow
            # upstream mid-restart.
            await asyncio.sleep(4)


broker = ChannelBroker()


async def probe_stream(stream_url: str) -> dict:
    """Probe the source with ffprobe to find out what's actually in it."""
    info = {"has_video": True, "has_audio": True}

    ffprobe_cmd = [
        "ffprobe",
        "-loglevel", "error",
        "-user_agent", STREAM_USER_AGENT,
        "-print_format", "json",
        "-show_streams",
        stream_url,
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *ffprobe_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=PROBE_TIMEOUT)

        if proc.returncode != 0:
            logger.warning(f"ffprobe exited {proc.returncode}: {stderr.decode(errors='ignore').strip()}")
            return info

        import json
        data = json.loads(stdout.decode(errors="ignore") or "{}")
        streams = data.get("streams", [])

        video_streams = [s for s in streams if s.get("codec_type") == "video"]
        audio_streams = [s for s in streams if s.get("codec_type") == "audio"]

        info["has_video"] = bool(video_streams)
        info["has_audio"] = bool(audio_streams)

        logger.info(
            f"Probe result for {stream_url}: video={info['has_video']} audio={info['has_audio']}"
        )
    except asyncio.TimeoutError:
        logger.warning(f"ffprobe timed out after {PROBE_TIMEOUT}s, assuming standard layout.")
    except Exception as e:
        logger.warning(f"ffprobe failed ({e}), assuming standard layout.")

    return info


def build_ffmpeg_cmd(stream_url: str, probe: dict) -> list:
    try:
        clean_bitrate = "".join(c for c in VIDEO_BITRATE if c.isdigit())
        bitrate_int = int(clean_bitrate) if clean_bitrate else 3000
    except Exception:
        bitrate_int = 3000

    bufsize_str = f"{bitrate_int * 2}k"
    OUTPUT_FPS = 30
    GOP_SIZE = OUTPUT_FPS * 2

    ffmpeg_cmd = [
        "ffmpeg",
        "-nostdin",
        "-loglevel", "warning",
        "-progress", "pipe:2",
        "-stats_period", "5",
        "-sn",
        "-dn",
        "-analyzeduration", "15000000",
        "-probesize", "15000000",
        "-user_agent", STREAM_USER_AGENT,
        "-reconnect", "1",
        "-reconnect_at_eof", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-fflags", "+genpts+discardcorrupt+igndts",
        "-avoid_negative_ts", "make_zero",
        # NOTE: -use_wallclock_as_timestamps was previously used here to
        # survive A/V desync across a network stall/reconnect. It was removed
        # because in steady-state operation it stamps every frame's PTS with
        # its raw network arrival time. Live HTTP/TS proxy sources rarely
        # deliver packets at perfectly even intervals, and that sub-frame
        # jitter made -vsync cfr's dup/drop decisions uneven, producing
        # visibly jerky motion even though average frame rate was correct.
        # Relying on +genpts (synthesize timestamps from source DTS/duration)
        # gives -vsync cfr a smoother timestamp series to work from. Full
        # stalls/disconnects are handled by the broker's respawn loop.
    ]

    has_video = probe.get("has_video", True)
    has_audio = probe.get("has_audio", True)

    if VIDEO_CODEC == "h264_vaapi":
        ffmpeg_cmd.extend(["-vaapi_device", "/dev/dri/renderD128"])
        vf_filter = (
            f"scale=1280:720:force_original_aspect_ratio=decrease,"
            f"pad=1280:720:(ow-iw)/2:(oh-ih)/2:color=black,"
            f"fps={OUTPUT_FPS},format=nv12,hwupload"
        )
    else:
        vf_filter = (
            f"scale=1280:720:force_original_aspect_ratio=decrease,"
            f"pad=1280:720:(ow-iw)/2:(oh-ih)/2:color=black,"
            f"fps={OUTPUT_FPS},format=yuv420p"
        )

    if has_video:
        # -re paces reading of this input at its native/real-time rate.
        # Without it, if the upstream delivers data faster than real-time
        # (observed: ffmpeg sustaining 2x+ speed for 100+ seconds rather than
        # briefly catching up and settling near 1x), ffmpeg processes and
        # emits output in bursts rather than smooth real-time flow, which
        # downstream clients perceive as jittery/choppy playback.
        ffmpeg_cmd.extend(["-re", "-i", stream_url])
    else:
        ffmpeg_cmd.extend(["-f", "lavfi", "-i", f"color=c=black:s=1280x720:r={OUTPUT_FPS}"])
        ffmpeg_cmd.extend(["-re", "-i", stream_url])

    if not has_audio:
        ffmpeg_cmd.extend(["-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=48000"])

    if has_video:
        video_input_idx = 0
        real_source_idx = 0
        next_free_idx = 1
    else:
        video_input_idx = 0
        real_source_idx = 1
        next_free_idx = 2

    if has_audio:
        audio_map = f"{real_source_idx}:a:0?"
    else:
        audio_map = f"{next_free_idx}:a:0"

    ffmpeg_cmd.extend(["-map", f"{video_input_idx}:v:0"])
    ffmpeg_cmd.extend(["-map", audio_map])

    ffmpeg_cmd.extend([
        "-c:v", VIDEO_CODEC,
        "-vf", vf_filter,
        "-vsync", "cfr",
        "-r", str(OUTPUT_FPS),
        "-g", str(GOP_SIZE),
        "-keyint_min", str(GOP_SIZE),
        "-sc_threshold", "0",
        "-b:v", VIDEO_BITRATE,
        "-maxrate:v", VIDEO_BITRATE,
        "-bufsize:v", bufsize_str,
        "-c:a", "aac",
        "-ac", "2",
        "-ar", "48000",
        "-b:a", AUDIO_BITRATE,
        "-af", "aresample=async=1:first_pts=0",
        "-mpegts_flags", "+resend_headers",
        "-muxdelay", "0",
        "-muxpreload", "0",
        # Some live sources deliver audio/video packets at uneven, jittery
        # rates relative to each other (common on HTTP/TS sources with
        # separately-paced PIDs). Without enough queue depth, ffmpeg's
        # muxer backs up waiting to interleave the slower stream's packets,
        # producing escalating "buffers queued ... something may be wrong"
        # warnings and an effective stall/no-output condition.
        "-max_muxing_queue_size", "9999",
        "-f", "mpegts",
        "pipe:1",
    ])

    return ffmpeg_cmd


async def subscriber_generator(url: str, q: asyncio.Queue):
    """Async generator that yields chunks from the broker queue for one client."""
    try:
        while True:
            chunk = await q.get()
            if chunk is None:
                break
            yield chunk
    except asyncio.CancelledError:
        pass
    finally:
        await broker.unsubscribe(url, q)


@app.get("/", response_class=HTMLResponse)
async def web_ui(source: str = Query(None)):
    proxy_host = os.getenv("PROXY_PUBLIC_URL", "http://192.168.1.254:8096").rstrip("/")

    generated_url = ""
    if source:
        encoded_source = urllib.parse.quote(source.strip(), safe='')
        generated_url = f"{proxy_host}/playlist?url={encoded_source}"

    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Stream-Sanatizarr Dashboard</title>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; background: #121214; color: #e1e1e6; padding: 40px; max-width: 650px; margin: 0 auto; }}
            .container {{ background: #1d1d22; padding: 30px; border-radius: 8px; border: 1px solid #29292e; box-shadow: 0 4px 12px rgba(0,0,0,0.3); }}
            h1 {{ color: #4fffaf; margin-top: 0; }}
            input[type="url"] {{ width: 100%; padding: 12px; background: #121214; color: #fff; border: 1px solid #29292e; border-radius: 4px; box-sizing: border-box; margin-bottom: 15px; font-size: 14px; }}
            input[type="url"]:focus {{ border-color: #4fffaf; outline: none; }}
            button {{ background: #4fffaf; color: #000; border: none; padding: 12px; font-weight: bold; border-radius: 4px; cursor: pointer; width: 100%; font-size: 14px; }}
            button:hover {{ background: #3ae099; }}
            .result-box {{ margin-top: 25px; padding: 15px; background: #121214; border-radius: 4px; border-left: 4px solid #4fffaf; }}
            .result-url {{ font-family: monospace; background: #29292e; padding: 10px; border-radius: 4px; color: #4fffaf; word-break: break-all; font-size: 13px; user-select: all; cursor: pointer; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Stream-Sanatizarr</h1>
            <p>Paste your Masqueradarr M3U output URL below:</p>
            <form method="get" action="/">
                <input type="url" id="source" name="source" placeholder="http://192.168.1.254:6001/api/v1/..." value="{source or ''}" required autocomplete="off">
                <button type="submit">Generate Link for Dispatcharr</button>
            </form>
            {f'''
            <div class="result-box">
                <div class="result-title">Copy this URL into Dispatcharr:</div>
                <div class="result-url" onclick="navigator.clipboard.writeText(this.innerText); alert('Copied!');">{generated_url}</div>
            </div>
            ''' if generated_url else ''}
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


@app.get("/stream")
async def stream_proxy(request: Request):
    raw_query_bytes = request.scope.get("query_string", b"")
    raw_query = raw_query_bytes.decode("utf-8")

    if not raw_query.startswith("url="):
        raise HTTPException(status_code=400, detail="Missing target URL token prefix.")

    target_url = raw_query[4:]
    target_url = urllib.parse.unquote(target_url)

    if not target_url:
        raise HTTPException(status_code=400, detail="Target stream URL payload is empty.")

    if target_url.count('?') > 1:
        path_part, sep, qs_part = target_url.partition('?')
        qs_part = qs_part.replace('?', '&')
        target_url = f"{path_part}{sep}{qs_part}"

    logger.info(f"Targeting sanitized stream destination: {target_url}")

    q = await broker.subscribe(target_url)
    return StreamingResponse(
        subscriber_generator(target_url, q),
        media_type="video/mp2t",
    )


@app.get("/playlist")
async def playlist_proxy(url: str):
    if not url:
        raise HTTPException(status_code=400, detail="Missing required 'url' parameter.")

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            raw_m3u = response.text
    except Exception as e:
        logger.error(f"Failed to reach upstream M3U file: {e}")
        raise HTTPException(status_code=502, detail=f"Failed to fetch source playlist: {e}")

    proxy_host = os.getenv("PROXY_PUBLIC_URL", "http://192.168.1.254:8096").rstrip("/")
    sanitized_lines = []

    for line in raw_m3u.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            # IMPORTANT: do NOT pre-decode (unquote) the line before encoding
            # it here. Some upstream sources (e.g. masqueradarr's LocalNow
            # handler) deliberately embed an ALREADY percent-encoded opaque
            # token in the path - e.g. "localnow%3A%2F%2FGUID%3Fslug%3Dalf"
            # followed by a genuine literal '?' separator for real query
            # params (token=...&pl=...). Their router string-matches against
            # that STILL-ENCODED path token, not its decoded form.
            #
            # Applying quote(line, safe='') directly here re-encodes the
            # whole line one extra layer (turning the existing "%3A" into
            # "%253A", and the genuine literal "?" into "%3F"). A single
            # unquote() downstream in /stream then peels exactly one layer
            # back off - which restores the path token to its ORIGINAL
            # still-encoded form (e.g. back to "%3A"), while correctly
            # restoring the one genuine literal "?" separator too. This
            # round-trips back to exactly what the source intended.
            #
            # Pre-decoding with unquote() here (as a previous version of
            # this code did) instead fully collapses the opaque path token
            # into literal characters (e.g. "localnow://GUID"), which the
            # upstream router then fails to match, producing a 502 Bad
            # Gateway. Do not reintroduce that pre-decode step.
            encoded_url = urllib.parse.quote(line, safe='')
            sanitized_line = f"{proxy_host}/stream?url={encoded_url}"
            sanitized_lines.append(sanitized_line)
        else:
            sanitized_lines.append(line)

    return Response(content="\n".join(sanitized_lines), media_type="application/x-mpegurl")


if __name__ == "__main__":
    import uvicorn
    run_port = int(os.getenv("PORT", "8096"))
    uvicorn.run(app, host="0.0.0.0", port=run_port)
