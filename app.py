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
STALL_TIMEOUT = float(os.getenv("STALL_TIMEOUT", "5"))

# Separate, longer watchdog for time-to-FIRST-byte. ffmpeg is configured
# with -analyzeduration/-probesize 15000000 (15s) below, and Masqueradarr's
# own LocalNow/Pluto stitcher can take several seconds to start serving a
# freshly-spawned session on top of that. Using STALL_TIMEOUT here too
# would kill ffmpeg mid-analysis on every single connection attempt,
# before it ever produces a byte - indistinguishable from a dead source.
# Defaults a bit above the analyzeduration ceiling to leave room for
# Masqueradarr's own startup latency.
STARTUP_TIMEOUT = float(os.getenv("STARTUP_TIMEOUT", "20"))

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
        # NOTE: there is deliberately NO ffprobe/preflight pass on `url`
        # before ffmpeg connects. Masqueradarr's FAST channel adapters
        # (Pluto, LocalNow) hand out single-use tokenized URLs - a
        # preflight probe consumes the token, so by the time ffmpeg
        # itself connects, the URL is already dead/expired. ffmpeg makes
        # the ONE and ONLY connection to `url`; missing audio is handled
        # via the optional "?" stream mapping in build_ffmpeg_cmd()
        # instead of pre-detecting stream layout.

        # Track how many consecutive attempts produced zero output bytes.
        # When Masqueradarr's upstream CDN source is dead or the CDN feed
        # is stuck mid-GOP, every respawn hits the same wall: a burst of
        # undecodeable H.264 frames (missing SPS/PPS/IDR) then silence.
        # Masqueradarr runs ffmpeg in remux/copy mode — it cannot inject
        # IDR keyframes on connection, so every fresh connection lands
        # mid-GOP by definition. If the CDN never sends a keyframe in our
        # analysis window, ffmpeg produces zero output.
        # Rapid-firing respawns hammer Masqueradarr while it may itself be
        # mid-reconnect-cycle to the CDN (-reconnect_streamed is set in
        # Masqueradarr's own ffmpeg command). Exponential backoff gives
        # both Masqueradarr and the CDN time to stabilise, capping at
        # MAX_BACKOFF so we recover quickly once the source comes back.
        zero_output_streak = 0
        MAX_BACKOFF = 60  # seconds

        while True:
            ffmpeg_cmd = build_ffmpeg_cmd(url)
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
            bytes_produced = 0
            got_first_byte = False

            try:
                while True:
                    try:
                        # Time-to-first-byte gets a much longer leash than
                        # the steady-state stall watchdog. ffmpeg is given
                        # -analyzeduration/-probesize 15000000 (15s) to
                        # analyze the input before it can emit anything on
                        # stdout, and Masqueradarr's LocalNow/Pluto stitcher
                        # can itself take several seconds to start serving
                        # a freshly-spawned session. STALL_TIMEOUT (5s) is
                        # tuned to catch a genuine mid-stream freeze on an
                        # already-flowing connection - applying it to the
                        # initial connect+analyze phase kills ffmpeg before
                        # it ever has a chance to produce its first packet,
                        # which looks identical to a dead source (zero
                        # bytes, no ffmpeg error, repeating every respawn).
                        read_timeout = STALL_TIMEOUT if got_first_byte else STARTUP_TIMEOUT
                        chunk = await asyncio.wait_for(
                            process.stdout.read(65536), timeout=read_timeout
                        )
                    except asyncio.TimeoutError:
                        if got_first_byte:
                            logger.warning(
                                f"[broker] No data from FFmpeg stdout for {STALL_TIMEOUT}s "
                                f"(upstream stall, connection still open). Forcing respawn."
                            )
                        else:
                            logger.warning(
                                f"[broker] No initial data from FFmpeg within {STARTUP_TIMEOUT}s "
                                f"(startup/analyze phase never produced output). Forcing respawn."
                            )
                        stalled = True
                        break
                    if not chunk:
                        break
                    if not got_first_byte:
                        got_first_byte = True
                        logger.info(f"[broker] First output bytes received for {url}")
                    bytes_produced += len(chunk)
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

            # Update zero-output streak for backoff calculation.
            if bytes_produced == 0:
                zero_output_streak += 1
                logger.warning(
                    f"[broker] Zero output streak: {zero_output_streak} "
                    f"(upstream source may be dead or stuck mid-GOP)"
                )
            else:
                if zero_output_streak > 0:
                    logger.info(f"[broker] Upstream recovered after {zero_output_streak} zero-output attempts.")
                zero_output_streak = 0

            # Sleep before next attempt. Two factors:
            #
            # 1. BACKOFF: if the upstream is consistently delivering nothing,
            #    apply exponential backoff (2^streak seconds, capped at
            #    MAX_BACKOFF) to stop hammering a dead source. A dead LocalNow
            #    channel with rapid retries can interfere with Masqueradarr's own
            #    restart cycle, making recovery take longer.
            #
            # 2. HOT vs COLD: when clients are still connected, we want to
            #    recover as fast as possible (minimum 1s base) since every
            #    extra second is silence that drains Dispatcharr's buffer.
            #    When no clients are waiting, use a 4s base to avoid
            #    hammering Masqueradarr during its own CDN reconnect cycle.
            clients_waiting = url in self._channels and len(self._channels[url]["queues"]) > 0
            base_sleep = 1 if clients_waiting else 4

            if zero_output_streak > 1:
                # Exponential backoff starting on the second consecutive failure.
                backoff = min(2 ** (zero_output_streak - 1), MAX_BACKOFF)
                sleep_secs = backoff
                logger.info(
                    f"[broker] Backoff sleep {sleep_secs}s "
                    f"(streak={zero_output_streak}, clients_waiting={clients_waiting})"
                )
            else:
                sleep_secs = base_sleep
                logger.info(f"[broker] Respawn sleep {sleep_secs}s (clients_waiting={clients_waiting})")

            await asyncio.sleep(sleep_secs)


broker = ChannelBroker()


def build_ffmpeg_cmd(stream_url: str) -> list:
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

    # NOTE: -re was previously used here to pace input reading at 1x
    # real-time rate. It was removed because with a live transcoding
    # upstream (Masqueradarr in remux/copy mode), -re throttles how fast
    # ffmpeg consumes data from the HTTP connection. When ffmpeg connects
    # mid-GOP and the decoder is stuck on undecodeable P/B frames (no
    # SPS/PPS yet), -re causes ffmpeg to consume packets so slowly that
    # Masqueradarr's output buffer fills up and stops sending — producing
    # a permanent stall before the first IDR frame ever arrives.
    # Without -re, ffmpeg reads as fast as the network allows, burns
    # through the initial mid-GOP junk quickly, and reaches the next IDR
    # frame before any buffer backs up. Output rate is already controlled
    # by -vsync cfr and -r 30 on the encode side, so removing -re does
    # not cause burst/jitter issues on a live source that delivers at
    # real-time rate. If a future source delivers significantly faster
    # than real-time and causes burst issues, re-introduce -re only for
    # that source via a separate ffmpeg profile.
    #
    # There is a single input — `stream_url` — and nothing else. No
    # preflight ffprobe, no second lavfi input keyed off probe results:
    # Masqueradarr's FAST adapters (Pluto, LocalNow) issue single-use
    # tokenized URLs, so the only connection this process is allowed to
    # make to `url` is the one ffmpeg itself opens here. Whether the
    # source actually carries an audio stream is discovered by ffmpeg at
    # connect time, not predicted beforehand.
    ffmpeg_cmd.extend(["-i", stream_url])

    # "0:a:0?" — the trailing "?" makes the audio map optional, so an
    # audio-less source (or one whose audio track hasn't appeared yet
    # within the analyze window) doesn't abort the whole ffmpeg process.
    # This is what replaced the old ffprobe-then-branch approach.
    ffmpeg_cmd.extend(["-map", "0:v:0"])
    ffmpeg_cmd.extend(["-map", "0:a:0?"])

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
