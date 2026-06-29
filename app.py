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


async def probe_stream(stream_url: str) -> dict:
    """Probe the source with ffprobe to find out what's actually in it
    (video present?, audio present?) so the ffmpeg command can be built
    to match instead of assuming a fixed video+stereo-audio layout."""
    info = {"has_video": True, "has_audio": True}

    ffprobe_cmd = [
        "ffprobe",
        "-nostdin",
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

    # Fixed output frame rate, used for both the fps filter and the GOP size,
    # so every output - regardless of the source's native fps/keyframe
    # interval - gets a consistent, predictable closed-GOP structure. This
    # matters a lot for Dispatcharr's TS segmenting and for client-side
    # channel-change/seek stability.
    OUTPUT_FPS = 30
    GOP_SIZE = OUTPUT_FPS * 2  # one keyframe every 2 seconds

    ffmpeg_cmd = [
        "ffmpeg",
        "-nostdin",
        "-loglevel", "warning",
        "-analyzeduration", "10000000",
        "-probesize", "10000000",
        "-user_agent", STREAM_USER_AGENT,
        "-reconnect", "1",
        "-reconnect_at_eof", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-fflags", "+genpts+discardcorrupt+igndts",
        "-avoid_negative_ts", "make_zero",
    ]

    has_video = probe.get("has_video", True)
    has_audio = probe.get("has_audio", True)

    if VIDEO_CODEC == "h264_vaapi":
        ffmpeg_cmd.extend(["-vaapi_device", "/dev/dri/renderD128"])
        # format=nv12,hwupload is required ahead of a vaapi encoder no matter
        # what pixel format/colorspace the source actually was (yuv420p,
        # yuv422p, 10-bit, full-range "yuvj" variants, etc).
        vf_filter = f"scale=1280:720,fps={OUTPUT_FPS},format=nv12,hwupload"
    else:
        # format=yuv420p forces every source down to the one pixel format
        # virtually every TS decoder downstream can rely on, regardless of
        # the source's native chroma subsampling/bit depth.
        vf_filter = f"scale=1280:720,fps={OUTPUT_FPS},format=yuv420p"

    if has_video:
        ffmpeg_cmd.extend(["-i", stream_url])
    else:
        # No video track in the source at all (e.g. an audio-only FAST
        # slate/ad-break feed). Synthesize a static color "card" so
        # Dispatcharr/clients that expect a video PID don't drop the channel.
        ffmpeg_cmd.extend(["-f", "lavfi", "-i", f"color=c=black:s=1280x720:r={OUTPUT_FPS}"])
        ffmpeg_cmd.extend(["-i", stream_url])

    if not has_audio:
        # No audio track in the source at all - synthesize a silent one so
        # downstream players/Dispatcharr that expect an audio PID don't choke.
        ffmpeg_cmd.extend(["-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=48000"])

    # Build -map args from which inputs actually exist, rather than assuming
    # a fixed "input 0 has both video and audio" layout - input indices
    # shift depending on whether the synthetic color/silence generators
    # were inserted above.
    if has_video:
        video_input_idx = 0
        real_source_idx = 0
        next_free_idx = 1
    else:
        video_input_idx = 0       # synthetic color source
        real_source_idx = 1       # real (audio-only) source
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
        "-f", "mpegts",
        "pipe:1",
    ])

    return ffmpeg_cmd


async def ffmpeg_stream_generator(stream_url: str):
    probe = await probe_stream(stream_url)

    while True:
        ffmpeg_cmd = build_ffmpeg_cmd(stream_url, probe)
        logger.info(f"Spawning FFmpeg worker for stream: {stream_url}")
        process = await asyncio.create_subprocess_exec(
            *ffmpeg_cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
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

        try:
            while True:
                chunk = await process.stdout.read(65536)
                if not chunk:
                    break
                yield chunk
        except asyncio.CancelledError:
            logger.info("Client disconnected. Terminating FFmpeg process.")
            try:
                process.terminate()
                await process.wait()
            except ProcessLookupError:
                pass
            raise
        except Exception as e:
            logger.error(f"Error while reading stream chunks: {e}")
        finally:
            if process.returncode is None:
                try:
                    process.terminate()
                    await process.wait()
                except ProcessLookupError:
                    pass
            stderr_task.cancel()
        
        logger.warning("Upstream stream disconnected. Re-spawning worker...")
        await asyncio.sleep(1)

@app.get("/", response_class=HTMLResponse)
async def web_ui(source: str = Query(None)):
    proxy_host = os.getenv("PROXY_PUBLIC_URL", "http://localhost:8000").rstrip("/")
    
    generated_url = ""
    if source:
        encoded_source = urllib.parse.quote_plus(source.strip())
        generated_url = f"{proxy_host}/playlist?url={encoded_source}"

    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Stream-Sanatizarr Dashboard</title>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #121214; color: #e1e1e6; padding: 40px 20px; max-width: 650px; margin: 0 auto; }}
            .container {{ background: #1d1d22; padding: 30px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.3); border: 1px solid #29292e; }}
            h1 {{ margin-top: 0; color: #4fffaf; font-size: 24px; }}
            p {{ color: #a8a8b3; font-size: 14px; line-height: 1.5; }}
            label {{ display: block; margin-bottom: 8px; font-weight: 600; font-size: 14px; }}
            input[type="url"] {{ width: 100%; padding: 12px; border: 1px solid #29292e; background: #121214; color: #fff; border-radius: 4px; box-sizing: border-box; font-size: 14px; margin-bottom: 15px; }}
            input[type="url"]:focus {{ border-color: #4fffaf; outline: none; }}
            button {{ background: #4fffaf; color: #000; border: none; padding: 12px 20px; font-weight: bold; border-radius: 4px; cursor: pointer; font-size: 14px; width: 100%; transition: background 0.2s; }}
            button:hover {{ background: #3ae099; }}
            .result-box {{ margin-top: 25px; padding: 15px; background: #121214; border-radius: 4px; border-left: 4px solid #4fffaf; }}
            .result-title {{ font-weight: bold; font-size: 12px; color: #a8a8b3; text-transform: uppercase; margin-bottom: 8px; }}
            .result-url {{ font-family: monospace; word-break: break-all; background: #29292e; padding: 10px; border-radius: 4px; font-size: 13px; color: #4fffaf; user-select: all; cursor: pointer; }}
            .copy-hint {{ font-size: 11px; color: #737380; margin-top: 5px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Stream-Sanatizarr</h1>
            <p>Paste a raw FAST M3U playlist URL below to wrap it with the stabilization engine layer.</p>
            
            <form method="get" action="/">
                <label for="source">Source M3U Playlist URL:</label>
                <input type="url" id="source" name="source" placeholder="https://example.com/source.m3u" value="{source or ''}" required autocomplete="off">
                <button type="submit">Generate Sanitized URL</button>
            </form>

            {f'''
            <div class="result-box">
                <div class="result-title">Generated Link for Dispatcharr:</div>
                <div class="result-url" onclick="navigator.clipboard.writeText(this.innerText); alert('Copied!');">{generated_url}</div>
                <div class="copy-hint">💡 Click the link above to copy it.</div>
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
    # Use unquote_plus to match the quote_plus() encoding used when the
    # playlist URL was generated (quote_plus turns spaces into '+').
    target_url = urllib.parse.unquote_plus(target_url)

    if not target_url:
        raise HTTPException(status_code=400, detail="Target stream URL payload is empty.")

    # Some upstream URLs (e.g. Dispatcharr LocalNow) embed a percent-encoded
    # query string inside the path segment - e.g. localnow%3A%2F%2FGUID%3Fslug%3Dalf
    # After unquote_plus the %3F becomes a literal '?' producing a double-'?' URL
    # (path?slug=alf?token=...) that FFmpeg rejects as invalid.
    # Fix: partition on the FIRST '?' only (the real query string), then
    # re-encode any remaining bare '?' characters in the path segment.
    if target_url.count('?') > 1:
        path_part, _, qs_part = target_url.partition('?')
        path_part = path_part.replace('?', '%3F')
        target_url = f"{path_part}?{qs_part}"

    logger.info(f"Targeting sanitized stream destination: {target_url}")
    return StreamingResponse(ffmpeg_stream_generator(target_url), media_type="video/mp2t")

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

    proxy_host = os.getenv("PROXY_PUBLIC_URL", "http://localhost:8000").rstrip("/")
    sanitized_lines = []
    
    for line in raw_m3u.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            # Some upstream playlist sources hand back stream URLs that are
            # already partially percent-encoded (e.g. a redirect param
            # embedded as "%3Fslug%3Dalf" inside an otherwise-raw URL).
            # Use plain unquote() (NOT unquote_plus) here - unquote_plus
            # would also treat any literal '+' character in a token as an
            # encoded space and corrupt it. Decoding %XX sequences only
            # collapses any pre-existing encoding down to raw characters,
            # so quote_plus() below applies exactly one consistent layer
            # of encoding instead of double-escaping what the source sent.
            normalized_line = urllib.parse.unquote(line)
            encoded_url = urllib.parse.quote_plus(normalized_line)
            sanitized_line = f"{proxy_host}/stream?url={encoded_url}"
            sanitized_lines.append(sanitized_line)
        else:
            sanitized_lines.append(line)
            
    return Response(content="\n".join(sanitized_lines), media_type="application/x-mpegurl")

if __name__ == "__main__":
    import uvicorn
    # Dynamically read the custom port from environmental variable definition
    run_port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=run_port)