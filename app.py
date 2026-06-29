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

async def ffmpeg_stream_generator(stream_url: str):
    try:
        clean_bitrate = "".join(c for c in VIDEO_BITRATE if c.isdigit())
        bitrate_int = int(clean_bitrate) if clean_bitrate else 3000
    except Exception:
        bitrate_int = 3000
        
    bufsize_str = f"{bitrate_int * 2}k"

    ffmpeg_cmd = [
        "ffmpeg",
        "-nostdin",
        "-reconnect", "1",
        "-reconnect_at_eof", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-fflags", "+genpts",
        "-avoid_negative_ts", "make_zero",
    ]

    if VIDEO_CODEC == "h264_vaapi":
        ffmpeg_cmd.extend(["-vaapi_device", "/dev/dri/renderD128"])
        vf_filter = "scale=1280:720,fps=30,format=nv12,hwupload"
    else:
        vf_filter = "scale=1280:720,fps=30"

    ffmpeg_cmd.extend([
        "-i", stream_url,
        "-map", "0:v:0",
        "-map", "0:a:0",
        "-c:v", VIDEO_CODEC,
        "-vf", vf_filter,
        "-vsync", "cfr",
        "-b:v", VIDEO_BITRATE,
        "-maxrate:v", VIDEO_BITRATE,
        "-bufsize:v", bufsize_str,
        "-c:a", "aac",
        "-ac", "2",
        "-ar", "48000",
        "-b:a", AUDIO_BITRATE,
        "-f", "mpegts",
        "pipe:1"
    ])

    while True:
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