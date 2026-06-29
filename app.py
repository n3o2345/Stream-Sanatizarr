import asyncio
import logging
import os
import urllib.parse
import requests
from fastapi import FastAPI, HTTPException, Response, Query, Request
from fastapi.responses import StreamingResponse, HTMLResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("StreamSanitizarr")

app = FastAPI(title="FAST Stream Sanitizer Proxy")

VIDEO_CODEC = os.getenv("VIDEO_CODEC", "libx264")  
VIDEO_BITRATE = os.getenv("VIDEO_BITRATE", "3000k")
AUDIO_BITRATE = os.getenv("AUDIO_BITRATE", "128k")

async def ffmpeg_stream_generator(stream_url: str):
    ffmpeg_cmd = [
        "ffmpeg",
        "-reconnect", "1",
        "-reconnect_at_eof", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-fflags", "+genpts+async",
        "-avoid_negative_ts", "make_zero",
    ]

    if VIDEO_CODEC == "h264_vaapi":
        ffmpeg_cmd.extend(["-vaapi_device", "/dev/dri/renderD128"])

    ffmpeg_cmd.extend([
        "-i", stream_url,
        "-c:v", VIDEO_CODEC,
        "-vf", "scale=1280:720,fps=30",
        "-vsync", "cfr",
        "-b:v", VIDEO_BITRATE,
        "-maxrate:v", VIDEO_BITRATE,
        "-bufsize:v", f"{int(VIDEO_BITRATE.replace('k',''))*2}k",
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
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL
        )

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
        
        logger.warning("Upstream stream disconnected. Re-spawning worker...")
        await asyncio.sleep(1)

@app.get("/", response_class=HTMLResponse)
async def web_ui(source: str = Query(None)):
    proxy_host = os.getenv("PROXY_PUBLIC_URL", "http://localhost:8089").rstrip("/")
    
    predefined_env = os.getenv("PREDEFINED_PLAYLISTS", "")
    playlists = []
    if predefined_env:
        for item in predefined_env.split(","):
            if "|" in item:
                name, url = item.split("|", 1)
                playlists.append({"name": name.strip(), "url": url.strip()})

    generated_url = ""
    if source:
        encoded_source = urllib.parse.quote_plus(source.strip())
        generated_url = f"{proxy_host}/playlist?url={encoded_source}"

    playlists_html = ""
    if playlists:
        playlists_html = "<h3>Preconfigured Playlists</h3>"
        for pl in playlists:
            enc_url = urllib.parse.quote_plus(pl['url'])
            sanitized_url = f"{proxy_host}/playlist?url={enc_url}"
            playlists_html += f"""
            <div class="playlist-item">
                <strong>{pl['name']}</strong>
                <div class="result-url" onclick="navigator.clipboard.writeText('{sanitized_url}'); alert('Copied {pl['name']} link!');">{sanitized_url}</div>
            </div>
            """

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
            h3 {{ margin-top: 25px; border-bottom: 1px solid #29292e; padding-bottom: 8px; color: #fff; }}
            p {{ color: #a8a8b3; font-size: 14px; line-height: 1.5; }}
            label {{ display: block; margin-bottom: 8px; font-weight: 600; font-size: 14px; }}
            input[type="url"] {{ width: 100%; padding: 12px; border: 1px solid #29292e; background: #121214; color: #fff; border-radius: 4px; box-sizing: border-box; font-size: 14px; margin-bottom: 15px; }}
            input[type="url"]:focus {{ border-color: #4fffaf; outline: none; }}
            button {{ background: #4fffaf; color: #000; border: none; padding: 12px 20px; font-weight: bold; border-radius: 4px; cursor: pointer; font-size: 14px; width: 100%; transition: background 0.2s; }}
            button:hover {{ background: #3ae099; }}
            .result-box {{ margin-top: 25px; padding: 15px; background: #121214; border-radius: 4px; border-left: 4px solid #4fffaf; }}
            .playlist-item {{ background: #121214; padding: 12px; border-radius: 6px; margin-bottom: 12px; border: 1px solid #29292e; }}
            .playlist-item strong {{ display: block; margin-bottom: 6px; font-size: 14px; color: #fff; }}
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

            {playlists_html}
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.get("/stream")
async def stream_proxy(request: Request):
    """
    Extracts the full raw bytes scope directly from the ASGI server layer
    to prevent broken parameter mapping on complex nested query string tags.
    """
    raw_query_bytes = request.scope.get("query_string", b"")
    raw_query = raw_query_bytes.decode("utf-8")
    
    if not raw_query.startswith("url="):
        raise HTTPException(status_code=400, detail="Missing target URL token prefix.")
    
    # Grab absolutely everything after 'url=' completely intact
    target_url = raw_query[4:]
    
    # Unquote twice to cleanly clean double-wrapped symbols (%253F -> %3F -> ?)
    target_url = urllib.parse.unquote(target_url)
    target_url = urllib.parse.unquote(target_url)
    
    if not target_url:
        raise HTTPException(status_code=400, detail="Target stream URL payload is empty.")
        
    logger.info(f"Targeting sanitized stream destination: {target_url}")
    return StreamingResponse(ffmpeg_stream_generator(target_url), media_type="video/mp2t")

@app.get("/playlist")
async def playlist_proxy(url: str):
    if not url:
        raise HTTPException(status_code=400, detail="Missing required 'url' parameter.")
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        raw_m3u = response.text
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch source playlist: {e}")

    proxy_host = os.getenv("PROXY_PUBLIC_URL", "http://localhost:8089").rstrip("/")
    sanitized_lines = []
    
    for line in raw_m3u.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            encoded_url = urllib.parse.quote_plus(line)
            sanitized_line = f"{proxy_host}/stream?url={encoded_url}"
            sanitized_lines.append(sanitized_line)
        else:
            sanitized_lines.append(line)
            
    return Response(content="\n".join(sanitized_lines), media_type="application/x-mpegurl")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)