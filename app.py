import asyncio
import logging
import os
import urllib.parse
import requests
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import StreamingResponse

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("StreamSanitizer")

app = FastAPI(title="FAST Stream Sanitizer Proxy")

# Environment configuration defaults
VIDEO_CODEC = os.getenv("VIDEO_CODEC", "libx264")  
VIDEO_BITRATE = os.getenv("VIDEO_BITRATE", "3000k")
AUDIO_BITRATE = os.getenv("AUDIO_BITRATE", "128k")

async def ffmpeg_stream_generator(stream_url: str):
    """
    Spawns FFmpeg workers to normalize the stream. 
    Loops infinitely to resume if the upstream connection drops during ad breaks.
    """
    ffmpeg_cmd = [
        "ffmpeg",
        "-reconnect", "1",
        "-reconnect_at_eof", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-fflags", "+genpts+async",
        "-avoid_negative_ts", "make_zero",
    ]

    # Required initialization parameter ONLY if using generic VAAPI (AMD/Intel)
    if VIDEO_CODEC == "h264_vaapi":
        ffmpeg_cmd.extend(["-vaapi_device", "/dev/dri/renderD128"])

    ffmpeg_cmd.extend([
        "-i", stream_url,
        
        # Video Normalization
        "-c:v", VIDEO_CODEC,
        "-vf", "scale=1280:720,fps=30",
        "-vsync", "cfr",
        "-b:v", VIDEO_BITRATE,
        "-maxrate:v", VIDEO_BITRATE,
        "-bufsize:v", f"{int(VIDEO_BITRATE.replace('k',''))*2}k",
        
        # Audio Normalization (Forces standard stereo AAC)
        "-c:a", "aac",
        "-ac", "2",
        "-ar", "48000",
        "-b:a", AUDIO_BITRATE,
        
        # Output Format
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
                chunk = await process.stdout.read(65536) # 64KB chunks
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
        
        logger.warning("Upstream stream disconnected/shifted. Re-spawning worker in 1 second...")
        await asyncio.sleep(1)

@app.get("/stream")
async def stream_proxy(url: str):
    if not url:
        raise HTTPException(status_code=400, detail="Missing required 'url' parameter.")
    
    logger.info(f"Received stream request for raw URL: {url}")
    return StreamingResponse(
        ffmpeg_stream_generator(url),
        media_type="video/mp2t"
    )

@app.get("/playlist")
async def playlist_proxy(url: str):
    """
    Fetches a remote source M3U playlist and rewrites all channel stream URLs 
    to route through this stabilizer proxy before heading to Dispatcharr.
    """
    if not url:
        raise HTTPException(status_code=400, detail="Missing required 'url' parameter.")
    
    try:
        logger.info(f"Fetching raw source playlist from: {url}")
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        raw_m3u = response.text
    except Exception as e:
        logger.error(f"Failed to fetch source playlist: {e}")
        raise HTTPException(status_code=502, detail=f"Failed to fetch source playlist: {e}")

    # Set your host URL environment parameter dynamically
    proxy_host = os.getenv("PROXY_PUBLIC_URL", "http://localhost:8089")
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