import asyncio
import logging
import os
import sys
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("StreamSanitizer")

app = FastAPI(title="FAST Stream Sanitizer Proxy")

# Environment configuration defaults
VIDEO_CODEC = os.getenv("VIDEO_CODEC", "libx264")  # Use 'h264_nvenc' for NVIDIA hardware acceleration
VIDEO_BITRATE = os.getenv("VIDEO_BITRATE", "3000k")
AUDIO_BITRATE = os.getenv("AUDIO_BITRATE", "128k")

async def ffmpeg_stream_generator(stream_url: str):
    """
    Spawns FFmpeg workers to normalize the stream. 
    Loops infinitely to resume if the upstream connection drops during ad breaks.
    """
    # FFmpeg arguments to flatten discontinuities and normalize parameters
    ffmpeg_cmd = [
        "ffmpeg",
        "-reconnect", "1",
        "-reconnect_at_eof", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-fflags", "+genpts+async",
        "-avoid_negative_ts", "make_zero",
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
    ]

    while True:
        logger.info(f"Spawning FFmpeg worker for stream: {stream_url}")
        process = await asyncio.create_subprocess_exec(
            *ffmpeg_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL
        )

        try:
            # Continuously read chunks from stdout and yield to client
            while True:
                chunk = await process.stdout.read(65536) # 64KB chunks
                if not chunk:
                    # No data means FFmpeg exited (likely stream dropped or profile shifted)
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
            # Ensure the process is dead before looping or exiting
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)