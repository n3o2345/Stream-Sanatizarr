# =========================================================================
# 1. BASE IMAGE SELECTION 
# =========================================================================
# Option A: For NVIDIA GPU setups (Leave uncommented if using NVIDIA)
FROM nvidia/cuda:12.4.1-runtime-ubuntu22.04

# Option B: For Intel or AMD setups (Uncomment below and comment out NVIDIA above)
# FROM ubuntu:22.04
# =========================================================================

ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies, Python, FFmpeg, and multi-platform hardware drivers
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    ffmpeg \
    va-driver-all \
    vdpau-driver-all \
    intel-media-va-driver-non-free \
    libva-drm2 \
    libva2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip3 install --no-cache-dir fastapi "uvicorn[standard]" httpx

COPY app.py .

EXPOSE 8000

# Respect the PORT env var at runtime (falls back to 8000) instead of a hardcoded port
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}"]
