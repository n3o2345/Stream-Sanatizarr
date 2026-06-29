# =========================================================================
# BASE IMAGE — universal, works for NVIDIA / Intel / AMD / CPU
# =========================================================================
# Plain Ubuntu is enough for ALL hardware paths, including NVIDIA NVENC.
# NVENC does NOT require the nvidia/cuda base image — the NVIDIA Container
# Toolkit injects the necessary driver libraries into the container at
# runtime, as long as the host has the toolkit installed and the container
# is started with the nvidia device reservation (see docker-compose.yml).
# This single image supports h264_nvenc, h264_qsv, h264_vaapi, and
# libx264 — pick your codec via the VIDEO_CODEC env var in
# docker-compose.yml. No rebuild required when switching hardware.
# =========================================================================
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install Python, FFmpeg, and driver packages for every supported platform:
#   - va-driver-all / libva*          -> Intel & AMD VAAPI
#   - intel-media-va-driver-non-free  -> Intel QuickSync (QSV) via VAAPI
#   - vdpau-driver-all                -> legacy VDPAU fallback
# NVIDIA NVENC needs no packages here — its driver libs are injected from
# the host by the NVIDIA Container Toolkit at container runtime.
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