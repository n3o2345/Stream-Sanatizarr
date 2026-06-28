# We use the NVIDIA runtime base as the default since it's the hardest to set up manually
FROM nvidia/cuda:12.4.1-runtime-ubuntu22.04

# Standard Ubuntu Base Alternative (RECOMMENDED for Intel / AMD)
# If using Intel or AMD, uncomment the line below and comment out the NVIDIA FROM line above
# FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies, Python, FFmpeg, and Intel/AMD VAAPI runtimes
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    ffmpeg \
    # --- INTEL & AMD DRIVER PACKAGES ---
    va-driver-all \
    vdpau-driver-all \
    intel-media-va-driver-non-free \
    libva-drm2 \
    libva2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip3 install --no-cache-dir fastapi uvicorn

COPY app.py .

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]