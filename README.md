# Stream-Sanatizarr

A tiny self-hosted proxy that sits between a flaky live/FAST‑channel M3U playlist (Pluto TV, LocalNow, etc.) and your media player (Dispatcharr, Plex, Jellyfin, etc.), and "sanitizes" every stream in it by re-encoding it through FFmpeg in real time.

Live FAST channels are notorious for things that break players: timestamp jumps across ad/SSAI breaks, variable framerates, mixed codecs, and broken reconnects. Stream-Sanatizarr fixes this by transcoding each stream to a clean, constant-framerate MPEG-TS feed with sane timestamps — and it does this **on the fly, per-channel**, so you don't need to pre-process anything.

## How it works

1. You give Stream-Sanatizarr the URL of your source M3U playlist (e.g. a Pluto TV or zap2xml-style playlist).
2. It fetches that playlist and rewrites every channel URL so it points back at itself instead of the original stream.
3. When something (e.g. Dispatcharr) requests one of those rewritten channel URLs, Stream-Sanatizarr spins up an FFmpeg worker that pulls the original stream and re-encodes it into a clean, stable MPEG-TS output, which gets streamed straight through to the client.
4. If the upstream source drops (ad break glitch, network hiccup, channel restart), the FFmpeg worker automatically respawns and keeps streaming — the client never sees the disconnect.

```
[Source M3U]  →  GET /playlist?url=...   →  [Rewritten M3U]
                                                   |
                                                   v
[Dispatcharr/Player] → GET /stream?url=<original channel> → [FFmpeg worker] → clean MPEG-TS
```

## Features

- 🩹 **Self-healing streams** — auto-reconnect and FFmpeg worker respawn on upstream failure, so a single broken segment doesn't kill the whole channel.
- 🎛️ **Hardware acceleration** — NVENC (NVIDIA), QSV (Intel QuickSync), and VAAPI (Intel/AMD) are all supported via a single environment variable, with a CPU (`libx264`) fallback.
- 🌐 **Simple web UI** — paste a playlist URL into the dashboard and get back a ready-to-use proxied playlist URL for Dispatcharr/Plex/etc.
- 🔌 **Drop-in proxy** — no playlist editing required; it fetches and rewrites the source M3U for you.
- 🐳 **Docker-first** — ships with a Dockerfile and Compose file tuned for self-hosted/home-lab use (TrueNAS SCALE, Unraid, plain Docker, etc.).

## Quick start (Docker Compose)

1. Clone this repo and `cd` into it.
2. Edit `docker-compose.yml`:
   - Set `PROXY_PUBLIC_URL` to the address other devices on your network will use to reach this container (e.g. `http://192.168.1.50:8089`).
   - Pick the `VIDEO_CODEC` that matches your hardware (see [Hardware acceleration](#hardware-acceleration) below).
3. Start it:

   ```bash
   docker compose up -d --build
   ```

4. Open `http://<your-server>:8089/` in a browser, paste your source M3U playlist URL into the dashboard, and click **Generate Sanitized URL**.
5. Add the generated URL as an M3U source in Dispatcharr (or your player of choice).

## Manual usage (no UI)

You can build the proxied playlist URL by hand instead of using the dashboard:

```
http://<your-server>:8089/playlist?url=<url-encoded source M3U URL>
```

Each channel inside the playlist this returns will already point back at `/stream?url=...`, so you never need to touch those individually.

## Configuration

All configuration is via environment variables (set them in `docker-compose.yml`):

| Variable | Default | Description |
|---|---|---|
| `VIDEO_CODEC` | `libx264` | FFmpeg video encoder. One of `libx264` (CPU), `h264_nvenc` (NVIDIA), `h264_qsv` (Intel QuickSync), `h264_vaapi` (Intel/AMD VAAPI). |
| `VIDEO_BITRATE` | `3000k` | Target/max video bitrate passed to FFmpeg (`-b:v` / `-maxrate:v`). |
| `AUDIO_BITRATE` | `128k` | Audio bitrate (`-b:a`), encoded as AAC stereo at 48kHz. |
| `PROXY_PUBLIC_URL` | `http://localhost:8000` | The externally-reachable base URL for this container. Used to build the links shown in the dashboard and embedded in rewritten playlists. **Must be set to your real LAN/host address+port**, or downstream players won't be able to reach the proxy. |
| `PORT` | `8000` | Internal port the app listens on inside the container. Only change this if you're not using the Compose file's port mapping. |

### Hardware acceleration

Pick **one** codec/hardware combination and update both files:

- **NVIDIA (NVENC)** — default. Uses the `nvidia/cuda` base image. Requires the [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html) installed on the host and the `deploy.resources.reservations.devices` block in `docker-compose.yml` (already included by default).
- **Intel QuickSync (`h264_qsv`)** or **AMD/Intel VAAPI (`h264_vaapi`)** — switch the base image in the `Dockerfile` to plain `ubuntu:22.04`, set `VIDEO_CODEC` accordingly, and uncomment the `devices: - /dev/dri:/dev/dri` line in `docker-compose.yml` (and remove/comment the NVIDIA `deploy:` block).
- **CPU only (`libx264`)** — works anywhere, no special host setup needed, but uses significantly more CPU per stream than hardware encoding.

## Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /` | Web dashboard for generating a proxied playlist URL. |
| `GET /playlist?url=<source m3u>` | Fetches the given M3U playlist and returns a copy with every channel URL rewritten to go through `/stream`. |
| `GET /stream?url=<channel url>` | Transcodes the given upstream stream through FFmpeg and returns it as a live MPEG-TS feed. |

## Requirements

- Docker + Docker Compose
- FFmpeg with hardware encoder support baked into the image (already handled by the provided Dockerfile)
- For hardware acceleration: a GPU and the corresponding host drivers/runtime (NVIDIA Container Toolkit for NVENC, `/dev/dri` access for QSV/VAAPI)

## Troubleshooting

- **Stream won't play / immediately errors out** — check container logs (`docker logs stream-sanitizer`). FFmpeg's stderr output is logged at `DEBUG` level; set the logger level to `DEBUG` in `app.py` (or run with `PYTHONUNBUFFERED=1` and adjust `logging.basicConfig` level) to see the raw encoder errors.
- **Player can't reach the proxy / playlist links are wrong** — double check `PROXY_PUBLIC_URL` matches an address that your *player*, not just the container, can reach.
- **Choppy/garbled video with VAAPI** — confirm `/dev/dri` is actually being passed through and that the in-container user has permission to access it.
- **High CPU usage** — you're likely falling back to `libx264`. Confirm your `VIDEO_CODEC` and host GPU/driver setup are correct; check FFmpeg debug logs for "no such device" or similar hardware errors.

## License

This project is licensed under the GNU General Public License v3.0 (GPLv3).

In short: you're free to use, modify, and distribute this software, but any distributed copies or derivative works must also be licensed under GPLv3, must include the source code, and must preserve copyright/license notices. See the LICENSE file for the full terms, or read the canonical text at https://www.gnu.org/licenses/gpl-3.0.html.
