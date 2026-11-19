"""Local fake live HLS source for testing the recorder.

Serves a sliding-window media playlist over a pre-generated MPEG-TS seed
segment. Query parameters on /live.m3u8 are propagated to segment URLs:

  delay_ms=N            sleep N ms before serving each segment (slow source)
  missing=7,9           return 404 for those sequence numbers (missing segments)
  jump_at=K&jump_by=J   once the clock reaches seq K, shift all sequence
                        numbers by J (window jumps over undiscovered numbers)
  rewrite=7,9           for those sequence numbers, keep the same segment URL
                        but rewrite the TS bytes from the rewrite_after-th
                        request on (default: the 2nd request). This emulates a
                        live source that mutates a segment while its sequence
                        number and address stay unchanged, so the recorder's
                        SHA-256 re-fetch verification can detect the conflict.
  rewrite_after=N       request index from which rewritten bytes are served

Also serves invalid playlists (/master.m3u8, /enc.m3u8, /disc.m3u8) and a
finite /vod.m3u8 for validation tests.
"""

import asyncio
import os
import threading
import time

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse, Response

WINDOW_SIZE = int(os.environ.get("WINDOW_SIZE", "5"))
SEGMENT_DURATION = float(os.environ.get("SEGMENT_DURATION", "2"))
SEED_TS = os.environ.get("SEED_TS", "/srv/seed.ts")

EPOCH = time.time()
app = FastAPI(title="fake live HLS source")

# Per-sequence request counters for the rewrite demo. A segment keeps the same
# URL for its whole lifetime; only the bytes change after the threshold.
_rewrite_lock = threading.Lock()
_rewrite_hits: dict[int, int] = {}
_seed_cache: bytes | None = None


def _seed_bytes() -> bytes:
    global _seed_cache
    if _seed_cache is None:
        with open(SEED_TS, "rb") as fh:
            _seed_cache = fh.read()
    return _seed_cache


def _rewritten_bytes() -> bytes:
    """A valid-looking extra MPEG-TS packet (0x47 sync byte) carrying a marker,
    so the rewritten segment has different bytes but the same .ts URL."""
    payload = b"REWRITTEN-BY-LIVESOURCE"
    block = b"\x47" + b"\x1f\xff\x10" + payload
    return block + b"\xff" * (188 - len(block))


def _now_seq() -> int:
    return int((time.time() - EPOCH) // SEGMENT_DURATION) + 1


def _parse_int_list(raw: str) -> set:
    out = set()
    for part in (raw or "").split(","):
        part = part.strip()
        if part:
            out.add(int(part))
    return out


def _playlist(seqs, extra_lines=None, endlist=False) -> str:
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{max(1, round(SEGMENT_DURATION))}",
        f"#EXT-X-MEDIA-SEQUENCE:{seqs[0]}",
    ]
    if extra_lines:
        lines.extend(extra_lines)
    for s in seqs:
        lines.append(f"#EXTINF:{SEGMENT_DURATION:.3f},")
        lines.append(f"/seg/{s}.ts")
    if endlist:
        lines.append("#EXT-X-ENDLIST")
    return "\n".join(lines) + "\n"


@app.get("/live.m3u8", response_class=PlainTextResponse)
def live(delay_ms: int = 0, missing: str = "", jump_at: int = 0, jump_by: int = 0,
         rewrite: str = "", rewrite_after: int = 1):
    cur = _now_seq()
    eff = cur + (jump_by if jump_at and cur >= jump_at else 0)
    first = max(1, eff - WINDOW_SIZE + 1)
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{max(1, round(SEGMENT_DURATION))}",
        f"#EXT-X-MEDIA-SEQUENCE:{first}",
    ]
    q = (f"?delay_ms={delay_ms}&missing={missing}"
         f"&rewrite={rewrite}&rewrite_after={rewrite_after}")
    for s in range(first, eff + 1):
        lines.append(f"#EXTINF:{SEGMENT_DURATION:.3f},")
        lines.append(f"/seg/{s}.ts{q}")
    return "\n".join(lines) + "\n"


@app.get("/seg/{seq}.ts")
async def seg(seq: int, delay_ms: int = 0, missing: str = "",
              rewrite: str = "", rewrite_after: int = 1):
    if seq in _parse_int_list(missing):
        raise HTTPException(404, f"segment {seq} intentionally missing")
    if delay_ms > 0:
        await asyncio.sleep(delay_ms / 1000.0)
    serve_rewritten = False
    if rewrite and seq in _parse_int_list(rewrite):
        with _rewrite_lock:
            hits = _rewrite_hits.get(seq, 0) + 1
            _rewrite_hits[seq] = hits
        serve_rewritten = hits > max(1, rewrite_after)
    if serve_rewritten:
        # Same URL, different bytes: exactly what re-fetch verification must catch.
        return Response(content=_seed_bytes() + _rewritten_bytes(),
                        media_type="video/mp2t",
                        headers={"Cache-Control": "no-store",
                                 "X-Segment-Rewritten": "1"})
    return FileResponse(SEED_TS, media_type="video/mp2t",
                        headers={"Cache-Control": "no-store"})


@app.get("/vod.m3u8", response_class=PlainTextResponse)
def vod():
    return _playlist([1, 2, 3], endlist=True)


@app.get("/reuse.m3u8", response_class=PlainTextResponse)
def reuse():
    # invalid: every sequence maps to the same segment URL (address reuse)
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{max(1, round(SEGMENT_DURATION))}",
        "#EXT-X-MEDIA-SEQUENCE:1",
    ]
    for _ in range(3):
        lines.append(f"#EXTINF:{SEGMENT_DURATION:.3f},")
        lines.append("/seg/1.ts")
    return "\n".join(lines) + "\n"


@app.get("/master.m3u8", response_class=PlainTextResponse)
def master():
    return ("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=320x240\n"
            "/live.m3u8\n")


@app.get("/enc.m3u8", response_class=PlainTextResponse)
def enc():
    return _playlist([1, 2, 3], extra_lines=[
        '#EXT-X-KEY:METHOD=AES-128,URI="/key.bin",IV=0x00000000000000000000000000000001'
    ])


@app.get("/disc.m3u8", response_class=PlainTextResponse)
def disc():
    seqs = [1, 2, 3]
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{max(1, round(SEGMENT_DURATION))}",
        "#EXT-X-MEDIA-SEQUENCE:1",
        "#EXTINF:2.000,",
        "/seg/1.ts",
        "#EXT-X-DISCONTINUITY",
        "#EXTINF:2.000,",
        "/seg/2.ts",
        "#EXTINF:2.000,",
        "/seg/3.ts",
    ]
    return "\n".join(lines)


@app.get("/health")
def health():
    return {"ok": True, "now_seq": _now_seq()}


@app.get("/", response_class=PlainTextResponse)
def index():
    return (
        "fake live HLS source\n"
        "  /live.m3u8?delay_ms=0&missing=&jump_at=&jump_by=&rewrite=&rewrite_after=1\n"
        "  /vod.m3u8 /reuse.m3u8 /master.m3u8 /enc.m3u8 /disc.m3u8\n"
    )
