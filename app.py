import asyncio
import base64
import os
import re
import shutil
import tempfile
import time
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, HttpUrl
from starlette.background import BackgroundTask
import yt_dlp

APP_NAME = "Baixou API"
MAX_DURATION_SECONDS = int(os.getenv("MAX_DURATION_SECONDS", "10800"))
MAX_CONCURRENT_DOWNLOADS = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "2"))
RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "20"))
RATE_LIMIT_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW", "60"))
INSTAGRAM_COOKIES_B64 = os.getenv("INSTAGRAM_COOKIES_B64", "").strip()
YOUTUBE_WORKER_URL = os.getenv("YOUTUBE_WORKER_URL", "").strip().rstrip("/")
YOUTUBE_WORKER_TOKEN = os.getenv("YOUTUBE_WORKER_TOKEN", "").strip()

ALLOWED_HOSTS = {
    "youtube.com",
    "www.youtube.com",
    "m.youtube.com",
    "music.youtube.com",
    "youtu.be",
    "youtube-nocookie.com",
    "www.youtube-nocookie.com",
    "instagram.com",
    "www.instagram.com",
}

cors_env = os.getenv("CORS_ORIGINS", "*")
CORS_ORIGINS = [x.strip() for x in cors_env.split(",") if x.strip()]

app = FastAPI(title=APP_NAME, version="1.4.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

_download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
_rate_state: dict[str, list[float]] = {}


class MediaRequest(BaseModel):
    url: HttpUrl
    format: Literal["mp4", "mp3"] = "mp4"


class InfoRequest(BaseModel):
    url: HttpUrl


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _check_rate_limit(request: Request) -> None:
    ip = _client_ip(request)
    now = time.time()
    cutoff = now - RATE_LIMIT_WINDOW
    hits = [t for t in _rate_state.get(ip, []) if t >= cutoff]
    if len(hits) >= RATE_LIMIT_REQUESTS:
        raise HTTPException(status_code=429, detail="Muitas solicitações. Tente novamente em instantes.")
    hits.append(now)
    _rate_state[ip] = hits


def _host(raw_url: str) -> str:
    return (urlparse(raw_url).hostname or "").lower()


def _is_instagram(raw_url: str) -> bool:
    return _host(raw_url) in {"instagram.com", "www.instagram.com"}


def _is_youtube(raw_url: str) -> bool:
    return _host(raw_url) in {
        "youtube.com",
        "www.youtube.com",
        "m.youtube.com",
        "music.youtube.com",
        "youtu.be",
        "youtube-nocookie.com",
        "www.youtube-nocookie.com",
    }


def _validate_url(raw_url: str) -> None:
    parsed = urlparse(raw_url)
    host = (parsed.hostname or "").lower()
    if parsed.scheme not in {"http", "https"} or host not in ALLOWED_HOSTS:
        raise HTTPException(status_code=400, detail="Plataforma não suportada.")


def _safe_filename(value: str) -> str:
    value = re.sub(r"[^\w\-. ()]+", "_", value, flags=re.UNICODE).strip(" ._")
    return value[:140] or "baixou"


def _base_ydl_options(tmpdir: str) -> dict:
    return {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "cachedir": False,
        "socket_timeout": 30,
        "retries": 5,
        "fragment_retries": 5,
        "outtmpl": str(Path(tmpdir) / "%(title).120s [%(id)s].%(ext)s"),
    }


def _attach_instagram_cookiefile(opts: dict, raw_url: str) -> str | None:
    if not _is_instagram(raw_url) or not INSTAGRAM_COOKIES_B64:
        return None
    try:
        cookie_text = base64.b64decode(INSTAGRAM_COOKIES_B64).decode("utf-8")
    except Exception as exc:
        raise RuntimeError("Configuração de autenticação do Instagram inválida.") from exc

    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", prefix="baixou-instagram-",
        delete=False, encoding="utf-8"
    )
    try:
        tmp.write(cookie_text)
        tmp.flush()
    finally:
        tmp.close()
    opts["cookiefile"] = tmp.name
    return tmp.name


def _remove_temp_file(path: str | None) -> None:
    if path:
        try:
            os.unlink(path)
        except OSError:
            pass


def _friendly_download_error(exc: Exception) -> str:
    text = str(exc)
    low = text.lower()
    if "instagram" in low and (
        "rate-limit" in low or "rate limit" in low or "login required" in low
        or "requested content is not available" in low or "authentication" in low
    ):
        return "O Instagram solicitou autenticação para este conteúdo. Tente novamente ou use outro link público."
    return f"Não foi possível processar essa mídia: {text}"


async def _worker_request(path: str, payload: dict):
    if not YOUTUBE_WORKER_URL or not YOUTUBE_WORKER_TOKEN:
        raise HTTPException(
            status_code=503,
            detail="O processador do YouTube está offline ou ainda não foi configurado."
        )

    client = httpx.AsyncClient(
        timeout=httpx.Timeout(connect=20.0, read=None, write=60.0, pool=20.0),
        follow_redirects=True,
    )
    req = client.build_request(
        "POST",
        f"{YOUTUBE_WORKER_URL}{path}",
        json=payload,
        headers={"X-Worker-Token": YOUTUBE_WORKER_TOKEN},
    )

    try:
        response = await client.send(req, stream=True)
    except Exception:
        await client.aclose()
        raise HTTPException(
            status_code=502,
            detail="Não foi possível conectar ao processador do YouTube no celular."
        )

    return client, response


async def _proxy_youtube_download(raw_url: str, media_format: str):
    client, response = await _worker_request(
        "/download", {"url": raw_url, "format": media_format}
    )

    if response.status_code >= 400:
        body = await response.aread()
        await response.aclose()
        await client.aclose()
        detail = "O processador do YouTube não conseguiu concluir o download."
        try:
            import json
            parsed = json.loads(body.decode("utf-8", errors="replace"))
            detail = parsed.get("detail") or detail
        except Exception:
            pass
        raise HTTPException(status_code=response.status_code, detail=detail)

    async def iterator():
        try:
            async for chunk in response.aiter_bytes():
                yield chunk
        finally:
            await response.aclose()
            await client.aclose()

    headers = {}
    cd = response.headers.get("content-disposition")
    if cd:
        headers["Content-Disposition"] = cd

    return StreamingResponse(
        iterator(),
        media_type=response.headers.get(
            "content-type",
            "audio/mpeg" if media_format == "mp3" else "video/mp4"
        ),
        headers=headers,
    )


def _download_instagram_sync(raw_url: str, media_format: str, tmpdir: str) -> tuple[str, str]:
    opts = _base_ydl_options(tmpdir)
    if media_format == "mp3":
        opts.update({
            "format": "bestaudio/best",
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }],
        })
    else:
        opts.update({
            "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
            "merge_output_format": "mp4",
        })

    cookie_file = _attach_instagram_cookiefile(opts, raw_url)
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(raw_url, download=True)
    finally:
        _remove_temp_file(cookie_file)

    duration = info.get("duration")
    if duration and duration > MAX_DURATION_SECONDS:
        raise ValueError("Conteúdo acima do limite de duração permitido.")

    wanted_ext = ".mp3" if media_format == "mp3" else ".mp4"
    files = [p for p in Path(tmpdir).iterdir() if p.is_file()]
    candidates = [p for p in files if p.suffix.lower() == wanted_ext]
    if not candidates:
        raise FileNotFoundError("Arquivo final não encontrado.")

    final_path = max(candidates, key=lambda p: p.stat().st_mtime)
    title = _safe_filename(str(info.get("title") or "baixou"))
    return str(final_path), f"{title}{wanted_ext}"


def _cleanup_dir(path: str) -> None:
    shutil.rmtree(path, ignore_errors=True)


@app.get("/")
def root():
    return {
        "name": APP_NAME,
        "status": "online",
        "version": "1.4.0",
        "youtube_worker_configured": bool(YOUTUBE_WORKER_URL and YOUTUBE_WORKER_TOKEN),
        "instagram_server_auth_configured": bool(INSTAGRAM_COOKIES_B64),
    }


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/api/download")
async def download(payload: MediaRequest, request: Request):
    _check_rate_limit(request)
    raw_url = str(payload.url)
    _validate_url(raw_url)

    if _is_youtube(raw_url):
        return await _proxy_youtube_download(raw_url, payload.format)

    tmpdir = tempfile.mkdtemp(prefix="baixou-")
    try:
        async with _download_semaphore:
            path, download_name = await asyncio.to_thread(
                _download_instagram_sync, raw_url, payload.format, tmpdir
            )
    except ValueError as exc:
        _cleanup_dir(tmpdir)
        raise HTTPException(status_code=413, detail=str(exc))
    except yt_dlp.utils.DownloadError as exc:
        _cleanup_dir(tmpdir)
        raise HTTPException(status_code=422, detail=_friendly_download_error(exc))
    except RuntimeError as exc:
        _cleanup_dir(tmpdir)
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception:
        _cleanup_dir(tmpdir)
        raise HTTPException(status_code=500, detail="Falha ao processar o arquivo.")

    return FileResponse(
        path,
        media_type="audio/mpeg" if payload.format == "mp3" else "video/mp4",
        filename=download_name,
        background=BackgroundTask(_cleanup_dir, tmpdir),
    )
