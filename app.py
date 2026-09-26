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

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, HttpUrl
from starlette.background import BackgroundTask
import yt_dlp

APP_NAME = "Baixou API"
MAX_DURATION_SECONDS = int(os.getenv("MAX_DURATION_SECONDS", "10800"))
MAX_CONCURRENT_DOWNLOADS = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "2"))
RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "20"))
RATE_LIMIT_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW", "60"))
INSTAGRAM_COOKIES_B64 = os.getenv("INSTAGRAM_COOKIES_B64", "").strip()

ALLOWED_HOSTS = {
    "youtube.com",
    "www.youtube.com",
    "m.youtube.com",
    "youtu.be",
    "instagram.com",
    "www.instagram.com",
}

cors_env = os.getenv("CORS_ORIGINS", "*")
CORS_ORIGINS = [x.strip() for x in cors_env.split(",") if x.strip()]

app = FastAPI(title=APP_NAME, version="1.1.0")
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


def _validate_url(raw_url: str) -> None:
    parsed = urlparse(raw_url)
    host = (parsed.hostname or "").lower()
    if parsed.scheme not in {"http", "https"}:
        raise HTTPException(status_code=400, detail="URL inválida.")
    if host not in ALLOWED_HOSTS:
        raise HTTPException(status_code=400, detail="Plataforma não suportada.")


def _is_instagram(raw_url: str) -> bool:
    host = (urlparse(raw_url).hostname or "").lower()
    return host in {"instagram.com", "www.instagram.com"}


def _safe_filename(value: str) -> str:
    value = re.sub(r"[^\w\-. ()]+", "_", value, flags=re.UNICODE).strip(" ._")
    return value[:140] or "baixou"


def _base_ydl_options(tmpdir: str) -> dict:
    return {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "restrictfilenames": False,
        "cachedir": False,
        "socket_timeout": 30,
        "retries": 3,
        "fragment_retries": 3,
        "outtmpl": str(Path(tmpdir) / "%(title).120s [%(id)s].%(ext)s"),
    }


def _attach_instagram_cookiefile(opts: dict, raw_url: str) -> str | None:
    """Attach a temporary Netscape cookie file only for Instagram requests."""
    if not _is_instagram(raw_url) or not INSTAGRAM_COOKIES_B64:
        return None
    try:
        cookie_text = base64.b64decode(INSTAGRAM_COOKIES_B64).decode("utf-8")
    except Exception as exc:
        raise RuntimeError("Configuração de autenticação do Instagram inválida.") from exc

    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", prefix="baixou-instagram-", delete=False, encoding="utf-8"
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
        "rate-limit" in low
        or "rate limit" in low
        or "login required" in low
        or "requested content is not available" in low
        or "authentication" in low
    ):
        return "O Instagram solicitou autenticação para este conteúdo. Tente novamente ou use outro link público."
    return f"Não foi possível processar essa mídia: {text}"


def _extract_info_sync(raw_url: str) -> dict:
    opts = _base_ydl_options(tempfile.gettempdir())
    opts["skip_download"] = True
    cookie_file = _attach_instagram_cookiefile(opts, raw_url)
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(raw_url, download=False)
    finally:
        _remove_temp_file(cookie_file)


def _download_sync(raw_url: str, media_format: str, tmpdir: str) -> tuple[str, str]:
    opts = _base_ydl_options(tmpdir)

    if media_format == "mp3":
        opts.update(
            {
                "format": "bestaudio/best",
                "postprocessors": [
                    {
                        "key": "FFmpegExtractAudio",
                        "preferredcodec": "mp3",
                        "preferredquality": "192",
                    }
                ],
            }
        )
    else:
        opts.update(
            {
                "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
                "merge_output_format": "mp4",
            }
        )

    cookie_file = _attach_instagram_cookiefile(opts, raw_url)
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(raw_url, download=True)
            duration = info.get("duration")
            if duration and duration > MAX_DURATION_SECONDS:
                raise ValueError("Conteúdo acima do limite de duração permitido.")
    finally:
        _remove_temp_file(cookie_file)

    files = [p for p in Path(tmpdir).iterdir() if p.is_file()]
    if not files:
        raise FileNotFoundError("Arquivo final não encontrado.")

    wanted_ext = ".mp3" if media_format == "mp3" else ".mp4"
    candidates = [p for p in files if p.suffix.lower() == wanted_ext]
    final_path = max(candidates or files, key=lambda p: p.stat().st_mtime)
    title = _safe_filename(str(info.get("title") or "baixou"))
    download_name = f"{title}{wanted_ext}"
    return str(final_path), download_name


def _cleanup_dir(path: str) -> None:
    shutil.rmtree(path, ignore_errors=True)


@app.get("/")
def root():
    return {
        "name": APP_NAME,
        "status": "online",
        "version": "1.1.0",
        "message": "API do Baixou pronta para processar links públicos suportados.",
        "instagram_server_auth_configured": bool(INSTAGRAM_COOKIES_B64),
    }


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/api/info")
async def info(payload: InfoRequest, request: Request):
    _check_rate_limit(request)
    raw_url = str(payload.url)
    _validate_url(raw_url)
    try:
        data = await asyncio.to_thread(_extract_info_sync, raw_url)
    except yt_dlp.utils.DownloadError as exc:
        raise HTTPException(status_code=422, detail=_friendly_download_error(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception:
        raise HTTPException(status_code=500, detail="Falha ao consultar a mídia.")

    duration = data.get("duration")
    if duration and duration > MAX_DURATION_SECONDS:
        raise HTTPException(status_code=413, detail="Conteúdo acima do limite de duração permitido.")

    return {
        "title": data.get("title"),
        "thumbnail": data.get("thumbnail"),
        "duration": duration,
        "uploader": data.get("uploader") or data.get("channel"),
        "extractor": data.get("extractor_key") or data.get("extractor"),
        "webpage_url": data.get("webpage_url") or raw_url,
    }


@app.post("/api/download")
async def download(payload: MediaRequest, request: Request):
    _check_rate_limit(request)
    raw_url = str(payload.url)
    _validate_url(raw_url)

    tmpdir = tempfile.mkdtemp(prefix="baixou-")
    try:
        async with _download_semaphore:
            path, download_name = await asyncio.to_thread(
                _download_sync, raw_url, payload.format, tmpdir
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

    media_type = "audio/mpeg" if payload.format == "mp3" else "video/mp4"
    return FileResponse(
        path,
        media_type=media_type,
        filename=download_name,
        background=BackgroundTask(_cleanup_dir, tmpdir),
    )
