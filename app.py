import asyncio
import base64
import os
import re
import shutil
import tempfile
import time
import uuid
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Request, Response
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
YOUTUBE_JOB_TIMEOUT = int(os.getenv("YOUTUBE_JOB_TIMEOUT", "900"))
MAX_WORKER_UPLOAD_BYTES = int(os.getenv("MAX_WORKER_UPLOAD_BYTES", str(1024 * 1024 * 1024)))
INSTAGRAM_COOKIES_B64 = os.getenv("INSTAGRAM_COOKIES_B64", "").strip()
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

app = FastAPI(title=APP_NAME, version="1.5.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

_download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
_rate_state: dict[str, list[float]] = {}
_jobs: dict[str, dict] = {}
_jobs_lock = asyncio.Lock()
_last_worker_seen = 0.0


class MediaRequest(BaseModel):
    url: HttpUrl
    format: Literal["mp4", "mp3"] = "mp4"


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
    if "there is no video in this post" in low:
        return "Essa publicação do Instagram não contém um vídeo disponível para download. Tente um Reel ou uma publicação com vídeo."
    if "instagram" in low and (
        "rate-limit" in low or "rate limit" in low or "login required" in low
        or "requested content is not available" in low or "authentication" in low
    ):
        return "O Instagram solicitou autenticação para este conteúdo. Tente novamente ou use outro link público."
    return f"Não foi possível processar essa mídia: {text}"


def _worker_authorized(request: Request) -> bool:
    supplied = request.headers.get("x-worker-token", "")
    return bool(YOUTUBE_WORKER_TOKEN) and supplied == YOUTUBE_WORKER_TOKEN


def _cleanup_dir(path: str) -> None:
    shutil.rmtree(path, ignore_errors=True)


async def _cleanup_job(job_id: str) -> None:
    async with _jobs_lock:
        job = _jobs.pop(job_id, None)
    if job:
        _cleanup_dir(job["tmpdir"])


async def _queue_youtube_download(raw_url: str, media_format: str):
    if not YOUTUBE_WORKER_TOKEN:
        raise HTTPException(status_code=503, detail="O processador do YouTube ainda não foi configurado.")

    job_id = uuid.uuid4().hex
    tmpdir = tempfile.mkdtemp(prefix="baixou-job-")
    event = asyncio.Event()
    job = {
        "id": job_id,
        "url": raw_url,
        "format": media_format,
        "status": "pending",
        "created_at": time.time(),
        "tmpdir": tmpdir,
        "event": event,
        "file_path": None,
        "filename": None,
        "media_type": None,
        "error": None,
        "error_status": 502,
    }
    async with _jobs_lock:
        _jobs[job_id] = job

    try:
        await asyncio.wait_for(event.wait(), timeout=YOUTUBE_JOB_TIMEOUT)
    except asyncio.TimeoutError:
        await _cleanup_job(job_id)
        raise HTTPException(
            status_code=504,
            detail="O processador do YouTube demorou além do limite. Verifique se o worker do celular está ativo."
        )

    async with _jobs_lock:
        current = _jobs.get(job_id)

    if not current:
        raise HTTPException(status_code=502, detail="O processamento do YouTube foi interrompido.")

    if current.get("error"):
        status = int(current.get("error_status") or 502)
        detail = str(current["error"])
        await _cleanup_job(job_id)
        raise HTTPException(status_code=status, detail=detail)

    file_path = current.get("file_path")
    if not file_path or not Path(file_path).is_file():
        await _cleanup_job(job_id)
        raise HTTPException(status_code=502, detail="O worker não devolveu um arquivo válido.")

    return FileResponse(
        file_path,
        media_type=current.get("media_type") or ("audio/mpeg" if media_format == "mp3" else "video/mp4"),
        filename=current.get("filename") or f"baixou.{media_format}",
        background=BackgroundTask(_cleanup_job, job_id),
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


@app.get("/")
def root():
    worker_online = bool(_last_worker_seen and (time.time() - _last_worker_seen) < 30)
    return {
        "name": APP_NAME,
        "status": "online",
        "version": "1.5.0",
        "youtube_worker_configured": bool(YOUTUBE_WORKER_TOKEN),
        "youtube_worker_online": worker_online,
        "instagram_server_auth_configured": bool(INSTAGRAM_COOKIES_B64),
    }


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/worker/next")
async def worker_next(request: Request):
    global _last_worker_seen
    if not _worker_authorized(request):
        raise HTTPException(status_code=401, detail="Não autorizado.")
    _last_worker_seen = time.time()

    async with _jobs_lock:
        pending = [j for j in _jobs.values() if j.get("status") == "pending"]
        if not pending:
            return Response(status_code=204)
        job = min(pending, key=lambda j: j["created_at"])
        job["status"] = "processing"
        job["picked_at"] = time.time()
        return {"id": job["id"], "url": job["url"], "format": job["format"]}


@app.post("/worker/result/{job_id}")
async def worker_result(job_id: str, request: Request):
    global _last_worker_seen
    if not _worker_authorized(request):
        raise HTTPException(status_code=401, detail="Não autorizado.")
    _last_worker_seen = time.time()

    async with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Trabalho não encontrado ou expirado.")

    ext = ".mp3" if job["format"] == "mp3" else ".mp4"
    target = Path(job["tmpdir"]) / f"result{ext}"
    total = 0
    try:
        with target.open("wb") as handle:
            async for chunk in request.stream():
                if not chunk:
                    continue
                total += len(chunk)
                if total > MAX_WORKER_UPLOAD_BYTES:
                    raise HTTPException(status_code=413, detail="Arquivo acima do limite permitido.")
                handle.write(chunk)
    except Exception:
        try:
            target.unlink(missing_ok=True)
        except Exception:
            pass
        raise

    if total == 0:
        target.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail="Arquivo vazio.")

    filename = f"baixou{ext}"
    encoded_name = request.headers.get("x-filename-b64", "")
    if encoded_name:
        try:
            filename = base64.b64decode(encoded_name).decode("utf-8")
        except Exception:
            pass
    filename = _safe_filename(filename)
    if not filename.lower().endswith(ext):
        filename += ext

    async with _jobs_lock:
        current = _jobs.get(job_id)
        if not current:
            target.unlink(missing_ok=True)
            raise HTTPException(status_code=404, detail="Trabalho expirado.")
        current["file_path"] = str(target)
        current["filename"] = filename
        current["media_type"] = request.headers.get("x-media-type") or ("audio/mpeg" if ext == ".mp3" else "video/mp4")
        current["status"] = "done"
        current["event"].set()

    return {"ok": True, "bytes": total}


@app.post("/worker/error/{job_id}")
async def worker_error(job_id: str, request: Request):
    global _last_worker_seen
    if not _worker_authorized(request):
        raise HTTPException(status_code=401, detail="Não autorizado.")
    _last_worker_seen = time.time()

    data = await request.json()
    detail = str(data.get("detail") or "O worker não conseguiu concluir o download.")[:2000]
    status_code = int(data.get("status") or 422)
    if status_code < 400 or status_code > 599:
        status_code = 422

    async with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Trabalho não encontrado ou expirado.")
        job["error"] = detail
        job["error_status"] = status_code
        job["status"] = "error"
        job["event"].set()

    return {"ok": True}


@app.post("/api/download")
async def download(payload: MediaRequest, request: Request):
    _check_rate_limit(request)
    raw_url = str(payload.url)
    _validate_url(raw_url)

    if _is_youtube(raw_url):
        return await _queue_youtube_download(raw_url, payload.format)

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
