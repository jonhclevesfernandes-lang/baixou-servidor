# Baixou Servidor

Backend para o projeto **Baixou**, preparado para Railway.

## Endpoints

- `GET /health` — status do serviço.
- `POST /api/info` — consulta metadados de uma URL suportada.
- `POST /api/download` — processa e devolve MP4 ou MP3.

Exemplo:

```json
{
  "url": "https://www.youtube.com/watch?v=...",
  "format": "mp4"
}
```

## Plataformas

O código restringe a entrada a URLs públicas de YouTube e Instagram. Conteúdo privado, protegido por login, DRM, paywall ou outros controles de acesso não é contornado.

## Variáveis opcionais

- `CORS_ORIGINS`: origens permitidas, separadas por vírgula. Em produção, substitua `*` pelo domínio do site.
- `MAX_DURATION_SECONDS`: padrão `10800` (3 horas).
- `MAX_CONCURRENT_DOWNLOADS`: padrão `2`.
- `RATE_LIMIT_REQUESTS`: padrão `20`.
- `RATE_LIMIT_WINDOW`: padrão `60` segundos.

## Railway

O `Dockerfile` instala FFmpeg e o `railway.toml` configura `/health` como healthcheck.
