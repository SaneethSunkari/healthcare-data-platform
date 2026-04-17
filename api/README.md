# API

FastAPI entrypoint for the healthcare middleware lives in `api/main.py`.
The copied Project 1 backend structure now lives under `api/app/`, with healthcare-specific services, prompts, safe views, and masking layered into that structure inside Project 2.

Run locally with:

```bash
uvicorn api.main:app --reload
```

Set `OPENAI_API_KEY` in the project `.env` before using `POST /query/ask`.

Key endpoints:

```bash
GET  /connections
POST /connections/test
POST /query/run
POST /query/ask
```
