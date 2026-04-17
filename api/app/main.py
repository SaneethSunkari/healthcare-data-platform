import json
import os
from contextlib import closing

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.responses import JSONResponse

from api.app.api.routes.connections import router as connections_router
from api.app.api.routes.health import router as health_router
from api.app.api.routes.patients import router as patients_router
from api.app.api.routes.query import router as query_router
from api.app.api.routes.schema import router as schema_router
from api.app.api.routes.tools import router as tools_router
from api.app.core.security import SecurityContextMiddleware, SecurityHeadersMiddleware
from api.app.core.settings import get_settings
from api.app.schemas.responses import RootResponse
from api.app.services import connection_registry as registry
from api.app.services.query_service import connect_db, ensure_safe_views
from compliance.pii_masker import mask_ai_response_payload

load_dotenv()
SETTINGS = get_settings()

API_DESCRIPTION = """
Healthcare-aware AI middleware connected to the unified patient warehouse.

This backend is carried over from Project 1's middleware structure and adapted
for the healthcare demo. It connects to `healthcare_db`, limits AI queries to
safe analytics views, and masks AI responses for non-doctor roles.
"""

TAGS_METADATA = [
    {"name": "health", "description": "Basic service health checks."},
    {"name": "connections", "description": "Verify healthcare database credentials before querying."},
    {"name": "patients", "description": "Role-aware patient record access with audit logging."},
    {"name": "schema", "description": "Inspect the anonymised analytics views exposed to the AI layer."},
    {"name": "query", "description": "Run validated read-only SQL or ask clinical questions in plain English."},
    {"name": "tools", "description": "Agent-compatible tool manifest and invoke endpoint."},
]

SWAGGER_UI_PARAMETERS = {
    "docExpansion": "none",
    "defaultModelsExpandDepth": -1,
    "displayRequestDuration": True,
    "filter": True,
    "tryItOutEnabled": True,
}


class PIIMaskingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        if not (request.url.path.startswith("/query/") or request.url.path.startswith("/tools/invoke")):
            return response

        if request.headers.get("X-User-Role", "analyst").strip().lower() == "doctor":
            return response

        content_type = response.headers.get("content-type", "")
        if "application/json" not in content_type:
            return response

        body = b""
        async for chunk in response.body_iterator:
            body += chunk

        if not body:
            return response

        payload = json.loads(body.decode("utf-8"))
        masked_payload = mask_ai_response_payload(payload, request.headers.get("X-User-Role", "analyst"))
        headers = {k: v for k, v in response.headers.items() if k.lower() != "content-length"}
        return JSONResponse(content=masked_payload, status_code=response.status_code, headers=headers)


app = FastAPI(
    title="Healthcare AI Middleware",
    version="0.1.0",
    description=API_DESCRIPTION,
    openapi_tags=TAGS_METADATA,
    swagger_ui_parameters=SWAGGER_UI_PARAMETERS,
    middleware=[
        Middleware(TrustedHostMiddleware, allowed_hosts=SETTINGS.allowed_hosts),
    ],
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=SETTINGS.cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1024)
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(SecurityContextMiddleware, settings=SETTINGS)
app.add_middleware(PIIMaskingMiddleware)

app.include_router(health_router, prefix="/health", tags=["health"])
app.include_router(connections_router, prefix="/connections", tags=["connections"])
app.include_router(patients_router, prefix="/patients", tags=["patients"])
app.include_router(query_router, prefix="/query", tags=["query"])
app.include_router(schema_router, prefix="/schema", tags=["schema"])
app.include_router(tools_router, prefix="/tools", tags=["tools"])

_static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=_static_dir), name="static")


@app.on_event("startup")
def startup() -> None:
    registry.seed_default_connection()
    with closing(
        connect_db(
            host=os.getenv("DB_HOST", "127.0.0.1"),
            port=int(os.getenv("DB_PORT", "15432")),
            database=os.getenv("DB_NAME", "healthcare_db"),
            username=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres"),
            connect_timeout_s=SETTINGS.connect_timeout_s,
        )
    ) as connection:
        ensure_safe_views(connection)


@app.get("/ui", include_in_schema=False)
def serve_ui():
    return FileResponse(os.path.join(_static_dir, "index.html"))


@app.get(
    "/",
    response_model=RootResponse,
    summary="API Status",
    description="Quick status check for the healthcare middleware API.",
)
def root() -> RootResponse:
    return RootResponse(message="Healthcare AI Middleware is running")
