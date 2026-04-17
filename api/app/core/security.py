from __future__ import annotations

from uuid import uuid4

from fastapi import HTTPException, Request
from starlette.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from api.app.core.settings import Settings

ALLOWED_ROLES = {"admin", "doctor", "provider", "analyst"}
API_KEY_HEADER = "X-API-Key"
ROLE_HEADER = "X-User-Role"
EXEMPT_PATH_PREFIXES = (
    "/",
    "/health",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/ui",
    "/static",
)


def normalize_role(header_value: str | None) -> str:
    role = (header_value or "analyst").strip().lower() or "analyst"
    if role not in ALLOWED_ROLES:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported user role '{role}'. Allowed roles: {', '.join(sorted(ALLOWED_ROLES))}.",
        )
    return role


def ensure_role(role: str, allowed_roles: set[str]) -> None:
    if role not in allowed_roles:
        raise HTTPException(
            status_code=403,
            detail="You do not have permission to access this resource.",
        )


def is_exempt_path(path: str) -> bool:
    return path in {"/", ""} or any(
        path == prefix or path.startswith(f"{prefix}/")
        for prefix in EXEMPT_PATH_PREFIXES
        if prefix not in {"", "/"}
    )


class SecurityContextMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, settings: Settings):
        super().__init__(app)
        self.settings = settings

    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get(self.settings.request_id_header) or str(uuid4())
        request.state.request_id = request_id
        try:
            request.state.user_role = normalize_role(request.headers.get(ROLE_HEADER))
        except HTTPException as exc:
            return JSONResponse(
                status_code=exc.status_code,
                content={"detail": exc.detail},
                headers={self.settings.request_id_header: request_id},
            )

        if (
            self.settings.require_api_key
            and not is_exempt_path(request.url.path)
        ):
            provided_key = request.headers.get(API_KEY_HEADER)
            if not provided_key or provided_key != self.settings.api_key:
                return JSONResponse(
                    status_code=401,
                    content={"detail": "Missing or invalid API key."},
                    headers={self.settings.request_id_header: request_id},
                )

        response = await call_next(request)
        response.headers[self.settings.request_id_header] = request_id
        return response


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
        response.headers.setdefault("Cache-Control", "no-store")
        return response
