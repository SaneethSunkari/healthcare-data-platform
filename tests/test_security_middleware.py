from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from api.app.core.security import SecurityContextMiddleware, SecurityHeadersMiddleware
from api.app.core.settings import Settings


def build_app(require_api_key: bool = True) -> FastAPI:
    app = FastAPI()
    settings = Settings(
        app_env="test",
        api_key="secret-key",
        require_api_key=require_api_key,
        cors_origins=["http://localhost:3000"],
        allowed_hosts=["testserver"],
        query_timeout_ms=8000,
        connect_timeout_s=5,
        max_query_rows=200,
    )
    app.add_middleware(SecurityHeadersMiddleware)
    app.add_middleware(SecurityContextMiddleware, settings=settings)

    @app.get("/secure")
    def secure(request: Request) -> dict[str, str]:
        return {
            "role": request.state.user_role,
            "request_id": request.state.request_id,
        }

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


def test_api_key_is_required_for_protected_routes() -> None:
    client = TestClient(build_app(require_api_key=True))

    response = client.get("/secure", headers={"X-User-Role": "provider"})

    assert response.status_code == 401
    assert response.json()["detail"] == "Missing or invalid API key."


def test_valid_api_key_sets_role_request_id_and_security_headers() -> None:
    client = TestClient(build_app(require_api_key=True))

    response = client.get(
        "/secure",
        headers={
            "X-API-Key": "secret-key",
            "X-User-Role": "doctor",
        },
    )

    body = response.json()
    assert response.status_code == 200
    assert body["role"] == "doctor"
    assert body["request_id"]
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["Cache-Control"] == "no-store"


def test_health_route_is_exempt_from_api_key_requirement() -> None:
    client = TestClient(build_app(require_api_key=True))

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
