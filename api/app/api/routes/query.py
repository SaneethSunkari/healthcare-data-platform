from fastapi import APIRouter, Header, Request

from api.healthcare_prompt import TEST_QUERIES
from api.app.core.security import normalize_role
from api.app.schemas.ai_query import AIQueryRequest
from api.app.schemas.query import QueryRequest
from api.app.schemas.responses import AskQueryResponse, QueryResultResponse
from api.app.services import connection_registry as registry
from api.app.services.query_service import execute_nl_query, execute_sql_query

router = APIRouter()


def _client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def _role(header_value: str | None) -> str:
    return normalize_role(header_value)


@router.post(
    "/run",
    response_model=QueryResultResponse,
    response_model_exclude_none=True,
    summary="Run Read-Only SQL",
    description="Executes a validated read-only query against the safe healthcare analytics views.",
)
def run_query(
    payload: QueryRequest,
    request: Request,
    x_user_role: str = Header(default="analyst", alias="X-User-Role"),
) -> QueryResultResponse:
    try:
        params = registry.resolve(payload)
    except ValueError as exc:
        return QueryResultResponse(success=False, sql=payload.sql, error=str(exc))

    result = execute_sql_query(
        sql=payload.sql,
        user_role=_role(x_user_role),
        ip=_client_ip(request),
        request_id=getattr(request.state, "request_id", None),
        **params,
    )
    return QueryResultResponse(**result)


@router.post(
    "/ask",
    response_model=AskQueryResponse,
    response_model_exclude_none=True,
    summary="Ask In Plain English",
    description="Uses the healthcare-aware prompt and live analytics schema to generate a safe query.",
)
def ask_query(
    payload: AIQueryRequest,
    request: Request,
    x_user_role: str = Header(default="analyst", alias="X-User-Role"),
) -> AskQueryResponse:
    try:
        params = registry.resolve(payload)
    except ValueError as exc:
        return AskQueryResponse(success=False, question=payload.question, sql="", error=str(exc))

    result = execute_nl_query(
        question=payload.question,
        user_role=_role(x_user_role),
        ip=_client_ip(request),
        request_id=getattr(request.state, "request_id", None),
        **params,
    )
    return AskQueryResponse(**result)


@router.get(
    "/test-queries",
    summary="List Demo Clinical Questions",
    description="Returns sample clinical questions for the healthcare demo.",
)
def list_test_queries() -> dict[str, list[str]]:
    return {"queries": TEST_QUERIES}
