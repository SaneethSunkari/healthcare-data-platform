from fastapi import APIRouter

from api.app.schemas.responses import HealthResponse

router = APIRouter()


@router.get("", response_model=HealthResponse)
@router.get("/", response_model=HealthResponse, include_in_schema=False)
def health_check() -> HealthResponse:
    return HealthResponse(status="ok")
