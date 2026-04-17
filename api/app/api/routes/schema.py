from fastapi import APIRouter

from api.app.schemas.connection import ConnectionRequest
from api.app.schemas.responses import SchemaScanResponse
from api.app.services import connection_registry as registry
from api.app.services.schema_service import get_schema_metadata

router = APIRouter()


@router.post(
    "/scan",
    response_model=SchemaScanResponse,
    response_model_exclude_none=True,
    summary="Scan Tables And Relationships",
    description="Returns the anonymised analytics views exposed to the AI layer.",
)
def scan_schema(payload: ConnectionRequest) -> SchemaScanResponse:
    try:
        params = registry.resolve(payload)
    except ValueError as exc:
        return SchemaScanResponse(error=str(exc))

    schema = get_schema_metadata(**params)
    return SchemaScanResponse(
        tables=schema.get("tables", {}),
        relationships=schema.get("relationships", []),
        error=schema.get("error"),
    )
