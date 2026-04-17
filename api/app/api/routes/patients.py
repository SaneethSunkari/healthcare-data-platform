from contextlib import closing
import os
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query, Request

from api.app.core.security import ensure_role, normalize_role
from api.app.services.query_service import connect_db
from api.app.services.provider_chart_service import get_provider_chart, search_patients
from compliance.pii_masker import get_patient_by_role, log_access, mask_response_for_role

router = APIRouter()


def _client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def _role(header_value: str | None) -> str:
    return normalize_role(header_value)


def _open_connection():
    return connect_db(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "15432")),
        database=os.getenv("DB_NAME", "healthcare_db"),
        username=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
    )


@router.get("/search", summary="Search Unified Patients")
def search_unified_patients(
    request: Request,
    q: str = Query(default="", description="Golden ID, source patient ID, patient name, DOB, ZIP, or source system"),
    limit: int = Query(default=10, ge=1, le=50),
    x_user_role: str = Header(default="analyst", alias="X-User-Role"),
) -> dict[str, Any]:
    user_role = _role(x_user_role)
    client_ip = _client_ip(request)
    with closing(_open_connection()) as connection:
        results = search_patients(connection, q, limit)
        log_access(connection, user_role, "SEARCH_PATIENT", None, f"GET /patients/search?q={q}", client_ip)
    return {
        "query": q,
        "limit": limit,
        "count": len(results),
        "rows": mask_response_for_role(results, user_role),
    }


@router.get("/chart/{golden_id}", summary="Read Provider Patient Chart")
def read_provider_chart(
    golden_id: str,
    request: Request,
    break_glass: bool = Query(default=False, description="Emergency access audit flag"),
    x_user_role: str = Header(default="analyst", alias="X-User-Role"),
) -> dict[str, Any]:
    role = _role(x_user_role)
    ensure_role(role, {"admin", "doctor", "provider"})
    with closing(_open_connection()) as connection:
        chart = get_provider_chart(
            connection,
            golden_id,
            role,
            _client_ip(request),
            "BREAK_GLASS_READ_PATIENT_CHART" if break_glass else "READ_PATIENT_CHART",
        )
    if chart is None:
        raise HTTPException(status_code=404, detail="Patient chart not found")
    return chart


@router.get("/{patient_id}", summary="Read Patient Record")
def read_patient(
    patient_id: str,
    request: Request,
    x_user_role: str = Header(default="analyst", alias="X-User-Role"),
) -> dict[str, Any]:
    with closing(_open_connection()) as connection:
        record = get_patient_by_role(connection, patient_id, _role(x_user_role), _client_ip(request))
    if record is None:
        raise HTTPException(status_code=404, detail="Patient not found")
    return record
