from fastapi import APIRouter

from api.app.api.routes.connections import router as connections_router
from api.app.api.routes.health import router as health_router
from api.app.api.routes.patients import router as patients_router
from api.app.api.routes.query import router as query_router
from api.app.api.routes.schema import router as schema_router
from api.app.api.routes.tools import router as tools_router

api_router = APIRouter()
api_router.include_router(health_router, prefix="/health")
api_router.include_router(connections_router, prefix="/connections")
api_router.include_router(patients_router, prefix="/patients")
api_router.include_router(query_router, prefix="/query")
api_router.include_router(schema_router, prefix="/schema")
api_router.include_router(tools_router, prefix="/tools")
