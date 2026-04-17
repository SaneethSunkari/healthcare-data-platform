from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from api.app.services import connection_registry as registry
from api.app.services.connection_service import test_connection
from api.app.services.query_service import execute_nl_query, execute_sql_query
from api.app.services.schema_service import get_schema_metadata

router = APIRouter()

TOOL_MANIFEST = [
    {
        "type": "function",
        "function": {
            "name": "test_connection",
            "description": "Test whether a database connection is reachable with the given credentials or a saved connection_id.",
            "parameters": {
                "type": "object",
                "properties": {
                    "connection_id": {"type": "string"},
                    "db_type": {"type": "string", "enum": ["postgresql", "mysql", "sqlite"]},
                    "host": {"type": "string"},
                    "port": {"type": "integer"},
                    "database": {"type": "string"},
                    "username": {"type": "string"},
                    "password": {"type": "string"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "register_connection",
            "description": "Save database credentials under a friendly name and receive a connection_id for reuse.",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "db_type": {"type": "string", "enum": ["postgresql", "mysql", "sqlite"]},
                    "host": {"type": "string"},
                    "port": {"type": "integer"},
                    "database": {"type": "string"},
                    "username": {"type": "string"},
                    "password": {"type": "string"},
                },
                "required": ["name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "inspect_schema",
            "description": "Return all safe analytics views and their columns for the healthcare AI layer.",
            "parameters": {
                "type": "object",
                "properties": {
                    "connection_id": {"type": "string"},
                    "db_type": {"type": "string", "enum": ["postgresql", "mysql", "sqlite"]},
                    "host": {"type": "string"},
                    "port": {"type": "integer"},
                    "database": {"type": "string"},
                    "username": {"type": "string"},
                    "password": {"type": "string"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_database",
            "description": "Ask a clinical question in plain English. The middleware converts it to a safe read-only SQL query using the live healthcare schema.",
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {"type": "string"},
                    "connection_id": {"type": "string"},
                    "db_type": {"type": "string", "enum": ["postgresql", "mysql", "sqlite"]},
                    "host": {"type": "string"},
                    "port": {"type": "integer"},
                    "database": {"type": "string"},
                    "username": {"type": "string"},
                    "password": {"type": "string"},
                },
                "required": ["question"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_sql",
            "description": "Execute a raw read-only SQL query against the safe analytics views and return columns and rows.",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string"},
                    "connection_id": {"type": "string"},
                    "db_type": {"type": "string", "enum": ["postgresql", "mysql", "sqlite"]},
                    "host": {"type": "string"},
                    "port": {"type": "integer"},
                    "database": {"type": "string"},
                    "username": {"type": "string"},
                    "password": {"type": "string"},
                },
                "required": ["sql"],
            },
        },
    },
]


class InvokeRequest(BaseModel):
    tool: str = Field(..., description="Tool name from the manifest.")
    arguments: dict[str, Any] = Field(default_factory=dict, description="Tool arguments as a JSON object.")


class InvokeResponse(BaseModel):
    success: bool
    result: Any | None = None
    error: str | None = None


def _resolve_from_args(args: dict) -> dict:
    class _Namespace:
        pass

    payload = _Namespace()
    payload.connection_id = args.get("connection_id")
    payload.db_type = args.get("db_type", "postgresql")
    payload.host = args.get("host")
    payload.port = args.get("port")
    payload.database = args.get("database")
    payload.username = args.get("username")
    payload.password = args.get("password") or ""
    return registry.resolve(payload)


@router.get("/manifest", summary="Tool Manifest")
def get_manifest():
    return {"tools": TOOL_MANIFEST}


@router.post("/invoke", response_model=InvokeResponse, summary="Invoke a Tool")
def invoke_tool(payload: InvokeRequest) -> InvokeResponse:
    tool = payload.tool
    args = payload.arguments

    try:
        if tool == "test_connection":
            params = _resolve_from_args(args)
            success, message = test_connection(**params)
            return InvokeResponse(success=success, result={"message": message})

        if tool == "register_connection":
            conn_id = registry.register_connection(
                name=args.get("name", "unnamed"),
                db_type=args.get("db_type", "postgresql"),
                host=args.get("host"),
                port=args.get("port"),
                database=args.get("database"),
                username=args.get("username"),
                password=args.get("password"),
            )
            return InvokeResponse(success=True, result={"connection_id": conn_id, "name": args.get("name")})

        if tool == "inspect_schema":
            params = _resolve_from_args(args)
            return InvokeResponse(success=True, result=get_schema_metadata(**params))

        if tool == "query_database":
            question = args.get("question", "")
            if not question:
                return InvokeResponse(success=False, error="'question' argument is required.")
            params = _resolve_from_args(args)
            result = execute_nl_query(question=question, **params)
            return InvokeResponse(success=result.get("success", False), result=result)

        if tool == "run_sql":
            sql = args.get("sql", "")
            if not sql:
                return InvokeResponse(success=False, error="'sql' argument is required.")
            params = _resolve_from_args(args)
            result = execute_sql_query(sql=sql, **params)
            return InvokeResponse(success=result.get("success", False), result=result)

        return InvokeResponse(success=False, error=f"Unknown tool '{tool}'. See GET /tools/manifest.")
    except ValueError as exc:
        return InvokeResponse(success=False, error=str(exc))
    except Exception as exc:
        return InvokeResponse(success=False, error=str(exc))
