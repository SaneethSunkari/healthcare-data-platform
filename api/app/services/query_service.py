from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import RealDictCursor

from api.app.core.settings import get_settings
from api.app.services.llm_service import UNANSWERABLE_SQL, generate_sql_from_question
from api.app.services.log_service import build_query_log, write_query_log
from api.app.services.sql_validator import normalize_sql_for_execution, validate_safe_read_only_sql
from compliance.pii_masker import log_access

SAFE_VIEWS = [
    "patient_analytics",
    "encounter_analytics",
    "condition_analytics",
    "medication_analytics",
]


def connect_db(
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connect_timeout_s: int | None = None,
    application_name: str = "healthcare-api",
):
    settings = get_settings()
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=database,
        user=username,
        password=password,
        connect_timeout=connect_timeout_s or settings.connect_timeout_s,
        application_name=application_name,
    )


def _prepare_read_only_session(connection) -> None:
    settings = get_settings()
    with connection.cursor() as cursor:
        cursor.execute("SET default_transaction_read_only = on")
        cursor.execute("SET statement_timeout = %s", (settings.query_timeout_ms,))
        cursor.execute("SET lock_timeout = %s", (min(settings.query_timeout_ms, 2000),))
        cursor.execute(
            "SET idle_in_transaction_session_timeout = %s",
            (max(settings.query_timeout_ms, 3000),),
        )


def _write_audit_log(
    user_role: str,
    action: str,
    query_text: str,
    ip: str,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
) -> None:
    with connect_db(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        application_name="healthcare-audit",
    ) as audit_connection:
        log_access(audit_connection, user_role, action, None, query_text, ip)


def clean_error_message(error: str) -> str:
    normalized = (error or "").strip().lower()
    if "only select queries are allowed" in normalized:
        return "Only SELECT queries are allowed"
    if "multiple sql statements are not allowed" in normalized:
        return "Multiple SQL statements are not allowed"
    if "queries may only use safe analytics views" in normalized:
        return error.strip()
    if "password authentication failed" in normalized:
        return "Unable to connect to the database with the provided credentials"
    if "role \"" in normalized and "does not exist" in normalized:
        return "Unable to connect to the database with the provided credentials"
    if "connection refused" in normalized or "could not connect to server" in normalized:
        return "Unable to connect to the database server"
    if "does not exist" in normalized:
        return "Generated SQL referenced a relation or column that does not exist"
    if not error:
        return "Query failed"
    return error.strip().splitlines()[0]


def _serialize_value(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _serialize_rows(rows: list[dict]) -> list[dict]:
    serialized = []
    for row in rows:
        serialized.append({key: _serialize_value(value) for key, value in row.items()})
    return serialized


def ensure_safe_views(connection) -> None:
    safe_views_path = Path(__file__).resolve().parents[2] / "safe_views.sql"
    sql = safe_views_path.read_text(encoding="utf-8")
    with connection.cursor() as cursor:
        cursor.execute(sql)
    connection.commit()


def get_schema_metadata(connection) -> dict:
    query = """
    SELECT table_name, column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = ANY(%s)
    ORDER BY table_name, ordinal_position
    """
    with connection.cursor() as cursor:
        cursor.execute(query, (SAFE_VIEWS,))
        rows = cursor.fetchall()

    schema: dict[str, list[dict]] = {"tables": {}}
    for table_name, column_name, data_type, is_nullable in rows:
        schema["tables"].setdefault(table_name, []).append(
            {
                "name": column_name,
                "type": data_type,
                "nullable": is_nullable == "YES",
            }
        )
    return schema


def test_connection(**params) -> tuple[bool, str]:
    try:
        with connect_db(
            host=params["host"],
            port=params["port"],
            database=params["database"],
            username=params["username"],
            password=params["password"],
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
        return True, "Connection successful"
    except OperationalError as exc:
        return False, clean_error_message(str(exc))


def execute_sql_query(
    sql: str,
    user_role: str = "analyst",
    ip: str = "unknown",
    request_id: str | None = None,
    audit_action: str | None = "RUN_SQL",
    audit_query_text: str | None = None,
    db_type: str = "postgresql",
    host: str = "127.0.0.1",
    port: int = 15432,
    database: str = "healthcare_db",
    username: str = "postgres",
    password: str = "postgres",
):
    is_valid, message = validate_safe_read_only_sql(sql)
    if not is_valid:
        write_query_log(
            build_query_log(
                question="",
                generated_sql=sql,
                success=False,
                error=message,
                request_id=request_id,
                user_role=user_role,
            )
        )
        return {"success": False, "sql": sql, "error": message}

    normalized_sql = normalize_sql_for_execution(sql)

    try:
        with connect_db(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            application_name="healthcare-query",
        ) as connection:
            ensure_safe_views(connection)
            _prepare_read_only_session(connection)
            settings = get_settings()
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(normalized_sql)
                fetched_rows = cursor.fetchmany(settings.max_query_rows + 1)
                truncated = len(fetched_rows) > settings.max_query_rows
                rows = _serialize_rows(
                    [dict(row) for row in fetched_rows[: settings.max_query_rows]]
                )
                columns = list(rows[0].keys()) if rows else [desc.name for desc in cursor.description or []]
    except Exception as exc:
        error = clean_error_message(str(exc))
        write_query_log(
            build_query_log(
                question="",
                generated_sql=normalized_sql,
                success=False,
                error=error,
                request_id=request_id,
                user_role=user_role,
            )
        )
        return {"success": False, "sql": normalized_sql, "error": error}

    if audit_action:
        _write_audit_log(
            user_role=user_role,
            action=audit_action,
            query_text=audit_query_text or normalized_sql,
            ip=ip,
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
        )

    write_query_log(
        build_query_log(
            question="",
            generated_sql=normalized_sql,
            success=True,
            row_count=len(rows),
            truncated=truncated,
            request_id=request_id,
            user_role=user_role,
        )
    )
    return {
        "success": True,
        "sql": normalized_sql,
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
        "truncated": truncated,
        "request_id": request_id,
    }


def execute_nl_query(
    question: str,
    user_role: str = "analyst",
    ip: str = "unknown",
    request_id: str | None = None,
    db_type: str = "postgresql",
    host: str = "127.0.0.1",
    port: int = 15432,
    database: str = "healthcare_db",
    username: str = "postgres",
    password: str = "postgres",
):
    try:
        with connect_db(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            application_name="healthcare-nl-query",
        ) as connection:
            ensure_safe_views(connection)
            _prepare_read_only_session(connection)
            schema_metadata = get_schema_metadata(connection)
            generated_sql = generate_sql_from_question(question, schema_metadata)
            if generated_sql == UNANSWERABLE_SQL:
                return {
                    "success": False,
                    "question": question,
                    "sql": generated_sql,
                    "error": "Question cannot be answered from the available healthcare analytics schema",
                }

            result = execute_sql_query(
                sql=generated_sql,
                user_role=user_role,
                ip=ip,
                request_id=request_id,
                audit_action=None,
                db_type=db_type,
                host=host,
                port=port,
                database=database,
                username=username,
                password=password,
            )
            if not result.get("success"):
                return {
                    "success": False,
                    "question": question,
                    "sql": result.get("sql", generated_sql),
                    "error": result.get("error", "Query execution failed"),
                }

    except Exception as exc:
        error = clean_error_message(str(exc))
        write_query_log(
            build_query_log(
                question=question,
                generated_sql="",
                success=False,
                error=error,
                request_id=request_id,
                user_role=user_role,
            )
        )
        return {"success": False, "question": question, "sql": "", "error": error}

    _write_audit_log(
        user_role=user_role,
        action="AI_QUERY",
        query_text=f"Q: {question}\nSQL: {result['sql']}",
        ip=ip,
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
    )

    write_query_log(
        build_query_log(
            question=question,
            generated_sql=result["sql"],
            success=True,
            row_count=result["row_count"],
            truncated=result.get("truncated"),
            request_id=request_id,
            user_role=user_role,
        )
    )
    return {
        "success": True,
        "question": question,
        "sql": result["sql"],
        "columns": result["columns"],
        "rows": result["rows"],
        "row_count": result["row_count"],
        "truncated": result.get("truncated"),
        "request_id": request_id,
    }
