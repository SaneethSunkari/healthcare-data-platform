import os
from datetime import datetime, timezone
from uuid import uuid4


DEFAULT_CONNECTION_ID = "healthcare-db"
_registry: dict[str, dict] = {}


def seed_default_connection() -> None:
    if DEFAULT_CONNECTION_ID in _registry:
        return

    _registry[DEFAULT_CONNECTION_ID] = {
        "id": DEFAULT_CONNECTION_ID,
        "name": "Healthcare DB",
        "db_type": "postgresql",
        "host": os.getenv("DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("DB_PORT", "15432")),
        "database": os.getenv("DB_NAME", "healthcare_db"),
        "username": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def register_connection(
    name: str,
    db_type: str,
    host: str | None,
    port: int | None,
    database: str | None,
    username: str | None,
    password: str | None,
) -> str:
    conn_id = str(uuid4())
    _registry[conn_id] = {
        "id": conn_id,
        "name": name,
        "db_type": db_type,
        "host": host,
        "port": port,
        "database": database,
        "username": username,
        "password": password,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    return conn_id


def get_connection(connection_id: str) -> dict | None:
    seed_default_connection()
    return _registry.get(connection_id)


def list_connections() -> list[dict]:
    seed_default_connection()
    return [{key: value for key, value in conn.items() if key != "password"} for conn in _registry.values()]


def delete_connection(connection_id: str) -> bool:
    if connection_id in _registry and connection_id != DEFAULT_CONNECTION_ID:
        del _registry[connection_id]
        return True
    return False


def _value_or_default(value, default):
    return default if value in (None, "") else value


def resolve(payload) -> dict:
    seed_default_connection()
    connection_id = getattr(payload, "connection_id", None)
    if connection_id:
        conn = get_connection(connection_id)
        if not conn:
            raise ValueError(
                f"Saved connection '{connection_id}' not found. Register it first via POST /connections/register."
            )
        return {
            "db_type": conn["db_type"],
            "host": conn["host"],
            "port": conn["port"],
            "database": conn["database"],
            "username": conn["username"],
            "password": conn["password"],
        }

    return {
        "db_type": _value_or_default(getattr(payload, "db_type", None), "postgresql"),
        "host": _value_or_default(getattr(payload, "host", None), os.getenv("DB_HOST", "127.0.0.1")),
        "port": _value_or_default(getattr(payload, "port", None), int(os.getenv("DB_PORT", "15432"))),
        "database": _value_or_default(getattr(payload, "database", None), os.getenv("DB_NAME", "healthcare_db")),
        "username": _value_or_default(getattr(payload, "username", None), os.getenv("DB_USER", "postgres")),
        "password": _value_or_default(getattr(payload, "password", None), os.getenv("DB_PASSWORD", "postgres")),
    }
