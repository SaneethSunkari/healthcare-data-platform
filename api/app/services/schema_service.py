from contextlib import closing

from api.app.services.query_service import connect_db, ensure_safe_views, get_schema_metadata as get_safe_schema_metadata


def get_schema_metadata(
    db_type: str = "postgresql",
    host: str = "127.0.0.1",
    port: int = 15432,
    database: str = "healthcare_db",
    username: str = "postgres",
    password: str = "postgres",
):
    with closing(
        connect_db(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
        )
    ) as connection:
        ensure_safe_views(connection)
        schema = get_safe_schema_metadata(connection)

    schema.setdefault("relationships", [])
    return schema
