from api.app.services.query_service import test_connection


def test_postgres_connection(
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
) -> tuple[bool, str]:
    return test_connection(
        db_type="postgresql",
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
    )
