from types import SimpleNamespace

from api.app.services.connection_registry import resolve


def test_resolve_falls_back_to_environment_defaults(monkeypatch) -> None:
    monkeypatch.setenv("DB_HOST", "127.0.0.1")
    monkeypatch.setenv("DB_PORT", "15432")
    monkeypatch.setenv("DB_NAME", "healthcare_db")
    monkeypatch.setenv("DB_USER", "postgres")
    monkeypatch.setenv("DB_PASSWORD", "postgres")

    payload = SimpleNamespace(
        connection_id=None,
        db_type="postgresql",
        host=None,
        port=None,
        database=None,
        username=None,
        password=None,
    )

    resolved = resolve(payload)

    assert resolved == {
        "db_type": "postgresql",
        "host": "127.0.0.1",
        "port": 15432,
        "database": "healthcare_db",
        "username": "postgres",
        "password": "postgres",
    }
