from api.app.services.sql_validator import (
    normalize_sql_for_execution,
    validate_safe_read_only_sql,
)


def test_limit_is_added_for_non_aggregate_query() -> None:
    sql = "SELECT golden_id FROM patient_analytics"
    normalized = normalize_sql_for_execution(sql)

    assert normalized.endswith("LIMIT 100;")


def test_limit_is_not_added_for_aggregate_query() -> None:
    sql = "SELECT COUNT(*) FROM patient_analytics;"
    normalized = normalize_sql_for_execution(sql)

    assert normalized == "SELECT COUNT(*) FROM patient_analytics;"


def test_invalid_relation_is_rejected() -> None:
    is_valid, message = validate_safe_read_only_sql("SELECT * FROM patients;")

    assert not is_valid
    assert "safe analytics views" in message
