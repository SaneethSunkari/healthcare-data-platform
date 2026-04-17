import re

ALLOWED_RELATIONS = {
    "patient_analytics",
    "encounter_analytics",
    "condition_analytics",
    "medication_analytics",
}

FORBIDDEN_KEYWORDS = [
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "truncate",
    "create",
    "grant",
    "revoke",
    "copy",
]


def clean_sql_output(raw_text: str) -> str:
    text = raw_text.strip()
    text = re.sub(r"^```sql\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^```\s*", "", text)
    text = re.sub(r"\s*```$", "", text)

    match = re.search(r"\b(select|with)\b.*", text, flags=re.IGNORECASE | re.DOTALL)
    if match:
        text = match.group(0).strip()

    if ";" in text:
        text = text.split(";", maxsplit=1)[0].strip() + ";"

    return text


def extract_referenced_relations(sql: str) -> set[str]:
    return {
        match.lower().split(".")[-1]
        for match in re.findall(r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_\.]*)", sql, flags=re.IGNORECASE)
    }


def _is_aggregate_query(sql: str) -> bool:
    lowered = sql.lower()
    aggregate_markers = (" count(", " avg(", " sum(", " min(", " max(", " group by ", " distinct ")
    return any(marker in f" {lowered}" for marker in aggregate_markers)


def normalize_sql_for_execution(sql: str) -> str:
    cleaned = clean_sql_output(sql)
    if not cleaned.endswith(";"):
        cleaned = cleaned + ";"

    if not _is_aggregate_query(cleaned) and " limit " not in cleaned.lower():
        cleaned = cleaned[:-1] + " LIMIT 100;"

    return cleaned


def validate_safe_read_only_sql(sql: str) -> tuple[bool, str]:
    cleaned = clean_sql_output(sql).strip()
    lowered = cleaned.lower()

    if not cleaned:
        return False, "SQL query cannot be empty"

    if not lowered.startswith(("select", "with")):
        return False, "Only SELECT queries are allowed"

    if ";" in cleaned[:-1]:
        return False, "Multiple SQL statements are not allowed"

    for keyword in FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{keyword}\b", lowered):
            return False, f"Forbidden SQL keyword detected: {keyword}"

    relations = extract_referenced_relations(cleaned)
    if not relations:
        return False, "Query must reference at least one analytics view"

    if not relations.issubset(ALLOWED_RELATIONS):
        invalid = ", ".join(sorted(relations - ALLOWED_RELATIONS))
        return False, f"Queries may only use safe analytics views. Invalid relations: {invalid}"

    return True, "Query is valid"


def validate_read_only_sql(sql: str) -> tuple[bool, str]:
    return validate_safe_read_only_sql(sql)
