import json
from datetime import datetime
from pathlib import Path


# Resolve project root dynamically
BASE_DIR = Path(__file__).resolve().parents[3]

LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "query_logs.jsonl"


def write_query_log(entry: dict) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


def build_query_log(
    question: str,
    generated_sql: str,
    success: bool,
    row_count: int | None = None,
    error: str | None = None,
    truncated: bool | None = None,
    request_id: str | None = None,
    user_role: str | None = None,
) -> dict:
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "request_id": request_id,
        "user_role": user_role,
        "question": question,
        "generated_sql": generated_sql,
        "success": success,
        "row_count": row_count,
        "truncated": truncated,
        "error": error,
    }
