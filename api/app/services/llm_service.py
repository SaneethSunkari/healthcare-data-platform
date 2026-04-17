import os

from openai import OpenAI

from api.healthcare_prompt import HEALTHCARE_SYSTEM_PROMPT
from api.app.services.sql_validator import clean_sql_output, extract_referenced_relations

UNANSWERABLE_SQL = "SELECT 'UNANSWERABLE' AS error;"


def _schema_text(schema_metadata: dict) -> str:
    parts = []
    for table_name, columns in schema_metadata.get("tables", {}).items():
        column_defs = ", ".join(f"{col['name']} ({col['type']})" for col in columns)
        parts.append(f"{table_name}: {column_defs}")
    return "\n".join(parts)


def _get_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY is not configured in this project. Add it to your environment or .env file.")
    return OpenAI(api_key=api_key)


def generate_sql_from_question(question: str, schema_metadata: dict) -> str:
    client = _get_client()
    schema_text = _schema_text(schema_metadata)
    prompt = f"""
Schema available to query:
{schema_text}

User question:
{question}
"""

    response = client.chat.completions.create(
        model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
        temperature=0,
        messages=[
            {"role": "system", "content": HEALTHCARE_SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
    )

    raw_output = response.choices[0].message.content or ""
    cleaned_sql = clean_sql_output(raw_output)
    if not cleaned_sql.lower().startswith(("select", "with")):
        return UNANSWERABLE_SQL

    relations = extract_referenced_relations(cleaned_sql)
    if relations and not relations.issubset(schema_metadata.get("tables", {}).keys()):
        return UNANSWERABLE_SQL

    return cleaned_sql
