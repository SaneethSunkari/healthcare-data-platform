#!/usr/bin/env python3
import argparse
import logging
import os
import re
from typing import Any

import psycopg2
from dotenv import load_dotenv
from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.nlp_engine import NlpEngineProvider
from presidio_anonymizer import AnonymizerEngine

load_dotenv()

LOGGER = logging.getLogger("pii_masker")
MASK_TOKEN = "<MASKED>"
FULL_ACCESS_ROLES = {"doctor", "provider"}
STRUCTURED_PII_FIELDS = {
    "first_name",
    "last_name",
    "birth_date",
    "zip_code",
    "patient_names",
    "zip_codes",
    "source_patient_id",
    "address",
    "phone_number",
    "email",
    "ssn",
    "full_name",
}
TEXT_MASK_FIELDS = {
    "report_text",
    "note_excerpt",
    "activity_summary",
}
ANALYZER_ENTITIES = [
    "PERSON",
    "DATE_TIME",
    "US_SSN",
    "PHONE_NUMBER",
    "EMAIL_ADDRESS",
    "LOCATION",
    "US_DRIVER_LICENSE",
]
FALLBACK_PATTERNS = [
    (re.compile(r"\b\d{3}-\d{2}-\d{4}\b"), "<US_SSN>"),
]


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def build_analyzer() -> AnalyzerEngine:
    configuration = {
        "nlp_engine_name": "spacy",
        "models": [{"lang_code": "en", "model_name": "en_core_web_lg"}],
    }
    provider = NlpEngineProvider(nlp_configuration=configuration)
    nlp_engine = provider.create_engine()
    return AnalyzerEngine(nlp_engine=nlp_engine, supported_languages=["en"])


ANALYZER = build_analyzer()
ANONYMIZER = AnonymizerEngine()


def connect_db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "healthcare_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
    )


def role_has_full_access(user_role: str | None) -> bool:
    return (user_role or "").strip().lower() in FULL_ACCESS_ROLES


def mask_text(text: str) -> str:
    """Mask all supported PII in a text string."""
    if not text:
        return text

    results = ANALYZER.analyze(
        text=text,
        language="en",
        entities=ANALYZER_ENTITIES,
    )
    masked = ANONYMIZER.anonymize(text=text, analyzer_results=results).text
    for pattern, replacement in FALLBACK_PATTERNS:
        masked = pattern.sub(replacement, masked)
    return masked


def mask_patient_record(record: dict[str, Any]) -> dict[str, Any]:
    """Mask structured patient fields for non-doctor roles."""
    masked = record.copy()
    for field in STRUCTURED_PII_FIELDS:
        if field in masked and masked[field] not in (None, ""):
            masked[field] = MASK_TOKEN
    return masked


def mask_response_for_role(payload: Any, user_role: str) -> Any:
    """Recursively mask patient-shaped responses for non-doctor roles."""
    if role_has_full_access(user_role) or payload is None:
        return payload

    if isinstance(payload, list):
        return [mask_response_for_role(item, user_role) for item in payload]

    if isinstance(payload, dict):
        masked = payload.copy()
        if "first_name" in masked or "last_name" in masked or "birth_date" in masked:
            masked = mask_patient_record(masked)
        for key, value in list(masked.items()):
            if isinstance(value, (dict, list)):
                masked[key] = mask_response_for_role(value, user_role)
            elif key in TEXT_MASK_FIELDS and isinstance(value, str):
                masked[key] = mask_text(value)
            elif key in STRUCTURED_PII_FIELDS and value not in (None, ""):
                masked[key] = MASK_TOKEN
        return masked

    return payload


def mask_ai_response_payload(payload: Any, user_role: str) -> Any:
    """Mask JSON payloads returned by AI query endpoints for non-doctor roles."""
    if role_has_full_access(user_role) or payload is None:
        return payload

    if isinstance(payload, list):
        return [mask_ai_response_payload(item, user_role) for item in payload]

    if isinstance(payload, dict):
        masked = {}
        for key, value in payload.items():
            if key in {"sql", "question", "error"}:
                masked[key] = value
                continue
            if key == "rows" and isinstance(value, list):
                masked[key] = [mask_ai_response_payload(row, user_role) for row in value]
                continue
            if isinstance(value, dict):
                masked[key] = mask_ai_response_payload(value, user_role)
                continue
            if isinstance(value, list):
                masked[key] = [mask_ai_response_payload(item, user_role) for item in value]
                continue
            if isinstance(value, str):
                if key in TEXT_MASK_FIELDS:
                    masked[key] = mask_text(value)
                elif key in STRUCTURED_PII_FIELDS:
                    masked[key] = MASK_TOKEN
                else:
                    masked[key] = value
                continue
            masked[key] = value
        return masked

    return payload


def log_access(conn, user_role: str, action: str, patient_id: str | None, query_text: str, ip: str) -> None:
    """Write to the HIPAA audit log."""
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO audit_log (user_role, action, patient_id, query_text, ip_address)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (user_role, action, patient_id, query_text, ip),
        )
    conn.commit()


def get_patient_by_role(conn, patient_id: str, user_role: str, ip: str = "unknown") -> dict[str, Any] | None:
    """Role-based access with automatic audit logging."""
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT id, first_name, last_name, birth_date, gender, zip_code
            FROM patients
            WHERE id = %s
            """,
            (patient_id,),
        )
        row = cursor.fetchone()

    if not row:
        return None

    record = {
        "id": row[0],
        "first_name": row[1],
        "last_name": row[2],
        "birth_date": str(row[3]) if row[3] is not None else None,
        "gender": row[4],
        "zip_code": row[5],
    }

    log_access(conn, user_role, "READ_PATIENT", patient_id, f"GET /patients/{patient_id}", ip)
    return mask_response_for_role(record, user_role)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a quick Presidio masking test.")
    parser.add_argument(
        "--text",
        default="Patient John Smith, DOB 01/15/1965, SSN 123-45-6789, email john@example.com",
        help="Sample text to mask.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    configure_logging()
    args = parse_args()

    sample_record = {
        "id": "demo-1",
        "first_name": "John",
        "last_name": "Smith",
        "birth_date": "1965-01-15",
        "gender": "male",
        "zip_code": "02139",
    }

    print("Before text: ", args.text)
    print("After text:  ", mask_text(args.text))
    print("Before record:", sample_record)
    print("After record: ", mask_patient_record(sample_record))
