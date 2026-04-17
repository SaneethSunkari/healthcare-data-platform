#!/usr/bin/env python3
import argparse
import base64
import glob
import html
import json
import logging
import os
import re
import sys
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg2
from dotenv import load_dotenv
from psycopg2 import OperationalError
from psycopg2.extras import execute_values

load_dotenv()

LOGGER = logging.getLogger("fhir_parser")
LOAD_ORDER = (
    "Patient",
    "Encounter",
    "Condition",
    "MedicationRequest",
    "AllergyIntolerance",
    "Observation",
    "Procedure",
    "DiagnosticReport",
    "Immunization",
    "CarePlan",
    "Claim",
)
COUNTED_TABLES = (
    "patients",
    "encounters",
    "conditions",
    "medications",
    "allergies",
    "observations",
    "procedures",
    "diagnostic_reports",
    "immunizations",
    "care_plans",
)
CLAIM_TOTALS_BY_ENCOUNTER: defaultdict[str, Decimal] = defaultdict(Decimal)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load Synthea FHIR bundles into the healthcare database.",
    )
    parser.add_argument(
        "--input-dir",
        default=os.getenv("FHIR_INPUT_DIR", "synthea/output/fhir"),
        help="Directory containing Synthea FHIR bundle JSON files.",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="Logging level (DEBUG, INFO, WARNING, ERROR).",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=100,
        help="Log progress every N processed files.",
    )
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def connect_db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "healthcare_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
    )


def first_item(items: list[dict[str, Any]]) -> dict[str, Any]:
    return items[0] if items else {}


def ensure_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def normalize_reference(reference: str | None) -> str | None:
    if not reference:
        return None
    if reference.startswith("urn:uuid:"):
        return reference.rsplit(":", 1)[-1]
    if "/" in reference:
        return reference.rsplit("/", 1)[-1]
    return reference


def as_date(value: str | None) -> str | None:
    if not value:
        return None
    return value[:10]


def get_code_details(codeable_concept: dict[str, Any]) -> tuple[str | None, str | None]:
    coding = first_item(codeable_concept.get("coding", []))
    code = coding.get("code")
    description = coding.get("display") or codeable_concept.get("text")
    return code, description


def get_status_code(status_obj: Any) -> str | None:
    if isinstance(status_obj, str):
        return status_obj
    if not isinstance(status_obj, dict):
        return None
    coding = first_item(status_obj.get("coding", []))
    return coding.get("code") or coding.get("display") or status_obj.get("text")


def join_labels(values: list[str]) -> str | None:
    cleaned = [value.strip() for value in values if value and value.strip()]
    return ", ".join(dict.fromkeys(cleaned)) if cleaned else None


def get_category_label(categories: Any) -> str | None:
    labels: list[str] = []
    for category in ensure_list(categories):
        if isinstance(category, str):
            labels.append(category)
            continue
        if not isinstance(category, dict):
            continue
        coding = first_item(category.get("coding", []))
        labels.append(coding.get("display") or coding.get("code") or category.get("text") or "")
    return join_labels(labels)


def get_reaction_details(resource: dict[str, Any]) -> tuple[str | None, str | None]:
    reaction = first_item(resource.get("reaction", []))
    if not reaction:
        return None, None

    manifestation_labels: list[str] = []
    for manifestation in reaction.get("manifestation", []):
        _, description = get_code_details(manifestation)
        manifestation_labels.append(description or manifestation.get("text") or "")

    reaction_description = join_labels(manifestation_labels)
    reaction_severity = reaction.get("severity")
    return reaction_description, reaction_severity


def format_timing(timing: dict[str, Any]) -> str | None:
    if not timing:
        return None

    repeat = timing.get("repeat", {})
    frequency = repeat.get("frequency")
    period = repeat.get("period")
    period_unit = repeat.get("periodUnit")
    bounds = repeat.get("boundsDuration", {})

    unit_map = {
        "h": "hour",
        "d": "day",
        "wk": "week",
        "mo": "month",
        "a": "year",
        "min": "minute",
    }

    if frequency and period and period_unit:
        period_value = int(period) if float(period).is_integer() else period
        unit_label = unit_map.get(period_unit, period_unit)
        cycle = unit_label if period_value == 1 else f"{period_value} {unit_label}s"
        return f"{frequency}x per {cycle}"

    if bounds.get("value") and bounds.get("unit"):
        return f"for {bounds['value']} {bounds['unit']}"

    code = timing.get("code", {})
    _, description = get_code_details(code)
    return description or code.get("text")


def format_dose(dosage_instruction: dict[str, Any]) -> str | None:
    if dosage_instruction.get("text"):
        return dosage_instruction["text"]

    dose_and_rate = first_item(dosage_instruction.get("doseAndRate", []))
    dose_quantity = dose_and_rate.get("doseQuantity", {})
    value = dose_quantity.get("value")
    unit = dose_quantity.get("unit") or dose_quantity.get("code")
    if value is None:
        return None
    if unit:
        return f"{value:g} {unit}" if isinstance(value, (int, float)) else f"{value} {unit}"
    return str(value)


def extract_medication_details(resource: dict[str, Any]) -> tuple[str | None, str | None, str | None, str | None, bool | None]:
    dosage_instruction = first_item(resource.get("dosageInstruction", []))
    if not dosage_instruction:
        return None, None, None, resource.get("requester", {}).get("display"), None

    route_code, route_description = get_code_details(dosage_instruction.get("route", {}))
    route = route_description or route_code
    frequency = format_timing(dosage_instruction.get("timing", {}))
    dose_details = format_dose(dosage_instruction)
    prescriber = resource.get("requester", {}).get("display")
    as_needed = dosage_instruction.get("asNeededBoolean")
    return dose_details, frequency, route, prescriber, as_needed


def get_encounter_type(resource: dict[str, Any]) -> str | None:
    encounter_type = first_item(resource.get("type", []))
    _, description = get_code_details(encounter_type)
    if description:
        return description
    if encounter_type.get("text"):
        return encounter_type["text"]
    return resource.get("class", {}).get("code")


def get_provider_name(resource: dict[str, Any]) -> str | None:
    service_provider = resource.get("serviceProvider", {}).get("display")
    if service_provider:
        return service_provider

    participant = first_item(resource.get("participant", []))
    participant_name = participant.get("individual", {}).get("display")
    if participant_name:
        return participant_name

    location = first_item(resource.get("location", []))
    return location.get("location", {}).get("display")


def get_total_cost(resource: dict[str, Any]) -> float | None:
    for extension in resource.get("extension", []):
        if "totalCost" in extension.get("url", ""):
            return extension.get("valueMoney", {}).get("value")
    return None


def get_claim_total(resource: dict[str, Any]) -> Decimal | None:
    total = resource.get("total")
    if isinstance(total, dict):
        value = total.get("value")
        return Decimal(str(value)) if value is not None else None
    if isinstance(total, list):
        for total_entry in total:
            amount = total_entry.get("amount", {})
            value = amount.get("value")
            if value is not None:
                return Decimal(str(value))
    return None


def get_claim_encounter_ids(resource: dict[str, Any]) -> list[str]:
    encounter_ids: set[str] = set()
    for item in resource.get("item", []):
        for encounter in item.get("encounter", []):
            encounter_id = normalize_reference(encounter.get("reference"))
            if encounter_id:
                encounter_ids.add(encounter_id)
    return sorted(encounter_ids)


def parse_numeric_value(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def extract_value_fields(resource: dict[str, Any]) -> tuple[Decimal | None, str | None, str | None]:
    quantity = resource.get("valueQuantity")
    if quantity:
        numeric = parse_numeric_value(quantity.get("value"))
        unit = quantity.get("unit") or quantity.get("code")
        if numeric is not None:
            text = f"{numeric.normalize()} {unit}".strip() if unit else str(numeric.normalize())
        else:
            text = None
        return numeric, unit, text

    if "valueString" in resource:
        return None, None, resource.get("valueString")

    if "valueCodeableConcept" in resource:
        code, description = get_code_details(resource.get("valueCodeableConcept", {}))
        return None, code, description

    if "valueInteger" in resource:
        numeric = parse_numeric_value(resource.get("valueInteger"))
        return numeric, None, str(resource.get("valueInteger"))

    if "valueDecimal" in resource:
        numeric = parse_numeric_value(resource.get("valueDecimal"))
        return numeric, None, str(resource.get("valueDecimal"))

    if "valueBoolean" in resource:
        value = resource.get("valueBoolean")
        return None, None, str(value).lower()

    return None, None, None


def get_interpretation(resource: dict[str, Any]) -> str | None:
    interpretation = first_item(resource.get("interpretation", []))
    coding = first_item(interpretation.get("coding", []))
    return coding.get("display") or coding.get("code") or interpretation.get("text")


def strip_html(raw_text: str | None) -> str | None:
    if not raw_text:
        return None
    normalized = html.unescape(raw_text)
    normalized = re.sub(r"<[^>]+>", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized or None


def decode_report_text(resource: dict[str, Any]) -> str | None:
    for form in resource.get("presentedForm", []):
        data = form.get("data")
        if not data:
            continue
        try:
            decoded = base64.b64decode(data).decode("utf-8", errors="ignore")
        except Exception:
            continue
        normalized = re.sub(r"\s+\n", "\n", decoded).strip()
        if normalized:
            return normalized
    return strip_html(resource.get("text", {}).get("div"))


def get_activity_summary(resource: dict[str, Any]) -> str | None:
    items: list[str] = []
    for activity in resource.get("activity", []):
        detail = activity.get("detail", {})
        _, description = get_code_details(detail.get("code", {}))
        summary = description or detail.get("description")
        if not summary:
            continue
        location = detail.get("location", {}).get("display")
        status = detail.get("status")
        if status:
            summary = f"{summary} ({status})"
        if location:
            summary = f"{summary} @ {location}"
        items.append(summary)
    if items:
        return "; ".join(items)
    return strip_html(resource.get("text", {}).get("div"))


def parse_patient(cursor, resource: dict[str, Any]) -> None:
    name = first_item(resource.get("name", []))
    address = first_item(resource.get("address", []))
    given_name = " ".join(name.get("given", []))
    family_name = name.get("family", "")

    cursor.execute(
        """
        INSERT INTO patients (
          id, golden_id, first_name, last_name, birth_date,
          gender, zip_code, source_system
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            birth_date = EXCLUDED.birth_date,
            gender = EXCLUDED.gender,
            zip_code = EXCLUDED.zip_code,
            source_system = EXCLUDED.source_system
        """,
        (
            resource["id"],
            None,
            given_name,
            family_name,
            resource.get("birthDate"),
            resource.get("gender"),
            address.get("postalCode"),
            "synthea_v1",
        ),
    )


def parse_encounter(cursor, resource: dict[str, Any]) -> None:
    period = resource.get("period", {})
    patient_id = normalize_reference(resource.get("subject", {}).get("reference"))

    cursor.execute(
        """
        INSERT INTO encounters (
          id, patient_id, start_date, end_date,
          encounter_type, provider, cost
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET patient_id = EXCLUDED.patient_id,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            encounter_type = EXCLUDED.encounter_type,
            provider = EXCLUDED.provider,
            cost = EXCLUDED.cost
        """,
        (
            resource["id"],
            patient_id,
            period.get("start"),
            period.get("end"),
            get_encounter_type(resource),
            get_provider_name(resource),
            get_total_cost(resource),
        ),
    )


def parse_condition(cursor, resource: dict[str, Any]) -> None:
    patient_id = normalize_reference(resource.get("subject", {}).get("reference"))
    encounter_id = normalize_reference(resource.get("encounter", {}).get("reference"))
    code, description = get_code_details(resource.get("code", {}))

    cursor.execute(
        """
        INSERT INTO conditions (
          id, patient_id, encounter_id,
          code, description, onset_date
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET patient_id = EXCLUDED.patient_id,
            encounter_id = EXCLUDED.encounter_id,
            code = EXCLUDED.code,
            description = EXCLUDED.description,
            onset_date = EXCLUDED.onset_date
        """,
        (
            resource["id"],
            patient_id,
            encounter_id,
            code,
            description,
            as_date(resource.get("onsetDateTime")),
        ),
    )


def parse_medication(cursor, resource: dict[str, Any]) -> None:
    patient_id = normalize_reference(resource.get("subject", {}).get("reference"))
    encounter_id = normalize_reference(resource.get("encounter", {}).get("reference"))
    code, description = get_code_details(resource.get("medicationCodeableConcept", {}))
    dose_details, frequency, route, prescriber, as_needed = extract_medication_details(resource)

    cursor.execute(
        """
        INSERT INTO medications (
          id, patient_id, encounter_id, code, description,
          status, category, dose_details, frequency, route,
          prescriber, as_needed, start_date, stop_date, cost
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET patient_id = EXCLUDED.patient_id,
            encounter_id = EXCLUDED.encounter_id,
            code = EXCLUDED.code,
            description = EXCLUDED.description,
            status = EXCLUDED.status,
            category = EXCLUDED.category,
            dose_details = EXCLUDED.dose_details,
            frequency = EXCLUDED.frequency,
            route = EXCLUDED.route,
            prescriber = EXCLUDED.prescriber,
            as_needed = EXCLUDED.as_needed,
            start_date = EXCLUDED.start_date,
            stop_date = EXCLUDED.stop_date,
            cost = EXCLUDED.cost
        """,
        (
            resource["id"],
            patient_id,
            encounter_id,
            code,
            description,
            resource.get("status"),
            get_category_label(resource.get("category")),
            dose_details,
            frequency,
            route,
            prescriber,
            as_needed,
            as_date(resource.get("authoredOn")),
            as_date(
                resource.get("dispenseRequest", {})
                .get("validityPeriod", {})
                .get("end")
            ),
            None,
        ),
    )


def parse_allergy(cursor, resource: dict[str, Any]) -> None:
    patient_id = normalize_reference(
        resource.get("patient", {}).get("reference")
        or resource.get("subject", {}).get("reference")
    )
    code, description = get_code_details(resource.get("code", {}))
    reaction_description, reaction_severity = get_reaction_details(resource)

    cursor.execute(
        """
        INSERT INTO allergies (
          id, patient_id, category, code, description,
          clinical_status, verification_status, criticality,
          reaction_description, reaction_severity, recorded_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET patient_id = EXCLUDED.patient_id,
            category = EXCLUDED.category,
            code = EXCLUDED.code,
            description = EXCLUDED.description,
            clinical_status = EXCLUDED.clinical_status,
            verification_status = EXCLUDED.verification_status,
            criticality = EXCLUDED.criticality,
            reaction_description = EXCLUDED.reaction_description,
            reaction_severity = EXCLUDED.reaction_severity,
            recorded_date = EXCLUDED.recorded_date
        """,
        (
            resource["id"],
            patient_id,
            get_category_label(resource.get("category")),
            code,
            description,
            get_status_code(resource.get("clinicalStatus")),
            get_status_code(resource.get("verificationStatus")),
            resource.get("criticality"),
            reaction_description,
            reaction_severity,
            as_date(resource.get("recordedDate")),
        ),
    )


def parse_observation(cursor, resource: dict[str, Any]) -> None:
    patient_id = normalize_reference(resource.get("subject", {}).get("reference"))
    encounter_id = normalize_reference(resource.get("encounter", {}).get("reference"))
    category = get_category_label(resource.get("category"))
    status = resource.get("status")
    effective_at = resource.get("effectiveDateTime") or resource.get("issued")
    issued_at = resource.get("issued")
    interpretation = get_interpretation(resource)

    rows: list[tuple[Any, ...]] = []
    components = resource.get("component", [])
    if components:
        for index, component in enumerate(components, start=1):
            code, description = get_code_details(component.get("code", {}))
            value_numeric, value_unit, value_text = extract_value_fields(component)
            rows.append(
                (
                    f"{resource['id']}::{code or index}",
                    patient_id,
                    encounter_id,
                    category,
                    code,
                    description,
                    status,
                    effective_at,
                    issued_at,
                    value_numeric,
                    value_unit,
                    value_text,
                    interpretation,
                )
            )
    else:
        code, description = get_code_details(resource.get("code", {}))
        value_numeric, value_unit, value_text = extract_value_fields(resource)
        rows.append(
            (
                resource["id"],
                patient_id,
                encounter_id,
                category,
                code,
                description,
                status,
                effective_at,
                issued_at,
                value_numeric,
                value_unit,
                value_text,
                interpretation,
            )
        )

    for row in rows:
        cursor.execute(
            """
            INSERT INTO observations (
              id, patient_id, encounter_id, category, code,
              description, status, effective_at, issued_at,
              value_numeric, value_unit, value_text, interpretation
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE
            SET patient_id = EXCLUDED.patient_id,
                encounter_id = EXCLUDED.encounter_id,
                category = EXCLUDED.category,
                code = EXCLUDED.code,
                description = EXCLUDED.description,
                status = EXCLUDED.status,
                effective_at = EXCLUDED.effective_at,
                issued_at = EXCLUDED.issued_at,
                value_numeric = EXCLUDED.value_numeric,
                value_unit = EXCLUDED.value_unit,
                value_text = EXCLUDED.value_text,
                interpretation = EXCLUDED.interpretation
            """,
            row,
        )


def parse_procedure(cursor, resource: dict[str, Any]) -> None:
    patient_id = normalize_reference(resource.get("subject", {}).get("reference"))
    encounter_id = normalize_reference(resource.get("encounter", {}).get("reference"))
    code, description = get_code_details(resource.get("code", {}))
    performed_period = resource.get("performedPeriod", {})
    performed_at = resource.get("performedDateTime")
    performed_start = performed_period.get("start") or performed_at
    performed_end = performed_period.get("end") or performed_at

    cursor.execute(
        """
        INSERT INTO procedures (
          id, patient_id, encounter_id, code, description,
          status, performed_start, performed_end
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET patient_id = EXCLUDED.patient_id,
            encounter_id = EXCLUDED.encounter_id,
            code = EXCLUDED.code,
            description = EXCLUDED.description,
            status = EXCLUDED.status,
            performed_start = EXCLUDED.performed_start,
            performed_end = EXCLUDED.performed_end
        """,
        (
            resource["id"],
            patient_id,
            encounter_id,
            code,
            description,
            resource.get("status"),
            performed_start,
            performed_end,
        ),
    )


def parse_diagnostic_report(cursor, resource: dict[str, Any]) -> None:
    patient_id = normalize_reference(resource.get("subject", {}).get("reference"))
    encounter_id = normalize_reference(resource.get("encounter", {}).get("reference"))
    code, description = get_code_details(resource.get("code", {}))

    cursor.execute(
        """
        INSERT INTO diagnostic_reports (
          id, patient_id, encounter_id, category, code,
          description, status, effective_at, issued_at, report_text
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET patient_id = EXCLUDED.patient_id,
            encounter_id = EXCLUDED.encounter_id,
            category = EXCLUDED.category,
            code = EXCLUDED.code,
            description = EXCLUDED.description,
            status = EXCLUDED.status,
            effective_at = EXCLUDED.effective_at,
            issued_at = EXCLUDED.issued_at,
            report_text = EXCLUDED.report_text
        """,
        (
            resource["id"],
            patient_id,
            encounter_id,
            get_category_label(resource.get("category")),
            code,
            description,
            resource.get("status"),
            resource.get("effectiveDateTime"),
            resource.get("issued"),
            decode_report_text(resource),
        ),
    )


def parse_immunization(cursor, resource: dict[str, Any]) -> None:
    patient_id = normalize_reference(
        resource.get("patient", {}).get("reference")
        or resource.get("subject", {}).get("reference")
    )
    encounter_id = normalize_reference(resource.get("encounter", {}).get("reference"))
    code, description = get_code_details(resource.get("vaccineCode", {}))
    occurrence_at = resource.get("occurrenceDateTime") or resource.get("occurrenceString")

    cursor.execute(
        """
        INSERT INTO immunizations (
          id, patient_id, encounter_id, status, vaccine_code, description, occurrence_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET patient_id = EXCLUDED.patient_id,
            encounter_id = EXCLUDED.encounter_id,
            status = EXCLUDED.status,
            vaccine_code = EXCLUDED.vaccine_code,
            description = EXCLUDED.description,
            occurrence_at = EXCLUDED.occurrence_at
        """,
        (
            resource["id"],
            patient_id,
            encounter_id,
            resource.get("status"),
            code,
            description,
            occurrence_at,
        ),
    )


def parse_care_plan(cursor, resource: dict[str, Any]) -> None:
    patient_id = normalize_reference(resource.get("subject", {}).get("reference"))
    encounter_id = normalize_reference(resource.get("encounter", {}).get("reference"))
    period = resource.get("period", {})

    cursor.execute(
        """
        INSERT INTO care_plans (
          id, patient_id, encounter_id, category, description,
          status, intent, start_date, end_date, activity_summary
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET patient_id = EXCLUDED.patient_id,
            encounter_id = EXCLUDED.encounter_id,
            category = EXCLUDED.category,
            description = EXCLUDED.description,
            status = EXCLUDED.status,
            intent = EXCLUDED.intent,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            activity_summary = EXCLUDED.activity_summary
        """,
        (
            resource["id"],
            patient_id,
            encounter_id,
            get_category_label(resource.get("category")),
            strip_html(resource.get("text", {}).get("div")),
            resource.get("status"),
            resource.get("intent"),
            period.get("start"),
            period.get("end"),
            get_activity_summary(resource),
        ),
    )


def parse_claim(_cursor, resource: dict[str, Any]) -> None:
    claim_total = get_claim_total(resource)
    encounter_ids = get_claim_encounter_ids(resource)

    if claim_total is None or not encounter_ids:
        return

    allocation = claim_total / Decimal(len(encounter_ids))
    for encounter_id in encounter_ids:
        CLAIM_TOTALS_BY_ENCOUNTER[encounter_id] += allocation


PARSERS = {
    "Patient": parse_patient,
    "Encounter": parse_encounter,
    "Condition": parse_condition,
    "MedicationRequest": parse_medication,
    "AllergyIntolerance": parse_allergy,
    "Observation": parse_observation,
    "Procedure": parse_procedure,
    "DiagnosticReport": parse_diagnostic_report,
    "Immunization": parse_immunization,
    "CarePlan": parse_care_plan,
    "Claim": parse_claim,
}


def iter_supported_resources(bundle: dict[str, Any]):
    grouped: dict[str, list[dict[str, Any]]] = {resource_type: [] for resource_type in LOAD_ORDER}
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        resource_type = resource.get("resourceType")
        if resource_type in grouped:
            grouped[resource_type].append(resource)

    for resource_type in LOAD_ORDER:
        for resource in grouped[resource_type]:
            yield resource_type, resource


def load_bundle(cursor, bundle_path: Path, stats: dict[str, Any]) -> None:
    with bundle_path.open() as handle:
        bundle = json.load(handle)

    if bundle.get("resourceType") != "Bundle":
        LOGGER.warning("Skipping non-bundle file: %s", bundle_path)
        return

    for resource_type, resource in iter_supported_resources(bundle):
        stats["seen"][resource_type] += 1
        cursor.execute("SAVEPOINT resource_load")
        try:
            PARSERS[resource_type](cursor, resource)
        except Exception:
            cursor.execute("ROLLBACK TO SAVEPOINT resource_load")
            stats["failed"][resource_type] += 1
            LOGGER.exception(
                "Failed to load %s %s from %s",
                resource_type,
                resource.get("id"),
                bundle_path.name,
            )
        else:
            cursor.execute("RELEASE SAVEPOINT resource_load")
            stats["loaded"][resource_type] += 1


def fetch_table_counts(cursor) -> dict[str, int]:
    counts = {}
    for table_name in COUNTED_TABLES:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        counts[table_name] = cursor.fetchone()[0]
    return counts


def apply_claim_costs(cursor) -> tuple[int, Decimal]:
    cursor.execute("UPDATE encounters SET cost = NULL")

    if not CLAIM_TOTALS_BY_ENCOUNTER:
        return 0, Decimal("0")

    cursor.execute(
        """
        CREATE TEMP TABLE temp_encounter_costs (
          id VARCHAR(64) PRIMARY KEY,
          cost NUMERIC(14, 2)
        ) ON COMMIT DROP
        """
    )
    execute_values(
        cursor,
        "INSERT INTO temp_encounter_costs (id, cost) VALUES %s",
        [
            (encounter_id, cost.quantize(Decimal("0.01")))
            for encounter_id, cost in CLAIM_TOTALS_BY_ENCOUNTER.items()
        ],
        page_size=1000,
    )
    cursor.execute(
        """
        UPDATE encounters AS e
        SET cost = t.cost
        FROM temp_encounter_costs AS t
        WHERE e.id = t.id
        """
    )
    cursor.execute("SELECT COALESCE(SUM(cost), 0) FROM temp_encounter_costs")
    total_cost = cursor.fetchone()[0]
    return len(CLAIM_TOTALS_BY_ENCOUNTER), total_cost


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)
    CLAIM_TOTALS_BY_ENCOUNTER.clear()

    input_dir = Path(args.input_dir)
    files = sorted(glob.glob(str(input_dir / "*.json")))
    if not files:
        LOGGER.error("No JSON files found in %s", input_dir)
        return 1

    LOGGER.info("Parsing %s FHIR bundles from %s", len(files), input_dir)

    stats = {
        "seen": Counter(),
        "loaded": Counter(),
        "failed": Counter(),
        "files_failed": 0,
    }

    try:
        connection = connect_db()
    except OperationalError:
        LOGGER.exception(
            "Database connection failed. Check DB_HOST, DB_PORT, DB_NAME, DB_USER, and DB_PASSWORD."
        )
        return 1

    try:
        with connection.cursor() as cursor:
            for index, file_path in enumerate(files, start=1):
                bundle_path = Path(file_path)
                try:
                    load_bundle(cursor, bundle_path, stats)
                    connection.commit()
                except Exception:
                    connection.rollback()
                    stats["files_failed"] += 1
                    LOGGER.exception("Failed to process bundle %s", bundle_path.name)

                if index % args.log_every == 0 or index == len(files):
                    LOGGER.info("Processed %s/%s bundles", index, len(files))

            updated_encounters, total_claim_cost = apply_claim_costs(cursor)
            table_counts = fetch_table_counts(cursor)
            connection.commit()
    finally:
        connection.close()

    LOGGER.info(
        "Load summary: loaded=%s failed=%s file_failures=%s",
        dict(stats["loaded"]),
        dict(stats["failed"]),
        stats["files_failed"],
    )
    LOGGER.info(
        "Cost summary: updated_encounters=%s total_claim_cost=%s",
        updated_encounters,
        total_claim_cost,
    )
    LOGGER.info("Database row counts: %s", table_counts)
    print(
        f"Done. Run: SELECT COUNT(*) FROM patients; -- current count = {table_counts['patients']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
