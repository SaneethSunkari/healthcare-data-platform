#!/usr/bin/env python3
import argparse
import logging
import os
from collections import Counter, defaultdict

import pandas as pd
import psycopg2
import recordlinkage
from dotenv import load_dotenv
from psycopg2 import OperationalError

load_dotenv()

LOGGER = logging.getLogger("deduplicator")
CONFIRMED_THRESHOLD = 0.85
REVIEW_THRESHOLD = 0.70


class UnionFind:
    def __init__(self, values: list[str]) -> None:
        self.parent = {value: value for value in values}

    def find(self, value: str) -> str:
        parent = self.parent[value]
        if parent != value:
            self.parent[value] = self.find(parent)
        return self.parent[value]

    def union(self, left: str, right: str) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return
        if left_root < right_root:
            self.parent[right_root] = left_root
        else:
            self.parent[left_root] = right_root


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Link duplicate patients and assign golden IDs.",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="Logging level (DEBUG, INFO, WARNING, ERROR).",
    )
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    if level.upper() != "DEBUG":
        logging.getLogger("recordlinkage").setLevel(logging.WARNING)


def connect_db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "healthcare_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
    )


def fetch_patients(connection) -> pd.DataFrame:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT id, first_name, last_name, birth_date, gender, zip_code
            FROM patients
            ORDER BY id
            """
        )
        rows = cursor.fetchall()
        columns = [description[0] for description in cursor.description]
    return pd.DataFrame(rows, columns=columns)


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    text_columns = ("first_name", "last_name", "gender", "zip_code")
    for column in text_columns:
        normalized[column] = (
            normalized[column]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
        )

    normalized["birth_date"] = pd.to_datetime(normalized["birth_date"], errors="coerce")
    normalized = normalized.dropna(subset=["birth_date"])
    normalized["birth_year"] = normalized["birth_date"].dt.year
    normalized.set_index("id", inplace=True)
    return normalized


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    indexer = recordlinkage.Index()
    indexer.block(["birth_year", "gender"])
    candidate_pairs = indexer.index(df)
    LOGGER.info("Comparing %s candidate pairs", len(candidate_pairs))

    compare = recordlinkage.Compare()
    compare.string("first_name", "first_name", method="jarowinkler", label="first_name")
    compare.string("last_name", "last_name", method="jarowinkler", label="last_name")
    compare.exact("birth_date", "birth_date", label="birth_date")
    compare.exact("zip_code", "zip_code", label="zip_code")

    features = compare.compute(candidate_pairs, df).fillna(0.0)
    features["score"] = (
        features[["first_name", "last_name", "birth_date", "zip_code"]].sum(axis=1) / 4.0
    )
    return features


def derive_match_assignments(features: pd.DataFrame, patient_ids: list[str]):
    assignments = {
        patient_id: {
            "golden_id": patient_id,
            "match_confidence": None,
            "match_status": "unique record",
        }
        for patient_id in patient_ids
    }

    union_find = UnionFind(patient_ids)
    best_confirmed: defaultdict[str, float] = defaultdict(float)
    best_review: defaultdict[str, float] = defaultdict(float)
    review_pairs: list[tuple[str, str, float]] = []
    stats = Counter()

    confirmed = features[features["score"] >= CONFIRMED_THRESHOLD]
    review = features[(features["score"] >= REVIEW_THRESHOLD) & (features["score"] < CONFIRMED_THRESHOLD)]

    for (left_id, right_id), row in confirmed.iterrows():
        score = round(float(row["score"]), 3)
        union_find.union(left_id, right_id)
        best_confirmed[left_id] = max(best_confirmed[left_id], score)
        best_confirmed[right_id] = max(best_confirmed[right_id], score)

    groups: defaultdict[str, list[str]] = defaultdict(list)
    for patient_id in patient_ids:
        groups[union_find.find(patient_id)].append(patient_id)

    for root_id, members in groups.items():
        canonical_id = min(members)
        if len(members) > 1:
            stats["confirmed_groups"] += 1
        for patient_id in members:
            assignments[patient_id]["golden_id"] = canonical_id
            if patient_id in best_confirmed:
                assignments[patient_id]["match_confidence"] = best_confirmed[patient_id]
                assignments[patient_id]["match_status"] = "confirmed match"
                stats["confirmed_patients"] += 1

    for (left_id, right_id), row in review.iterrows():
        if assignments[left_id]["match_status"] == "confirmed match":
            continue
        if assignments[right_id]["match_status"] == "confirmed match":
            continue

        score = round(float(row["score"]), 3)
        best_review[left_id] = max(best_review[left_id], score)
        best_review[right_id] = max(best_review[right_id], score)
        pair = tuple(sorted((left_id, right_id)))
        review_pairs.append((pair[0], pair[1], score))

    for patient_id, score in best_review.items():
        assignments[patient_id]["match_confidence"] = score
        assignments[patient_id]["match_status"] = "review needed"
        stats["review_patients"] += 1

    stats["confirmed_pairs"] = len(confirmed)
    stats["review_pairs"] = len(review_pairs)
    return assignments, stats, review_pairs


def apply_assignments(
    connection,
    assignments: dict[str, dict[str, object]],
    review_pairs: list[tuple[str, str, float]],
) -> None:
    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE patient_match_candidates")
        cursor.execute(
            """
            UPDATE patients
            SET golden_id = id,
                match_confidence = NULL,
                match_status = 'unique record'
            """
        )

        for patient_id, values in assignments.items():
            cursor.execute(
                """
                UPDATE patients
                SET golden_id = %s,
                    match_confidence = %s,
                    match_status = %s
                WHERE id = %s
                """,
                (
                    values["golden_id"],
                    values["match_confidence"],
                    values["match_status"],
                    patient_id,
                ),
            )

        if review_pairs:
            cursor.executemany(
                """
                INSERT INTO patient_match_candidates (
                    left_patient_id,
                    right_patient_id,
                    match_score,
                    review_status
                )
                VALUES (%s, %s, %s, 'review needed')
                ON CONFLICT (left_patient_id, right_patient_id)
                DO UPDATE SET
                    match_score = EXCLUDED.match_score,
                    review_status = EXCLUDED.review_status,
                    created_at = NOW()
                """,
                review_pairs,
            )

    connection.commit()


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)

    try:
        connection = connect_db()
    except OperationalError:
        LOGGER.exception(
            "Database connection failed. Check DB_HOST, DB_PORT, DB_NAME, DB_USER, and DB_PASSWORD."
        )
        return 1

    try:
        patients = fetch_patients(connection)
        LOGGER.info("Loaded %s patients", len(patients))
        if patients.empty:
            LOGGER.warning("No patients found. Nothing to deduplicate.")
            return 0

        prepared = prepare_dataframe(patients)
        LOGGER.info("Prepared %s patients for matching", len(prepared))
        features = build_features(prepared)
        assignments, stats, review_pairs = derive_match_assignments(features, prepared.index.tolist())
        apply_assignments(connection, assignments, review_pairs)
    finally:
        connection.close()

    LOGGER.info(
        "Deduplication summary: confirmed_pairs=%s review_pairs=%s confirmed_groups=%s confirmed_patients=%s review_patients=%s",
        stats["confirmed_pairs"],
        stats["review_pairs"],
        stats["confirmed_groups"],
        stats["confirmed_patients"],
        stats["review_patients"],
    )
    print("Done. Duplicates linked via golden_id, match_confidence, and match_status.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
