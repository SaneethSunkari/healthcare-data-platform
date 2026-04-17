#!/usr/bin/env python3
import argparse
import csv
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def connect_db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "healthcare_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
    )


def generate_hipaa_report(days: int = 30, output_path: str | None = None) -> pd.DataFrame:
    """Generate a HIPAA audit report and optionally persist it to CSV."""
    since = datetime.now() - timedelta(days=days)
    conn = connect_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_role,
                       action,
                       COUNT(*) AS access_count,
                       COUNT(DISTINCT patient_id) AS unique_patients
                FROM audit_log
                WHERE created_at >= %s
                GROUP BY user_role, action
                ORDER BY access_count DESC, user_role ASC, action ASC
                """,
                (since,),
            )
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
    finally:
        conn.close()

    dataframe = pd.DataFrame(rows, columns=columns)
    print(f"=== HIPAA Audit Report (last {days} days) ===")
    if dataframe.empty:
        print("No audit log entries found for the selected window.")
    else:
        print(dataframe.to_string(index=False))

    if output_path is None:
        output_path = f"hipaa_report_{datetime.now().date()}.csv"

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(output, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"Report saved to {output}.")
    return dataframe


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a HIPAA audit report from audit_log.")
    parser.add_argument("--days", type=int, default=30, help="How many trailing days to include.")
    parser.add_argument("--output", default=None, help="Optional CSV output path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    generate_hipaa_report(days=args.days, output_path=args.output)
