# healthcare-data-platform

Starter workspace for a healthcare data platform built around realistic synthetic FHIR data from Synthea.

## Project layout

```text
healthcare-data-platform/
├── ingestion/          # FHIR parser + ETL
├── matching/           # Patient deduplication
├── compliance/         # HIPAA masking
├── api/                # FastAPI backend (from Project 1)
├── analytics/          # dbt models
├── dashboard/          # Grafana config
├── synthea/            # Synthetic patient generator + local output
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Prerequisites

- Python 3.11+
- Java 17+
- Git

`synthea-with-dependencies.jar` from the latest Synthea release now requires Java 17+. The current release used here is `v4.0.0`, published on March 5, 2026.

## Quick start

```bash
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

./ingestion/generate_synthea_data.sh
```

By default the generator targets 1,000 living synthetic patients and writes FHIR JSON to `synthea/output/fhir/`. Synthea may also include deceased patient histories, so the final patient bundle count can be higher than 1,000.

To generate a different number of patients:

```bash
./ingestion/generate_synthea_data.sh 250
```

## Next steps

- Build a FHIR parser and ETL flow in `ingestion/`
- Bring the Project 1 FastAPI service into `api/`
- Add matching logic in `matching/` and HIPAA-safe transforms in `compliance/`
- Model warehouse tables in `analytics/` and expose metrics in Grafana
