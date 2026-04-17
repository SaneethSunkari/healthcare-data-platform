# healthcare-data-platform

Starter workspace for a healthcare data platform built around realistic synthetic FHIR data from Synthea.

## Project layout

```text
healthcare-data-platform/
├── ingestion/          # FHIR parser + ETL
├── matching/           # Patient deduplication
├── compliance/         # HIPAA masking
├── api/                # Project 1 FastAPI backend, adapted for healthcare
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

## Database setup

Start PostgreSQL and Grafana with Docker:

```bash
docker compose up -d
```

PostgreSQL connection details:

- Host: `127.0.0.1`
- Port: `15432`
- Database: `healthcare_db`
- User: `postgres`
- Password: `postgres`

The schema is defined in `schema.sql` and is loaded automatically the first time the Postgres container starts with an empty data volume. The repo’s local `.env` uses port `15432` to avoid conflicts with other PostgreSQL instances on the machine.

## ETL loader

Install the ingestion dependencies in a virtual environment and run the FHIR loader:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install psycopg2-binary==2.9.9 python-dotenv==1.0.0

python ingestion/fhir_parser.py --input-dir synthea/output/fhir
```

Database connection settings can be overridden with a local `.env` file based on `.env.example`.

Run the patient matching step with:

```bash
python matching/deduplicator.py
```

## Compliance

Install Presidio dependencies and the spaCy English model in the virtualenv:

```bash
pip install presidio-analyzer presidio-anonymizer spacy
python -m spacy download en_core_web_lg
```

Run the masking test:

```bash
python compliance/pii_masker.py
```

Run the API locally:

```bash
uvicorn api.main:app --reload
```

## Healthcare AI middleware

The API layer is fully implemented inside this repo and includes:

- the copied Project 1 backend structure under `api/app/`
- a default middleware connection to `healthcare_db`
- anonymised analytics views in `api/safe_views.sql`
- a healthcare-aware system prompt in `api/healthcare_prompt.py`
- natural-language clinical querying via `POST /query/ask`
- response masking for non-doctor roles on AI query endpoints

Set `OPENAI_API_KEY` in your local `.env` before using the LLM-backed endpoints.

For a production-style API deployment, also set:

- `APP_ENV=production`
- `APP_API_KEY=<strong shared secret>`
- `REQUIRE_API_KEY=true`
- `CORS_ALLOW_ORIGINS` to your approved frontend origins
- `ALLOWED_HOSTS` to your deployed hostnames

The API now adds request IDs, security headers, trusted-host checks, read-only query timeouts, and row caps. Provider chart access is restricted to `admin`, `doctor`, and `provider` roles.

Example test queries are listed at `GET /query/test-queries`.

## Population health dashboard

Build the analytics layer with:

```bash
./.venv/bin/dbt run --project-dir analytics --profiles-dir analytics
./.venv/bin/dbt test --project-dir analytics --profiles-dir analytics
```

Grafana is provisioned automatically from the repo. Open:

- `http://localhost:3000`
- Username: `admin`
- Password: `admin`

The prebuilt dashboard is available at:

- `http://localhost:3000/d/population-health-overview/population-health-overview`

## Tests

Run the API safety tests with:

```bash
pytest tests
```

## Next steps

- Expand dbt metrics, alerts, and board-ready dashboard panels as new KPIs are added
