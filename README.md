# Healthcare Data Platform

> A HIPAA-aware healthcare data platform that unifies patient records, supports safer provider workflows, and enables AI-powered analytics over a clean clinical warehouse.

This project brings together synthetic FHIR data generation, ingestion, patient identity matching, compliance controls, provider-facing dashboards, and an AI query layer in one repo.

It is best described today as a strong end-to-end healthcare data platform MVP and provider demo, not a fully production-certified hospital system.

## Why This Matters

Healthcare data is often fragmented across hospitals, clinics, specialists, labs, and insurers. That creates real operational and clinical risk:

- doctors may not see the full medication or allergy history
- duplicate patient records can delay claims and care
- care managers spend too much time assembling a patient story from disconnected systems
- compliance teams still need auditability and role-aware masking when analytics or AI are involved

This project is designed to address those problems with a unified patient record and a governed analytics layer.

## Three Real-World Scenarios

### 1. Emergency Care

A patient arrives in the emergency room after receiving care at multiple health systems. The treating team needs a single view of recent encounters, active medications, allergies, and risk signals.

How this platform helps:

- patient records are unified under a `golden_id`
- provider dashboards show an emergency snapshot
- medication safety and allergy context are surfaced quickly
- break-glass access is audited

### 2. Identity Matching For Claims And Operations

The same patient appears in multiple systems with slightly different names or demographics. That leads to duplicate records, delayed claims, and operational confusion.

How this platform helps:

- probabilistic matching links records under a shared `golden_id`
- `match_confidence` and `match_status` support confirmed matches vs review-needed cases
- an MPI review queue surfaces uncertain pairs for manual follow-up

### 3. Chronic Care Coordination

A patient sees multiple providers across different organizations. The care team needs one place to review utilization, medications, labs, allergies, conditions, and follow-up needs.

How this platform helps:

- provider dashboards summarize active problems, recent acute care, and care gaps
- medication reconciliation and safety alerts help identify risk
- population health dashboards highlight high-risk cohorts for intervention

## What The Pipeline Does

```text
Synthea FHIR Bundles
        |
        v
FHIR Parser + ETL
        |
        v
PostgreSQL Clinical Warehouse
        |
        +--> Patient Matching / golden_id
        |
        +--> HIPAA masking + audit logging
        |
        +--> dbt analytics models
        |
        +--> FastAPI query layer
        |
        +--> Grafana dashboards
```

This means the platform is useful at multiple levels:

- data engineering: load and normalize raw FHIR bundles
- master data management: link duplicate patient identities
- compliance: mask sensitive data and record access
- provider workflow: surface a usable patient summary
- analytics: support population health and operational dashboards
- AI enablement: let approved users ask questions in plain English over safe views

## Current Capabilities

### Data Generation And Ingestion

- generates realistic synthetic patients with Synthea
- loads FHIR R4 JSON bundles into PostgreSQL
- supports patients, encounters, conditions, medications, observations, allergies, procedures, diagnostic reports, immunizations, and care plans

### Unified Patient Identity

- deduplicates likely duplicate patients with `recordlinkage`
- assigns a `golden_id`
- stores match confidence and review status
- exposes an MPI review queue for uncertain matches

### HIPAA And Access Controls

- Presidio-based masking for non-provider roles
- role-aware access for provider vs analyst workflows
- audit logging for patient and query access
- optional API key enforcement for production-style deployments
- request IDs, security headers, trusted-host checks, row caps, and query timeouts

### Provider Workflow

- patient search from the main dashboard
- provider summary dashboard
- emergency snapshot
- acute care / admission visibility
- care gaps and follow-up priorities
- allergy and medication safety views
- patient 360 drill-down dashboards

### Analytics And Reporting

- dbt models for patient summary, condition prevalence, and readmission risk
- Grafana population health dashboard
- risk-based patient drill-down
- audit reporting support

### AI Query Layer

- FastAPI middleware adapted from Project 1
- healthcare-aware system prompt
- safe analytics views for query generation
- natural language to SQL over read-only views
- response masking for non-provider users

## Tech Stack

| Area | Technology |
|---|---|
| Language | Python 3.11+ |
| API | FastAPI |
| Database | PostgreSQL 16 |
| Standard | HL7 FHIR R4 |
| Synthetic data | Synthea |
| Matching | recordlinkage, pandas |
| Compliance | Microsoft Presidio, spaCy |
| Analytics | dbt |
| Dashboards | Grafana |
| AI | OpenAI API |
| Runtime | Docker-compatible container runtime |

## Project Structure

```text
healthcare-data-platform/
├── ingestion/
│   ├── fhir_parser.py
│   └── generate_synthea_data.sh
├── matching/
│   └── deduplicator.py
├── compliance/
│   ├── pii_masker.py
│   └── generate_report.py
├── api/
│   ├── app/
│   ├── healthcare_prompt.py
│   ├── safe_views.sql
│   └── main.py
├── analytics/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
├── dashboard/
│   └── provisioning/
├── synthea/
├── tests/
├── docker-compose.yml
├── schema.sql
├── requirements.txt
└── README.md
```

## Quick Start

### Prerequisites

- Python 3.11+
- Java 17+
- Git
- Docker Desktop, Colima, or another Docker-compatible runtime

### 1. Clone And Install

```bash
git clone https://github.com/SaneethSunkari/healthcare-data-platform.git
cd healthcare-data-platform

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# then set OPENAI_API_KEY in .env
```

### 2. Generate Synthetic FHIR Data

```bash
./ingestion/generate_synthea_data.sh
```

To generate a custom patient volume:

```bash
./ingestion/generate_synthea_data.sh 500
```

FHIR output is written to `synthea/output/fhir/`.

### 3. Start PostgreSQL And Grafana

```bash
docker compose up -d
```

Local services:

| Service | URL / Address | Credentials |
|---|---|---|
| PostgreSQL | `127.0.0.1:15432` | `postgres / postgres` |
| Grafana | [http://localhost:3000](http://localhost:3000) | `admin / admin` |
| API | [http://127.0.0.1:8000](http://127.0.0.1:8000) | header-based role access |
| API docs | [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs) | same API |

### 4. Load The Warehouse And Build Analytics

```bash
python ingestion/fhir_parser.py --input-dir synthea/output/fhir
python matching/deduplicator.py
dbt run --project-dir analytics --profiles-dir analytics
dbt test --project-dir analytics --profiles-dir analytics
```

### 5. Run The API

```bash
uvicorn api.main:app --reload
```

### 6. Open The UI

- API UI: [http://127.0.0.1:8000/ui](http://127.0.0.1:8000/ui)
- Population dashboard: [http://localhost:3000/d/population-health-overview/population-health-overview](http://localhost:3000/d/population-health-overview/population-health-overview)

## Example Questions To Ask

Clinical and operational questions you can try:

- `How many unique patients do we have?`
- `What are the top 5 most common conditions?`
- `Which medications are prescribed most often?`
- `What is the average encounter cost by encounter type?`
- `How many patients were seen more than 3 times?`

Provider-style workflow examples:

- search for a `golden_id` from the main dashboard
- open the patient summary and review acute visits in the last 90 days
- check medication safety alerts and allergy details before reviewing treatment history
- use care gaps to prioritize follow-up outreach

## API Overview

### Query Endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/query/ask` | Natural language to SQL over safe analytics views |
| `POST` | `/query/run` | Execute validated read-only SQL |
| `GET` | `/query/test-queries` | Example clinical questions |

### Patient Endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| `GET` | `/patients/search` | Search unified patients |
| `GET` | `/patients/chart/{golden_id}` | Provider patient chart |
| `GET` | `/patients/{patient_id}` | Role-aware patient record access |

### Schema And Tool Endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/schema/scan` | Inspect safe analytics schema |
| `GET` | `/tools/manifest` | Function-style tool manifest |
| `POST` | `/tools/invoke` | Invoke tool endpoints programmatically |

## Data Model Highlights

| Table | Description |
|---|---|
| `patients` | source patient identities plus `golden_id`, match confidence, and match status |
| `encounters` | care events with dates, type, provider, and cost |
| `conditions` | coded clinical conditions tied to patients and encounters |
| `medications` | medication records plus status and reconciliation detail |
| `observations` | labs and vitals |
| `allergies` | allergy records including reaction detail when available |
| `procedures` | procedures and interventions |
| `diagnostic_reports` | narrative and coded reports |
| `immunizations` | vaccination history |
| `care_plans` | care management plans |
| `audit_log` | access and query audit trail |
| `patient_match_candidates` | MPI review queue for uncertain duplicate candidates |

`golden_id` is the patient key you should use when counting unique patients across source systems.

## Security And Compliance Notes

Implemented today:

- role-aware masking
- audit logging
- read-only SQL validation
- request IDs
- security headers
- trusted-host checks
- query timeouts and row caps
- optional shared-secret API protection for production-style deployments

Important honesty note:

This repo demonstrates HIPAA-aware controls and safer access patterns, but it is still a portfolio-quality MVP built on synthetic data. It is not a certified production hospital platform, and it is not a substitute for enterprise IAM, formal compliance programs, validated clinical decision support, or live EHR interoperability.

## Tests

Run the current automated checks with:

```bash
pytest tests
```

Current test coverage includes:

- SQL safety validation
- security middleware behavior
- connection resolution defaults

## Environment Variables

Key settings from `.env.example`:

| Variable | Purpose |
|---|---|
| `OPENAI_API_KEY` | OpenAI key for AI-backed query endpoints |
| `OPENAI_MODEL` | model used by the NL-to-SQL layer |
| `DB_HOST` / `DB_PORT` / `DB_NAME` / `DB_USER` / `DB_PASSWORD` | PostgreSQL connection settings |
| `APP_ENV` | environment name such as `development` or `production` |
| `APP_API_KEY` | shared secret for protected API access |
| `REQUIRE_API_KEY` | enforce API key usage for protected routes |
| `CORS_ALLOW_ORIGINS` | allowed browser origins |
| `ALLOWED_HOSTS` | trusted hostnames |
| `QUERY_TIMEOUT_MS` | database statement timeout |
| `MAX_QUERY_ROWS` | maximum rows returned by query endpoints |

## What Would Move This Closer To Production

The biggest next steps would be:

- live EHR / payer / lab integrations
- enterprise identity and SSO
- stronger database permissions and append-only audit enforcement
- clinically validated drug interaction logic
- infrastructure monitoring and alerting
- deployment automation and environment promotion
- broader automated test coverage for ETL, matching, and masking correctness

## Author

Built by **Saneeth Sunkari**.

- LinkedIn: [https://www.linkedin.com/in/saneeth-sunkari-329391313](https://www.linkedin.com/in/saneeth-sunkari-329391313)
- Project 1: [https://github.com/SaneethSunkari/Ai-Business-Analyst](https://github.com/SaneethSunkari/Ai-Business-Analyst)

## Suggested GitHub Metadata

Repo description:

`HIPAA-aware healthcare data platform that unifies patient records across systems and enables AI-powered clinical analytics.`

Topics:

`healthcare`, `hipaa`, `fhir`, `data-engineering`, `fastapi`, `python`, `dbt`, `grafana`, `patient-matching`, `healthcare-analytics`
