# Roadmap

The platform is a strong MVP and interview-ready product demo. This roadmap describes the work needed to move it closer to a production-grade healthcare data product.

## Now

- Keep the stack reproducible locally with Docker, dbt, and seed/generated data.
- Maintain tests for SQL safety, role behavior, provider summary generation, and connection handling.
- Keep all healthcare claims grounded in synthetic data and clearly marked as non-certified.

## Next

- Add CI that runs unit tests and dbt tests on every pull request.
- Add a stronger authentication and authorization model beyond header-based local demo roles.
- Add database migrations instead of relying only on direct schema setup.
- Add structured audit exports for patient access and query activity.
- Add richer MPI review workflows for uncertain patient matches.
- Add API examples for patient search, safe SQL, and AI query calls.

## Later

- Add FHIR server integration patterns for real interoperability testing.
- Add row-level security and policy tests for provider, analyst, and admin personas.
- Add deployment notes for cloud Postgres, managed Grafana, secrets, backups, and monitoring.
- Add synthetic load tests for higher patient volumes.
- Add a de-identification path for analytics-only users.

## Done Criteria For Production Readiness

- All role and masking rules are enforced by policy tests, not only UI/API convention.
- Patient matching has review states, confidence thresholds, and operational handoff paths.
- The AI query layer can only reach approved safe views and logs every query.
- Operational runbooks exist for backups, migrations, data refresh, and incident response.
- A compliance/security review document exists before any real PHI is considered.
