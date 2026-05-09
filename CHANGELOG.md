# Changelog

This changelog tracks meaningful platform milestones and portfolio hardening work.

## Current

- Added documentation for iteration history, roadmap, and validation boundaries.
- Clarified that the platform is an MVP on synthetic data, not a certified production hospital system.
- Added stronger links between product scenarios, tests, security controls, and production-readiness work.

## v0.4 - Provider Workspace

- Added patient search and patient 360 views for longitudinal clinical review.
- Added provider handoff summaries grounded in structured warehouse facts.
- Added medication, allergy, lab, condition, encounter, and utilization context to the provider workflow.
- Added dashboard screenshots to make the product surface reviewable from GitHub.

## v0.3 - Governed API And AI Query Layer

- Added FastAPI endpoints for query, schema, patient, and tool-style access.
- Added read-only SQL validation and safe views.
- Added header-based role controls, masking behavior, and audit-oriented query logging.
- Added tests around SQL validation and security middleware.

## v0.2 - Analytics Layer

- Added dbt models for patient summary, condition prevalence, and readmission-risk style analytics.
- Added Grafana provisioning for population health and patient 360 dashboards.
- Added local Docker orchestration for PostgreSQL and Grafana.

## v0.1 - Ingestion And Patient Matching

- Added Synthea/FHIR ingestion into a PostgreSQL clinical warehouse.
- Added patient matching and golden-record logic for duplicate identity handling.
- Added compliance utilities for masking and reporting.
