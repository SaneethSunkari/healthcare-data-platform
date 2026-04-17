-- Anonymised views for AI to query safely
-- No raw PII exposed to the query engine

CREATE OR REPLACE VIEW patient_analytics AS
SELECT
  golden_id,
  DATE_PART('year', AGE(birth_date)) AS age,
  gender,
  zip_code,
  source_system
FROM patients;

CREATE OR REPLACE VIEW encounter_analytics AS
SELECT
  e.id,
  p.golden_id,
  DATE_PART('year', AGE(p.birth_date)) AS patient_age,
  p.gender,
  e.start_date,
  e.encounter_type,
  e.cost
FROM encounters e
JOIN patients p ON e.patient_id = p.id;

CREATE OR REPLACE VIEW condition_analytics AS
SELECT
  c.id,
  p.golden_id,
  DATE_PART('year', AGE(p.birth_date)) AS patient_age,
  p.gender,
  c.code,
  c.description,
  c.onset_date
FROM conditions c
JOIN patients p ON c.patient_id = p.id;

CREATE OR REPLACE VIEW medication_analytics AS
SELECT
  m.id,
  p.golden_id,
  DATE_PART('year', AGE(p.birth_date)) AS patient_age,
  p.gender,
  m.code,
  m.description,
  m.start_date,
  m.stop_date
FROM medications m
JOIN patients p ON m.patient_id = p.id;
