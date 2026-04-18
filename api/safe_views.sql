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

CREATE OR REPLACE VIEW risk_analytics AS
WITH patient_base AS (
  SELECT
    golden_id,
    MAX(DATE_PART('year', AGE(birth_date))) AS age,
    MAX(gender) AS gender
  FROM patients
  WHERE golden_id IS NOT NULL
  GROUP BY golden_id
),
recent_encounter_rollup AS (
  SELECT
    p.golden_id,
    COUNT(DISTINCT e.id) FILTER (
      WHERE e.start_date >= NOW() - INTERVAL '12 months'
    ) AS encounter_count_12m,
    COALESCE(
      SUM(e.cost) FILTER (
        WHERE e.start_date >= NOW() - INTERVAL '12 months'
      ),
      0
    )::NUMERIC(14, 2) AS total_cost_12m,
    MAX(e.start_date) FILTER (
      WHERE e.start_date >= NOW() - INTERVAL '12 months'
    ) AS last_encounter_12m
  FROM patients p
  LEFT JOIN encounters e
    ON e.patient_id = p.id
  WHERE p.golden_id IS NOT NULL
  GROUP BY p.golden_id
),
acute_rollup AS (
  SELECT
    p.golden_id,
    COUNT(DISTINCT e.id) FILTER (
      WHERE LOWER(COALESCE(e.encounter_type, '')) ~ '(emergency|urgent|hospital admission|admission to|inpatient|surgical department|encounter for problem|encounter for symptom)'
        AND e.start_date >= NOW() - INTERVAL '30 days'
    ) AS acute_visits_30d,
    COUNT(DISTINCT e.id) FILTER (
      WHERE LOWER(COALESCE(e.encounter_type, '')) ~ '(emergency|urgent|hospital admission|admission to|inpatient|surgical department|encounter for problem|encounter for symptom)'
        AND e.start_date >= NOW() - INTERVAL '90 days'
    ) AS acute_visits_90d,
    COUNT(DISTINCT e.id) FILTER (
      WHERE LOWER(COALESCE(e.encounter_type, '')) ~ 'emergency'
        AND e.start_date >= NOW() - INTERVAL '90 days'
    ) AS ed_visits_90d,
    COUNT(DISTINCT e.id) FILTER (
      WHERE LOWER(COALESCE(e.encounter_type, '')) ~ 'urgent'
        AND e.start_date >= NOW() - INTERVAL '90 days'
    ) AS urgent_visits_90d,
    COUNT(DISTINCT e.id) FILTER (
      WHERE LOWER(COALESCE(e.encounter_type, '')) ~ '(hospital admission|admission to|inpatient|surgical department)'
        AND e.start_date >= NOW() - INTERVAL '365 days'
    ) AS admissions_365d,
    COALESCE(
      SUM(e.cost) FILTER (
        WHERE LOWER(COALESCE(e.encounter_type, '')) ~ '(emergency|urgent|hospital admission|admission to|inpatient|surgical department|encounter for problem|encounter for symptom)'
          AND e.start_date >= NOW() - INTERVAL '90 days'
      ),
      0
    )::NUMERIC(14, 2) AS acute_cost_90d,
    MAX(e.start_date) FILTER (
      WHERE LOWER(COALESCE(e.encounter_type, '')) ~ '(emergency|urgent|hospital admission|admission to|inpatient|surgical department|encounter for problem|encounter for symptom)'
    ) AS last_acute_visit
  FROM patients p
  LEFT JOIN encounters e
    ON e.patient_id = p.id
  WHERE p.golden_id IS NOT NULL
  GROUP BY p.golden_id
)
SELECT
  b.golden_id,
  b.age,
  b.gender,
  COALESCE(r.encounter_count_12m, 0) AS encounter_count_12m,
  COALESCE(r.total_cost_12m, 0)::NUMERIC(14, 2) AS total_cost_12m,
  r.last_encounter_12m,
  CASE
    WHEN COALESCE(r.encounter_count_12m, 0) >= 4 THEN 'high'
    WHEN COALESCE(r.encounter_count_12m, 0) >= 2 THEN 'medium'
    ELSE 'low'
  END AS readmission_risk,
  COALESCE(a.acute_visits_30d, 0) AS acute_visits_30d,
  COALESCE(a.acute_visits_90d, 0) AS acute_visits_90d,
  COALESCE(a.ed_visits_90d, 0) AS ed_visits_90d,
  COALESCE(a.urgent_visits_90d, 0) AS urgent_visits_90d,
  COALESCE(a.admissions_365d, 0) AS admissions_365d,
  COALESCE(a.acute_cost_90d, 0)::NUMERIC(14, 2) AS acute_cost_90d,
  a.last_acute_visit
FROM patient_base b
LEFT JOIN recent_encounter_rollup r
  ON r.golden_id = b.golden_id
LEFT JOIN acute_rollup a
  ON a.golden_id = b.golden_id;
