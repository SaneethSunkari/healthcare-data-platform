CREATE TABLE IF NOT EXISTS patients (
  id            VARCHAR(64) PRIMARY KEY,
  golden_id     VARCHAR(64),
  match_confidence NUMERIC(4,3),
  match_status  VARCHAR(32),
  first_name    VARCHAR(100),
  last_name     VARCHAR(100),
  birth_date    DATE,
  gender        VARCHAR(10),
  zip_code      VARCHAR(10),
  source_system VARCHAR(50),
  created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS encounters (
  id             VARCHAR(64) PRIMARY KEY,
  patient_id     VARCHAR(64) REFERENCES patients(id),
  start_date     TIMESTAMP,
  end_date       TIMESTAMP,
  encounter_type VARCHAR(100),
  provider       VARCHAR(200),
  cost           DECIMAL(10,2),
  created_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS conditions (
  id            VARCHAR(64) PRIMARY KEY,
  patient_id    VARCHAR(64) REFERENCES patients(id),
  encounter_id  VARCHAR(64) REFERENCES encounters(id),
  code          VARCHAR(20),
  description   TEXT,
  onset_date    DATE,
  created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS medications (
  id            VARCHAR(64) PRIMARY KEY,
  patient_id    VARCHAR(64) REFERENCES patients(id),
  encounter_id  VARCHAR(64) REFERENCES encounters(id),
  code          VARCHAR(20),
  description   TEXT,
  status        VARCHAR(32),
  category      VARCHAR(64),
  dose_details  TEXT,
  frequency     VARCHAR(100),
  route         VARCHAR(100),
  prescriber    VARCHAR(200),
  as_needed     BOOLEAN,
  start_date    DATE,
  stop_date     DATE,
  cost          DECIMAL(10,2),
  created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS observations (
  id              VARCHAR(128) PRIMARY KEY,
  patient_id      VARCHAR(64) REFERENCES patients(id),
  encounter_id    VARCHAR(64) REFERENCES encounters(id),
  category        VARCHAR(100),
  code            VARCHAR(32),
  description     TEXT,
  status          VARCHAR(32),
  effective_at    TIMESTAMP,
  issued_at       TIMESTAMP,
  value_numeric   DECIMAL(14,4),
  value_unit      VARCHAR(32),
  value_text      TEXT,
  interpretation  VARCHAR(64),
  created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS allergies (
  id                  VARCHAR(64) PRIMARY KEY,
  patient_id          VARCHAR(64) REFERENCES patients(id),
  category            VARCHAR(64),
  code                VARCHAR(32),
  description         TEXT,
  clinical_status     VARCHAR(32),
  verification_status VARCHAR(32),
  criticality         VARCHAR(32),
  reaction_description TEXT,
  reaction_severity   VARCHAR(32),
  recorded_date       DATE,
  created_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS procedures (
  id              VARCHAR(64) PRIMARY KEY,
  patient_id      VARCHAR(64) REFERENCES patients(id),
  encounter_id    VARCHAR(64) REFERENCES encounters(id),
  code            VARCHAR(32),
  description     TEXT,
  status          VARCHAR(32),
  performed_start TIMESTAMP,
  performed_end   TIMESTAMP,
  created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS diagnostic_reports (
  id              VARCHAR(64) PRIMARY KEY,
  patient_id      VARCHAR(64) REFERENCES patients(id),
  encounter_id    VARCHAR(64) REFERENCES encounters(id),
  category        VARCHAR(100),
  code            VARCHAR(32),
  description     TEXT,
  status          VARCHAR(32),
  effective_at    TIMESTAMP,
  issued_at       TIMESTAMP,
  report_text     TEXT,
  created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS immunizations (
  id              VARCHAR(64) PRIMARY KEY,
  patient_id      VARCHAR(64) REFERENCES patients(id),
  encounter_id    VARCHAR(64) REFERENCES encounters(id),
  status          VARCHAR(32),
  vaccine_code    VARCHAR(32),
  description     TEXT,
  occurrence_at   TIMESTAMP,
  created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS care_plans (
  id                VARCHAR(64) PRIMARY KEY,
  patient_id        VARCHAR(64) REFERENCES patients(id),
  encounter_id      VARCHAR(64) REFERENCES encounters(id),
  category          VARCHAR(100),
  description       TEXT,
  status            VARCHAR(32),
  intent            VARCHAR(32),
  start_date        TIMESTAMP,
  end_date          TIMESTAMP,
  activity_summary  TEXT,
  created_at        TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_log (
  id            SERIAL PRIMARY KEY,
  user_role     VARCHAR(50),
  action        VARCHAR(100),
  patient_id    VARCHAR(64),
  query_text    TEXT,
  ip_address    VARCHAR(45),
  created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS patient_match_candidates (
  left_patient_id   VARCHAR(64) REFERENCES patients(id),
  right_patient_id  VARCHAR(64) REFERENCES patients(id),
  match_score       NUMERIC(4,3) NOT NULL,
  review_status     VARCHAR(32) DEFAULT 'review needed',
  created_at        TIMESTAMP DEFAULT NOW(),
  PRIMARY KEY (left_patient_id, right_patient_id)
);

ALTER TABLE patients
  ADD COLUMN IF NOT EXISTS match_confidence NUMERIC(4,3);

ALTER TABLE patients
  ADD COLUMN IF NOT EXISTS match_status VARCHAR(32);

ALTER TABLE encounters
  ADD COLUMN IF NOT EXISTS provider VARCHAR(200);

ALTER TABLE medications
  ADD COLUMN IF NOT EXISTS status VARCHAR(32);

ALTER TABLE medications
  ADD COLUMN IF NOT EXISTS category VARCHAR(64);

ALTER TABLE medications
  ADD COLUMN IF NOT EXISTS dose_details TEXT;

ALTER TABLE medications
  ADD COLUMN IF NOT EXISTS frequency VARCHAR(100);

ALTER TABLE medications
  ADD COLUMN IF NOT EXISTS route VARCHAR(100);

ALTER TABLE medications
  ADD COLUMN IF NOT EXISTS prescriber VARCHAR(200);

ALTER TABLE medications
  ADD COLUMN IF NOT EXISTS as_needed BOOLEAN;

ALTER TABLE allergies
  ADD COLUMN IF NOT EXISTS reaction_description TEXT;

ALTER TABLE allergies
  ADD COLUMN IF NOT EXISTS reaction_severity VARCHAR(32);

CREATE INDEX IF NOT EXISTS idx_patients_golden_id
  ON patients(golden_id);

CREATE INDEX IF NOT EXISTS idx_patients_source_system
  ON patients(source_system);

CREATE INDEX IF NOT EXISTS idx_patients_match_status
  ON patients(match_status);

CREATE INDEX IF NOT EXISTS idx_encounters_patient_id
  ON encounters(patient_id);

CREATE INDEX IF NOT EXISTS idx_encounters_start_date
  ON encounters(start_date);

CREATE INDEX IF NOT EXISTS idx_conditions_patient_id
  ON conditions(patient_id);

CREATE INDEX IF NOT EXISTS idx_conditions_encounter_id
  ON conditions(encounter_id);

CREATE INDEX IF NOT EXISTS idx_medications_patient_id
  ON medications(patient_id);

CREATE INDEX IF NOT EXISTS idx_medications_encounter_id
  ON medications(encounter_id);

CREATE INDEX IF NOT EXISTS idx_medications_status
  ON medications(status);

CREATE INDEX IF NOT EXISTS idx_observations_patient_id
  ON observations(patient_id);

CREATE INDEX IF NOT EXISTS idx_observations_encounter_id
  ON observations(encounter_id);

CREATE INDEX IF NOT EXISTS idx_observations_effective_at
  ON observations(effective_at);

CREATE INDEX IF NOT EXISTS idx_observations_code
  ON observations(code);

CREATE INDEX IF NOT EXISTS idx_allergies_patient_id
  ON allergies(patient_id);

CREATE INDEX IF NOT EXISTS idx_procedures_patient_id
  ON procedures(patient_id);

CREATE INDEX IF NOT EXISTS idx_procedures_encounter_id
  ON procedures(encounter_id);

CREATE INDEX IF NOT EXISTS idx_diagnostic_reports_patient_id
  ON diagnostic_reports(patient_id);

CREATE INDEX IF NOT EXISTS idx_diagnostic_reports_encounter_id
  ON diagnostic_reports(encounter_id);

CREATE INDEX IF NOT EXISTS idx_immunizations_patient_id
  ON immunizations(patient_id);

CREATE INDEX IF NOT EXISTS idx_immunizations_encounter_id
  ON immunizations(encounter_id);

CREATE INDEX IF NOT EXISTS idx_care_plans_patient_id
  ON care_plans(patient_id);

CREATE INDEX IF NOT EXISTS idx_care_plans_encounter_id
  ON care_plans(encounter_id);

CREATE INDEX IF NOT EXISTS idx_audit_log_patient_id
  ON audit_log(patient_id);

CREATE INDEX IF NOT EXISTS idx_audit_log_created_at
  ON audit_log(created_at);

CREATE INDEX IF NOT EXISTS idx_match_candidates_status
  ON patient_match_candidates(review_status);

CREATE INDEX IF NOT EXISTS idx_match_candidates_score
  ON patient_match_candidates(match_score DESC);

CREATE OR REPLACE VIEW provider_allergy_details AS
SELECT
  p.golden_id,
  a.id AS allergy_id,
  a.recorded_date,
  COALESCE(a.description, a.code) AS allergy,
  INITCAP(COALESCE(a.category, 'unknown')) AS category,
  INITCAP(COALESCE(a.clinical_status, 'active')) AS clinical_status,
  INITCAP(COALESCE(a.verification_status, 'confirmed')) AS verification_status,
  INITCAP(COALESCE(a.criticality, 'low')) AS criticality,
  COALESCE(NULLIF(TRIM(a.reaction_description), ''), 'Reaction not documented') AS reaction,
  INITCAP(COALESCE(NULLIF(TRIM(a.reaction_severity), ''), 'unknown')) AS reaction_severity,
  CASE
    WHEN LOWER(COALESCE(a.description, a.code, '')) ~ 'penicillin'
      THEN 'Avoid penicillin-class therapy until the allergy is reviewed.'
    WHEN LOWER(COALESCE(a.criticality, '')) = 'high'
      THEN 'High-criticality allergy: verify alternatives before ordering.'
    WHEN COALESCE(NULLIF(TRIM(a.reaction_description), ''), '') = ''
      THEN 'Confirm the reaction history during medication reconciliation.'
    ELSE 'Review allergy history before new medication orders.'
  END AS safety_note
FROM allergies a
JOIN patients p ON p.id = a.patient_id
WHERE p.golden_id IS NOT NULL
  AND COALESCE(a.clinical_status, 'active') = 'active';

CREATE OR REPLACE VIEW provider_acute_care_events AS
SELECT
  p.golden_id,
  e.id AS encounter_id,
  e.start_date,
  e.end_date,
  CASE
    WHEN LOWER(COALESCE(e.encounter_type, '')) ~ 'emergency'
      THEN 'Emergency'
    WHEN LOWER(COALESCE(e.encounter_type, '')) ~ 'urgent'
      THEN 'Urgent care'
    WHEN LOWER(COALESCE(e.encounter_type, '')) ~ '(hospital admission|admission to|inpatient|surgical department)'
      THEN 'Hospital admission'
    WHEN LOWER(COALESCE(e.encounter_type, '')) ~ '(encounter for problem|encounter for symptom)'
      THEN 'Same-day acute'
    WHEN LOWER(COALESCE(e.encounter_type, '')) ~ '(check up|general examination|follow-up)'
      THEN 'Routine/Follow-up'
    ELSE 'Ambulatory acute'
  END AS care_setting,
  COALESCE(e.encounter_type, 'Encounter') AS encounter_type,
  COALESCE(e.provider, 'Unknown') AS provider,
  COALESCE(e.cost, 0)::NUMERIC(12, 2) AS cost,
  CASE
    WHEN LOWER(COALESCE(e.encounter_type, '')) ~ '(emergency|urgent|hospital admission|admission to|inpatient|surgical department|encounter for problem|encounter for symptom)'
      THEN TRUE
    ELSE FALSE
  END AS is_acute
FROM encounters e
JOIN patients p ON p.id = e.patient_id
WHERE p.golden_id IS NOT NULL;

CREATE OR REPLACE VIEW provider_acute_care_summary AS
WITH rollup AS (
  SELECT
    golden_id,
    COUNT(*) FILTER (
      WHERE is_acute
        AND start_date >= NOW() - INTERVAL '30 days'
    ) AS acute_visits_30d,
    COUNT(*) FILTER (
      WHERE is_acute
        AND start_date >= NOW() - INTERVAL '90 days'
    ) AS acute_visits_90d,
    COUNT(*) FILTER (
      WHERE care_setting = 'Emergency'
        AND start_date >= NOW() - INTERVAL '90 days'
    ) AS ed_visits_90d,
    COUNT(*) FILTER (
      WHERE care_setting = 'Urgent care'
        AND start_date >= NOW() - INTERVAL '90 days'
    ) AS urgent_visits_90d,
    COUNT(*) FILTER (
      WHERE care_setting = 'Hospital admission'
        AND start_date >= NOW() - INTERVAL '365 days'
    ) AS admissions_365d,
    COALESCE(
      SUM(cost) FILTER (
        WHERE is_acute
          AND start_date >= NOW() - INTERVAL '90 days'
      ),
      0
    )::NUMERIC(12, 2) AS acute_cost_90d,
    MAX(start_date) FILTER (WHERE is_acute) AS last_acute_visit
  FROM provider_acute_care_events
  GROUP BY golden_id
),
last_event AS (
  SELECT DISTINCT ON (golden_id)
    golden_id,
    care_setting AS last_acute_setting,
    provider AS last_acute_provider
  FROM provider_acute_care_events
  WHERE is_acute
  ORDER BY golden_id, start_date DESC NULLS LAST
)
SELECT
  r.golden_id,
  r.acute_visits_30d,
  r.acute_visits_90d,
  r.ed_visits_90d,
  r.urgent_visits_90d,
  r.admissions_365d,
  r.acute_cost_90d,
  r.last_acute_visit,
  COALESCE(l.last_acute_setting, 'No acute encounter documented') AS last_acute_setting,
  COALESCE(l.last_acute_provider, 'Unknown') AS last_acute_provider
FROM rollup r
LEFT JOIN last_event l USING (golden_id);

CREATE OR REPLACE VIEW provider_care_gaps AS
WITH patient_base AS (
  SELECT
    golden_id,
    MIN(birth_date) AS birth_date,
    MAX(DATE_PART('year', AGE(birth_date)))::INT AS age
  FROM patients
  WHERE golden_id IS NOT NULL
  GROUP BY golden_id
),
latest_labs AS (
  SELECT DISTINCT ON (p.golden_id, COALESCE(o.description, o.code))
    p.golden_id,
    COALESCE(o.description, o.code) AS metric,
    o.value_numeric,
    COALESCE(
      NULLIF(o.value_text, ''),
      TRIM(CONCAT(COALESCE(o.value_numeric::TEXT, ''), ' ', COALESCE(o.value_unit, '')))
    ) AS display_value,
    COALESCE(o.effective_at, o.issued_at)::DATE AS observed_date
  FROM observations o
  JOIN patients p ON p.id = o.patient_id
  WHERE p.golden_id IS NOT NULL
    AND LOWER(COALESCE(o.category, '')) LIKE '%laboratory%'
  ORDER BY p.golden_id, COALESCE(o.description, o.code), COALESCE(o.effective_at, o.issued_at) DESC NULLS LAST
),
latest_vitals AS (
  SELECT DISTINCT ON (p.golden_id, COALESCE(o.description, o.code))
    p.golden_id,
    COALESCE(o.description, o.code) AS metric,
    o.value_numeric,
    COALESCE(
      NULLIF(o.value_text, ''),
      TRIM(CONCAT(COALESCE(o.value_numeric::TEXT, ''), ' ', COALESCE(o.value_unit, '')))
    ) AS display_value,
    COALESCE(o.effective_at, o.issued_at)::DATE AS observed_date
  FROM observations o
  JOIN patients p ON p.id = o.patient_id
  WHERE p.golden_id IS NOT NULL
    AND LOWER(COALESCE(o.category, '')) LIKE '%vital%'
  ORDER BY p.golden_id, COALESCE(o.description, o.code), COALESCE(o.effective_at, o.issued_at) DESC NULLS LAST
),
active_meds AS (
  SELECT
    p.golden_id,
    COUNT(DISTINCT NULLIF(TRIM(COALESCE(m.description, m.code)), '')) AS current_meds
  FROM medications m
  JOIN patients p ON p.id = m.patient_id
  WHERE p.golden_id IS NOT NULL
    AND NULLIF(TRIM(COALESCE(m.description, m.code)), '') IS NOT NULL
    AND (
      COALESCE(m.status, 'active') NOT IN ('stopped', 'cancelled', 'entered-in-error')
      OR m.stop_date IS NULL
      OR m.stop_date >= CURRENT_DATE
    )
  GROUP BY p.golden_id
),
allergy_quality AS (
  SELECT
    p.golden_id,
    COUNT(*) FILTER (
      WHERE COALESCE(a.clinical_status, 'active') = 'active'
    ) AS active_allergies,
    COUNT(*) FILTER (
      WHERE COALESCE(a.clinical_status, 'active') = 'active'
        AND COALESCE(NULLIF(TRIM(a.reaction_description), ''), '') = ''
    ) AS undocumented_reactions
  FROM allergies a
  JOIN patients p ON p.id = a.patient_id
  WHERE p.golden_id IS NOT NULL
  GROUP BY p.golden_id
),
primary_care AS (
  SELECT
    p.golden_id,
    MAX(e.start_date::DATE) AS last_primary_care_visit
  FROM encounters e
  JOIN patients p ON p.id = e.patient_id
  WHERE p.golden_id IS NOT NULL
    AND LOWER(COALESCE(e.encounter_type, '')) ~ '(check up|general examination|follow-up encounter|follow-up visit)'
  GROUP BY p.golden_id
),
flu_shots AS (
  SELECT
    p.golden_id,
    MAX(i.occurrence_at::DATE) AS last_flu_shot
  FROM immunizations i
  JOIN patients p ON p.id = i.patient_id
  WHERE p.golden_id IS NOT NULL
    AND LOWER(COALESCE(i.description, '')) LIKE '%influenza%'
  GROUP BY p.golden_id
)
SELECT *
FROM (
  SELECT
    pb.golden_id,
    'High' AS severity,
    'Chronic disease' AS domain,
    'Uncontrolled diabetes' AS care_gap,
    a1c.display_value || ' on ' || a1c.observed_date AS evidence,
    'Schedule diabetes follow-up and review the regimen.' AS suggested_action,
    '1-2 weeks' AS due_window,
    10 AS sort_order
  FROM patient_base pb
  JOIN latest_labs a1c
    ON a1c.golden_id = pb.golden_id
   AND a1c.metric ILIKE 'Hemoglobin A1c%'
  WHERE COALESCE(a1c.value_numeric, 0) >= 8

  UNION ALL

  SELECT
    pb.golden_id,
    'Medium',
    'Chronic disease',
    'Diabetes follow-up due',
    a1c.display_value || ' on ' || a1c.observed_date,
    'Review A1c trend and reinforce the care plan.',
    'Routine',
    20
  FROM patient_base pb
  JOIN latest_labs a1c
    ON a1c.golden_id = pb.golden_id
   AND a1c.metric ILIKE 'Hemoglobin A1c%'
  WHERE COALESCE(a1c.value_numeric, 0) >= 6.5
    AND COALESCE(a1c.value_numeric, 0) < 8

  UNION ALL

  SELECT
    pb.golden_id,
    'High',
    'Vitals',
    'Blood pressure urgently elevated',
    sbp.display_value || ' / ' || COALESCE(dbp.display_value, 'n/a') || ' on ' || sbp.observed_date,
    'Repeat blood pressure and review antihypertensives.',
    '1-2 weeks',
    30
  FROM patient_base pb
  JOIN latest_vitals sbp
    ON sbp.golden_id = pb.golden_id
   AND sbp.metric = 'Systolic Blood Pressure'
  LEFT JOIN latest_vitals dbp
    ON dbp.golden_id = pb.golden_id
   AND dbp.metric = 'Diastolic Blood Pressure'
  WHERE COALESCE(sbp.value_numeric, 0) >= 160
     OR COALESCE(dbp.value_numeric, 0) >= 100

  UNION ALL

  SELECT
    pb.golden_id,
    'Medium',
    'Vitals',
    'Blood pressure follow-up',
    sbp.display_value || ' / ' || COALESCE(dbp.display_value, 'n/a') || ' on ' || sbp.observed_date,
    'Repeat blood pressure and review treatment adherence.',
    'Routine',
    40
  FROM patient_base pb
  JOIN latest_vitals sbp
    ON sbp.golden_id = pb.golden_id
   AND sbp.metric = 'Systolic Blood Pressure'
  LEFT JOIN latest_vitals dbp
    ON dbp.golden_id = pb.golden_id
   AND dbp.metric = 'Diastolic Blood Pressure'
  WHERE (
      COALESCE(sbp.value_numeric, 0) BETWEEN 140 AND 159.9999
      OR COALESCE(dbp.value_numeric, 0) BETWEEN 90 AND 99.9999
    )

  UNION ALL

  SELECT
    pb.golden_id,
    CASE
      WHEN COALESCE(egfr.value_numeric, 999) < 30 THEN 'High'
      ELSE 'Medium'
    END,
    'Renal',
    'Renal function monitoring',
    egfr.display_value || ' on ' || egfr.observed_date,
    'Review kidney function, medication dosing, and follow-up interval.',
    CASE
      WHEN COALESCE(egfr.value_numeric, 999) < 30 THEN '1-2 weeks'
      ELSE 'Routine'
    END,
    50
  FROM patient_base pb
  JOIN latest_labs egfr
    ON egfr.golden_id = pb.golden_id
   AND egfr.metric ILIKE 'Glomerular filtration rate%'
  WHERE COALESCE(egfr.value_numeric, 999) < 60

  UNION ALL

  SELECT
    pb.golden_id,
    CASE
      WHEN COALESCE(ldl.value_numeric, 0) >= 190 THEN 'High'
      ELSE 'Medium'
    END,
    'Cardiovascular',
    'Lipid management review',
    ldl.display_value || ' on ' || ldl.observed_date,
    'Review statin therapy, adherence, and cardiovascular risk.',
    'Routine',
    60
  FROM patient_base pb
  JOIN latest_labs ldl
    ON ldl.golden_id = pb.golden_id
   AND ldl.metric ILIKE 'Cholesterol in LDL%'
  WHERE COALESCE(ldl.value_numeric, 0) >= 130

  UNION ALL

  SELECT
    pb.golden_id,
    CASE
      WHEN COALESCE(acs.acute_visits_90d, 0) >= 3 THEN 'High'
      ELSE 'Medium'
    END,
    'Utilization',
    'Frequent acute care use',
    COALESCE(acs.acute_visits_90d, 0)::TEXT || ' acute visits in the last 90 days',
    'Schedule follow-up and confirm the care plan after recent acute utilization.',
    '1 week',
    70
  FROM patient_base pb
  LEFT JOIN provider_acute_care_summary acs
    ON acs.golden_id = pb.golden_id
  WHERE COALESCE(acs.acute_visits_90d, 0) >= 2

  UNION ALL

  SELECT
    pb.golden_id,
    CASE
      WHEN COALESCE(am.current_meds, 0) >= 15 THEN 'High'
      ELSE 'Medium'
    END,
    'Medication safety',
    'Polypharmacy review',
    COALESCE(am.current_meds, 0)::TEXT || ' active medications',
    'Perform medication reconciliation and deprescribing review.',
    'Next visit',
    80
  FROM patient_base pb
  LEFT JOIN active_meds am
    ON am.golden_id = pb.golden_id
  WHERE COALESCE(am.current_meds, 0) >= 10

  UNION ALL

  SELECT
    pb.golden_id,
    'Medium',
    'Prevention',
    'Influenza vaccine due',
    COALESCE(fs.last_flu_shot::TEXT, 'No influenza vaccine on file'),
    'Offer or document seasonal influenza vaccination.',
    'Next visit',
    90
  FROM patient_base pb
  LEFT JOIN flu_shots fs
    ON fs.golden_id = pb.golden_id
  WHERE pb.age >= 65
    AND (
      fs.last_flu_shot IS NULL
      OR fs.last_flu_shot < CURRENT_DATE - INTERVAL '18 months'
    )

  UNION ALL

  SELECT
    pb.golden_id,
    'Medium',
    'Continuity',
    'Primary care follow-up overdue',
    COALESCE(pc.last_primary_care_visit::TEXT, 'No documented follow-up visit'),
    'Schedule routine follow-up and close open care gaps.',
    'Routine',
    100
  FROM patient_base pb
  LEFT JOIN primary_care pc
    ON pc.golden_id = pb.golden_id
  WHERE pc.last_primary_care_visit IS NULL
     OR pc.last_primary_care_visit < CURRENT_DATE - INTERVAL '12 months'

  UNION ALL

  SELECT
    pb.golden_id,
    'Medium',
    'Documentation',
    'Complete allergy reaction history',
    aq.undocumented_reactions::TEXT || ' active allergies missing reaction detail',
    'Confirm reaction details during the next reconciliation or acute intake.',
    'Next visit',
    110
  FROM patient_base pb
  JOIN allergy_quality aq
    ON aq.golden_id = pb.golden_id
  WHERE COALESCE(aq.active_allergies, 0) > 0
    AND COALESCE(aq.undocumented_reactions, 0) > 0
) gaps;
