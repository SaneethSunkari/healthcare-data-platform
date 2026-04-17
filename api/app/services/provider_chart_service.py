from datetime import date, datetime
from decimal import Decimal
from typing import Any

from psycopg2.extras import RealDictCursor

from compliance.pii_masker import log_access, mask_response_for_role

MEDICATION_NAME_SQL = "nullif(trim(coalesce(m.description, m.code)), '')"
PROBLEM_NOISE_REGEX = (
    r"(medication review due|full-time employment|part-time employment|not in labor force|"
    r"employment status|social isolation|limited social contact|received higher education|"
    r"educated to high school level|has a criminal record)"
)
KEY_LAB_REGEX = (
    r"(Hemoglobin A1c|Cholesterol in LDL|Creatinine|Glomerular filtration rate|Glucose|"
    r"Hemoglobin|Potassium|Sodium|Leukocytes|Platelets|Calcium|Carbon dioxide)"
)
BLOOD_TYPE_REGEX = r"(blood group|blood type|rhesus)"
PENICILLIN_MED_REGEX = r"(penicillin|amoxicillin|ampicillin|dicloxacillin|clavulanate)"
WARFARIN_REGEX = r"\mwarfarin\M"
ANTIPLATELET_OR_NSAID_REGEX = r"(aspirin|clopidogrel|ibuprofen|naproxen)"
OPIOID_REGEX = r"(fentanyl|tramadol|hydrocodone|oxycodone|codeine|morphine)"
STATIN_REGEX = r"(simvastatin|atorvastatin|rosuvastatin|pravastatin|lovastatin)"
ALBUTEROL_REGEX = r"(albuterol|ventolin|proair|proventil)"


def _serialize_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _serialize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{key: _serialize_value(value) for key, value in row.items()} for row in rows]


def _fetch_one(connection, sql: str, params: Any) -> dict[str, Any] | None:
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(sql, params)
        row = cursor.fetchone()
    if row is None:
        return None
    return {key: _serialize_value(value) for key, value in dict(row).items()}


def _fetch_all(connection, sql: str, params: Any) -> list[dict[str, Any]]:
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(sql, params)
        rows = cursor.fetchall()
    return _serialize_rows([dict(row) for row in rows])


SEARCH_SQL = """
with search_results as (
    select
        p.golden_id,
        string_agg(
            distinct trim(concat_ws(' ', p.first_name, p.last_name)),
            ' | '
            order by trim(concat_ws(' ', p.first_name, p.last_name))
        ) as patient_names,
        min(p.birth_date) as birth_date,
        max(date_part('year', age(p.birth_date)))::int as age,
        max(p.gender) as gender,
        string_agg(distinct p.zip_code, ', ' order by p.zip_code) as zip_codes,
        count(distinct p.id) as linked_records,
        max(e.start_date) as last_visit,
        coalesce(sum(e.cost), 0)::numeric(14, 2) as lifetime_cost
    from patients p
    left join encounters e on e.patient_id = p.id
    where p.golden_id is not null
      and (
        %(term)s = ''
        or p.golden_id ilike %(wildcard)s
        or p.id ilike %(wildcard)s
        or trim(concat_ws(' ', p.first_name, p.last_name)) ilike %(wildcard)s
        or p.last_name ilike %(wildcard)s
        or p.zip_code ilike %(wildcard)s
        or coalesce(p.source_system, '') ilike %(wildcard)s
        or to_char(p.birth_date, 'YYYY-MM-DD') ilike %(wildcard)s
        or to_char(p.birth_date, 'MM/DD/YYYY') ilike %(wildcard)s
      )
    group by p.golden_id
)
select
    s.*,
    coalesce(rr.readmission_risk, 'low') as readmission_risk
from search_results s
left join analytics.readmission_risk rr on rr.golden_id = s.golden_id
order by
    case
        when s.golden_id = %(exact)s then 0
        when s.golden_id ilike %(prefix)s then 1
        else 2
    end,
    s.last_visit desc nulls last,
    s.patient_names
limit %(limit)s
"""

PROFILE_SQL = """
select
    p.golden_id,
    string_agg(
        distinct trim(concat_ws(' ', p.first_name, p.last_name)),
        ' | '
        order by trim(concat_ws(' ', p.first_name, p.last_name))
    ) as patient_names,
    min(p.birth_date) as birth_date,
    max(date_part('year', age(p.birth_date)))::int as age,
    max(p.gender) as gender,
    string_agg(distinct p.zip_code, ', ' order by p.zip_code) as zip_codes,
    string_agg(distinct p.source_system, ', ' order by p.source_system) as source_systems,
    count(distinct p.id) as linked_records
from patients p
where p.golden_id = %s
group by p.golden_id
"""

SUMMARY_SQL = f"""
with patient_ids as (
    select id
    from patients
    where golden_id = %s
)
select
    coalesce((select count(distinct e.id) from encounters e where e.patient_id in (select id from patient_ids)), 0) as encounter_count,
    coalesce((select count(distinct c.id) from conditions c where c.patient_id in (select id from patient_ids)), 0) as condition_count,
    coalesce(
        (
            select count(distinct {MEDICATION_NAME_SQL})
            from medications m
            where m.patient_id in (select id from patient_ids)
              and {MEDICATION_NAME_SQL} is not null
              and (
                coalesce(m.status, 'active') not in ('stopped', 'cancelled', 'entered-in-error')
                or m.stop_date is null
                or m.stop_date >= current_date
              )
        ),
        0
    ) as active_medication_count,
    coalesce(
        (
            select count(distinct a.id)
            from allergies a
            where a.patient_id in (select id from patient_ids)
              and coalesce(a.clinical_status, 'active') = 'active'
        ),
        0
    ) as allergy_count,
    coalesce(
        (
            select count(distinct cp.id)
            from care_plans cp
            where cp.patient_id in (select id from patient_ids)
              and coalesce(cp.status, '') not in ('revoked', 'completed')
        ),
        0
    ) as open_care_plan_count,
    coalesce((select sum(e.cost) from encounters e where e.patient_id in (select id from patient_ids)), 0)::numeric(14, 2) as lifetime_cost,
    (select max(e.start_date) from encounters e where e.patient_id in (select id from patient_ids)) as last_visit,
    coalesce((select rr.readmission_risk from analytics.readmission_risk rr where rr.golden_id = %s limit 1), 'low') as readmission_risk
"""

EMERGENCY_SNAPSHOT_SQL = f"""
with blood_type as (
    select
        coalesce(
            nullif(o.value_text, ''),
            trim(concat(coalesce(o.value_numeric::text, ''), ' ', coalesce(o.value_unit, '')))
        ) as blood_type,
        coalesce(o.effective_at, o.issued_at) as observed_at
    from observations o
    join patients p on p.id = o.patient_id
    where p.golden_id = %s
      and (
        coalesce(o.description, '') ~* '{BLOOD_TYPE_REGEX}'
        or coalesce(o.code, '') ~* '(ABO|RH)'
      )
    order by coalesce(o.effective_at, o.issued_at) desc nulls last
    limit 1
),
allergy_rollup as (
    select
        count(*) as active_allergy_count,
        count(*) filter (
            where lower(coalesce(a.description, a.code, '')) ~* 'penicillin'
        ) as penicillin_allergy_count,
        string_agg(
            distinct coalesce(a.description, a.code),
            '; '
            order by coalesce(a.description, a.code)
        ) as allergy_summary
    from allergies a
    join patients p on p.id = a.patient_id
    where p.golden_id = %s
      and coalesce(a.clinical_status, 'active') = 'active'
),
med_rollup as (
    select count(distinct {MEDICATION_NAME_SQL}) as current_med_count
    from medications m
    join patients p on p.id = m.patient_id
    where p.golden_id = %s
      and {MEDICATION_NAME_SQL} is not null
      and (
        coalesce(m.status, 'active') not in ('stopped', 'cancelled', 'entered-in-error')
        or m.stop_date is null
        or m.stop_date >= current_date
      )
),
recent_encounter as (
    select
        e.start_date::date as last_encounter_date,
        coalesce(e.provider, 'Unknown') as last_provider,
        coalesce(e.encounter_type, 'Encounter') as last_encounter_type
    from encounters e
    join patients p on p.id = e.patient_id
    where p.golden_id = %s
    order by e.start_date desc nulls last
    limit 1
),
safety_rollup as (
    with active_meds as (
        select distinct lower({MEDICATION_NAME_SQL}) as med_name
        from medications m
        join patients p on p.id = m.patient_id
        where p.golden_id = %s
          and {MEDICATION_NAME_SQL} is not null
          and (
            coalesce(m.status, 'active') not in ('stopped', 'cancelled', 'entered-in-error')
            or m.stop_date is null
            or m.stop_date >= current_date
          )
    ),
    active_allergies as (
        select distinct lower(coalesce(a.description, a.code)) as allergy_name
        from allergies a
        join patients p on p.id = a.patient_id
        where p.golden_id = %s
          and coalesce(a.clinical_status, 'active') = 'active'
    ),
    med_pairs as (
        select
            m1.med_name as left_med,
            m2.med_name as right_med
        from active_meds m1
        join active_meds m2 on m1.med_name < m2.med_name
    )
    select
        (
            select count(*)
            from med_pairs
            where (left_med ~* '{WARFARIN_REGEX}' and right_med ~* '{ANTIPLATELET_OR_NSAID_REGEX}')
               or (right_med ~* '{WARFARIN_REGEX}' and left_med ~* '{ANTIPLATELET_OR_NSAID_REGEX}')
        )
        +
        (
            select count(*)
            from med_pairs
            where (left_med ~* '{OPIOID_REGEX}' and right_med ~* '{OPIOID_REGEX}')
        )
        +
        (
            select count(*)
            from med_pairs
            where (left_med ~* '{STATIN_REGEX}' and right_med ~* '{STATIN_REGEX}')
        )
        +
        (
            select count(*)
            from med_pairs
            where (left_med ~* '{ALBUTEROL_REGEX}' and right_med ~* '{ALBUTEROL_REGEX}')
        )
        +
        (
            select count(*)
            from active_allergies a
            join active_meds m on true
            where a.allergy_name ~* 'penicillin'
              and m.med_name ~* '{PENICILLIN_MED_REGEX}'
        ) as high_alert_count
)
select
    coalesce((select blood_type from blood_type), 'Not documented in connected sources') as blood_type,
    coalesce((select allergy_summary from allergy_rollup), 'None documented') as allergy_summary,
    coalesce((select penicillin_allergy_count from allergy_rollup), 0) as penicillin_allergy_count,
    coalesce((select active_allergy_count from allergy_rollup), 0) as active_allergy_count,
    coalesce((select current_med_count from med_rollup), 0) as current_med_count,
    coalesce((select high_alert_count from safety_rollup), 0) as high_alert_count,
    (select last_encounter_date from recent_encounter) as last_encounter_date,
    coalesce((select last_provider from recent_encounter), 'Unknown') as last_provider,
    coalesce((select last_encounter_type from recent_encounter), 'No encounter documented') as last_encounter_type
"""

LINKED_RECORDS_SQL = """
select
    id as source_patient_id,
    first_name,
    last_name,
    birth_date,
    gender,
    zip_code,
    source_system,
    match_confidence,
    match_status
from patients
where golden_id = %s
order by source_system, id
"""

ALLERGIES_SQL = """
select
    recorded_date,
    allergy,
    category,
    clinical_status,
    verification_status,
    criticality,
    reaction,
    reaction_severity,
    safety_note
from provider_allergy_details
where golden_id = %s
order by recorded_date desc nulls last, allergy
limit 50
"""

ACUTE_CARE_SUMMARY_SQL = """
select
    acute_visits_30d,
    acute_visits_90d,
    ed_visits_90d,
    urgent_visits_90d,
    admissions_365d,
    acute_cost_90d,
    last_acute_visit,
    last_acute_setting,
    last_acute_provider
from provider_acute_care_summary
where golden_id = %s
"""

ACUTE_CARE_EVENTS_SQL = """
select
    start_date,
    end_date,
    care_setting,
    encounter_type,
    provider,
    cost
from provider_acute_care_events
where golden_id = %s
  and is_acute
order by start_date desc nulls last
limit 20
"""

CARE_GAPS_SQL = """
select
    severity,
    domain,
    care_gap,
    evidence,
    suggested_action,
    due_window
from provider_care_gaps
where golden_id = %s
order by sort_order, care_gap
limit 20
"""

MEDICATION_SAFETY_ALERTS_SQL = f"""
with active_meds as (
    select distinct
        lower({MEDICATION_NAME_SQL}) as med_name,
        {MEDICATION_NAME_SQL} as display_name
    from medications m
    join patients p on p.id = m.patient_id
    where p.golden_id = %s
      and {MEDICATION_NAME_SQL} is not null
      and (
        coalesce(m.status, 'active') not in ('stopped', 'cancelled', 'entered-in-error')
        or m.stop_date is null
        or m.stop_date >= current_date
      )
),
active_allergies as (
    select distinct
        lower(coalesce(a.description, a.code)) as allergy_name,
        coalesce(a.description, a.code) as allergy_display
    from allergies a
    join patients p on p.id = a.patient_id
    where p.golden_id = %s
      and coalesce(a.clinical_status, 'active') = 'active'
),
med_pairs as (
    select
        least(m1.display_name, m2.display_name) as med_left,
        greatest(m1.display_name, m2.display_name) as med_right,
        m1.med_name as left_norm,
        m2.med_name as right_norm
    from active_meds m1
    join active_meds m2 on m1.display_name < m2.display_name
)
select *
from (
    select
        'High' as severity,
        'Penicillin allergy conflict' as alert,
        string_agg(distinct a.allergy_display || ' + ' || m.display_name, '; ' order by a.allergy_display || ' + ' || m.display_name) as evidence,
        'Avoid penicillin-class therapy and verify alternatives before ordering.' as suggested_action
    from active_allergies a
    join active_meds m on true
    where a.allergy_name ~* 'penicillin'
      and m.med_name ~* '{PENICILLIN_MED_REGEX}'
    having count(*) > 0

    union all

    select
        'High',
        'Major bleeding risk',
        string_agg(distinct med_left || ' + ' || med_right, '; ' order by med_left || ' + ' || med_right),
        'Review anticoagulant, antiplatelet, and NSAID combination before continuing therapy.'
    from med_pairs
    where (left_norm ~* '{WARFARIN_REGEX}' and right_norm ~* '{ANTIPLATELET_OR_NSAID_REGEX}')
       or (right_norm ~* '{WARFARIN_REGEX}' and left_norm ~* '{ANTIPLATELET_OR_NSAID_REGEX}')
    having count(*) > 0

    union all

    select
        'High',
        'Opioid combination review',
        string_agg(distinct med_left || ' + ' || med_right, '; ' order by med_left || ' + ' || med_right),
        'Multiple opioid therapies are active; verify intent and overdose risk.'
    from med_pairs
    where left_norm ~* '{OPIOID_REGEX}'
      and right_norm ~* '{OPIOID_REGEX}'
    having count(*) > 0

    union all

    select
        'Medium',
        'Duplicate statin therapy',
        string_agg(distinct med_left || ' + ' || med_right, '; ' order by med_left || ' + ' || med_right),
        'Confirm only one statin regimen should remain active.'
    from med_pairs
    where left_norm ~* '{STATIN_REGEX}'
      and right_norm ~* '{STATIN_REGEX}'
    having count(*) > 0

    union all

    select
        'Medium',
        'Duplicate rescue inhaler review',
        string_agg(distinct med_left || ' + ' || med_right, '; ' order by med_left || ' + ' || med_right),
        'Multiple albuterol products are active; verify patient instructions and duplication.'
    from med_pairs
    where left_norm ~* '{ALBUTEROL_REGEX}'
      and right_norm ~* '{ALBUTEROL_REGEX}'
    having count(*) > 0
) alerts
order by case severity when 'High' then 0 else 1 end, alert
"""

ACTIVE_MEDICATIONS_SQL = f"""
with med_base as (
    select
        {MEDICATION_NAME_SQL} as medication,
        initcap(coalesce(m.status, 'active')) as status,
        coalesce(initcap(m.category), 'Community') as category,
        coalesce(nullif(trim(m.dose_details), ''), 'Dose not specified') as dose_details,
        coalesce(nullif(trim(m.frequency), ''), 'Frequency not specified') as frequency,
        coalesce(initcap(nullif(trim(m.route), '')), 'Route not stated') as route,
        coalesce(nullif(trim(m.prescriber), ''), 'Prescriber not listed') as prescriber,
        coalesce(m.as_needed, false) as as_needed,
        m.start_date,
        m.stop_date,
        count(*) over (partition by {MEDICATION_NAME_SQL}) as regimen_count
    from medications m
    join patients p on p.id = m.patient_id
    where p.golden_id = %s
      and {MEDICATION_NAME_SQL} is not null
      and (
        coalesce(m.status, 'active') not in ('stopped', 'cancelled', 'entered-in-error')
        or m.stop_date is null
        or m.stop_date >= current_date
      )
)
,
review_rows as (
    select distinct
        medication,
        status,
        category,
        dose_details,
        frequency,
        route,
        prescriber,
        as_needed,
        start_date,
        stop_date,
        case
            when regimen_count > 1 then 'Review duplicate therapy'
            when frequency = 'Frequency not specified' and dose_details = 'Dose not specified' then 'Clarify medication instructions'
            when frequency = 'Frequency not specified' then 'Add frequency'
            when prescriber = 'Prescriber not listed' then 'Confirm prescriber'
            when as_needed then 'Confirm PRN indication'
            when start_date is not null and start_date < current_date - interval '8 years' then 'Confirm still active'
            else 'Continue'
        end as review_flag,
        case
            when regimen_count > 1 then 'Multiple active rows exist for the same medication.'
            when frequency = 'Frequency not specified' and dose_details = 'Dose not specified' then 'Dose and timing are both missing.'
            when frequency = 'Frequency not specified' then 'Timing is missing from the active medication record.'
            when prescriber = 'Prescriber not listed' then 'Ordering provider is missing.'
            when as_needed then 'Verify the PRN indication and patient instructions.'
            when start_date is not null and start_date < current_date - interval '8 years' then 'Long-running therapy should be confirmed at reconciliation.'
            else 'No immediate medication issue detected.'
        end as review_reason,
        case
            when regimen_count > 1 then 0
            when frequency = 'Frequency not specified' and dose_details = 'Dose not specified' then 1
            when frequency = 'Frequency not specified' then 2
            when prescriber = 'Prescriber not listed' then 3
            when as_needed then 4
            when start_date is not null and start_date < current_date - interval '8 years' then 5
            else 6
        end as sort_rank
    from med_base
)
select
    medication,
    status,
    category,
    dose_details,
    frequency,
    route,
    prescriber,
    as_needed,
    start_date,
    stop_date,
    review_flag,
    review_reason
from review_rows
order by
    sort_rank,
    start_date desc nulls last,
    medication
limit 50
"""

ACTIVE_PROBLEMS_SQL = f"""
with problem_rollup as (
    select
        c.description as problem,
        case
            when c.description ilike '%%diabet%%' or c.description ilike '%%glucose%%' or c.description ilike '%%obesity%%' then 'Metabolic'
            when c.description ilike '%%hypertension%%' or c.description ilike '%%hyperlip%%' or c.description ilike '%%cholesterol%%' or c.description ilike '%%card%%' then 'Cardiovascular'
            when c.description ilike '%%bronch%%' or c.description ilike '%%asthma%%' or c.description ilike '%%copd%%' or c.description ilike '%%sinus%%' or c.description ilike '%%pharyngitis%%' then 'Pulmonary/ENT'
            when c.description ilike '%%pain%%' or c.description ilike '%%arthritis%%' or c.description ilike '%%musculoskeletal%%' or c.description ilike '%%back%%' then 'MSK/Pain'
            when c.description ilike '%%anemia%%' or c.description ilike '%%renal%%' or c.description ilike '%%kidney%%' or c.description ilike '%%dialysis%%' then 'Renal/Heme'
            when c.description ilike '%%alcohol%%' or c.description ilike '%%tobacco%%' or c.description ilike '%%depress%%' or c.description ilike '%%anxiety%%' or c.description ilike '%%dementia%%' or c.description ilike '%%abuse%%' or c.description ilike '%%violence%%' then 'Behavioral/Psychosocial'
            else 'General Medicine'
        end as focus_area,
        min(c.onset_date)::date as first_seen,
        max(c.onset_date)::date as last_seen,
        count(*) as occurrences,
        case
            when c.description ilike '%%diabet%%' or c.description ilike '%%prediabet%%' then 'Review glycemic control and medication plan.'
            when c.description ilike '%%hypertension%%' then 'Check blood pressure trend and antihypertensive adherence.'
            when c.description ilike '%%hyperlip%%' or c.description ilike '%%cholesterol%%' then 'Review lipid control and statin therapy.'
            when c.description ilike '%%anemia%%' then 'Trend CBC and evaluate symptoms.'
            when c.description ilike '%%pain%%' then 'Assess pain control, function, and medication burden.'
            when c.description ilike '%%dementia%%' then 'Confirm caregiver support and safety planning.'
            when c.description ilike '%%abuse%%' or c.description ilike '%%violence%%' then 'Assess safety and support needs.'
            else 'Review with recent vitals, labs, and treatment plan.'
        end as suggested_follow_up,
        case
            when max(c.onset_date) >= current_date - interval '24 months' then 'Active'
            else 'Historical'
        end as clinical_status
    from conditions c
    join patients p on p.id = c.patient_id
    where p.golden_id = %s
      and c.description is not null
      and c.description !~* '{PROBLEM_NOISE_REGEX}'
    group by c.description
)
select
    problem,
    focus_area,
    suggested_follow_up,
    first_seen,
    last_seen,
    occurrences,
    clinical_status
from problem_rollup
where clinical_status = 'Active'
order by
    case focus_area
        when 'Metabolic' then 0
        when 'Cardiovascular' then 1
        when 'Renal/Heme' then 2
        else 3
    end,
    last_seen desc nulls last,
    occurrences desc,
    problem
limit 25
"""

CLINICAL_ALERTS_SQL = f"""
with latest_labs as (
    select distinct on (coalesce(o.description, o.code))
        coalesce(o.description, o.code) as lab_name,
        o.value_numeric,
        o.value_text,
        o.interpretation,
        o.effective_at
    from observations o
    join patients p on p.id = o.patient_id
    where p.golden_id = %s
      and lower(coalesce(o.category, '')) like '%%laboratory%%'
    order by coalesce(o.description, o.code), o.effective_at desc nulls last
),
latest_vitals as (
    select distinct on (coalesce(o.description, o.code))
        coalesce(o.description, o.code) as vital_name,
        o.value_numeric,
        o.value_text,
        o.effective_at
    from observations o
    join patients p on p.id = o.patient_id
    where p.golden_id = %s
      and lower(coalesce(o.category, '')) like '%%vital%%'
    order by coalesce(o.description, o.code), o.effective_at desc nulls last
),
utilization as (
    select count(distinct e.id) as visits_12m
    from encounters e
    join patients p on p.id = e.patient_id
    where p.golden_id = %s
      and e.start_date >= now() - interval '12 months'
),
meds as (
    select count(distinct {MEDICATION_NAME_SQL}) as current_meds
    from medications m
    join patients p on p.id = m.patient_id
    where p.golden_id = %s
      and {MEDICATION_NAME_SQL} is not null
      and (
        coalesce(m.status, 'active') not in ('stopped', 'cancelled', 'entered-in-error')
        or m.stop_date is null
        or m.stop_date >= current_date
      )
),
follow_up as (
    select current_date - max(e.start_date::date) as days_since_visit
    from encounters e
    join patients p on p.id = e.patient_id
    where p.golden_id = %s
),
risk as (
    select coalesce((select readmission_risk from analytics.readmission_risk where golden_id = %s limit 1), 'low') as readmission_risk
)
select *
from (
    select 'High' as severity, 'Diabetes control review' as priority, 'Schedule diabetes follow-up and assess therapy.' as suggested_action, coalesce(value_text, value_numeric::text) as evidence, '1-2 weeks' as due_window
    from latest_labs
    where lab_name ilike 'Hemoglobin A1c%%' and coalesce(value_numeric, 0) >= 8
    union all
    select 'Medium', 'Diabetes control review', 'Review A1c trend and reinforce care plan.', coalesce(value_text, value_numeric::text), 'Routine'
    from latest_labs
    where lab_name ilike 'Hemoglobin A1c%%' and coalesce(value_numeric, 0) >= 6.5 and coalesce(value_numeric, 0) < 8
    union all
    select 'High', 'Blood pressure reassessment', 'Repeat blood pressure and review antihypertensives.', coalesce(value_text, value_numeric::text), '1-2 weeks'
    from latest_vitals
    where vital_name = 'Systolic Blood Pressure' and coalesce(value_numeric, 0) >= 160
    union all
    select 'Medium', 'Blood pressure reassessment', 'Repeat blood pressure and review antihypertensives.', coalesce(value_text, value_numeric::text), 'Routine'
    from latest_vitals
    where vital_name = 'Systolic Blood Pressure' and coalesce(value_numeric, 0) >= 140 and coalesce(value_numeric, 0) < 160
    union all
    select 'Medium', 'Renal function monitoring', 'Review kidney function and medication dosing.', coalesce(value_text, value_numeric::text), 'Routine'
    from latest_labs
    where lab_name ilike 'Glomerular filtration rate%%' and coalesce(value_numeric, 999) < 60
    union all
    select 'Medium', 'Lipid management review', 'Review statin adherence and lipid therapy.', coalesce(value_text, value_numeric::text), 'Routine'
    from latest_labs
    where lab_name ilike 'Cholesterol in LDL%%' and coalesce(value_numeric, 0) >= 130
    union all
    select 'High', 'Care-management outreach', 'Recent utilization suggests closer follow-up is needed.', (select visits_12m::text from utilization), '1 week'
    where (select readmission_risk from risk) = 'high' and coalesce((select visits_12m from utilization), 0) >= 5
    union all
    select 'Medium', 'Medication reconciliation', 'Review the active medication list with the patient.', (select current_meds::text || ' active meds' from meds), 'Next visit'
    where (select current_meds from meds) >= 10
    union all
    select 'Medium', 'Follow-up scheduling', 'Last visit is older than six months; schedule follow-up.', (select days_since_visit::text || ' days' from follow_up), 'Routine'
    where coalesce((select days_since_visit from follow_up), 0) >= 180
) alerts
order by case severity when 'High' then 1 when 'Medium' then 2 else 3 end, priority
"""

ABNORMAL_LABS_SQL = f"""
with latest_labs as (
    select distinct on (coalesce(o.description, o.code))
        o.effective_at::date as collected,
        coalesce(o.description, o.code) as lab_name,
        coalesce(
            nullif(o.value_text, ''),
            trim(concat(coalesce(o.value_numeric::text, ''), ' ', coalesce(o.value_unit, '')))
        ) as result,
        case
            when coalesce(o.description, o.code) ilike 'Hemoglobin A1c%%' and coalesce(o.value_numeric, 0) >= 8 then 'High'
            when coalesce(o.description, o.code) ilike 'Hemoglobin A1c%%' and coalesce(o.value_numeric, 0) >= 6.5 then 'Borderline'
            when coalesce(o.description, o.code) ilike 'Cholesterol in LDL%%' and coalesce(o.value_numeric, 0) >= 160 then 'High'
            when coalesce(o.description, o.code) ilike 'Cholesterol in LDL%%' and coalesce(o.value_numeric, 0) >= 130 then 'Borderline'
            when coalesce(o.description, o.code) ilike 'Glomerular filtration rate%%' and coalesce(o.value_numeric, 999) < 30 then 'Critical'
            when coalesce(o.description, o.code) ilike 'Glomerular filtration rate%%' and coalesce(o.value_numeric, 999) < 60 then 'High'
            when coalesce(o.description, o.code) ilike 'Creatinine%%' and coalesce(o.value_numeric, 0) >= 1.5 then 'High'
            when coalesce(o.description, o.code) ilike 'Glucose%%' and coalesce(o.value_numeric, 0) >= 200 then 'High'
            when coalesce(o.description, o.code) ilike 'Hemoglobin%%' and coalesce(o.value_numeric, 999) < 10 then 'High'
            when coalesce(o.description, o.code) ilike 'Potassium%%' and (coalesce(o.value_numeric, 4.0) < 3.5 or coalesce(o.value_numeric, 4.0) > 5.3) then 'High'
            when coalesce(o.description, o.code) ilike 'Sodium%%' and (coalesce(o.value_numeric, 140.0) < 130 or coalesce(o.value_numeric, 140.0) > 150) then 'High'
            else 'Normal'
        end as flag,
        case
            when coalesce(o.description, o.code) ilike 'Hemoglobin A1c%%' then 'Diabetes control'
            when coalesce(o.description, o.code) ilike 'Cholesterol in LDL%%' then 'Cardiovascular risk'
            when coalesce(o.description, o.code) ilike 'Glomerular filtration rate%%' or coalesce(o.description, o.code) ilike 'Creatinine%%' then 'Renal function'
            when coalesce(o.description, o.code) ilike 'Glucose%%' then 'Metabolic status'
            when coalesce(o.description, o.code) ilike 'Hemoglobin%%' then 'Anemia screening'
            when coalesce(o.description, o.code) ilike 'Potassium%%' or coalesce(o.description, o.code) ilike 'Sodium%%' then 'Electrolytes'
            else 'Clinical review'
        end as clinical_focus,
        case
            when coalesce(o.description, o.code) ilike 'Hemoglobin A1c%%' then 'Review diabetic regimen and follow-up interval.'
            when coalesce(o.description, o.code) ilike 'Cholesterol in LDL%%' then 'Review statin therapy and adherence.'
            when coalesce(o.description, o.code) ilike 'Glomerular filtration rate%%' or coalesce(o.description, o.code) ilike 'Creatinine%%' then 'Review renal dosing and kidney disease monitoring.'
            when coalesce(o.description, o.code) ilike 'Glucose%%' then 'Assess acute glycemic symptoms and medications.'
            when coalesce(o.description, o.code) ilike 'Hemoglobin%%' then 'Consider anemia workup or repeat CBC.'
            when coalesce(o.description, o.code) ilike 'Potassium%%' or coalesce(o.description, o.code) ilike 'Sodium%%' then 'Assess medication and fluid/electrolyte causes.'
            else 'Review in clinical context.'
        end as suggested_follow_up
    from observations o
    join patients p on p.id = o.patient_id
    where p.golden_id = %s
      and lower(coalesce(o.category, '')) like '%%laboratory%%'
      and coalesce(o.description, o.code) ~* '{KEY_LAB_REGEX}'
    order by coalesce(o.description, o.code), o.effective_at desc nulls last
)
select
    collected,
    lab_name,
    result,
    flag,
    clinical_focus,
    suggested_follow_up
from latest_labs
where flag <> 'Normal'
order by
    case flag when 'Critical' then 0 when 'High' then 1 when 'Borderline' then 2 else 3 end,
    collected desc nulls last,
    lab_name
limit 25
"""

TIMELINE_SQL = """
select *
from (
    select e.start_date::date as event_date, 'Encounter' as event_type, coalesce(e.encounter_type, 'Encounter') as title, coalesce(e.provider, 'Unknown') as detail
    from encounters e
    join patients p on p.id = e.patient_id
    where p.golden_id = %s
    union all
    select pr.performed_start::date, 'Procedure', pr.description, initcap(coalesce(pr.status, 'completed'))
    from procedures pr
    join patients p on p.id = pr.patient_id
    where p.golden_id = %s
    union all
    select coalesce(dr.effective_at, dr.issued_at)::date, 'Report', dr.description, initcap(coalesce(dr.status, 'final'))
    from diagnostic_reports dr
    join patients p on p.id = dr.patient_id
    where p.golden_id = %s
    union all
    select i.occurrence_at::date, 'Immunization', i.description, initcap(coalesce(i.status, 'completed'))
    from immunizations i
    join patients p on p.id = i.patient_id
    where p.golden_id = %s
) timeline
where event_date is not null
order by event_date desc, event_type, title
limit 80
"""

RECENT_ENCOUNTERS_SQL = """
select
    e.id as encounter_id,
    e.start_date,
    e.end_date,
    e.encounter_type,
    coalesce(e.provider, 'Unknown') as provider,
    e.cost
from encounters e
join patients p on p.id = e.patient_id
where p.golden_id = %s
order by e.start_date desc nulls last
limit 25
"""

RECENT_CONDITIONS_SQL = f"""
select distinct
    c.onset_date,
    c.code,
    c.description,
    c.encounter_id
from conditions c
join patients p on p.id = c.patient_id
where p.golden_id = %s
  and c.description is not null
  and c.description !~* '{PROBLEM_NOISE_REGEX}'
order by c.onset_date desc nulls last, c.description
limit 50
"""

LATEST_VITALS_SQL = """
with ranked_vitals as (
    select distinct on (coalesce(o.description, o.code))
        coalesce(o.description, o.code) as vital_name,
        coalesce(
            nullif(o.value_text, ''),
            trim(concat(coalesce(o.value_numeric::text, ''), ' ', coalesce(o.value_unit, '')))
        ) as latest_value,
        case
            when coalesce(o.description, o.code) = 'Systolic Blood Pressure' and coalesce(o.value_numeric, 0) >= 140 then 'High'
            when coalesce(o.description, o.code) = 'Diastolic Blood Pressure' and coalesce(o.value_numeric, 0) >= 90 then 'High'
            when coalesce(o.description, o.code) = 'Body mass index (BMI) [Ratio]' and coalesce(o.value_numeric, 0) >= 30 then 'High'
            when coalesce(o.description, o.code) = 'Heart rate' and (coalesce(o.value_numeric, 0) >= 100 or coalesce(o.value_numeric, 999) < 50) then 'High'
            when coalesce(o.description, o.code) = 'Body temperature' and coalesce(o.value_numeric, 0) >= 38 then 'High'
            else 'Normal'
        end as interpretation,
        case
            when coalesce(o.description, o.code) in ('Systolic Blood Pressure', 'Diastolic Blood Pressure') then 'Assess trend and treatment response.'
            when coalesce(o.description, o.code) = 'Body mass index (BMI) [Ratio]' then 'Use with nutrition and activity counseling.'
            when coalesce(o.description, o.code) = 'Heart rate' then 'Review symptoms, rhythm, and medications if persistent.'
            when coalesce(o.description, o.code) = 'Body temperature' then 'Correlate with infection symptoms.'
            else 'Monitor in clinical context.'
        end as clinical_note,
        o.effective_at
    from observations o
    join patients p on p.id = o.patient_id
    where p.golden_id = %s
      and lower(coalesce(o.category, '')) like '%%vital%%'
      and coalesce(o.description, o.code) in (
        'Systolic Blood Pressure',
        'Diastolic Blood Pressure',
        'Body mass index (BMI) [Ratio]',
        'Heart rate',
        'Respiratory rate',
        'Body temperature',
        'Body Weight'
      )
    order by coalesce(o.description, o.code), o.effective_at desc nulls last
)
select *
from ranked_vitals
order by
    case vital_name
        when 'Systolic Blood Pressure' then 0
        when 'Diastolic Blood Pressure' then 1
        when 'Heart rate' then 2
        when 'Body mass index (BMI) [Ratio]' then 3
        when 'Body Weight' then 4
        when 'Respiratory rate' then 5
        when 'Body temperature' then 6
        else 7
    end,
    effective_at desc nulls last
"""

RECENT_LABS_SQL = f"""
select
    o.effective_at,
    coalesce(o.description, o.code) as test_name,
    coalesce(
        nullif(o.value_text, ''),
        trim(concat(coalesce(o.value_numeric::text, ''), ' ', coalesce(o.value_unit, '')))
    ) as result,
    case
        when coalesce(o.description, o.code) ilike 'Hemoglobin A1c%%' and coalesce(o.value_numeric, 0) >= 6.5 then 'High'
        when coalesce(o.description, o.code) ilike 'Cholesterol in LDL%%' and coalesce(o.value_numeric, 0) >= 130 then 'High'
        when coalesce(o.description, o.code) ilike 'Glomerular filtration rate%%' and coalesce(o.value_numeric, 999) < 60 then 'High'
        when coalesce(o.description, o.code) ilike 'Creatinine%%' and coalesce(o.value_numeric, 0) >= 1.5 then 'High'
        when coalesce(o.description, o.code) ilike 'Glucose%%' and coalesce(o.value_numeric, 0) >= 200 then 'High'
        when coalesce(o.description, o.code) ilike 'Hemoglobin%%' and coalesce(o.value_numeric, 999) < 10 then 'Low'
        else initcap(coalesce(nullif(o.interpretation, ''), 'normal'))
    end as interpretation
from observations o
join patients p on p.id = o.patient_id
where p.golden_id = %s
  and lower(coalesce(o.category, '')) like '%%laboratory%%'
  and coalesce(o.description, o.code) ~* '{KEY_LAB_REGEX}'
order by o.effective_at desc nulls last, test_name
limit 40
"""

PROCEDURES_SQL = """
select distinct
    pr.performed_start,
    pr.performed_end,
    pr.code,
    pr.description,
    pr.status,
    pr.encounter_id
from procedures pr
join patients p on p.id = pr.patient_id
where p.golden_id = %s
order by pr.performed_start desc nulls last, pr.description
limit 40
"""

REPORTS_SQL = """
select
    coalesce(dr.effective_at, dr.issued_at) as report_date,
    dr.code,
    dr.description,
    dr.status,
    left(coalesce(dr.report_text, ''), 300) as note_excerpt,
    dr.encounter_id
from diagnostic_reports dr
join patients p on p.id = dr.patient_id
where p.golden_id = %s
order by coalesce(dr.effective_at, dr.issued_at) desc nulls last, dr.description
limit 20
"""

IMMUNIZATIONS_SQL = """
select distinct
    i.occurrence_at,
    i.vaccine_code,
    i.description,
    i.status
from immunizations i
join patients p on p.id = i.patient_id
where p.golden_id = %s
order by i.occurrence_at desc nulls last, i.description
limit 30
"""

CARE_PLANS_SQL = """
select distinct
    cp.category,
    cp.description,
    cp.status,
    cp.intent,
    cp.start_date,
    cp.end_date,
    cp.activity_summary
from care_plans cp
join patients p on p.id = cp.patient_id
where p.golden_id = %s
  and coalesce(cp.status, 'active') in ('active', 'draft', 'on-hold')
order by cp.start_date desc nulls last, cp.description
limit 25
"""


def search_patients(connection, query: str, limit: int = 10) -> list[dict[str, Any]]:
    cleaned_query = (query or "").strip()
    params = {
        "term": cleaned_query,
        "exact": cleaned_query,
        "prefix": f"{cleaned_query}%" if cleaned_query else "%",
        "wildcard": f"%{cleaned_query}%" if cleaned_query else "%",
        "limit": max(1, min(limit, 50)),
    }
    return _fetch_all(connection, SEARCH_SQL, params)


def get_provider_chart(
    connection,
    golden_id: str,
    user_role: str,
    ip: str,
    action: str = "READ_PATIENT_CHART",
) -> dict[str, Any] | None:
    profile = _fetch_one(connection, PROFILE_SQL, (golden_id,))
    if profile is None:
        return None

    care_gaps = _fetch_all(connection, CARE_GAPS_SQL, (golden_id,))

    chart = {
        "golden_id": golden_id,
        "profile": profile,
        "summary": _fetch_one(connection, SUMMARY_SQL, (golden_id, golden_id)) or {},
        "emergency_snapshot": _fetch_one(
            connection,
            EMERGENCY_SNAPSHOT_SQL,
            (golden_id, golden_id, golden_id, golden_id, golden_id, golden_id),
        )
        or {},
        "acute_care_summary": _fetch_one(connection, ACUTE_CARE_SUMMARY_SQL, (golden_id,)) or {},
        "acute_care_events": _fetch_all(connection, ACUTE_CARE_EVENTS_SQL, (golden_id,)),
        "linked_records": _fetch_all(connection, LINKED_RECORDS_SQL, (golden_id,)),
        "allergies": _fetch_all(connection, ALLERGIES_SQL, (golden_id,)),
        "active_medications": _fetch_all(connection, ACTIVE_MEDICATIONS_SQL, (golden_id,)),
        "medication_safety_alerts": _fetch_all(
            connection,
            MEDICATION_SAFETY_ALERTS_SQL,
            (golden_id, golden_id),
        ),
        "active_problems": _fetch_all(connection, ACTIVE_PROBLEMS_SQL, (golden_id,)),
        "care_gaps": care_gaps,
        "clinical_alerts": care_gaps or _fetch_all(
            connection,
            CLINICAL_ALERTS_SQL,
            (golden_id, golden_id, golden_id, golden_id, golden_id, golden_id),
        ),
        "recent_encounters": _fetch_all(connection, RECENT_ENCOUNTERS_SQL, (golden_id,)),
        "recent_conditions": _fetch_all(connection, RECENT_CONDITIONS_SQL, (golden_id,)),
        "latest_vitals": _fetch_all(connection, LATEST_VITALS_SQL, (golden_id,)),
        "recent_labs": _fetch_all(connection, RECENT_LABS_SQL, (golden_id,)),
        "abnormal_labs": _fetch_all(connection, ABNORMAL_LABS_SQL, (golden_id,)),
        "procedures": _fetch_all(connection, PROCEDURES_SQL, (golden_id,)),
        "diagnostic_reports": _fetch_all(connection, REPORTS_SQL, (golden_id,)),
        "immunizations": _fetch_all(connection, IMMUNIZATIONS_SQL, (golden_id,)),
        "care_plans": _fetch_all(connection, CARE_PLANS_SQL, (golden_id,)),
        "timeline": _fetch_all(connection, TIMELINE_SQL, (golden_id, golden_id, golden_id, golden_id)),
    }

    log_access(
        connection,
        user_role,
        action,
        golden_id,
        f"GET /patients/chart/{golden_id}",
        ip,
    )
    return mask_response_for_role(chart, user_role)
