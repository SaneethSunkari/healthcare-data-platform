{{ config(materialized="table") }}

with patient_base as (
    select
        golden_id,
        max(date_part('year', age(birth_date))) as age,
        max(gender) as gender,
        max(zip_code) as zip_code
    from patients
    where golden_id is not null
    group by golden_id
),
encounters_by_patient as (
    select
        patient_id,
        count(distinct id) as total_encounters,
        coalesce(sum(cost), 0)::numeric(14, 2) as total_cost,
        max(start_date) as last_visit
    from encounters
    group by patient_id
),
conditions_by_patient as (
    select
        patient_id,
        count(distinct id) as total_conditions
    from conditions
    group by patient_id
),
medications_by_patient as (
    select
        patient_id,
        count(distinct id) as total_medications
    from medications
    group by patient_id
),
patient_rollup as (
    select
        p.golden_id,
        coalesce(sum(e.total_encounters), 0) as total_encounters,
        coalesce(sum(c.total_conditions), 0) as total_conditions,
        coalesce(sum(m.total_medications), 0) as total_medications,
        coalesce(sum(e.total_cost), 0)::numeric(14, 2) as total_cost,
        max(e.last_visit) as last_visit
    from patients p
    left join encounters_by_patient e on e.patient_id = p.id
    left join conditions_by_patient c on c.patient_id = p.id
    left join medications_by_patient m on m.patient_id = p.id
    where p.golden_id is not null
    group by p.golden_id
)
select
    b.golden_id,
    b.age,
    b.gender,
    b.zip_code,
    r.total_encounters,
    r.total_conditions,
    r.total_medications,
    r.total_cost,
    r.last_visit
from patient_base b
join patient_rollup r on r.golden_id = b.golden_id
