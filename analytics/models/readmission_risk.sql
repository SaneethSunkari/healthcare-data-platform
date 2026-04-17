{{ config(materialized="table") }}

with patient_base as (
    select
        golden_id,
        max(date_part('year', age(birth_date))) as age,
        max(gender) as gender
    from patients
    where golden_id is not null
    group by golden_id
),
recent_encounter_rollup as (
    select
        p.golden_id,
        count(distinct e.id) as encounter_count,
        coalesce(sum(e.cost), 0)::numeric(14, 2) as total_cost,
        max(e.start_date) as last_encounter
    from patients p
    left join encounters e
        on e.patient_id = p.id
       and e.start_date >= now() - interval '12 months'
    where p.golden_id is not null
    group by p.golden_id
)
select
    b.golden_id,
    b.age,
    b.gender,
    e.encounter_count,
    e.total_cost,
    e.last_encounter,
    case
        when e.encounter_count >= 4 then 'high'
        when e.encounter_count >= 2 then 'medium'
        else 'low'
    end as readmission_risk
from patient_base b
join recent_encounter_rollup e on e.golden_id = b.golden_id
order by e.encounter_count desc, e.total_cost desc
