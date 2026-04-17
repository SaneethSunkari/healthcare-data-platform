{{ config(materialized="table") }}

with population as (
    select count(distinct golden_id) as total_patients
    from patients
    where golden_id is not null
)
select
    c.code,
    c.description,
    count(distinct p.golden_id) as patient_count,
    round(
        count(distinct p.golden_id) * 100.0
        / nullif((select total_patients from population), 0),
        2
    ) as prevalence_pct,
    round(avg(date_part('year', age(p.birth_date)))::numeric, 1) as avg_patient_age
from conditions c
join patients p on c.patient_id = p.id
where p.golden_id is not null
group by c.code, c.description
order by patient_count desc, c.description
limit 50
