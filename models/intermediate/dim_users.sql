{{
    config(
        materialized="table",
    )
}}

with

user_metrics as (
  select
    user_id,
    min(first_order_date)         as first_order_date,
    max(order_date)               as last_order_date,
    count(distinct order_id)      as orders,
    count(distinct order_date)    as days_with_order,
    avg(datetime_diff(session_ended_at, session_started_at, second)) as avg_session_time_sec,
    avg(hours_until_shipped)      as avg_hours_until_shipped,
    sum(revenue)                  as revenue,
    sum(cost)                     as cost,
    sum(profit)                   as profit,
    avg(order_items)              as avg_order_items,
    avg(revenue)                  as revenue_per_order,
    avg(cost)                     as cost_per_order,
    avg(profit)                   as profit_per_order,
    avg(products)                 as products_per_order,
    avg(categories)               as categories_per_order,
    avg(brands)                   as brands_per_order,
  from {{ ref("fact_orders") }}
  group by 1
)

select
  u.*,
  um.*,
  date_trunc(first_order_date, month) as first_order_month,
  date_trunc(first_order_date, year) as first_order_year,
  case 
    when orders = 1 then 'First-Time'
    when orders > 1 then 'Repeat'
    else 'Never' end as segment_repeat
from {{ ref("stg_users") }} u
left join user_metrics um 
  on u.id = um.user_id
