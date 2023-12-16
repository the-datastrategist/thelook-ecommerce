{{
    config(
        materialized="table",
    )
}}

{% set features = [
    "sequences",
    "sequences_to_cart",
    "sequences_to_purchase",
    "log_session_time_sec",
    "log_time_sec_per_sequence",
    "log_time_to_cart_sec",
    "events_with_product",
    "events_with_department",
    "events_with_cart",
    "events_with_purchase"
    ] %}

with 

session_clusters as (
  SELECT 
    *,

  FROM ML.PREDICT(
    MODEL `the-data-strategist.thelook_dbt.model_kmeans_sessions_k5`, 
    (
    SELECT
      session_id,
      coalesce(sequences, 0) as sequences,
      coalesce(sequences_to_cart, 0) as sequences_to_cart,
      coalesce(sequences_to_purchase, 0 ) as sequences_to_purchase,

      log(session_time_sec + (1 * exp(-10))) as log_session_time_sec,
      log((session_time_sec / sequences) + (1 * exp(-10))) as log_time_sec_per_sequence,
      log(time_to_cart_sec + (1 * exp(-10))) as log_time_to_cart_sec,

      events_with_product,
      events_with_department,
      events_with_cart,
      events_with_purchase,

      from {{ ref("fact_session") }}
    )
  )
),

session_zscores_components as (
    -- Calculate metrics for average z-scores
    select *,
        {% for feature in features %}
        avg({{ feature }}) over (partition by centroid_id) as cluster_avg_{{ feature }},
        avg({{ feature }}) over () as all_cluster_avg_{{ feature }},
        stddev({{ feature }}) over () as all_cluster_stddev_{{ feature }},
        {% endfor %}
    from session_clusters 
),

cluster_zscores as (
    -- Calculate average z-scores by cluster
    select *,
        {% for feature in features %}
        (cluster_avg_{{ feature }} - all_cluster_avg_{{ feature }}) / all_cluster_stddev_{{ feature }} as zscore_{{ feature }},
        {% endfor %}
    from session_zscores_components 
    ),


session_zscore_segments as (
  select *,
    {% for feature in features %}
    case 
        when zscore_{{ feature }} > 1 then 'VH'
        when zscore_{{ feature }} > 0.5 and zscore_{{ feature }} <= 1 then 'H'
        when zscore_{{ feature }} < -0.5 and zscore_{{ feature }} >= -1 then 'L'
        when zscore_{{ feature }} < -1 then 'VL'
        else 'M' end as seg_{{ feature }},
    {% endfor %}
  from cluster_zscores
)


select 
  zs.session_id,
  s.visitor_id,
  s.user_id,
  s.session_date,
  s.session_traffic_source,
  zs.* except(session_id),
--   concat(
--     {% for feature in features %}
--     if(seg_{{ feature }} = 'M', '', concat(seg_{{ feature }}, ' ', replace({{ feature }}, '_', ' ', ','))),
--     {% endfor %}
--     ) as cluster_description
from session_zscore_segments zs
join {{ ref("fact_session") }} s
using(session_id)