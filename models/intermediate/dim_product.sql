{{
  config(
    materialized = "table",
    cluster_by = ["product_id", "date"],
  )
}}

{% set order_status = ["Shipped", "Complete", "Processing", "Cancelled"] %}

-- TO DO: Make updates to this table
select * from {{ ref("stg_product") }}
