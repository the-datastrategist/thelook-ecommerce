{{
  config(materialized = "table")
}}

{% set order_status = ["Shipped", "Complete", "Processing", "Cancelled"] %}

-- TO DO: Make updates to this table
select * from {{ ref("stg_product") }}
