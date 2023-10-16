{{
  config(
    materialized = "table",
    cluster_by = ["product_id", "date"],
  )
}}

{% set order_status = ["Shipped", "Complete", "Processing", "Cancelled"] %}

