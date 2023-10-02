{{ config(materialized="table") }}

with

    date_range as (select * from {{ ref("stg_order_date_range") }}),

    product_metrics_1d as (
        select
            asof_date,
            product_id,

            -- 1D metrics (asof_date = created_date)
            sum(
                case when asof_date = date(created_at) then sale_price else 0 end
            ) as sales_1d,
            approx_count_distinct(
                case when asof_date = date(created_at) then user_id else null end
            ) as users_1d,
            approx_count_distinct(
                case when asof_date = date(created_at) then order_id else null end
            ) as orders_1d,
            approx_count_distinct(
                case
                    when asof_date = date(created_at) then inventory_item_id else null
                end
            ) as items_1d,

            -- 7D metrics
            sum(
                case
                    when date_diff(asof_date, date(created_at), day) between 0 and 8
                    then sale_price
                    else 0
                end
            ) as sales_7d,
            approx_count_distinct(
                case
                    when date_diff(asof_date, date(created_at), day) between 0 and 8
                    then user_id
                    else null
                end
            ) as users_7d,
            approx_count_distinct(
                case
                    when date_diff(asof_date, date(created_at), day) between 0 and 8
                    then order_id
                    else null
                end
            ) as orders_7d,
            approx_count_distinct(
                case
                    when date_diff(asof_date, date(created_at), day) between 0 and 8
                    then inventory_item_id
                    else null
                end
            ) as items_7d,

            -- 28D metrics (entire window)
            sum(sale_price) as sales_28d,
            approx_count_distinct(user_id) as users_28d,
            approx_count_distinct(order_id) as orders_28d,
            approx_count_distinct(inventory_item_id) as items_28d,

        from {{ ref("stg_order_items") }}
        join date_range on date_diff(asof_date, date(created_at), day) between 0 and 29
        group by 1, 2
    )

select prod.category, prod.name, prod.brand, prod.department, prod.sku, metric.*,
from product_metrics_1d metric
join {{ ref("stg_products") }} prod on metric.product_id = prod.id
