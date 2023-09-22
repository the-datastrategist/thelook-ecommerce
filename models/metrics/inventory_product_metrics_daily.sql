{{ config(materialized="table") }}

with

    date_range as (
        -- Select the past 30 days from the latest created_at date
        select asof_date
        from
            (
                select
                    generate_date_array(
                        (
                            select date_sub(max(date(created_at)), interval 29 day)
                            from
                                `bigquery-public-data.thelook_ecommerce.inventory_items`
                        ),
                        (
                            select max(date(created_at))
                            from
                                `bigquery-public-data.thelook_ecommerce.inventory_items`
                        )
                    ) as asof_dates
            ),
            unnest(asof_dates) as asof_date
    )

select
    asof_date,
    product_id,
    product_sku,
    product_category,
    product_name,
    product_brand,
    product_department,

    -- total historical metrics
    approx_count_distinct(date(created_at)) as days,
    sum(1) as items_total,
    sum(case when sold_at is null then 1 else 0 end) as items_available_total,
    sum(case when sold_at is not null then 1 else 0 end) as items_sold_total,
    sum(cost) as cost_total,
    sum(product_retail_price) as sales_total,
    avg(
        datetime_diff(datetime(sold_at), datetime(created_at), hour)
    ) as avg_hours_to_sale_total,

    -- 1 day metrics (asof_date)
    sum(case when asof_date = date(created_at) then 1 else 0 end) as items_1d,
    sum(
        case when asof_date = date(created_at) and sold_at is null then 1 else 0 end
    ) as items_available_1d,
    sum(
        case when asof_date = date(created_at) and sold_at is not null then 1 else 0 end
    ) as items_sold_1d,
    sum(case when asof_date = date(created_at) then cost else null end) as cost_1d,
    sum(
        case when asof_date = date(created_at) then product_retail_price else null end
    ) as sales_1d,
    avg(
        case
            when asof_date = date(created_at)
            then datetime_diff(datetime(sold_at), datetime(created_at), hour)
            else null
        end
    ) as avg_hours_to_sale_1d,

    -- 3 day metrics (incl asof_date)
    sum(
        case
            when date_diff(asof_date, date(created_at), day) between 0 and 2
            then 1
            else 0
        end
    ) as items_3d,
    sum(
        case
            when
                date_diff(asof_date, date(created_at), day) between 0 and 2
                and sold_at is null
            then 1
            else 0
        end
    ) as items_available_3d,
    sum(
        case
            when
                date_diff(asof_date, date(created_at), day) between 0 and 2
                and sold_at is not null
            then 1
            else 0
        end
    ) as items_sold_3d,
    sum(
        case
            when date_diff(asof_date, date(created_at), day) between 0 and 2
            then cost
            else null
        end
    ) as cost_3d,
    sum(
        case
            when date_diff(asof_date, date(created_at), day) between 0 and 2
            then product_retail_price
            else null
        end
    ) as sales_3d,
    avg(
        case
            when date_diff(asof_date, date(created_at), day) between 0 and 2
            then datetime_diff(datetime(sold_at), datetime(created_at), hour)
            else null
        end
    ) as avg_hours_to_sale_3d,

    -- 7 day metrics (incl asof_date)
    sum(
        case
            when date_diff(asof_date, date(created_at), day) between 0 and 6
            then 1
            else 0
        end
    ) as items_7d,
    sum(
        case
            when
                date_diff(asof_date, date(created_at), day) between 0 and 6
                and sold_at is null
            then 1
            else 0
        end
    ) as items_available_7d,
    sum(
        case
            when
                date_diff(asof_date, date(created_at), day) between 0 and 6
                and sold_at is not null
            then 1
            else 0
        end
    ) as items_sold_7d,
    sum(
        case
            when date_diff(asof_date, date(created_at), day) between 0 and 6
            then cost
            else null
        end
    ) as cost_7d,
    sum(
        case
            when date_diff(asof_date, date(created_at), day) between 0 and 6
            then product_retail_price
            else null
        end
    ) as sales_7d,
    avg(
        case
            when date_diff(asof_date, date(created_at), day) between 0 and 6
            then datetime_diff(datetime(sold_at), datetime(created_at), hour)
            else null
        end
    ) as avg_hours_to_sale_7d,

from `bigquery-public-data.thelook_ecommerce.inventory_items`
-- Include all inventory items created prior to the asof_date
join date_range on asof_date >= date(created_at)
group by 1, 2, 3, 4, 5, 6, 7
