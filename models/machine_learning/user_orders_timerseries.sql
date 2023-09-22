with

    user_first_last_order as (
        -- Get the first/last order dates for each user
        select
            user_id,
            min(order_date) as first_order_date,
            max(order_date) as last_order_date
        from {{ ref("orders_extended") }} -- `the-data-strategist.thelook_dbt.order_items_extended`
        group by 1
    ),

    date_range as (
        -- Get a list of dates, from the first to last order, across all users
        select asof_date
        from
            unnest(
                generate_date_array(
                    (select date(min(first_order_date)) from user_first_last_order),
                    (select date(max(last_order_date)) from user_first_last_order),
                    interval 1 day
                )
            ) as asof_date
    ),

    user_dates as (
        select dr.asof_date, uo.*
        from user_first_last_order uo
        cross join date_range dr
        where
            dr.asof_date >= uo.first_order_date
            and date_diff(dr.asof_date, uo.last_order_date, day) <= 90
    )

select
    asof_date,
    ud.user_id,

    ud.first_order_date,
    max(oe.order_date) as last_order_date,

    date_diff(asof_date, ud.first_order_date, day) as days_since_first_order,
    date_diff(asof_date, ud.first_order_date, week) as weeks_since_first_order,
    date_diff(asof_date, ud.first_order_date, month) as months_since_first_order,

    min(date_diff(asof_date, oe.order_date, day)) as days_since_last_order,
    min(date_diff(asof_date, oe.order_date, week)) as weeks_since_last_order,
    min(date_diff(asof_date, oe.order_date, month)) as months_since_last_order,

    sum(1) as orders,
    sum(if(order_status = 'Shipped', 1, 0)) as orders_shipped,
    sum(if(order_status = 'Complete', 1, 0)) as orders_complete,
    sum(if(order_status = 'Processing', 1, 0)) as orders_processing,
    sum(if(order_status = 'Cancelled', 1, 0)) as orders_cancelled,
    sum(if(order_status = 'Returned', 1, 0)) as orders_returned,

    avg(order_items) as avg_order_items,
    avg(hours_until_shipped) as avg_hours_until_shipped,
    avg(days_until_returned) as avg_days_until_returned,

    avg(products) as avg_order_products,
    avg(categories) as avg_order_categories,
    avg(brands) as avg_order_brands,
    avg(revenue) as avg_order_revenue,
    sum(revenue) as revenue,
    sum(cost) as cost,
    sum(profit) as profit,
    sum(discount) as discount,

    max(traffic_source) as traffic_source,
    max(age) as age,
    max(gender) as gender,
    max(country) as country,

from user_dates ud
inner join
    --`the-data-strategist.thelook_dbt.orders_extended` oe
    {{ ref("orders_extended") }} oe
    on ud.user_id = oe.user_id
    and ud.asof_date >= oe.order_date
group by 1, 2, ud.first_order_date
