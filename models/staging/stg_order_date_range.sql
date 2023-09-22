select asof_date
from
    (
        select
            generate_date_array(
                (
                    select min(date(created_at))
                    from `bigquery-public-data.thelook_ecommerce.orders`
                ),
                (
                    select max(date(created_at))
                    from `bigquery-public-data.thelook_ecommerce.orders`
                )
            ) as asof_dates
    ),
    unnest(asof_dates) as asof_date
