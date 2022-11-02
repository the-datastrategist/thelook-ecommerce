SELECT  
  *,
  DATE(created_at) AS created_date,
  DATE_DIFF(current_date(), DATE(created_at), DAY) as days_since_registration,
  DATE_DIFF(current_date(), DATE(created_at), WEEK) as weeks_since_registration,
  DATE_DIFF(current_date(), DATE(created_at), MONTH) as months_since_registration
FROM `bigquery-public-data.thelook_ecommerce.users`
