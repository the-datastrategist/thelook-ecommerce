SELECT
  table_catalog as project_id,
  table_schema as dataset_id,
  table_name as table_id,
  concat(table_catalog, '.', table_schema, '.', table_name) as full_table_id,
  table_type,
  is_insertable_into,
  is_typed,
  date(creation_time) as created_date,
  creation_time as created_at,
  base_table_catalog,
  base_table_schema,
  base_table_name,
  ddl,

FROM
  `the-data-strategist.thelook_dbt.INFORMATION_SCHEMA.TABLES`