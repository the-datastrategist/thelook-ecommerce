from prefect import task
from prefect.client import Secret
from prefect.tasks.dbt.dbt import DbtShellTask

dbt = DbtShellTask(
    return_all=True,
    profile_name="snowflake_slate",
    environment="dev",
    # profiles_dir=".",
    overwrite_profiles=True,
    log_stdout=True,
    helper_script="cd dbt",
    log_stderr=True,
    dbt_kwargs={
        "type": "snowflake",
        "account": Secret("SNOWFLAKE_ACCOUNT").get(),
        # User/password auth
        "user": Secret("DBT__SNOWFLAKE_USER").get(),
        "password": Secret("DBT__SNOWFLAKE_PASS").get(),
        "role": Secret("DBT__SNOWFLAKE_ROLE").get(),
        "database": Secret("DBT__SNOWFLAKE_DATABASE").get(),
        "warehouse": Secret("DBT__SNOWFLAKE__WAREHOUSE").get(),
        "schema": Secret("DBT__SCHEMA").get(),
        "threads": 12,
        "client_session_keep_alive": False,
    },
)


@task(trigger=all_finished)
def output_print(output):
    logger = prefect.context.get("logger")
    for o in output:
        logger.info(o)
