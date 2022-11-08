import sys
import prefect
from prefect import task, flow, get_run_logger
# from prefect.tasks.dbt.dbt import DbtShellTask


@flow
def dbt_flow(cmd='dbt run'):

    # Execute specified command
    task = prefect.tasks.dbt.dbt.DbtShellTask(
        command=cmd,
        profile_name='default',
        environment='Development',
        dbt_kwargs={'type': 'bigquery'},
        overwrite_profiles=False,
        profiles_dir='/home/jovyan/.dbt/profiles.yml'
    )
    logger = get_run_logger()
    logger.info("Command Run: %s!", name)
    return task

if __name__ == "__main__":
    cmd = sys.argv[1]
    dbt_flow(cmd)


# with Flow(name="dbt_flow") as f:
#     task = tasks.dbt.DbtShellTask(
#         profile_name='default',
#         environment='Development',
#         dbt_kwargs={'type': 'bigquery'},
#         overwrite_profiles=False,
#         #profiles_dir=test_path
#     )(command='dbt run')
#
# out = f.run()