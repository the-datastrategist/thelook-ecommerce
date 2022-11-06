import sys
from prefect import tasks, flow, get_run_logger


@flow
def dbt_flow(cmd='dbt run'):
    #return tasks.dbt.DbtShellTask(
    task = DbtShellTask(
        command=cmd,
        profile_name='default',
        environment='Development',
        dbt_kwargs={'type': 'bigquery'},
        overwrite_profiles=False,
        #profiles_dir=test_path
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