from prefect import Flow
from prefect.tasks.dbt import DbtShellTask

with Flow(name="dbt_flow") as f:
    task = DbtShellTask(
        profile_name='default',
        environment='Development',
        dbt_kwargs={'type': 'bigquery'},
        overwrite_profiles=False,
        #profiles_dir=test_path
    )(command='dbt run')

out = f.run()
