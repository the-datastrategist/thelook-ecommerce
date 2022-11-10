"""
    dbt_shell_task_deployment.py
    
    Build  deployment from dbt_shell_task. Loads deployment to Orion UI.

    Docs:
        https://docs.prefect.io/api-ref/prefect/deployments/
"""

from dbt_shell_task import trigger_dbt_cli_command_flow
from prefect.deployments import Deployment
from prefect.filesystems import GCS
from prefect.infrastructure.docker import DockerContainer


# Load deployment storage and infrastructure
docker_container_block = DockerContainer.load("dbt-docker")
storage = GCS.load("tds-dbt-storage")

# Build deployment
deployment = Deployment.build_from_flow(
    flow=trigger_dbt_cli_command_flow,
    name="dbt_shell_task",
    tags=["dbt"],
    storage=storage,
    infrastructure=docker_container_block,
    parameters={"cmd": "dbt run"},
    work_queue_name="dbt_worker",
)

if __name__ == "__main__":
    deployment.apply()
    print("Deployment has been built! See Deployments in the Prefect Orion UI for details.")
    print(deployment.json)
