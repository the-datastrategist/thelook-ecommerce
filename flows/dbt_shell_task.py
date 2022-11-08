import sys
import config
from prefect import task, flow, get_run_logger
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from prefect_dbt.cli import DbtCliProfile


PROFILE_DIRECTORY = config.PROFILE_DIRECTORY
PROFILE_BLOCK = config.PROFILE_BLOCK


@flow
def trigger_dbt_cli_command_flow(cmd) -> str:
    result = trigger_dbt_cli_command(
        command=cmd,
        profiles_dir=PROFILE_DIRECTORY,
    )
    logger = get_run_logger()
    logger.info("Command Run: %s", cmd)
    logger.info("Command Result: %s", result)
    return result # Returns the last line the in CLI output


if __name__ == "__main__":
    cmd = sys.argv[1]
    trigger_dbt_cli_command_flow(cmd)
