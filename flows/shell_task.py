import datetime
import os

import prefect
from prefect import task, flow, get_run_logger
from prefect.tasks.shell import ShellTask



download = ShellTask(name="curl_task", max_retries=2, retry_delay=datetime.timedelta(seconds=10))




@flow
def shell_flow(cmd):

    task = ShellTask(name=cmd, max_retries=2, retry_delay=datetime.timedelta(seconds=10))
    logger = get_run_logger()
    logger.info("Command Run: %s!", cmd)
    logger.info("Command Result: %s!", task.json)
    return task


if __name__ == "__main__":
    cmd = sys.argv[1]
    shell_flow(cmd)
