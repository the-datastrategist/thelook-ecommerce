# deployment.py
from prefect.deployments import Deployment

def build_deployment(
    flow_object,
    flow_name,
    parameters={},
    infra_overrides={"env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}},
    work_queue_name='test'
):
    return Deployment.build_from_flow(
        flow=flow_object,
        name=flow_name,
        parameters=parameters,
        infra_overrides=infra_overrides,
        work_queue_name=work_queue_name,
    )


# deployment = Deployment.build_from_flow(
#     flow=log_flow,
#     name="log-simple-python",
#     parameters={"name": "Larry David"},
#     infra_overrides={"env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}},
#     work_queue_name="test",
# )

from log_flow import log_flow
deployment_1 = build_deployment(
    flow_object=log_flow,
    flow_name='log-flow-python',
    parameters={"name": "Larry David"},
    work_queue_name="worker_1"
)

from catfact import api_flow
deployment_2 = build_deployment(
    flow_object=api_flow,
    flow_name='cat-fact-python',
    parameters={"url": "https://catfact.ninja/fact"},
    work_queue_name="worker_2"
)

if __name__ == "__main__":
    deployment_1.apply()
    deployment_2.apply(url)
