# thelook-ecommerce

Building components of a modern data stack using sample e-commerce data from Google BigQuery. Specifically focused on:
- _Transformation_: using dbt to build pipelines with testing, CI/CD
- _Orchestration_: using Prefect to integrate with dbt and schedule automated processes in Python

<br>

## dbt

...


<br>

## Prefect

### Setting up Prefect

We must first set up our Prefect Cloud account before interacting with it locally.

1. Sign in or [register](https://docs.prefect.io/ui/cloud-getting-started/#sign-in-or-register) a Prefect Cloud account.
2. Create a [workspace](https://app.prefect.cloud/) for your account, or enter an existing workspace.
3. Create an API key to authorize a local execution environment. If you already have a key, access it [here](https://app.prefect.cloud/my/api-keys).

### Running Prefect

1. Log into Prefect Cloud, `prefect cloud login`. Use the API key you created during the setup.
2. (Optional) To change our workspace, enter: `prefect cloud workspace set`.
3. Start the Orion UI: `prefect orion start`

<br>

## Jupyter Notebooks

We use Jupyter Notebooks for ad hoc analysis. Runs locally but in a self-contained Docker image.

```
docker-compose -f docker-compose-jupyter.yaml up --build
```
