# thelook-ecommerce

Building components of a modern data stack using sample e-commerce data from Google BigQuery. Specifically focused on:
- Transformation: using dbt to build pipelines with testing, CI/CD
- Orchestration: using Prefect to integrate with dbt and schedule automated processes in Python

<br>

## Setting up

#### Setting up Jupyter Notebooks

Use Jupyter Notebooks for ad hoc analysis.

```
docker-compose -f docker-compose-jupyter.yaml up --build
```
