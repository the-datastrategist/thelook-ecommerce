# thelook-ecommerce

Building components of a modern data stack using sample e-commerce data from Google BigQuery. Specifically focused on:
- _Transformation_: using dbt to build pipelines with testing, CI/CD
- _Orchestration_: using Prefect to integrate with dbt and schedule automated processes in Python

<br>

## Jupyter Notebooks

We use Jupyter Notebooks for ad hoc analysis. Runs locally but in a self-contained Docker image.
We recommend that you run DBT + Prefect through the Jupyter Lab CLI or through their respective cloud UIs.


```
docker-compose -f docker-compose.yaml up --build
```

<br>


## dbt

### Setting up DBT

We will run dbt from our command line. However, you can also use DBT Cloud to run queries from the `/models` directory.
See more details about Getting Started with DBT Core [here](https://docs.getdbt.com/docs/get-started/getting-started-dbt-core).

1. Initialize the dbt project with `dbt init {project-name}`. You will add this `project-name` elsewhere.
2. Update values in the `dbt_project.yml` file as needed. At the least, you'll need to change:
	```
	name: jaffle_shop # Change from the default, `my_new_project`
	...

	profile: jaffle_shop # Change from the default profile name, `default`
	...

	models:
	  jaffle_shop: # Change from `my_new_project` to match the previous value for `name:`
	    ...
	```

3. Set up a profile. After initializing, dbt will request the following profile information from you:
	- data warehouse (e.g. bigquery, redshift, snowflake)
	- authentication method (oauth or service_account)
	- keyfile (of the Service Account key)
	- project (i.e. GCP project ID)
	- dataset
	- location (US or EU)

	This will then output the file `/{home-dir}/.dbt/profiles.yml`. You can make adjustments to this file as needed.
	If you're connected through Jupyter Lab's Docker image, the profile is stored in `/home/jovyan/.dbt/profiles.yml`.

4. Run dbt. From here, you can run dbt from the command line. Below are some samples:
	```
	dbt run  # run all dbt scripts
	dbt run -s order_metrics_by_day  # run a specific dbt script
	```


### Running DBT

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

