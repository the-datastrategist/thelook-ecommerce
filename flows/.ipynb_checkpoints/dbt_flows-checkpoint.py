from prefect.filesystems import GCS

# Load GCS storage block from Prefect
gcs_block = GCS.load("tds-dbt-storage")

