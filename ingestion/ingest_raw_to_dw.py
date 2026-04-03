import pyarrow.parquet as pq
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.io.path import ObjectStoragePath
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from pendulum import datetime
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from datetime import timezone, datetime as dt


@dag(
    start_date=datetime(2026, 3, 25),
    schedule=None,
    catchup=False,
    tags=['s3', 'bigquery']
)
def shipments_parquet_to_bigquery():

    @task
    def get_unprocessed_files() -> list[str]:
        """
        Scans S3 and BigQuery to return only files not yet loaded.
        """
        source_dir = ObjectStoragePath(
            Variable.get("shipments_parquet_dest_path"), conn_id="aws_dest"
        ) / "shipments"

        project_id = Variable.get("gcp_project_id")
        dataset_id = Variable.get("bigquery_dataset_id")
        table_id = Variable.get("bigquery_table_id")
        full_table = f"{project_id}.{dataset_id}.{table_id}"

        hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
        client = hook.get_client(project_id=project_id)

        loaded_files = set()
        try:
            query = f"SELECT DISTINCT _source_file FROM `{full_table}`"
            loaded_files = {row._source_file for row in client.query(query).result()}
            print(f"Already loaded: {loaded_files}")
        except NotFound:
            print("Table not found, will be created on first load.")

        unprocessed = []
        for parquet_file in source_dir.iterdir():
            if not str(parquet_file).endswith('.parquet'):
                continue
            if parquet_file.name not in loaded_files:
                unprocessed.append(parquet_file.name)
                print(f"Queued: {parquet_file.name}")

        print(f"Total files to process: {len(unprocessed)}")
        return unprocessed


    @task(max_active_tis_per_dag=1)
    def load_file_to_bigquery(file_name: str) -> None:
        """
        Streams one parquet file from S3 into BigQuery in small batches.
        Only batch_size rows in memory at a time — no OOM.
        Runs one file at a time via max_active_tis_per_dag=1.
        """
        source_dir = ObjectStoragePath(
            Variable.get("shipments_parquet_dest_path"), conn_id="aws_dest"
        ) / "shipments"

        project_id = Variable.get("gcp_project_id")
        dataset_id = Variable.get("bigquery_dataset_id")
        table_id = Variable.get("bigquery_table_id")
        full_table = f"{project_id}.{dataset_id}.{table_id}"

        hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
        client = hook.get_client(project_id=project_id)

        parquet_file = source_dir / file_name
        batch_size = 1000  # tune down to 1000 if still OOM

        print(f"Loading {file_name} in batches of {batch_size} rows...")

        first_batch = True

        with parquet_file.open('rb') as f:
            parquet_reader = pq.ParquetFile(f)

            for i, batch in enumerate(parquet_reader.iter_batches(batch_size=batch_size)):
                df = batch.to_pandas()

                # Add audit columns
                df['_source_file'] = file_name
                df['_loaded_at'] = dt.now(timezone.utc)

                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    autodetect=first_batch,
                )

                job = client.load_table_from_dataframe(
                    df, full_table, job_config=job_config
                )
                job.result()

                first_batch = False
                print(f"Loaded batch {i} of {file_name} ({len(df)} rows)")

        print(f"Finished {file_name} → {full_table}")


    # Dynamic task mapping — one task per file, one at a time
    files = get_unprocessed_files()
    load_file_to_bigquery.expand(file_name=files)


shipments_parquet_to_bigquery()