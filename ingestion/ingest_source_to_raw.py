import ijson
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
import os
import boto3
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.io.path import ObjectStoragePath
from airflow.hooks.base import BaseHook
from pendulum import datetime


@dag(
    start_date=datetime(2026, 3, 25),
    schedule=None,
    catchup=False,
    tags=['s3', 'cross-account']
)
def shipments_s3_json_to_parquet_etl():

    @task
    def stream_json_to_parquet():
        source_dir = ObjectStoragePath(
            Variable.get("shipments_json_source_path"), conn_id="aws_source"
        )

        # Get Account B credentials from Airflow connection for boto3
        dest_conn = BaseHook.get_connection("aws_dest")
        dest_path = Variable.get("shipments_parquet_dest_path").replace("s3://", "")
        dest_bucket = dest_path.split("/")[0]
        dest_prefix = "raw/shipments"

        s3_dest = boto3.client(
            "s3",
            aws_access_key_id=dest_conn.login,
            aws_secret_access_key=dest_conn.password,
            region_name=dest_conn.extra_dejson.get("region_name", "eu-west-2"),
        )

        # Build set of already processed files
        processed = set()
        try:
            response = s3_dest.list_objects_v2(Bucket=dest_bucket, Prefix=dest_prefix)
            for obj in response.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".parquet"):
                    file_name = key.split("/")[-1].replace(".parquet", "")
                    processed.add(file_name)
            print(f"Already processed: {processed}")
        except Exception as e:
            print(f"Could not list destination bucket: {e}")

        for json_file in source_dir.iterdir():
            if not str(json_file).endswith('.json'):
                continue

            file_id = json_file.name.replace('.json', '')

            if file_id in processed:
                print(f"Skipping {json_file.name} — already exists in destination")
                continue

            print(f"Processing {json_file.name}...")

            # 1. Write to local temp file
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
                tmp_path = tmp.name

            try:
                with json_file.open('rb') as f:
                    writer = None
                    chunk = []

                    for record in ijson.items(f, 'item'):
                        chunk.append(record)

                        if len(chunk) >= 50000:
                            table = pa.Table.from_pandas(pd.DataFrame(chunk))
                            if writer is None:
                                writer = pq.ParquetWriter(tmp_path, table.schema)
                            writer.write_table(table)
                            chunk = []
                            print(f"  Wrote 50000 rows for {json_file.name}")

                    if chunk:
                        table = pa.Table.from_pandas(pd.DataFrame(chunk))
                        if writer is None:
                            writer = pq.ParquetWriter(tmp_path, table.schema)
                        writer.write_table(table)
                        print(f"  Wrote final {len(chunk)} rows for {json_file.name}")

                    if writer:
                        writer.close()

                # 2. Upload to S3 using boto3 multipart upload
                s3_key = f"{dest_prefix}/{file_id}.parquet"
                print(f"Uploading {file_id}.parquet to Account B...")
                s3_dest.upload_file(
                    tmp_path,
                    dest_bucket,
                    s3_key,
                    ExtraArgs={"ContentType": "application/octet-stream"},
                    Config=boto3.s3.transfer.TransferConfig(
                        multipart_threshold=50 * 1024 * 1024,  # 50MB
                        multipart_chunksize=10 * 1024 * 1024,  # 10MB chunks
                        max_concurrency=4,
                    )
                )
                print(f"Uploaded {file_id}.parquet → s3://{dest_bucket}/{s3_key}")

            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
                    print(f"Cleaned up temp file {tmp_path}")

    stream_json_to_parquet()


shipments_s3_json_to_parquet_etl()