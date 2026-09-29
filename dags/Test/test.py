import io
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import boto3
import clickhouse_connect
import pandas as pd
import psycopg2
from botocore.config import Config
from dotenv import dotenv_values

try:                                    # Airflow 3
    from airflow.sdk import DAG, task
except ImportError:                     # Airflow 2
    from airflow import DAG
    from airflow.decorators import task
# Test pipeline


CFG = dotenv_values(Path(__file__).parent / ".env")


BAKU_TZ = ZoneInfo("Asia/Baku")

PG= { 'host':CFG.get('PG_HOST','localhost'),
         'port':CFG.get('PG_PORT','5433'),
         'dbname':CFG.get('PG_DATABASE','postgres'),
         'user':CFG.get('PG_USER','user'),
         'password':CFG.get('PG_PASSWORD','password')
         } 

CH = { 
        'host':CFG.get('CH_HOST','localhost'),
          'port':CFG.get('CH_PORT','8321'),
          'user':CFG.get('CH_USER','user'),
          'password':CFG.get('CH_PASSWORD','password')  
     } 

SW =    {
         'client':CFG.get('S3_ENDPOINT','localhost'),
          'server': CFG.get('S3_SERVER_ENDPOINT','localhost'),
          'bucket': CFG.get('S3_BUCKET','raw'),
          'key' : CFG.get('S3_KEY','key'),
          'secret': CFG.get('S3_SECRET','secret')   
        }

# the laptop uploads through one address; ClickHouse itself reads through anothe

TABLE = "raw_dev.pos_test"              # dev names only, hardcoded on purpose


def s3():
    return boto3.client("s3", endpoint_url=SW['client'],
                        aws_access_key_id=SW['key'], aws_secret_access_key=SW['secret'],
                        config=Config(s3={"addressing_style": "path"}))



with DAG("smoke_test_pos", start_date=datetime(2026, 9, 1, tzinfo=BAKU_TZ),
         schedule=None, catchup=False, tags=["test"]) as dag:

    @task
    def check_connections():
        with psycopg2.connect(**PG) as c, c.cursor() as cur:
            cur.execute("SELECT count(*) FROM turyan_retail.pos_transactions")
            print("Postgres OK, rows:", cur.fetchone()[0])
        print("SeaweedFS OK, buckets:",
              [b["Name"] for b in s3().list_buckets()["Buckets"]])
        print("ClickHouse OK, version:",
              clickhouse_connect.get_client(**CH).command("SELECT version()"))

    @task
    def extract(ds=None):
        with psycopg2.connect(**PG) as c:
            df = pd.read_sql("SELECT * FROM turyan_retail.pos_transactions "
                             "ORDER BY transaction_id LIMIT 100", c)
        key = f"_dev/smoke_test/dt={ds}/pos_test.parquet"
        client = s3()

        try:
            client.head_bucket(Bucket=CFG['S3_BUCKET']) 
        except Exception:    # noqa: BLE001
            client.create_bucket(Bucket=CFG['S3_BUCKET'])

        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        client.upload_fileobj(buf, CFG['S3_BUCKET'], key)
        print(f"Extracted {len(df)} rows -> s3://{CFG['S3_BUCKET']}/{key}")
        return {"key": key, "rows": len(df)}

    @task
    def load(info: dict):
        ch = clickhouse_connect.get_client(**CH)
        ch.command("CREATE DATABASE IF NOT EXISTS raw_dev")
        ch.command(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
            transaction_id Int64, store_code String, pos_sku String,
            customer_id String, quantity Int32, unit_price Decimal(10,2),
            transaction_date Date, updated_at DateTime
        ) ENGINE = MergeTree ORDER BY transaction_id""")
        ch.command(f"TRUNCATE TABLE {TABLE}")        # makes reruns idempotent
        ch.command(f"""INSERT INTO {TABLE}
            SELECT transaction_id, store_code, pos_sku, customer_id, quantity,
                   unit_price, transaction_date, updated_at
            FROM s3('{CFG['S3_SERVER_ENDPOINT']}/{CFG['S3_BUCKET']}/{info['key']}',
                    '{CFG['S3_KEY']}', '{CFG['S3_SECRET']}', 'Parquet')""")
        return info

    @task
    def verify(info: dict):
        n = clickhouse_connect.get_client(**CH).command(
            f"SELECT count() FROM {TABLE}")
        print(f"Extracted {info['rows']}, loaded {n}")
        assert n == info["rows"], "row count mismatch!"

    check_connections() >> verify(load(extract()))

if __name__ == "__main__":
    check_connections.function()
    info = extract.function()
    result = load.function(info)
    verify.function(result) 