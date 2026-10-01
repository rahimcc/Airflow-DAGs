import io
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3
import clickhouse_connect
import pandas as pd
import psycopg2
from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task
from botocore.config import Config

# Test pipeline

BAKU_TZ = ZoneInfo("Asia/Baku")
DBT_PROJECT_DIR = "/opt/airflow/dags_repo/current/dbt/turyan_retail"


def get_env():

    env = Variable.get("pos_env", default_var='dev')
    if env not in ("dev", "prod"):
        raise ValueError(f'pos_env must be dev or prod, GO {env!r}')
    return env

def get_table():
    env = get_env()
    db = 'raw' if env == 'prod' else 'raw_dev'
    return db

def s3_prefix():
    """Builds the key prefix, dev-scoped or not, based on that"""
    env = get_env()
    return '_dev/' if env == 'dev' else 'raw'



def get_pg_config():
    return { 'host': Variable.get('pg_host'),
         'port':Variable.get('pg_port'),
         'dbname':Variable.get('pg_database'),
         'user': Variable.get('pg_user'),
         'password': Variable.get('pg_postgres_password')
         }

def get_ch_config():
    return { 
        'host': Variable.get('ch_host'),
          'port': Variable.get('ch_port'),
          'user': Variable.get('ch_user'),
          'password': Variable.get('ch_password')
     } 


def get_sw_config():
   return {
         's3_client_endpoint': Variable.get('s3_client_endpoint'),
          's3_server_endpoint': Variable.get('s3_server_endpoint'),
          's3_bucket': Variable.get('s3_bucket'),
          's3_key' :  Variable.get('s3_key'),
          's3_secret': Variable.get('s3_secret')   
        }

# the laptop uploads through one address; ClickHouse itself reads through anothe

             # dev names only, hardcoded on purpose


def s3():

    SW = get_sw_config() 
    return boto3.client("s3", endpoint_url=SW['s3_client_endpoint'],
                        aws_access_key_id=SW['s3_key'], aws_secret_access_key=SW['s3_secret'],
                        config=Config(s3={"addressing_style": "path"}))



with DAG("smoke_test_pos", start_date=datetime(2026, 9, 1, tzinfo=BAKU_TZ),
         schedule=None, catchup=False, tags=["test"]) as dag:

    @task
    def check_connections():
        PG = get_pg_config()
        CH = get_ch_config()


        with psycopg2.connect(**PG) as c, c.cursor() as cur:
            cur.execute("SELECT count(*) FROM turyan_retail.pos_transactions")
            print("Postgres OK, rows:", cur.fetchone()[0])
        print("SeaweedFS OK, buckets:",
              [b["Name"] for b in s3().list_buckets()["Buckets"]])
        print("ClickHouse OK, version:",
              clickhouse_connect.get_client(**CH).command("SELECT version()"))

    @task
    def extract(ds=None):
        PG = get_pg_config()
        SW = get_sw_config()

        with psycopg2.connect(**PG) as c:
            df = pd.read_sql("SELECT * FROM turyan_retail.pos_transactions "
                             "ORDER BY transaction_id LIMIT 100", c)
        key = f"_dev/smoke_test/dt={ds}/pos_test.parquet"
        client = s3()

        try:
            client.head_bucket(Bucket=SW['s3_bucket']) 
        except Exception:    # noqa: BLE001
            client.create_bucket(Bucket=SW['s3_bucket'])

        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        client.upload_fileobj(buf, SW['s3_bucket'], key)
        print(f"Extracted {len(df)} rows -> s3://{SW['s3_bucket']}/{key}")
        return {"key": key, "rows": len(df)}

    @task
    def load(info: dict):
        CH = get_ch_config()
        SW = get_sw_config()
        TABLE = get_table()

        ch = clickhouse_connect.get_client(**CH)
        ch.command(f"CREATE DATABASE IF NOT EXISTS {TABLE}")
        ch.command(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
            transaction_id Int64, store_code String, pos_sku String,
            customer_id String, quantity Int32, unit_price Decimal(10,2),
            transaction_date Date, updated_at DateTime
        ) ENGINE = MergeTree ORDER BY transaction_id""")
        ch.command(f"TRUNCATE TABLE {TABLE}")        # makes reruns idempotent
        ch.command(f"""INSERT INTO {TABLE}
            SELECT transaction_id, store_code, pos_sku, customer_id, quantity,
                   unit_price, transaction_date, updated_at
            FROM s3('{SW['s3_server_endpoint']}/{SW['s3_bucket']}/{info['key']}',
                    '{SW['s3_key']}', '{SW['s3_secret']}', 'Parquet')""")
        return info

    @task
    def verify(info: dict):
        CH = get_ch_config()
        TABLE = get_table()

        n = clickhouse_connect.get_client(**CH).command(
            f"SELECT count() FROM {TABLE}")
        print(f"Extracted {info['rows']}, loaded {n}")
        assert n == info["rows"], "row count mismatch!"


    t_dbt_build = BashOperator(
                        task_id = "dbt_build",
                        bash_command = ( 
                                f'export PATH=/opt/dbt_venv/bin:$PATH &&'
                                f'cd {DBT_PROJECT_DIR} &&'
                                f'/opt/dbt_venv/bin/dbt-ol  build --target {{{{ var.value.pos_env }}}} --log-path /tmp/dbt_logs --target-path /tmp/dbt_target'),
                        env={
                            "CH_HOST": "{{ var.value.ch_host }}",
                            "CH_PORT": "{{ var.value.ch_port }}",
                            "CH_USER": "{{ var.value.ch_user }}",
                            "CH_PASSWORD": "{{ var.value.ch_password }}",
                            "OPENLINEAGE_URL": "{{ var.value.marquez_url }}",
                            "OPENLINEAGE_NAMESPACE": "turyan",
                        },
                        append_env = True
                )

    check_connections() >> verify(load(extract())) >> t_dbt_build