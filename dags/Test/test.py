import io
import logging
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
log = logging.getLogger(__name__)
BAKU_TZ = ZoneInfo("Asia/Baku")
DBT_PROJECT_DIR = "/opt/airflow/dags_repo/current/dbt/turyan_retail"


def get_env():

    env = Variable.get("pos_env", default_var='dev')
    if env not in ("dev", "prod"):
        raise ValueError(f'Env must be dev or prod, GO {env!r}')
    return env

def get_raw_db():
    """" Returns raw database based on environment. """
    env = get_env()
    db = 'raw' if env == 'prod' else 'raw_dev'
    return db

def get_analytics_db():
    """ Returns analytics database based on environment. """
    env = get_env()
    db = 'analytics' if env == 'prod' else 'analytics_dev'
    return db

def get_s3_prefix():
    """Builds the key prefix, based on environment"""
    env = get_env()
    return '_prod/smoke_test' if env == 'prod' else '_dev/smoke_test'



def get_pg_config():
    return { 
         'host':  Variable.get('postgres', deserialize_json=True)['pg_host'],
         'port':  Variable.get('postgres', deserialize_json=True)['pg_port'],
         'dbname':Variable.get('postgres', deserialize_json= True)['pg_database'],
         'user':  Variable.get('postgres', deserialize_json=True)['pg_user'],
         'password': Variable.get('postgres', deserialize_json=True)['pg_postgres_password']
         }

def get_ch_config():
    return { 
        'host': Variable.get('clickhouse',deserialize_json=True)['ch_host'],
        'port': Variable.get('clickhouse',deserialize_json=True )['ch_port'],
        'user': Variable.get('clickhouse',deserialize_json=True)['ch_user'],
        'password': Variable.get('clickhouse', deserialize_json=True)['ch_password']
     } 

def get_sw_config():
   return {
          's3_client_endpoint': Variable.get('s3', deserialize_json=True)['s3_client_endpoint'],
          's3_server_endpoint': Variable.get('s3', deserialize_json=True)['s3_server_endpoint'],
          's3_bucket': Variable.get('s3', deserialize_json=True)['s3_bucket'],
          's3_key' :   Variable.get('s3', deserialize_json=True)['s3_key'],
          's3_secret': Variable.get('s3', deserialize_json=True)['s3_secret']   
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
        SW = get_sw_config()

        log.info('Postgres: Testing Connection')
        with psycopg2.connect(**PG) as c, c.cursor() as cur:
            cur.execute("SELECT count(*) FROM turyan_retail.pos_transactions")
            log.info(f"Postgres OK, rows: {cur.fetchone()[0]}")
        log.info('Seaweedfs: Testing Connection')
        log.info(f'SeaweedFS OK, buckets:{[b["Name"] for b in s3().list_buckets()["Buckets"]]}')

        log.info('Clickhouse: Testing connection.')
        log.info(f'ClickHouse OK, version:{clickhouse_connect.get_client(**CH).command("SELECT version()")}')
        
        env = get_env()
        s3_prefix = get_s3_prefix()
        raw_db  = get_raw_db()
        analytics_db = get_analytics_db() 

        print("="*15)
        print(f'Environment: {env}')
        print(f'S3 path: s3://{SW["s3_bucket"]}/{s3_prefix}')
        print(f'Raw Database: {raw_db}')
        print(f'Analytics database: {analytics_db}')


    @task
    def extract(ds=None):
        PG = get_pg_config()
        SW = get_sw_config()
        prefix = get_s3_prefix()

        log.info('Postgres: Connecting to database')
        with psycopg2.connect(**PG) as c:
            df = pd.read_sql("SELECT * FROM turyan_retail.pos_transactions "
                             "ORDER BY transaction_id LIMIT 100", c)

        log.info(f'Postgres Connection succesful. returned rows: {df.shape[0]}')


        
        key = f"{prefix}/dt={ds}/pos_test.parquet"
        client = s3()

        log.info('seaweedfs: Connecting to s3')
        try:
            client.head_bucket(Bucket=SW['s3_bucket']) 
        except Exception:    # noqa: BLE001
            client.create_bucket(Bucket=SW['s3_bucket'])
        log.info('seaweedfs: Connected to s3')
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
        TABLE = get_raw_db()

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
        TABLE = get_raw_db()

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
                            "CH_HOST": "{{ var.json.clickhouse.ch_host }}",
                            "CH_PORT": "{{ var.json.clickhouse.ch_port }}",
                            "CH_USER": "{{ var.json.clickhouse.ch_user }}",
                            "CH_PASSWORD": "{{ var.json.clickhouse.ch_password }}",
                            "OPENLINEAGE_URL": "{{ var.value.marquez_url }}",
                            "OPENLINEAGE_NAMESPACE": "turyan_clickhouse",
                        },
                        append_env = True
                )

    check_connections() >> verify(load(extract())) >> t_dbt_build