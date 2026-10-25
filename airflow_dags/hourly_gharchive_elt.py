from datetime import datetime, timedelta
import os
import requests
import gzip
from airflow.sdk import dag, task
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.operators.bash import BashOperator
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

def get_snowflake_conn():
    PRIVATE_KEY_PATH = "/home/narumbunathan/keys/rsa_key.p8"
    with open(PRIVATE_KEY_PATH, "rb") as key_file:
        p_key = serialization.load_pem_private_key(key_file.read(), password=None, backend=default_backend())
    pkb = p_key.private_bytes(encoding=serialization.Encoding.DER, format=serialization.PrivateFormat.PKCS8, encryption_algorithm=serialization.NoEncryption())
    return snowflake.connector.connect(account="gp19057.me-central2.gcp", user="GH_AIRFLOW_USER", private_key=pkb, role="GH_ARCHIVE_ROLE", warehouse="GH_INGEST_WH", database="GH_ARCHIVE_LOCAL", schema="BRONZE")

default_args = {
    "owner": "data_eng",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="hourly_gharchive_elt",
    default_args=default_args,
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gharchive", "snowflake", "incremental", "dbt"],
)
def gharchive_elt_pipeline():

    @task.sensor(poke_interval=90, timeout=1800, mode="reschedule")
    def poll_for_archive(**context) -> PokeReturnValue:
        target_time = context["data_interval_start"] - timedelta(hours=2)
        url = f"https://data.gharchive.org/{target_time.strftime('%Y-%m-%d')}-{str(target_time.hour)}.json.gz"
        return PokeReturnValue(is_done=requests.head(url, timeout=10).status_code == 200, xcom_value=url)

    # FIX: Removed multiple_outputs=True, changing return type to list
    @task
    def download_and_shard_archive(archive_url: str, **context) -> list:
        target_time = context["data_interval_start"] - timedelta(hours=2)
        base_file_name = target_time.strftime('%Y-%m-%d-%H')
        staging_dir = os.path.expanduser("~/code/de_projects/gh_archive/staging")
        os.makedirs(staging_dir, exist_ok=True)
        master_file_path = os.path.join(staging_dir, f"{base_file_name}.json.gz")

        with requests.get(archive_url, stream=True, timeout=60) as response:
            response.raise_for_status()
            with open(master_file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024): f.write(chunk)

        shard_paths, shard_index, lines_per_shard = [], 0, 50000 
        try:
            with gzip.open(master_file_path, 'rt', encoding='utf-8') as master_file:
                current_shard_path = os.path.join(staging_dir, f"{base_file_name}_part{shard_index}.json.gz")
                current_shard = gzip.open(current_shard_path, 'wt', encoding='utf-8')
                shard_paths.append(current_shard_path)
                for line_number, line in enumerate(master_file):
                    if line_number > 0 and line_number % lines_per_shard == 0:
                        current_shard.close() 
                        shard_index += 1      
                        current_shard_path = os.path.join(staging_dir, f"{base_file_name}_part{shard_index}.json.gz")
                        current_shard = gzip.open(current_shard_path, 'wt', encoding='utf-8')
                        shard_paths.append(current_shard_path)
                    current_shard.write(line)
                current_shard.close()
        finally:
            os.remove(master_file_path)
        
        # FIX: Simply returning the list of paths now
        return shard_paths

    @task
    def put_shard_to_snowflake(shard_path: str, **context) -> str:
        partition_path = (context["data_interval_start"] - timedelta(hours=2)).strftime("%Y/%m/%d")
        conn = get_snowflake_conn()
        try:
            conn.cursor().execute(f"PUT file://{shard_path} @GH_ARCHIVE_LOCAL.BRONZE.INTERNAL_GHARCHIVE_STAGE/{partition_path}/ AUTO_COMPRESS = FALSE OVERWRITE = TRUE;")
        finally:
            conn.close()
            if os.path.exists(shard_path): os.remove(shard_path)
        return f"{partition_path}/{os.path.basename(shard_path)}"

    @task
    def load_bronze_only(staged_rel_paths: list):
        conn = get_snowflake_conn()
        try:
            files_str = ", ".join([f"'{path}'" for path in staged_rel_paths])
            conn.cursor().execute(f"""
                COPY INTO GH_ARCHIVE_LOCAL.BRONZE.RAW_EVENTS (raw_payload, source_file_name, source_file_row)
                FROM (SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER FROM @GH_ARCHIVE_LOCAL.BRONZE.INTERNAL_GHARCHIVE_STAGE)
                FILES = ({files_str}) PURGE = TRUE;
            """)
        finally:
            conn.close()

    run_dbt_silver_and_gold = BashOperator(
        task_id="run_dbt_silver_and_gold_models",
        bash_command="cd /home/narumbunathan/dbt/gh_archive_dbt && dbt run",
    )

    url = poll_for_archive()
    
    # FIX: Catching the list directly
    shards_list = download_and_shard_archive(url)
    
    # FIX: Passing the XComArg list directly into the expand function
    staged_shard_paths = put_shard_to_snowflake.expand(shard_path=shards_list)
    
    staged_shard_paths >> load_bronze_only(staged_shard_paths) >> run_dbt_silver_and_gold

dag_instance = gharchive_elt_pipeline()