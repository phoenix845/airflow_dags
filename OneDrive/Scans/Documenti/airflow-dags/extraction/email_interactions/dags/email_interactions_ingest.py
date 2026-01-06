from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime
import os
import pandas as pd
import yaml
import psycopg2

# =====================
# CONFIG
# =====================
DATASET = "email_interactions"

BASE_PATH = "/opt/airflow/extraction/email_interactions"
CSV_PATH = f"{BASE_PATH}/sample_data/email_interactions.csv"
SCHEMA_PATH = f"{BASE_PATH}/config/schema_expected.yaml"
DDL_PATH = f"{BASE_PATH}/config/create_table.sql"
TMP_PATH = "/opt/airflow/tmp/email_interactions_clean.csv"

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")


# =====================
# TASKS
# =====================
def check_file():
    if not os.path.exists(CSV_PATH):
        raise AirflowFailException(f"CSV file not found at {CSV_PATH}")


def transform_data():
    df = pd.read_csv(CSV_PATH)

    # 🔁 Normalize source column names → target schema names
    rename_map = {
        "message_body": "message_text",
        "email_types": "email_type",
        "email_status": "email_state",
    }
    df.rename(columns=rename_map, inplace=True)

    # 📖 Load schema
    with open(SCHEMA_PATH) as f:
        schema = yaml.safe_load(f)

    expected_cols = [c["name"] for c in schema["columns"]]

    # ➕ Ensure ALL expected columns exist
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    # 🧹 Clean data
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df.replace("", None, inplace=True)

    # 📐 Reorder columns SAFELY (no KeyError now)
    df = df[expected_cols]

    os.makedirs("/opt/airflow/tmp", exist_ok=True)
    df.to_csv(TMP_PATH, index=False)



def validate_schema():
    df = pd.read_csv(TMP_PATH)

    with open(SCHEMA_PATH) as f:
        schema = yaml.safe_load(f)

    expected_cols = [c["name"] for c in schema["columns"]]

    if list(df.columns) != expected_cols:
        raise AirflowFailException(
            f"Schema mismatch AFTER transform. "
            f"Got={list(df.columns)} Expected={expected_cols}"
        )


def load_to_postgres():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    cur = conn.cursor()

    # Create table if not exists
    with open(DDL_PATH) as f:
        cur.execute(f.read())

    df = pd.read_csv(TMP_PATH)
    columns = list(df.columns)

    insert_sql = f"""
        INSERT INTO public.email_interactions ({",".join(columns)})
        VALUES ({",".join(["%s"] * len(columns))})
        ON CONFLICT (thread_id) DO NOTHING
    """

    for _, row in df.iterrows():
        cur.execute(insert_sql, tuple(row))

    conn.commit()
    cur.close()
    conn.close()


# =====================
# DAG
# =====================
with DAG(
    dag_id="email_interactions_ingest",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["story3"],
) as dag:

    check = PythonOperator(
        task_id="check_file",
        python_callable=check_file,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    validate = PythonOperator(
        task_id="validate_schema",
        python_callable=validate_schema,
    )

    load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    # ✅ CORRECT ORDER
    check >> transform >> validate >> load
