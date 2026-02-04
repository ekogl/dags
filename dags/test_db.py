from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import os
import psycopg2

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="arbo_db_connectivity_test",
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    @task
    def test_postgres_connection():
        # 1. Pull from Env (The values set in your YAML)
        host = os.getenv("ARBO_DB_HOST")
        port_raw = os.getenv("ARBO_DB_PORT", "5432")
        db_name = os.getenv("ARBO_DB_NAME", "arbo_data")
        user = os.getenv("ARBO_DB_USER", "arbo_user")
        password = os.getenv("ARBO_DB_PASS", "arbo_pass")

        print(f"--- STARTING CONNECTION TEST ---")
        print(f"Testing Host: {host}")
        print(f"Testing Port (Raw String): {port_raw}")
        
        try:
            # 2. Convert to INT (This is what your library might be failing to do)
            port_int = int(port_raw)
            print(f"Connecting with Integer Port: {port_int}")

            conn = psycopg2.connect(
                host=host,
                port=port_int,
                database=db_name,
                user=user,
                password=password,
                connect_timeout=10
            )
            
            cur = conn.cursor()
            cur.execute("SELECT version();")
            version = cur.fetchone()
            print(f"SUCCESS! Connected to: {version}")
            
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            tables = cur.fetchall()
            print(f"Tables found in DB: {tables}")
            
            cur.close()
            conn.close()
            return "Connection Successful"

        except Exception as e:
            print(f"FAILED TO CONNECT!")
            print(f"Error details: {str(e)}")
            # This will show up in the terminal logs even if the UI hides it
            raise

    test_postgres_connection()