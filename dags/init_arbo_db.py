from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

CREATE_TABLES_SQL = """
CREATE TABLE task_models (
    task_name VARCHAR(255) PRIMARY KEY,
    t_base_1 REAL,            -- Baseline time at s=1 (seconds)
    p_obs REAL,               -- Current dynamic parallelizable portion
    c_startup REAL,           -- Fixed overhead (e.g., pod spin-up)
    alpha_p REAL DEFAULT 0.7,   -- Learning rate for p_obs
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    base_input_quantity FLOAT DEFAULT 1,
    alpha_k REAL DEFAULT 0.8,   -- learning rate for k
    k_exponent REAL DEFAULT 1.0,
    sample_count INT DEFAULT 0
);

CREATE TABLE execution_history (
    id SERIAL PRIMARY KEY,
    task_name VARCHAR(255) REFERENCES task_models(task_name),
    parallelism INT NOT NULL ,           -- s
    input_scale_factor REAL NOT NULL,  -- gamma
    cluster_load REAL NOT NULL,          -- L_cluster
    total_duration REAL NOT NULL,      -- Actual T (Wall Clock Time)
    residual REAL NOT NULL,            -- T_actual - T_amdahl
    cost_metric REAL,                 -- Cost of run
    p_snapshot REAL,                  -- p of execution
    time_amdahl REAL,                 -- predicted t_amdahl
    pred_residual REAL,               -- predicted residual
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster lookups by task name
CREATE INDEX idx_history_task ON execution_history(task_name);
"""

default_args = {
    "owner": "user",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="arbo_db_init",
    default_args=default_args,
    description="Initialize Arbo DB",
    schedule=None,
    catchup=False,
    tags=["arbo", "db"],
) as dag:

    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema",
        conn_id="postgres_default",
        sql=CREATE_TABLES_SQL
    )