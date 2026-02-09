import time
import os
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.decorators import task, task_group
from airflow.utils.task_group import TaskGroup
from kubernetes.client import models as k8s
from datetime import datetime, timedelta

from airflow.utils.trigger_rule import TriggerRule

from arbo_lib.airflow.optimizer import ArboOptimizer
from arbo_lib.config import Config
from arbo_lib.utils.logger import get_logger

logger = get_logger("arbo.genome_dag")

# Ensure this connection exists in Airflow UI (Admin -> Connections)
K8S_CONN_ID = "kubernetes_default"

default_args = {
    "owner": "user",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

TOTAL_ITEMS = 15000
FREQ_TOTAL_PLOTS = 1000

# K8s Internal FQDN: <service>.<namespace>.svc.cluster.local
MINIO_ENDPOINT = "minio.stefan-dev.svc.cluster.local:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
CHROM_NR = "22"
MINIO_BUCKET = "genome-data"
KEY_INPUT_INDIVIDUAL = f"ALL.chr22.{TOTAL_ITEMS}.vcf.gz"
KEY_INPUT_SIFTING = "ALL.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.sites.annotation.vcf.gz"

NAMESPACE = "kogler-dev"

minio_env_vars = [
    k8s.V1EnvVar(name="MINIO_ENDPOINT", value=MINIO_ENDPOINT),
    k8s.V1EnvVar(name="MINIO_ACCESS_KEY", value=MINIO_ACCESS_KEY),
    k8s.V1EnvVar(name="MINIO_SECRET_KEY", value=MINIO_SECRET_KEY),
    k8s.V1EnvVar(name="MINIO_SECURE", value="false"),
]

with DAG(
        dag_id='arbo_genome',
        default_args=default_args,
        description='Genome processing pipeline using KubernetesPodOperator',
        schedule=None,
        catchup=False,
        tags=['genome', 'kubernetes', 'minio'],
        max_active_tasks=20,
) as dag:

    populations = ["EUR", "AFR", "EAS", "ALL", "GBR", "SAS", "AMR"]

    # =================================
    # HELPER TASKS
    # =================================
    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def report_feedback(data: dict, task_name: str):
        optimizer = ArboOptimizer()
        end_time = time.time()
        duration = end_time - data["start_time"]

        optimizer.report_success(
            task_name=task_name,
            total_duration=duration,
            s=data["s"],
            gamma=data["gamma"],
            cluster_load=data["cluster_load"],
            predicted_amdahl=data["amdahl_time"],
            predicted_residual=data["pred_residual"]
        )

    @task
    def get_w_args(data: dict):
        return data["workers"]

    @task
    def get_m_args(data: dict):
        return data["merger"]

    @task
    def extract_pod_args(data: dict):
        return data["pod_arguments"]

    @task
    def extract_merge_keys(data: dict):
        return data["merge_keys_str"]

    # preparation tasks
    @task()
    def prepare_individual_tasks():
        # import os
        # # 1. SET ENV VARS BEFORE ANY LIBRARY IMPORTS
        # os.environ["ARBO_DB_HOST"] = "arbo-db-service"
        # os.environ["ARBO_DB_PORT"] = "5432"
        # os.environ["ARBO_DB_NAME"] = "arbo_data"
        #
        # # 2. NOW IMPORT THE CONFIG AND OVERRIDE MANUALLY
        # from arbo_lib.db.store import Config
        # Config.DB_HOST = "arbo-db-service"
        # Config.DB_PORT = 5432 # Integer, not string
        # Config.DB_NAME = "arbo_data"
        optimizer = ArboOptimizer()
        logger.info("Fetching cluster load for optimization")
        cluster_load = 0.5

        logger.info(f"K8s Internal Call: Targetting {MINIO_ENDPOINT}")

        input_quantity = optimizer.get_filesize(
            endpoint_url=f"http://{MINIO_ENDPOINT}",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            bucket_name=MINIO_BUCKET,
            file_key=f"input/{KEY_INPUT_INDIVIDUAL}"
        )
        

        if not input_quantity:
            logger.warning("Could not reach MinIO or file not found. Falling back to TOTAL_ITEMS.")
            input_quantity = TOTAL_ITEMS

        configs = optimizer.get_task_configs("genome_individual", input_quantity=input_quantity,
                                             cluster_load=cluster_load)
        
        if not configs:
            raise ValueError("Optimizer failed to generate configurations. Check your DB/Core logic.")

        s_opt = len(configs)
        calculated_gamma = configs[0]["gamma"]
        predicted_amdahl = configs[0]["amdahl_time"]
        predicted_residual = configs[0]["residual_prediction"]

        start_time = time.time()
        chunk_size = TOTAL_ITEMS // s_opt

        pod_argument_list = []
        merge_keys = []

        for i in range(s_opt):
            counter = i * chunk_size + 1
            stop = TOTAL_ITEMS + 1 if i == s_opt - 1 else (i + 1) * chunk_size + 1

            args = [
                "--key_input", KEY_INPUT_INDIVIDUAL,
                "--counter", str(counter),
                "--stop", str(stop),
                "--chromNr", CHROM_NR,
                "--bucket_name", MINIO_BUCKET
            ]
            pod_argument_list.append(args)
            merge_keys.append(f'chr22n-{counter}-{stop}.tar.gz')

        return {
            "pod_arguments": pod_argument_list,
            "merge_keys_str": ",".join(merge_keys),
            "s": s_opt,
            "start_time": start_time,
            "gamma": calculated_gamma,
            "cluster_load": cluster_load,
            "amdahl_time": predicted_amdahl,
            "pred_residual": predicted_residual,
        }

    @task
    def prepare_frequency_tasks(pop: str):
        optimizer = ArboOptimizer()
        cluster_load = optimizer.get_virtual_memory()

        pop_input_size = optimizer.get_filesize(
            endpoint_url=f"http://{MINIO_ENDPOINT}",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            bucket_name=MINIO_BUCKET,
            file_key=f"input/{pop}"
        )

        if not pop_input_size:
            pop_input_size = TOTAL_ITEMS

        configs = optimizer.get_task_configs(f"genome_frequency_{pop}", input_quantity=pop_input_size,
                                             cluster_load=cluster_load)
        
        if not configs:
            raise ValueError(f"No configs generated for population {pop}")

        s_opt = len(configs)
        calculated_gamma = configs[0]["gamma"]
        predicted_amdahl = configs[0]["amdahl_time"]
        predicted_residual = configs[0]["residual_prediction"]

        worker_args = []
        chunk_size = FREQ_TOTAL_PLOTS // s_opt

        for i in range(s_opt):
            start = i * chunk_size
            end = (i + 1) * chunk_size if i < s_opt - 1 else FREQ_TOTAL_PLOTS

            worker_args.append([
                "--mode", "calc_plot",
                "--chromNr", CHROM_NR,
                "--POP", pop,
                "--bucket_name", MINIO_BUCKET,
                "--start", str(start),
                "--end", str(end),
                "--chunk_id", str(i)
            ])

        merger_args = [[
            "--mode", "merge",
            "--chromNr", CHROM_NR,
            "--POP", pop,
            "--bucket_name", MINIO_BUCKET,
            "--chunks", str(s_opt)
        ]]

        return {
            "workers": worker_args,
            "merger": merger_args,
            "start_time": time.time(),
            "s": s_opt,
            "gamma": calculated_gamma,
            "cluster_load": cluster_load,
            "pop": pop,
            "amdahl_time": predicted_amdahl,
            "pred_residual": predicted_residual,
        }

    @task
    def mutations_overlap_data(pops: list):
        return [["--chromNr", CHROM_NR, "--POP", pop, "--bucket_name", MINIO_BUCKET] for pop in pops]

    # =================================
    # TASK GROUP DEFINITIONS
    # =================================
    @task_group(group_id="individual_tasks")
    def run_individual_tasks():        
        logger.info("Running individual tasks group")
        ind_plan = prepare_individual_tasks()

        workers = KubernetesPodOperator.partial(
            task_id="workers",
            kubernetes_conn_id=K8S_CONN_ID,
            name="individual-worker",
            namespace=NAMESPACE,
            image="kogsi/genome_dag:individual",
            cmds=["python3", "individual.py"],
            env_vars=minio_env_vars,
            is_delete_operator_pod=True,
        ).expand(
            arguments=extract_pod_args(ind_plan)
        )

        individual_merge = KubernetesPodOperator(
            task_id="merge",
            kubernetes_conn_id=K8S_CONN_ID,
            name="individuals_merge",
            namespace=NAMESPACE,
            image="kogsi/genome_dag:individuals-merge",
            cmds=["python3", "individuals-merge.py"],
            arguments=[
                "--chromNr", CHROM_NR,
                "--keys", extract_merge_keys(ind_plan),
                "--bucket_name", MINIO_BUCKET
            ],
            env_vars=minio_env_vars,
            is_delete_operator_pod=True,
        )

        feedback = report_feedback(ind_plan, "genome_individual")
        [individual_merge, feedback] << workers

    def run_frequency_tasks(pop: str):
        plan_data = prepare_frequency_tasks(pop)

        workers = KubernetesPodOperator.partial(
            task_id="workers",
            kubernetes_conn_id=K8S_CONN_ID,
            name=f"freq-workers-{pop.lower()}",
            namespace=NAMESPACE,
            image="kogsi/genome_dag:frequency_par2",
            cmds=["python3", "frequency_par2.py"],
            env_vars=minio_env_vars,
            is_delete_operator_pod=True,
        ).expand(
            arguments=get_w_args(plan_data)
        )

        merger = KubernetesPodOperator.partial(
            task_id="merge",
            kubernetes_conn_id=K8S_CONN_ID,
            name=f"freq-merge-{pop.lower()}",
            namespace=NAMESPACE,
            image="kogsi/genome_dag:frequency_par2",
            cmds=["python3", "frequency_par2.py"],
            env_vars=minio_env_vars,
            is_delete_operator_pod=True,
        ).expand(
            arguments=get_m_args(plan_data)
        )

        feedback = report_feedback(plan_data, f"genome_frequency_{pop}")
        workers >> merger >> feedback

    # =================================
    # WIRING OF DAG
    # =================================
    individual_group = run_individual_tasks()

    sifting_task = KubernetesPodOperator(
        task_id="sifting",
        kubernetes_conn_id=K8S_CONN_ID,
        name="sifting",
        namespace=NAMESPACE,
        image="kogsi/genome_dag:sifting",
        cmds=["python3", "sifting.py"],
        arguments=[
            "--key_datafile", KEY_INPUT_SIFTING,
            "--chromNr", CHROM_NR,
            "--bucket_name", MINIO_BUCKET
        ],
        env_vars=minio_env_vars,
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="IfNotPresent",
        execution_timeout=timedelta(hours=1),
    )

    # mutations_data = mutations_overlap_data(populations)
    # mutations_tasks = KubernetesPodOperator.partial(
    #     task_id="mutations_overlap",
    #     kubernetes_conn_id=K8S_CONN_ID,
    #     name="mutations-overlap",
    #     namespace=NAMESPACE,
    #     image="kogsi/genome_dag:mutations-overlap",
    #     cmds=["python3", "mutations-overlap.py"],
    #     env_vars=minio_env_vars,
    #     get_logs=True,
    #     is_delete_operator_pod=True,
    #     image_pull_policy="IfNotPresent",
    # ).expand(
    #     arguments=mutations_data
    # )

    # individual_group >> mutations_tasks
    # sifting_task >> mutations_tasks

    for pop in populations:
        with TaskGroup(group_id=f"freq_{pop}") as frequency_group:
            run_frequency_tasks(pop)
        individual_group >> frequency_group
        sifting_task >> frequency_group
