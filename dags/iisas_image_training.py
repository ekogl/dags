import time
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.decorators import task, task_group
from datetime import datetime, timedelta

from airflow.utils.trigger_rule import TriggerRule

from arbo_lib.airflow.optimizer import ArboOptimizer
from arbo_lib.utils.logger import get_logger

logger = get_logger("arbo.iisas_image_training")

default_args = {
    "owner": "user",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

MINIO_ENDPOINT = "minio.stefan-dev.svc.cluster.local:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "image-classification-data"

NAMESPACE = "kogler-dev"

minio_env_dict = {
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    "MINIO_SECURE": "false"
}

NUM_OF_PICTURES = 8

with DAG(
    dag_id="arbo_iisas_image_training",
    default_args=default_args,
    description="IISAS Image Classification Training Pipeline",
    schedule=None,
    catchup=False,
    tags=["iisas", "kubernetes", "minio"],
    max_active_tasks=20,
) as dag:

    # setup task
    @task
    def prepare_pipeline_configs():
        optimizer = ArboOptimizer()

        # TODO: change later
        cluster_load = optimizer.get_cluster_load(namespace="kogler-dev")

        input_quantity = optimizer.get_directory_size(
            endpoint_url=f"http://{MINIO_ENDPOINT}",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            bucket_name=MINIO_BUCKET,
            prefix="training/input"
        )

        if not input_quantity:
            # TODO: figure out a better way to handle this case
            logger.info("Falling back to default (= NUM_OF_PICTURES * 1024 * 1024)")
            input_quantity = NUM_OF_PICTURES * 1024 * 1024

        configs = optimizer.get_task_configs("iisas_image_training", input_quantity=input_quantity, cluster_load=cluster_load)
        s_opt = len(configs)

        calculated_gamma = configs[0]["gamma"]
        predicted_amdahl = configs[0]["amdahl_time"]
        predicted_residual = configs[0]["residual_prediction"]

        # TODO: change later
        start_time = time.time()

        logger.info(f"Configuration received: s={s_opt}, gamma={calculated_gamma}")

        configurations = []
        for i in range(s_opt):
            config = {
                "chunk_id": str(i),
                "offset_args": [
                    "--input_image_path", "training/input",
                    "--output_image_path", f"training/offsetted/{i}",
                    "--dx", "0", "--dy", "0",
                    "--bucket_name", MINIO_BUCKET,
                    "--chunk_id", str(i),
                    "--num_tasks", str(s_opt),
                ],
                "crop_args": [
                    "--input_image_path", f"training/offsetted/{i}",
                    "--output_image_path", f"training/cropped/{i}",
                    "--left", "20", "--top", "20",
                    "--right", "330", "--bottom", "330",
                    "--bucket_name", MINIO_BUCKET,
                    "--chunk_id", "0", "--num_tasks", "1",
                ],
                "enhance_brightness_args": [
                    "--input_image_path", f"training/cropped/{i}",
                    "--output_image_path", f"training/enhanced_brightness/{i}",
                    "--factor", str(1.2),
                    "--bucket_name", MINIO_BUCKET,
                    "--chunk_id", "0",
                    "--num_tasks", "1",
                ],
                "enhance_contrast_args": [
                    "--input_image_path", f"training/enhanced_brightness/{i}",
                    "--output_image_path", f"training/enhanced_contrast/{i}",
                    "--factor", str(1.2),
                    "--bucket_name", MINIO_BUCKET,
                    "--chunk_id", "0",
                    "--num_tasks", "1",
                ],
                "rotate_args": [
                    "--input_image_path", f"training/enhanced_contrast/{i}",
                    "--output_image_path", f"training/rotated/{i}",
                    "--rotation", " ".join(["0", "90", "180", "270"]),
                    "--bucket_name", MINIO_BUCKET,
                    "--chunk_id", "0",
                    "--num_tasks", "1",
                ],
                "grayscale_args": [
                    "--input_image_path", f"training/rotated/{i}",
                    "--output_image_path", f"training/grayscaled",
                    "--bucket_name", MINIO_BUCKET,
                    "--chunk_id", "0",
                    "--num_tasks", "1",
                ]
            }
            configurations.append(config)

        return {
            "configurations": configurations,
            "metadata": {
                "start_time": start_time,
                "s": s_opt,
                "gamma": calculated_gamma,
                "cluster_load": cluster_load,
                "amdahl_time": predicted_amdahl,
                "pred_residual": predicted_residual
            }
        }


    @task
    def extract_configs(data: dict):
        return data["configurations"]

    @task
    def extract_metadata(data: dict):
        return data["metadata"]

    @task_group(group_id="preprocessing_pipeline")
    def image_pipeline_group(config: dict):

        @task
        def get_args_by_key(conf: dict, key: str):
            return conf[key]

        # Use the helper to resolve arguments
        offset_args_list = get_args_by_key(config, "offset_args")
        crop_args_list = get_args_by_key(config, "crop_args")
        brightness_args_list = get_args_by_key(config, "enhance_brightness_args")
        contrast_args_list = get_args_by_key(config, "enhance_contrast_args")
        rotate_args_list = get_args_by_key(config, "rotate_args")
        grayscale_args_list = get_args_by_key(config, "grayscale_args")

        offset = KubernetesPodOperator(
            task_id="offset",
            name="offset-task",
            namespace=NAMESPACE,
            image="kogsi/image_classification:offset",
            arguments=offset_args_list,  # Inject dynamic args
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="IfNotPresent",
        )

        crop = KubernetesPodOperator(
            task_id="crop",
            name="crop-task",
            namespace=NAMESPACE,
            image="kogsi/image_classification:crop",
            arguments=crop_args_list,
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="IfNotPresent",
        )

        enhance_brightness = KubernetesPodOperator(
            task_id="enhance_brightness",
            name="enhance_brightness-task",
            namespace=NAMESPACE,
            image="kogsi/image_classification:enhance-brightness",
            arguments=brightness_args_list,
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="IfNotPresent",
        )

        enhance_contrast = KubernetesPodOperator(
            task_id="enhance_contrast",
            name="enhance_contrast-task",
            namespace=NAMESPACE,
            image="kogsi/image_classification:enhance-contrast",
            arguments=contrast_args_list,
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="IfNotPresent",
        )

        rotate = KubernetesPodOperator(
            task_id="rotate",
            name="rotate-task",
            namespace=NAMESPACE,
            image="kogsi/image_classification:rotate",
            arguments=rotate_args_list,
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="IfNotPresent",
        )

        grayscale = KubernetesPodOperator(
            task_id="grayscale",
            name="grayscale-task",
            namespace=NAMESPACE,
            image="kogsi/image_classification:to-grayscale",
            arguments=grayscale_args_list,
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="IfNotPresent",
        )

        offset >> crop >> enhance_brightness >> enhance_contrast >> rotate >> grayscale

    # classification_inference = KubernetesPodOperator(
    #     task_id="classification_inference_task_training",
    #     name="classification_inference_task_training",
    #     namespace=NAMESPACE,
    #     image="kogsi/image_classification:classification-train-tf2",
    #     arguments=[
    #         "--train_data_path", "training/grayscaled",
    #         "--output_artifact_path", "models/",
    #         "--bucket_name", MINIO_BUCKET,
    #         "--validation_split", "0.2",
    #         # "--validation_data_path", "training/validation",
    #         "--epochs", "5",
    #         "--batch_size", "32",
    #         "--early_stop_patience", "5",
    #         "--dropout_rate", "0.2",
    #         "--image_size", "256 256",
    #         "--num_layers", "3",
    #         "--filters_per_layer", "64 64 64",
    #         "--kernel_sizes", "3 3 3",
    #         "--workers", "4",
    #     ],
    #     env_vars=minio_env_dict,
    #     get_logs=True,
    #     is_delete_operator_pod=True,
    #     image_pull_policy="IfNotPresent",
    # )

    sleep_task = KubernetesPodOperator(
        task_id="sleep_10s",
        name="sleep-task",
        namespace=NAMESPACE,
        image="alpine:latest",
        cmds=["/bin/sh", "-c"],
        arguments=["echo 'Sleeping now...'; sleep 10; echo 'Awake!'"],
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="IfNotPresent",
    )

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def report_feedback(metadata: dict, **context):
        dag_run = context.get("dag_run")
        tis = dag_run.get_task_instances()
        
        group_prefix = "preprocessing_pipeline"
        group_tis = [ti for ti in tis if ti.task_id.startswith(group_prefix)]
        
        start_times = [ti.start_date for ti in group_tis if ti.start_date]
        end_times = [ti.end_date for ti in group_tis if ti.end_date]
        
        if start_times and end_times:
            actual_duration = (max(end_times) - min(start_times)).total_seconds()
            logger.info(f"Calculated TaskGroup duration: {actual_duration}s")
        else:
            actual_duration = time.time() - metadata["start_time"]
            logger.warning("Could not calculate duration, using timestamps.")
            
        optimizer = ArboOptimizer()

        optimizer.report_success(
            task_name="iisas_image_training",
            total_duration=actual_duration,
            s=metadata["s"],
            gamma=metadata["gamma"],
            cluster_load=metadata["cluster_load"],
            predicted_amdahl=metadata["amdahl_time"],
            predicted_residual=metadata["pred_residual"]
        )

    pipeline_configs = prepare_pipeline_configs()
    pod_config_list = extract_configs(pipeline_configs)
    pipeline_metadata = extract_metadata(pipeline_configs)

    pipeline_instances = image_pipeline_group.expand(config=pod_config_list)

    # pipeline_instances >> classification_inference
    pipeline_instances >> sleep_task
    pipeline_instances >> report_feedback(pipeline_metadata)