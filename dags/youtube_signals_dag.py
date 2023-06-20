from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

version = "1.0"
dag_id = f"youtube_signals_version_{version}"

# YouTube video categories
video_category_info = Variable.get("video_category_info", deserialize_json=True)
video_category_list = video_category_info["video_category_list"]

# Secret OpenAI info
secret_opeinai_info = Variable.get("secret_opeinai_info", deserialize_json=True)
openai_key = secret_opeinai_info["key"]

# Secret Reddit info
secret_google_info = Variable.get("secret_google_info", deserialize_json=True)
google_api_key = secret_google_info["google_api_key"]

# Secret DB info
secret_db_info = Variable.get("secret_db_info", deserialize_json=True)
db_username = secret_db_info["db_username"]
db_password = secret_db_info["db_password"]
db_host = secret_db_info["db_host"]
db_port = secret_db_info["db_port"]
db_name = secret_db_info["db_name"]

# ECS variables
youtube_signals_ecs = Variable.get("youtube_signals_ecs", deserialize_json=True)
cluster = youtube_signals_ecs["cluster"]
subnets_list = youtube_signals_ecs["subnets_list"]
task_definition = youtube_signals_ecs["task_definition"]
ecs_container_name = youtube_signals_ecs["ecs_container_name"]


def set_config(video_category):
    config = {
        "video_category": video_category,
        "openai_key": openai_key,
        "google_api_key": google_api_key,
        "db_username": db_username,
        "db_password": db_password,
        "db_host": db_host,
        "db_port": db_port,
        "db_name": db_name,
    }
    return config


def create_dag():
    """
    Format for schedule_interval:
         minute, hour, day_of_month, month_of_year, day_of_week
    """
    retry_delay_minutes = 3
    retries = 1
    default_args = {
        "owner": "admin",
        "depends_on_past": False,
        'email': ["arbin.timilsina@gmail.com"],
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": retries,
        "retry_delay": timedelta(minutes=retry_delay_minutes),
        "end_date": None,
    }

    # Run daily at 6 AM, 12 PM, and 6 PM ET
    return DAG(
        dag_id=dag_id,
        default_args=default_args,
        start_date=datetime(2023, 5, 17, 2, 0, 0),
        schedule_interval="0 10,16,22 * * *",
        catchup=False,
        concurrency=3,
        max_active_runs=1,
    )


def get_ecs_operator(
    entry_file,
    config,
    task_id,
    task_definition,
    ecs_container_name,
    cluster,
    subnets_list,
    dag,
):
    task_ecs = EcsRunTaskOperator(
        task_id=task_id,
        cluster=cluster,
        task_definition=task_definition,
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": ecs_container_name,
                    "command": [entry_file],
                    "environment": [
                        {"name": "config", "value": str(config)},
                        {"name": "execution_date", "value": "{}".format("{{ts}}")},
                    ],
                },
            ],
        },
        network_configuration={
            "awsvpcConfiguration": {
                "subnets": subnets_list,
            },
        },
        dag=dag,
        awslogs_group="/ecs/youtube-signals-dev-task-definition"
    )

    return task_ecs


dag = create_dag()
start_task = DummyOperator(task_id="start_task", dag=dag)

combined_data_task = get_ecs_operator(
    entry_file="entry_get_combined_data.py",
    config=str(set_config("None")),
    task_id="youtube_signals_task",
    task_definition=task_definition,
    ecs_container_name=ecs_container_name,
    cluster=cluster,
    subnets_list=subnets_list,
    dag=dag,
)

for video_category in video_category_list:
    video_category_task = f"video_category_task_{video_category}"
    video_category_task = get_ecs_operator(
        entry_file="entry_get_video_category_data.py",
        config=str(set_config(video_category)),
        task_id=f"video_category_{video_category}_signals_task",
        task_definition=task_definition,
        ecs_container_name=ecs_container_name,
        cluster=cluster,
        subnets_list=subnets_list,
        dag=dag,
    )
    start_task.set_downstream(video_category_task)
    video_category_task.set_downstream(combined_data_task)

write_to_db_task = get_ecs_operator(
    entry_file="entry_write_to_db.py",
    config=str(set_config("None")),
    task_id="write_to_db_task",
    task_definition=task_definition,
    ecs_container_name=ecs_container_name,
    cluster=cluster,
    subnets_list=subnets_list,
    dag=dag,
)

combined_data_task.set_downstream(write_to_db_task)
