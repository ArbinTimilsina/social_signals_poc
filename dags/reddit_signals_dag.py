from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

version = "1.0"
dag_id = f"reddit_signals_version_{version}"

# Subreddits
subreddits_info = Variable.get("subreddits_info", deserialize_json=True)
subreddit_list = subreddits_info["subreddit_list"]

# Secret Huggingface info
secret_huggingface_info = Variable.get("secret_huggingface_info", deserialize_json=True)
huggingface_token = secret_huggingface_info["token"]

# Secret OpenAI info
secret_opeinai_info = Variable.get("secret_opeinai_info", deserialize_json=True)
openai_key = secret_opeinai_info["key"]

# Secret Reddit info
secret_reddit_info = Variable.get("secret_reddit_info", deserialize_json=True)
reddit_id = secret_reddit_info["reddit_id"]
reddit_secret = secret_reddit_info["reddit_secret"]
reddit_username = secret_reddit_info["reddit_username"]
reddit_password = secret_reddit_info["reddit_password"]

# Secret DB info
secret_db_info = Variable.get("secret_db_info", deserialize_json=True)
db_username = secret_db_info["db_username"]
db_password = secret_db_info["db_password"]
db_host = secret_db_info["db_host"]
db_port = secret_db_info["db_port"]
db_name = secret_db_info["db_name"]

# ECS variables
reddit_signals_ecs = Variable.get("reddit_signals_ecs", deserialize_json=True)
cluster = reddit_signals_ecs["cluster"]
subnets_list = reddit_signals_ecs["subnets_list"]
task_definition = reddit_signals_ecs["task_definition"]
ecs_container_name = reddit_signals_ecs["ecs_container_name"]


def set_config(subreddit):
    config = {
        "subreddit": subreddit,
        "huggingface_token": huggingface_token,
        "openai_key": openai_key,
        "reddit_id": reddit_id,
        "reddit_secret": reddit_secret,
        "reddit_username": reddit_username,
        "reddit_password": reddit_password,
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
    retry_delay_minutes = 1
    retries = 1
    default_args = {
        "owner": "admin",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": retries,
        "retry_delay": timedelta(minutes=retry_delay_minutes),
        "end_date": None,
    }

    # Run daily at 8 UTC ==> 4 AM ET
    return DAG(
        dag_id=dag_id,
        default_args=default_args,
        start_date=datetime(2023, 5, 17, 2, 0, 0),
        schedule_interval="0 8 * * *",
        catchup=False,
        concurrency=5,
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
                        {"name": "execution_date", "value": "{}".format("{{ds}}")},
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
    )

    return task_ecs


dag = create_dag()
start_task = DummyOperator(task_id="start_task", dag=dag)

combined_data_task = get_ecs_operator(
    entry_file="entry_get_combined_data.py",
    config=str(set_config("None")),
    task_id="reddit_signals_task",
    task_definition=task_definition,
    ecs_container_name=ecs_container_name,
    cluster=cluster,
    subnets_list=subnets_list,
    dag=dag,
)

for subreddit in subreddit_list:
    subreddit_task = f"subreddit_task_{subreddit}"
    subreddit_task = get_ecs_operator(
        entry_file="entry_get_subreddit_data.py",
        config=str(set_config(subreddit)),
        task_id=f"subreddit_{subreddit}_signals_task",
        task_definition=task_definition,
        ecs_container_name=ecs_container_name,
        cluster=cluster,
        subnets_list=subnets_list,
        dag=dag,
    )
    start_task.set_downstream(subreddit_task)
    subreddit_task.set_downstream(combined_data_task)

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
