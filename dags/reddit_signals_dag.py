from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator

version = "1.0"
dag_id = f"reddit_signals_version_{version}"

# Subreddits
subreddits = Variable.get("subreddits", deserialize_json=True)
subreddits = subreddits["subreddits"]

# Secret Huggingface info
secret_huggingface_info = Variable.get("secret_huggingface_info", deserialize_json=True)
huggingface_token = secret_huggingface_info["token"]

# Secret Reddit info
secret_reddit_info = Variable.get("secret_reddit_info", deserialize_json=True)
reddit_id = secret_reddit_info["reddit_id"]
reddit_secret = secret_reddit_info["reddit_secret"]
reddit_username = secret_reddit_info["reddit_username"]
reddit_password = secret_reddit_info["reddit_password"]

# ECS variables
reddit_signals_ecs = Variable.get("reddit_signals_ecs", deserialize_json=True)
cluster = reddit_signals_ecs["cluster"]
subnets_list = reddit_signals_ecs["subnets_list"]
task_definition = reddit_signals_ecs["task_definition"]
task_concurrency = reddit_signals_ecs["task_concurrency"]
ecs_container_name = reddit_signals_ecs["ecs_container_name"]


def set_config(subreddit):
    config = {
        "subreddit": subreddit,
        "huggingface_token": huggingface_token,
        "reddit_id": reddit_id,
        "reddit_secret": reddit_secret,
        "reddit_username": reddit_username,
        "reddit_password": reddit_password,
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
        start_date=datetime(2023, 5, 15, 2, 0, 0),
        schedule_interval="0 8 * * *",
        concurrency=10,
        max_active_runs=1,
    )


def get_ecs_operator(
    entry_file,
    config,
    task_id,
    task_definition,
    task_concurrency,
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
        task_concurrency=task_concurrency,
        dag=dag,
    )

    return task_ecs


dag = create_dag()
start_task = DummyOperator(task_id="start_task", dag=dag)
end_task = DummyOperator(task_id="end_task", dag=dag)

for subreddit in subreddits:
    subreddit_task = f"subreddit_task_{subreddit}"
    subreddit_task = get_ecs_operator(
        entry_file="entry_subreddit_data.py",
        config=str(set_config(subreddit)),
        task_id=f"subreddit_signals_{subreddit}",
        task_definition=task_definition,
        task_concurrency=task_concurrency,
        ecs_container_name=ecs_container_name,
        cluster=cluster,
        subnets_list=subnets_list,
        dag=dag,
    )
    start_task.set_downstream(subreddit_task)
    subreddit_task.set_downstream(end_task)
