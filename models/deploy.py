import os
from pathlib import Path

import boto3
import sagemaker
from sagemaker.pytorch import PyTorchModel

SAGEMAKER_ROLE = "DS-ML-Team-SageMaker"
region_name = "us-east-1"
aws_access_key = os.getenv("aws_access_key")
aws_secret_access_key = os.getenv("aws_secret_access_key")

sentimentV2_classifier_en = {
    "MAX_LEN": "128",
    "BATCH_SIZE": "512",
    "SENTIMENT_THRESHOLD": "0.65",
    "CLASS_NAMES": "['negative', 'neutral', 'positive']",
    "PRETRAINED_MODEL_NAME": "cardiffnlp/twitter-roberta-base-sentiment",
    "CONTENT_TYPE": "text/csv",
    "INSTANCE_TYPE": "ml.p2.xlarge",
    "TS_DEFAULT_RESPONSE_TIMEOUT": "3600"
}

PATH = Path(__file__).resolve().parent


def create_and_deploy_model():
    trained_model_s3_path = (
        "s3://ds-ml-team-models/sentiment/version=2/sentiment.tar.gz"
    )

    print(f"Deployment started for model: {trained_model_s3_path}")

    sagemaker_session = sagemaker.Session(
        boto3.session.Session(
            region_name=region_name,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_access_key,
        )
    )
    print(f"Sagemaker session is created successfully for region: {region_name}")

    # Model config setup
    model_type = "sentimentV2"
    model_name = "sentimentV2-classifier-en"
    artifact_location = "s3://ds-ml-team-models/sentiment/version=2"

    config = sentimentV2_classifier_en
    source_dir = str(PATH.joinpath("containers", model_type))
    print(f"Successfully read configurations for model: {model_name}")

    # Intialize sagemaker Pytorch model
    model = PyTorchModel(
        name=model_name,
        model_data=trained_model_s3_path,
        source_dir=source_dir,
        role=SAGEMAKER_ROLE,
        entry_point="predict.py",
        framework_version="1.6.0",
        py_version="py3",
        code_location=artifact_location,
        env=config,
    )

    # add session object
    model.sagemaker_session = sagemaker_session

    # container definition object
    container_def = model.prepare_container_def(instance_type=config["INSTANCE_TYPE"])
    print(f"Container created with definition object details: {container_def}")

    # deploy sageMaker PyTorchModel
    sagemaker_session.create_model(
        f"{model_name}", SAGEMAKER_ROLE, container_def
    )
    print(f"model {model_name} deployed successfully.")


if __name__ == "__main__":
    create_and_deploy_model()
