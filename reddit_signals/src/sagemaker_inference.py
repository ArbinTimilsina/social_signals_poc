import os

import boto3
import sagemaker
from constants import SUB_CATEGORIES
from sagemaker.huggingface import HuggingFacePredictor

config = eval(os.environ["config"])
HUGGINGFACE_TOKEN = config["huggingface_token"]

ZERO_SHOT_MODEL_ID = "facebook/bart-large-mnli"

REGION_NAME = "us-east-1"
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


def get_sagemaker_response(payload, endpoint_name):
    sagemaker_session = sagemaker.Session(
        boto3.session.Session(
            region_name=REGION_NAME,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
    )

    predictor = HuggingFacePredictor(
        endpoint_name=endpoint_name, sagemaker_session=sagemaker_session
    )

    try:
        output = predictor.predict(payload)
        return output
    except Exception:
        print(
            f"Could not get SageMaker prediction for endpoint {endpoint_name} and payload {payload}"
        )


def get_emotion(text):
    payload = {"inputs": text, "options": {"wait_for_model": True}}
    endpoint_name = "ss-emotion-distilroberta-base-2023-06-06-20-55-21-949"
    output = get_sagemaker_response(payload, endpoint_name)

    return output


def get_ner(text):
    payload = {
        "inputs": text,
        "parameters": {"aggregation_strategy": "simple"},
        "options": {"wait_for_model": True},
    }
    endpoint_name = "ss-ner-bert-base-2023-06-06-19-36-28-904"
    output = get_sagemaker_response(payload, endpoint_name)

    return output


def get_categories(text):
    payload = {
        "inputs": text,
        "parameters": {"candidate_labels": SUB_CATEGORIES},
        "options": {"wait_for_model": True},
    }
    endpoint_name = "ss-catogery-bart-mnli-2023-06-06-22-17-10-558"
    output = get_sagemaker_response(payload, endpoint_name)

    return output
