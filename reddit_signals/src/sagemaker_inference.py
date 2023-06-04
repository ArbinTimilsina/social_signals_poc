import os

import boto3
import requests
import sagemaker
from constants import CATEGORIES
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
    endpoint_name = "social-signals-emotion-2023-06-03-05-35-02-830"
    output = get_sagemaker_response(payload, endpoint_name)

    return output


def get_ner(text):
    payload = {"inputs": text, "options": {"wait_for_model": True}}
    endpoint_name = "social-signals-ner-2023-06-04-03-05-56-782"
    output = get_sagemaker_response(payload, endpoint_name)

    return output


def get_categories(text):
    payload = {
        "inputs": text,
        "parameters": {"candidate_labels": CATEGORIES},
        "options": {"wait_for_model": True},
    }
    endpoint_name = "social-signals-ner-2023-06-04-03-05-56-782"
    output = get_sagemaker_response(payload, endpoint_name)

    return output


def get_huggingface_zero_shot_classificaiton_response(text):
    api_url = f"https://api-inference.huggingface.co/models/{ZERO_SHOT_MODEL_ID}"
    headers = {"Authorization": f"Bearer {HUGGINGFACE_TOKEN}"}

    payload = {
        "inputs": text,
        "parameters": {"candidate_labels": CATEGORIES},
        "options": {"wait_for_model": True},
    }
    response = requests.post(api_url, headers=headers, json=payload)

    if response.ok:
        response = response.json()
        return response
    else:
        print(f"Could not get Huggingface OK response for {ZERO_SHOT_MODEL_ID}")
        print(f"Response was {response}")
        return
