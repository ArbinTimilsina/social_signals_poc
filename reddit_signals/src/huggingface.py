import os

import requests
from constants import CATEGORIES
from transformers import pipeline

config = eval(os.environ["config"])
HUGGINGFACE_TOKEN = config["huggingface_token"]

NER_MODEL_ID = "dslim/bert-large-NER"
EMOTION_MODEL_ID = "j-hartmann/emotion-english-distilroberta-base"


def get_huggingface_response(text, model_id):
    api_url = f"https://api-inference.huggingface.co/models/{model_id}"
    headers = {"Authorization": f"Bearer {HUGGINGFACE_TOKEN}"}

    payload = {"inputs": text, "options": {"wait_for_model": True}}
    response = requests.post(api_url, headers=headers, json=payload)

    try:
        response = response.json()
    except Exception:
        print(f"Could not get Huggingface response for {model_id}")
        return
    return response


def get_huggingface_zero_shot_classificaiton(sequence_to_classify):
    classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")
    try:
        output = classifier(sequence_to_classify, CATEGORIES)
    except Exception:
        print(f"Could not get Huggingface response for Zero Shot Classification")
        return

    return output
