import os
import requests

config = eval(os.environ["config"])
HUGGINGFACE_TOKEN = config["huggingface_token"]

NER_MODEL_ID = "dslim/bert-large-NER"
SENTIMENT_MODEL_ID = "cardiffnlp/twitter-roberta-base-sentiment-latest"
EMOTION_MODEL_ID = "j-hartmann/emotion-english-distilroberta-base"
ESG_CATEGORIES_MODEL_ID = "yiyanghkust/finbert-esg-9-categories"


def get_huggingface_response(text, model_id):
    api_url = f"https://api-inference.huggingface.co/models/{model_id}"
    headers = {"Authorization": f"Bearer {HUGGINGFACE_TOKEN}"}

    payload = {"inputs": text, "options": {"wait_for_model": True}}
    response = requests.post(api_url, headers=headers, json=payload)
    return response.json()
