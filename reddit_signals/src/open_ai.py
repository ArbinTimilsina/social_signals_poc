import os

import openai
from constants import NONE_FILLER

config = eval(os.environ["config"])
openai.api_key = config["openai_key"]


def get_openai_summary(text):
    prompt = f"Summarize the following text in one sentence:\n\n{text}"
    
    try:
        response = openai.Completion.create(
            model="text-davinci-003",
            prompt=prompt,
            temperature=0.7,
            max_tokens=60,
            top_p=1.0,
            frequency_penalty=0.0,
            presence_penalty=1,
        )
    except Exception:
        print("Could not get OpenAI response")
        return NONE_FILLER

    return response["choices"][0]["text"]
