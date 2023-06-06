import boto3
import sagemaker
from sagemaker.huggingface import HuggingFaceModel
from sagemaker.serverless import ServerlessInferenceConfig


def deploy_model_in_sagemaker(
    model_name, model_s3_path, hf_task, memory_size_in_mb=3072
):
    print(f"\nCreating model {model_s3_path} in SageMaker")

    try:
        role = sagemaker.get_execution_role()
    except ValueError:
        iam = boto3.client("iam")
        role = iam.get_role(RoleName="SageMaker-SocialSignals")["Role"]["Arn"]
    print(f"Role is {role}")

    if model_name == "ss-emotion-distilroberta-base":
        transformers_version = "4.12"
        pytorch_version = "1.9"
        py_version = "py38"
    else:
        transformers_version = "4.26.0"
        pytorch_version = "1.13.1"
        py_version = "py39"

    env = {"HF_TASK": hf_task}

    # Intialize SageMaker HuggingFace model
    huggingface_model = HuggingFaceModel(
        name=model_name,
        model_data=model_s3_path,
        role=role,
        transformers_version=transformers_version,
        pytorch_version=pytorch_version,
        py_version=py_version,
        env=env,
    )

    # Specify MemorySizeInMB and MaxConcurrency in the serverless config object
    serverless_config = ServerlessInferenceConfig(memory_size_in_mb=memory_size_in_mb)

    # Create model
    huggingface_model.deploy(serverless_inference_config=serverless_config)

    print(f"Model {model_name} created successfully in SageMaker.")


if __name__ == "__main__":
    model_name = "ss-ner-bert-base"
    model_s3_path = "s3://social-signals-models/ner/bert-base-ner/model.tar.gz"
    hf_task = "token-classification"
    deploy_model_in_sagemaker(
        model_name, model_s3_path, hf_task, memory_size_in_mb=2048
    )

    model_name = "ss-emotion-distilroberta-base"
    model_s3_path = (
        "s3://social-signals-models/emotion/english-distilroberta-base/model.tar.gz"
    )
    hf_task = "text-classification"
    deploy_model_in_sagemaker(
        model_name, model_s3_path, hf_task, memory_size_in_mb=1024
    )

    model_name = "ss-catogery-bart-mnli"
    model_s3_path = (
        "s3://social-signals-models/category/bart-mnli-cnn-news/model.tar.gz"
    )
    hf_task = "zero-shot-classification"
    deploy_model_in_sagemaker(
        model_name, model_s3_path, hf_task, memory_size_in_mb=5120
    )
