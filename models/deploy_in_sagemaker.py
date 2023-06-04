import boto3
import sagemaker
from sagemaker.huggingface import HuggingFaceModel
from sagemaker.serverless import ServerlessInferenceConfig


def deploy_model_in_sagemaker(model_name, model_s3_path):
    print(f"Creating model {model_s3_path} in SageMaker")

    try:
        role = sagemaker.get_execution_role()
    except ValueError:
        iam = boto3.client("iam")
        role = iam.get_role(RoleName="SageMaker-SocialSignals")["Role"]["Arn"]
    print(f"Role is {role}")

    # Intialize SageMaker HuggingFace model
    huggingface_model = HuggingFaceModel(
        name=model_name,
        model_data=model_s3_path,
        role=role,
        transformers_version="4.12",
        pytorch_version="1.9",
        py_version="py38",
    )

    # Specify MemorySizeInMB and MaxConcurrency in the serverless config object
    serverless_config = ServerlessInferenceConfig(memory_size_in_mb=3072)

    # Create model
    huggingface_model.deploy(serverless_inference_config=serverless_config)

    print(f"Model {model_name} created successfully in SageMaker.")


if __name__ == "__main__":
    model_name = "social-signals-zero-shot-classification"
    model_s3_path = "s3://social-signals-models/zero-shot-classification/model.tar.gz"
    deploy_model_in_sagemaker(model_name, model_s3_path)

    # model_name = "social-signals-ner"
    # model_s3_path = "s3://social-signals-models/ner/model.tar.gz"
    # deploy_model_in_sagemaker(model_name, model_s3_path)

    # model_name = "social-signals-emotion"
    # model_s3_path = "s3://social-signals-models/emotion/model.tar.gz"
    # deploy_model_in_sagemaker(model_name, model_s3_path)

