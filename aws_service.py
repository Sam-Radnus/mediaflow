import os
from typing import Optional, Dict, Any

import boto3
from dotenv import load_dotenv


load_dotenv()


class AWSService:
    """
    Thin wrapper around boto3 that centralizes AWS session creation and
    provides convenience helpers for common S3 and DynamoDB operations.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.session = boto3.Session(
            aws_access_key_id=config.get("aws_access_key_id"),
            aws_secret_access_key=config.get("aws_secret_access_key"),
            region_name=config.get("region_name"),
            aws_session_token=config.get("aws_session_token"),
        )

    @classmethod
    def from_env(cls) -> "AWSService":
        """
        Construct an AWSService instance from environment variables.

        Supports:
        - AWS_ACCESS_KEY_ID / AWS_ACCESS_KEY
        - AWS_SECRET_ACCESS_KEY
        - AWS_SESSION_TOKEN (optional)
        - AWS_REGION
        """
        access_key = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        region = os.getenv("AWS_REGION", "ap-south-2")
        session_token = os.getenv("AWS_SESSION_TOKEN")

        config = {
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "region_name": region,
            "aws_session_token": session_token,
        }
        return cls(config=config)

    # --------- Low-level clients / resources ---------

    def s3(self):
        return self.session.client("s3")

    def s3_bucket(self, bucket_name: str):
        return self.session.resource("s3").Bucket(bucket_name)

    def dynamodb(self):
        return self.session.client("dynamodb")

    def dynamodb_table(self, table_name: str):
        return self.session.resource("dynamodb").Table(table_name)

    def lambda_client(self):
        return self.session.client("lambda")

    def lambda_function(self, function_name: str):
        client = self.session.client("lambda")
        return client.get_function(FunctionName=function_name)

    def ec2(self):
        return self.session.client("ec2")

    def ec2_instance(self, instance_id: str):
        return self.session.resource("ec2").Instance(instance_id)

    # --------- Convenience helpers used by this project ---------

    def upload_file_to_s3(
        self,
        file_path: str,
        bucket: str,
        key: str,
        extra_args: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Upload a local file to S3.
        """
        client = self.s3()
        client.upload_file(
            Filename=file_path,
            Bucket=bucket,
            Key=key,
            ExtraArgs=extra_args or {},
        )

    def put_item_dynamodb(
        self,
        table_name: str,
        item: Dict[str, Any],
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Put an item into a DynamoDB table.
        """
        table = self.dynamodb_table(table_name)
        return table.put_item(Item=item, **kwargs)

    def update_item_dynamodb(
        self,
        table_name: str,
        key: Dict[str, Any],
        update_expression: str,
        expression_attribute_values: Dict[str, Any],
        expression_attribute_names: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Update an item in a DynamoDB table.
        """
        table = self.dynamodb_table(table_name)
        params: Dict[str, Any] = {
            "Key": key,
            "UpdateExpression": update_expression,
            "ExpressionAttributeValues": expression_attribute_values,
            "ReturnValues": "ALL_NEW",
        }
        if expression_attribute_names:
            params["ExpressionAttributeNames"] = expression_attribute_names
        params.update(kwargs)
        return table.update_item(**params)