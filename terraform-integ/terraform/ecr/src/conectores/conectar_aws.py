import boto3
import os
import json
from botocore.exceptions import ClientError

class ConectAWS:

    def __init__(self):

        self.region = os.getenv("AWS_DEFAULT_REGION")
        self.secretid="aws-svc-data-engineering"
      

    def get_svc_user_credentials(self):

        boto_session = boto3.Session(
            region_name=self.region,
        )

        secretsmanager = boto_session.client("secretsmanager")
        secretsmanager_aws_response = secretsmanager.get_secret_value(
            SecretId=self.secretid
        )

        # Splitting AWS SVC credentials into variables
        secret_string = json.loads(secretsmanager_aws_response["SecretString"])
        aws_user = secret_string["access_key_id"]
        aws_password = secret_string["secret_access_key"]

        # Pipeline will use aws-svc-data-engineering credentials to use boto3
        boto_session = boto3.Session(
            aws_access_key_id=aws_user,
            aws_secret_access_key=aws_password,
            region_name=self.region,
        )

        return boto_session
