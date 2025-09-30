import requests
import json
import os

from src.conectores.conectar_aws import ConectAWS
from botocore.exceptions import ClientError

#buscar credencial que esta gravda dentro secret do aws
def get_secret():

    cred = ConectAWS() 
    awssecret = cred.get_svc_user_credentials()

    secret_name = "octadesk-credentials"
    region = os.getenv("AWS_DEFAULT_REGION")
   
    client = awssecret.client(
        service_name='secretsmanager',
        region_name=region 
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']

    # Your code goes here.
    # json.loads(secret)
    return secret


class CredencialApi:

    def __init__(self):
        pass
        
    def get_octadesk_token(self):

        credentials = json.loads(get_secret())
        # print("credencial octadesk:",credentials)

        username = credentials['username']
        apiToken = credentials['apiToken']
        subdomain = credentials['subdomain']

        # Getting token
        url = 'https://api.octadesk.services/login/apiToken'

        headers = {
            'Content-Type':'application/json',
            'subDomain': subdomain,
            'username':username,
            'apiToken': apiToken
        }

        response = requests.request("POST",url, headers=headers)
        
        token = response.json()['token']

        return token
