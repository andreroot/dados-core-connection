import requests
import json
import os

from src.conectores.conectar_aws import ConectAWS
from botocore.exceptions import ClientError

#buscar credencial que esta gravda dentro secret do aws
def get_secret():

    cred = ConectAWS() 
    awssecret = cred.get_svc_user_credentials()

    # credencial solar
    secret_name = "ibridge-credentials"

    # credencial simples
    #secret_name = "rdstation-credentials"

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

    # def get_rdstation_token(self):

    #     # Getting RD Station Credentials
    #     credentials = json.loads(get_secret())

    #     client_id = credentials['client_id']
    #     client_secret = credentials['client_secret']
    #     refresh_token = credentials['refresh_token']

    #     url_auth = "https://api.rd.services/auth/token"

    #     headers = {
    #         'Content-Type':'application/json'
    #     }

    #     payload = {'client_id':client_id,
    #             'client_secret':client_secret,
    #             'refresh_token':refresh_token
    #             }

    #     r = requests.post(url_auth, headers=headers, data=json.dumps(payload))
    #     response = json.loads(r.text)

    #     token = response['access_token'] 
        
    #     return token
