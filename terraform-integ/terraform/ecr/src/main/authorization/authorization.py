import os
import boto3

#from authorization.exceptions import GetTokenError
from src.main.authorization import exceptions

class Authorization:
    @classmethod
    def authorize(cls, auth_token: str) -> bool:
        """Checks if provided token is valid
        
        Args:
            auth_token: Authorization token to be checked
        Returns:
            True if token is valid, False if otherwise
        Raises:
            GetTokenError: if cannot get token from Amazon Secrets Manager
        """

        try:
            token = cls.get_token()
        except:
            raise exceptions.GetTokenError()

        return token == auth_token
    
    @staticmethod
    def get_token():
        boto_session = boto3.Session(region_name="us-east-1")
        secretsmanager = boto_session.client('secretsmanager')
        response = secretsmanager.get_secret_value(SecretId=f"{os.getenv('GIT_REPOSITORY_NAME')}_authorization_token")
        token = response['SecretString']
        return token
    