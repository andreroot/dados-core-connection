from firebase_admin import credentials, initialize_app
from firebase_admin.auth import create_user, UserRecord, delete_user as fb_delete_user

try:
    cred = credentials.Certificate("/credentials/firebase_admin_credentials.json")
except:
    cred = credentials.Certificate("/Users/safira/develop/core/dados-core/firebase_admin_credentials.json")

app = initialize_app(cred)

def create_user_with_email_and_password(email: str, password: str) -> UserRecord:
    user_auth = create_user(email=email, password=password, app=app) 
    return user_auth

def delete_user(uid: str) -> None:
    fb_delete_user(uid, app)


    

