from __future__ import annotations

from typing import ClassVar, Dict, Any

from dados_core.core.models.Base import Base
from dados_core.ui.user.APIKey import APIKey

from dados_core.exceptions.MissingIdError import MissingIdError
from dados_core.exceptions.EmailAlreadyExistError import EmailAlreadyExist

from dados_core.services.firebase import create_user_with_email_and_password, delete_user

from dados_core.database.utils.MongoJsonEncoder import MongoJSONEncoder

from uuid import uuid4

import json

class User(Base):
    COLLECTION: ClassVar[str] = "users"

    uid: str | None
    first_name: str | None
    last_name: str | None

    email_address: str

    @classmethod
    def create(cls, email_address: str, password: str, connection_data: Dict[str, Any], first_name: str = None, last_name: str = None) -> User:
        user = User(
            first_name=first_name,
            last_name=last_name,
            email_address=email_address,
            connection_data=connection_data
        )

        try:
            fb_user = create_user_with_email_and_password(email=email_address, password=password)
        except Exception as e:
            if "EMAIL_EXISTS" in str(e):
                raise EmailAlreadyExist()
            raise e
        else:
            user.uid = fb_user.uid

        try:
            user.save()
        except Exception as e:
            delete_user(fb_user.uid)
            raise e
        return user
    
    def add_api_key(self, name: str = None):
        if self.id is None:
            raise MissingIdError()

        api_key = APIKey.parse_obj(
            {
                'linked_user': self.id,
                'name': name,
                'api_key': str(uuid4()),
                'connection_data': self.connection_data
            }
        )
        api_key.save()
        return api_key
    
    def list_api_keys(self):
        collection = self.db[self.connection_data["database"]]['user_auth']
        docs = collection.find({'type': 'API_KEY', 'linked_user': self.id, 'disabled': False})

        api_keys = json.loads(MongoJSONEncoder().encode(list(docs)))
        return [APIKey.model_validate(x) for x in api_keys]
        
    
    def public_dict(self):
        return self.model_dump(include={'id', 'uid', 'first_name', 'last_name', 'email_address', 'created_at', 'updated_at'})
    
    def delete(self):
        delete_user(self.uid)
        return super().delete()
    




