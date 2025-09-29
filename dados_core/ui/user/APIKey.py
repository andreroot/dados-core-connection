from __future__ import annotations

from typing import ClassVar

from dados_core.core.models.Base import Base
from dados_core.exceptions.APIKeyDisabledError import APIKeyDisabledError

from datetime import datetime


class APIKey(Base):
    COLLECTION : ClassVar[str] = "user_auth"

    name: str | None
    disabled: bool = False
    api_key: str
    type: str = "API_KEY"

    linked_user: str

    @property
    def latest_use(self) -> datetime:
        raise NotImplementedError()
    
    def get_linked_user(self):
        from dados_core.ui.user.User import User
        user = User.load(self.linked_user, self.connection_data)
        return user

    def disable(self):
        if self.disabled:
            raise APIKeyDisabledError('The API Key is already disabled!')
        self.disabled = True
        self.deleted_at = datetime.utcnow().isoformat()
        
        
    def to_public_dict(self, mask_secret: bool = True):
        data = self.model_dump(include={'key', 'api_key', 'linked_user', 'created_at', 'updated_at', 'disabled', 'name'})
        if mask_secret:
            data['api_key'] = data['api_key'][:4] + '*' * len(data['api_key'][4:-4]) + data['api_key'][-4:]
        return data