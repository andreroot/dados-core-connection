from pydantic import field_validator
from datetime import datetime
from typing import ClassVar

from dados_core.core.models.Base import Base

class DataUpdate(Base):
    
    COLLECTION: ClassVar[str] = "data_table_updates"

    data_table_id: str | None = None
    type: str
    store_version: str | None = None
    data_version: str
    datetime: str | datetime
    data_entries: int
    schema_change: bool
    deprectated: dict


    @field_validator('type')
    @classmethod
    def ensure_right_store_type(cls, v):
        if v.upper() not in ["DATA", "STORE"]:
            raise ValueError('Incorrect store type, only DATA and STORE are allowed.')
        return v.upper()

    def rollback(self):
        pass