from __future__ import annotations

from typing import List
from typing import ClassVar

from dados_core.core.models.Base import Base

from pymongo import MongoClient
from bson import json_util
import json

from datetime import datetime
from typing import Any

from bson import ObjectId
from pydantic import ConfigDict


class MongoJSONEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime):
            return str(o)
        return json.JSONEncoder.default(self, o)

class Source(Base):
    model_config = ConfigDict(arbitrary_types_allowed=True, convert_underscores=True, use_enum_values=True)
    label: str
    primary_uri: str
    COLLECTION: ClassVar[str] = "data_sources"

    mongo_db: MongoClient | None = None

    @classmethod
    def load(cls, entity_id: str, connection_data: dict, store_data: dict = None) -> Source:
        source = super().load(entity_id=entity_id, connection_data=connection_data)
        
        if store_data:
            mongo_client = MongoClient(
                host=store_data["auth"]["host"],
                port=store_data["auth"]["port"], 
                username=store_data["auth"]["username"], 
                password=store_data["auth"]["password"]
            )

            source.mongo_db = mongo_client[store_data["store"]["database"]]
        return source

    @classmethod
    def create(cls, name: str, label: str, primary_uri: str, connection_data: dict, description: dict | str = None, store_data: dict = None) -> Source:
        source = Source(
            name=name,
            label=label,
            primary_uri=primary_uri,
            connection_data=connection_data,
            description=description
        )
        source.save()

        if store_data:
            source.store_data = store_data

        return source

    def tables(self, skip: int = 0, limit: int = 50) -> list[dict]:
        return self.get_related('data_tables', skip=skip, limit=limit)

    def get_related(self, collection_name: str, skip: int, limit: int) -> List[dict]:
        if collection_name in ["data_tables"]:
            collection = collection = self.db[self.connection_data["database"]][collection_name]
            cur =  collection.find({"source.id": self.id}, skip=skip, limit=limit)
        
        return json.loads(MongoJSONEncoder().encode(list(cur)))
