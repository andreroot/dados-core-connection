from __future__ import annotations
from pydantic import Field

from pymongo.collection import Collection
from typing import ClassVar

from dados_core.core.models.Base import Base

from dados_core.database.utils.MongoJsonEncoder import MongoJSONEncoder
from bson.objectid import ObjectId

import json

class DataItemCollection(Base):

    COLLECTION: ClassVar[str] = "data_item_collections"

    definition: dict | str | None = None
    meta: dict | None = Field(default=None, exclude=True)
    basic_data_type: str | None = None
    semantic_type: str | None = Field(default=None, exclude=True)
    semantic_tags: list | None = Field(default=None, exclude=True)
    data_tables: list | None = Field(default=None, exclude=True)

    @classmethod
    def load_by_name_and_data_table_id(cls, name, data_table_id, db, connection_data) -> DataItemCollection:
        collection : Collection  = db[connection_data["database"]][cls.COLLECTION]

        data = collection.find_one({'name': name, 'data_tables': ObjectId(data_table_id)})

        data = json.loads(MongoJSONEncoder().encode(data))

        if data is None:
            return None

        # Set connection attributes
        data["connection_data"] = connection_data

        return cls(**data)


    def self_destruct(self):
        result = self.db[self.connection_data["database"]][self.COLLECTION].delete_one({"_id": ObjectId(self.id)})
        print("DELETED ", self.id, " -- STATUS -- ", result)