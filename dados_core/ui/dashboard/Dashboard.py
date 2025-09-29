from __future__ import annotations

from typing import ClassVar, List

from dados_core.core.models.Base import Base

from pymongo import UpdateOne
from dados_core.database.MongoDB import MongoDB
from dados_core.database.utils.MongoJsonEncoder import MongoJSONEncoder

from bson.objectid import ObjectId

import json

class Dashboard(Base):
    COLLECTION: ClassVar[str] = "dashboards"

    owner_id: str

    layouts: dict | None = None
    panels: list | None = None
    active: bool | None = None

    @classmethod
    def create(cls, name: str, owner_id: str, layouts: dict, panels: list, connection_data: dict) -> Dashboard:
        dashboard = Dashboard(
            name=name,
            owner_id=owner_id,
            panels=panels,
            layouts=layouts,
            connection_data=connection_data
        )
        dashboard.save()
        dashboard.activate()

        return dashboard
    

    def activate(self):
        db = MongoDB.connect(
            host=self.connection_data['host'],
            port=self.connection_data['port'],
            username=self.connection_data['username'],
            password=self.connection_data['password'],
        )   
        collection = db[self.connection_data["database"]][self.COLLECTION]
        cur = collection.find({'owner_id': self.owner_id, '_id': {'$ne': ObjectId(self.id)}})
        dashboards = list(cur)

        if len(dashboards) > 0:
            collection.bulk_write([UpdateOne({'_id': x['_id']}, {'$set': {'active': False}}) for x in dashboards])
        
        self.active = True
        self.save()


    @classmethod
    def load_many_by_query(cls, query, connection_data, store_data=None, skip=0, limit=20) -> List[Base]:
        db = MongoDB.connect(
                host=connection_data['host'],
                port=connection_data['port'],
                username=connection_data['username'],
                password=connection_data['password'],
            )
        collection = db[connection_data["database"]][cls.COLLECTION]
        cur = collection.find(query, skip=skip, limit=limit)
        entities = json.loads(MongoJSONEncoder().encode(list(cur)))

        def __parse_and_create(e: dict):
            e["connection_data"] = connection_data
            e["db"] = db
            if store_data is not None:
                e["store_data"] = store_data
            instance = cls.model_validate(e)
            cls.model_rebuild()
            instance.connection_data = connection_data
            instance.db = db
            instance.store_data = store_data
            return instance
        
        return [__parse_and_create(e) for e in entities]