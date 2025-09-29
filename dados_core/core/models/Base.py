from __future__ import annotations

from abc import ABC
from typing import Any, ClassVar, List

from datetime import datetime
from datetime import timedelta

from pydantic import ConfigDict, BaseModel, Field

from dados_core.exceptions.InstanceTooOldError import InstanceTooOldError
from dados_core.exceptions.EntityDoesNotExist import EntityDoesNotExist
from dados_core.exceptions.EntitySaveFailure import EntitySaveFailure
from dados_core.core.models.enums.Provision import Provision
from dados_core.core.models.enums.Scope import Scope

from dados_core.database.MongoDB import MongoDB
from dados_core.database.utils.MongoJsonEncoder import MongoJSONEncoder

from pymongo import MongoClient
from bson.objectid import ObjectId
import json

class Base(BaseModel, ABC):
    model_config = ConfigDict(arbitrary_types_allowed=True, convert_underscores=True, use_enum_values=True)


    id: str = Field(default=None, alias='_id')

    COLLECTION: ClassVar[str] = Field(default="", exclude=True)

    connection_data: dict | None = Field(default=None, exclude=True)
    db: MongoClient | None = Field(default=None, exclude=True)
    store_data: dict | None = Field(default=None, exclude=True)


    name: dict | None | str = None
    description: dict | None = None
    meta: dict | None = None
    created_by: str | None = None
    provision: Provision | None = None
    scope: Scope | None = None


    deleted: bool | None = None
    deleted_at: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


    @classmethod
    def load(cls, entity_id, connection_data, store_data=None):
        db = MongoDB.connect(
            host=connection_data['host'],
            port=connection_data['port'],
            username=connection_data['username'],
            password=connection_data['password'],
        )
        collection = db[connection_data["database"]][cls.COLLECTION]

        try:
            bson = collection.find_one({"_id": ObjectId(entity_id)})
        except:
            raise EntityDoesNotExist(cls, entity_id)
        else:
            if bson is None:
                raise EntityDoesNotExist(cls, entity_id)
            

        entity = json.loads(MongoJSONEncoder().encode(bson))

        entity['connection_data'] = connection_data
        entity['db'] = db

        if store_data:
            entity['store_data'] = store_data

        instance = cls.model_validate(entity)
        cls.model_rebuild()
        
        return instance
    
    @classmethod
    def load_by_query(cls, query, connection_data, store_data=None):
        db = MongoDB.connect(
            host=connection_data['host'],
            port=connection_data['port'],
            username=connection_data['username'],
            password=connection_data['password'],
        )
        collection = db[connection_data["database"]][cls.COLLECTION]
        bson = list(collection.find(query, limit=1))        
        if bson is None or isinstance(bson, list) == False or len(bson) == 0:
            raise EntityDoesNotExist(cls, query)
        
        # Necessary since this is a list
        entity = json.loads(MongoJSONEncoder().encode(bson[0]))

        entity["connection_data"] = connection_data
        entity["db"] = db

        if store_data is not None:
            entity["store_data"] = store_data

        instance = cls.model_validate(entity)
        cls.model_rebuild()
        return instance

        
    @classmethod
    def load_many(cls, connection_data, store_data=None, skip=0, limit=20, sort: dict = None) -> List[Base]:
        db = MongoDB.connect(
            host=connection_data['host'],
            port=connection_data['port'],
            username=connection_data['username'],
            password=connection_data['password'],
        )
        collection = db[connection_data["database"]][cls.COLLECTION]
        cur = collection.find({}, skip=skip, limit=limit, sort=sort)
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
    

    @classmethod
    def load_many_by_query(cls, query: dict, connection_data: dict, store_data=None, skip=0, limit=20, sort: dict = None) -> List[Base]:
        db = MongoDB.connect(
            host=connection_data['host'],
            port=connection_data['port'],
            username=connection_data['username'],
            password=connection_data['password'],
        )
        collection = db[connection_data["database"]][cls.COLLECTION]
        cur = collection.find(query, skip=skip, limit=limit, sort=sort)
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
    


    @property
    def store_db(self):
        if self.db is None:
            self.db = MongoDB.connect(
            host=self.connection_data['host'],
            port=self.connection_data['port'],
            username=self.connection_data['username'],
            password=self.connection_data['password'],
        )
        return self.db


    def condense_to_dict_to_ids(self, d, depth=0):
        """
        Recursively checks dictionary for nested dictionaries containing "id", "rev", and "key" properties, and removes all
        other properties except "id".
        """
        if isinstance(d, dict):
            if depth > 0:
                if all(prop in d for prop in ["id"]):
                    return {"_id": d.get("id")}

            for key, value in d.items():
                d[key] = self.condense_to_dict_to_ids(value, depth=depth+1)

        elif isinstance(d, list):    
            d = [self.condense_to_dict_to_ids(item, depth=depth+1) for item in d]

        return d


    def to_store_dict(self, *args, **kwargs) -> Any:
            data = self.model_dump(*args, **kwargs)
            data = self.condense_to_dict_to_ids(data)
            return data
        
    
    def to_public_dict(self, exclude: list = []):
        raise NotImplementedError(f"Class {self.__class__.__name__} does not implement to_public_dict(). Please implement this method in your subclass.")
    
    def get_embeddings(self):
        raise NotImplementedError(f"Class {self.__class__.__name__} does not implement get_embeddings(). Please implement this method in your subclass.")

    def index_embeddings(self):
        raise NotImplementedError(f"Class {self.__class__.__name__} does not implement index_embeddings(). Please implement this method in your subclass.")

    def to_elastic_doc(self):
        raise NotImplementedError(f"Class {self.__class__.__name__} does not implement to_elastic_doc(). Please implement this method in your subclass.")

    def index_elastic(self):
        raise NotImplementedError(f"Class {self.__class__.__name__} does not implement index_elastic(). Please implement this method in your subclass.")
    
    def index_elastic_bulk(self):
        raise NotImplementedError(f"Class {self.__class__.__name__} does not implement index_elastic_bulk(). Please implement this method in your subclass.")


    def save(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow().isoformat()
            self.updated_at = datetime.utcnow().isoformat()

        if not self.db:
            self.db = MongoDB.connect(
                host=self.connection_data['host'],
                port=self.connection_data['port'],
                username=self.connection_data['username'],
                password=self.connection_data['password'],
            )
                        
        collection = self.db[self.connection_data["database"]][self.COLLECTION]
        d = self.to_store_dict(exclude={"id"}, exclude_none=True)
        try:
            if not self.id:
                id = collection.insert_one(d).inserted_id
                self.id = str(id)
            else:
                collection.update_one({'_id':ObjectId(self.id)}, {"$set": d}, upsert=False)
        except:
            raise EntitySaveFailure(self, self.id)


    def delete(self, force:bool=False):
        CASCADE_DELETE_TIME_THRESHOLD_MINUTES = 2
        created_at = datetime.fromisoformat(self.created_at)
        delta = timedelta(minutes=CASCADE_DELETE_TIME_THRESHOLD_MINUTES)
        if (datetime.utcnow() - created_at < delta) or force:
            collection = self.db[self.connection_data["database"]][self.COLLECTION]
            collection.delete_one({"_id": ObjectId(self.id)})
        else:
            raise InstanceTooOldError(
                f"The creation of this instance was more than {CASCADE_DELETE_TIME_THRESHOLD_MINUTES} minutes ago and "
                f"therefore the cascade delete is not possible.")