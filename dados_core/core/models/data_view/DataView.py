from __future__ import annotations

from typing import List
from typing import ClassVar

from dados_core.core.models.Base import Base
from dados_core.core.models.enums.DataViewType import DataViewType

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

class DataView(Base):
    model_config = ConfigDict(arbitrary_types_allowed=True, convert_underscores=True, use_enum_values=True)
    COLLECTION: ClassVar[str] = "data_views"

    type: DataViewType
    url: str


    @classmethod
    def create(cls, name: str, url: str, type: str ,connection_data: dict, description: dict | str = None) -> DataView:
        data_view =DataView(
            name=name,
            description=description,
            type=type,
            url=url,
            connection_data=connection_data
        )
        data_view.save()

        return data_view