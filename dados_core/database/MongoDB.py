from __future__ import annotations

from typing import Any

from pymongo import MongoClient
from bson import ObjectId
import json
from datetime import datetime

class MongoDB:
    """
    The main class for a connection to the Fusionbase MongoDB Database
    """
    __connection = None

    @classmethod
    def connect(cls, host: str = None, port: int = None, username: str = None, password: str = None) -> MongoClient:
        """
        This classmethod is used to establish a database connection to MongoDB
        """
        if not cls.__connection:
            try:
                cls.__connection = MongoClient(
                    host=host,
                    port=port,
                    username=username,
                    password=password,
                    authSource='admin',
                    authMechanism='DEFAULT',
                    connect=True
                )
            except Exception as e:
                raise ConnectionError(f'Failed to connect the MongoDB Database. \n Reason: {e}')

        return cls.__connection