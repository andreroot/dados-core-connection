from __future__ import annotations

import os

import redis


class Redis:
    """
    The main class for a connection to the Fusionbase Redis Database in order to cache data
    """
    __connection = None

    @classmethod
    def connect(cls, host: str = None, password: str = None, port: int = 6379, db: int = 0) -> redis.Redis:
        """
        This classmethod is used to establish a database connection to Redis.
        :param host: The Redis Database' Hostname if nothing is provided the "REDIS_HOST" env variable is used
        :param password: The Redis Database' Password if nothing is provided the "REDIS_PASSWORD" env variable is used
        :param port: The port number for the redis connection (default is 6379)
        :param db: the db index name (default 0)
        :return: a Redis connection Object | raises an Error if the connection fails
        """
        if not host:
            host = os.getenv("REDIS_HOST")

        if not password:
            password = os.getenv("REDIS_PASSWORD")

        if not cls.__connection:
            try:
                cls.__connection = redis.Redis(host=host, password=password, port=port, db=db)
            except Exception as e:
                raise ConnectionError(f'Failed to connect the Redis Database. \n Reason: {e}')

        return cls.__connection