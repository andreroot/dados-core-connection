
from redis.commands.json.path import Path
from dados_core.database.Redis import Redis
from threading import Thread
from datetime import datetime
from deepdiff import DeepHash

import json


def cache(cache_folder: str, ttl: int = 3600):
    def decorator(func):
        def wrapper(*args, **kwargs):
            def __create_hash(obj) -> str:
                hash = DeepHash(obj)[obj]
                return hash
    
            def __generate_key(input) -> str:
                key = f"{cache_folder}:{__create_hash(input)}"
                return key
            
            def __persist_in_database(key:str, data: dict) -> None:
                doc = {
                    'time_of_caching': datetime.utcnow().isoformat(),
                    'value': json.dumps(data, default=str, ensure_ascii=False)
                }
                try:
                    redis_conn.json().set(key, Path.root_path(), doc)
                    redis_conn.expire(key, ttl)
                except Exception as e:
                    return None
                return None
            
            def __save(key: str, data: dict) -> None:
                t = Thread(target=__persist_in_database, args=(key, data))
                t.start()
            
            def __request(key: str):
                try:
                    result = redis_conn.json().get(key)

                    if result is None:
                        print("NO CACHED DATA")
                        return None
                    else:
                        print("CACHED DATA")
                        return result['value']

                except Exception:
                    return None
            

            resource = args[0].id
            inputs = kwargs.copy()
    
            if "invoker_id" in inputs:
                del inputs["invoker_id"]
                invoker_id = kwargs["invoker_id"]
            else:
                invoker_id = None
                
            redis_conn = Redis.connect()
            
            key = __generate_key(inputs)

            cached_result = __request(key)
            if cached_result is not None:
                print("READ CACHED")
                result = json.loads(cached_result)
                return result['output']
            else:
                print("SHOULD BE SKIPPED")

                # If the result is not in the cache, call the original function
                result = func(*args, **kwargs)
                
                data = dict(
                    resource=resource,
                    indentiy_id=invoker_id,
                    input=inputs,
                    output=result
                )
                __save(key, data)
                return result
        return wrapper
    return decorator