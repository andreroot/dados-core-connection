from pymongo import MongoClient, InsertOne
import pymongo
from pymongo.errors import BulkWriteError, AutoReconnect

from typing import Any, List, Generator
import functools
from multiprocessing import Pool
import os
import sys
import logging
import time
import json
import pandas as pd

from dados_core.utils.data_table.Preprocessor import Preprocessor
from dados_core.database.MongoDB import MongoDB

from datetime import datetime, date, timedelta

MAX_AUTO_RECONNECT_ATTEMPTS = 5
def graceful_auto_reconnect(mongo_op_func):
    """Gracefully handle a reconnection event."""
    @functools.wraps(mongo_op_func)
    def wrapper(*args, **kwargs):
        for attempt in range(MAX_AUTO_RECONNECT_ATTEMPTS):
            try:
                return mongo_op_func(*args, **kwargs)
            except AutoReconnect as e:
                wait_t = 0.5 * pow(2, attempt) # exponential back off
                logging.warning("PyMongo auto-reconnecting... %s. Waiting %.1f seconds.", str(e), wait_t)
                time.sleep(wait_t)
    return wrapper


INDEX_PROPERTY_NAMES = ["reference_date", "reference_datetime", "reference_year", "country_iso_alpha3"]


class MongoDataWriter:

    @staticmethod
    def chunk_list_by_size(lst: List[Any], chunk_size: int) -> Generator[List[Any], None, None]:
        """
        Splits a list into chunks of a specified maximum size.

        :param lst: A list of items to be chunked.
        :param chunk_size: The maximum size in bytes for each chunk.
        :return: A generator that yields the chunks of the input list.
        """

        current_chunk = []
        current_size = 0

        for item in lst:
            item_size = sys.getsizeof(item)
            if current_size + item_size > chunk_size:
                if len(current_chunk) > 0:
                    yield current_chunk
                current_chunk = [item]
                current_size = item_size
            else:
                current_chunk.append(item)
                current_size += item_size

        if current_size > 0 and len(current_chunk) > 0:
            yield current_chunk


    @staticmethod
    @graceful_auto_reconnect
    def chunk_insert(connection_data, chunk):
        try:
            mongo_client = MongoClient(host=connection_data["host"], port=connection_data["port"],
                                    username=connection_data["username"],
                                    password=connection_data["password"],
                                    authSource=connection_data["auth_source"],
                                    authMechanism=connection_data["auth_mechanism"],
                                    connectTimeoutMS=None)
            # Get database
            mongo_db = mongo_client[connection_data['database']]
            collection = mongo_db[connection_data['collection_name']]

            m_result = collection.bulk_write([InsertOne(doc) for doc in chunk], ordered=False)
            data_entries = m_result.inserted_count                   
            mongo_client.close()
            return data_entries
        except BulkWriteError as bwe:
            if bwe.details["nInserted"] > 0:
                data_entries = bwe.details["nInserted"]
                return data_entries
            else:
                return 0


    @staticmethod
    def insert(connection_data, data, throttle_time=0):
        # Initialize connection to mongo store
        mongo_client = MongoClient(host=connection_data["host"], port=connection_data["port"],
                                username=connection_data["username"],
                                password=connection_data["password"],
                                authSource=connection_data["auth_source"],
                                authMechanism=connection_data["auth_mechanism"],
                                connectTimeoutMS=None)
        # Get database
        mongo_db = mongo_client[connection_data['database']]
        # Generate collection name
        collection_name = connection_data['collection_name']
        collection = mongo_db[collection_name]

        # Create unique index for entry_id and descending index on entry_datetime
        # It seems like it is unecessary but there is a weired bug of duplicates
        # -  at least on some collections
        collection.create_index([("entry_id", pymongo.ASCENDING)], unique=True)
        collection.create_index([("entry_id", pymongo.DESCENDING)], unique=True)
        collection.create_index([ ("entry_datetime", pymongo.DESCENDING) ])
        collection.create_index([ ("data_version", pymongo.DESCENDING) ])


        # Create index for well-known datetime and location reference properties
        for property_name in data[0].keys():
            if property_name in INDEX_PROPERTY_NAMES:
                collection.create_index([ (property_name, pymongo.DESCENDING) ])
        
        # assert not (collection_name in mongo_db.list_collection_names(
        # )), "MONGO_COLLECTION_NAME_CONFLICT"

        # Ordered False == Continue after errors like duplicate keys
        # Insert data
        # Chunk data if it's too large
        cpu_count = max(int(os.cpu_count()*0.4), 1)
        #cpu_count = 3
        pool = Pool(cpu_count)

        # Chunk data by actual data size
        dataset_chunks = MongoDataWriter.chunk_list_by_size(data, 1)

        # Batch the batch if things are getting too big
        result = []
        data_entries = []
        for index, _dataset_chunk in enumerate(dataset_chunks):
            result.append(pool.starmap(MongoDataWriter.chunk_insert, [(connection_data, chunk) for chunk in [_dataset_chunk[x:x+(1500*1)] for x in range(0, len(_dataset_chunk), (1500*1))]]))              

            if throttle_time > 0:
                time.sleep(throttle_time) # Let things settle

        # Filter out error messages, shouldn't be there but to be safe
        for _result in result:
            data_entries.extend([e for e in _result if isinstance(e, int)])

        if len(data_entries) > 0:
            data_entries = sum(data_entries)
        else:
            data_entries = 0


        return data_entries


    ## Novo insert impementado para atender estrutura incremental
    @staticmethod
    def insert_many(df_base, connection_data, type_insert):

        ## PREPARAÇÃO DOS DADOS ####
        result = Preprocessor.preProcessamentoDadosCore(df_base)
        
        # ## CONEXÃO AOS DADOS E INSERÇÃO NO MONGODB ####
        # con = ConectMongodb()
        # connection_data = con.get_string_connection(name_db, name_collection)

        # print(connection_data['host'])

        db = MongoDB.connect(
            host=connection_data['host'],
            port=connection_data['port'],
            username=connection_data['username'],
            password=connection_data['password'],
        )
        # Get database
        mongo_db = db[connection_data['database']]
        collection = mongo_db[connection_data['collection_name']]
        print(collection)

        #records = json.loads(df_base.T.to_json()).values()
        #collection.insert_many(result)

        if type_insert=='full':
            
            collection.insert_many( result )

        else:
            lista_param = df_base['dataDia'].drop_duplicates().tolist()

            for i in lista_param:
                print(i)
                lista = {'dataDia': i} #{'_id': ObjectId('656a10f1015952a422aeba03')}
                # #SE/CO
                x = collection.delete_many(lista)

                print(x.deleted_count, " documents deleted.")

            collection.insert_many( result )

        collection.create_index([("entry_id", pymongo.ASCENDING)], unique=True)
        collection.create_index([("entry_id", pymongo.DESCENDING)], unique=True)
        collection.create_index([ ("entry_datetime", pymongo.DESCENDING) ])
        collection.create_index([ ("data_version", pymongo.DESCENDING) ])

    @staticmethod
    def truncate(connection_data, collection_name, property_names=[]):
        mongo_client = MongoClient(host=connection_data["host"], port=connection_data["port"],
                                username=connection_data["username"],
                                password=connection_data["password"],
                                authSource=connection_data["auth_source"],
                                authMechanism=connection_data["auth_mechanism"],
                                connectTimeoutMS=None)
        # Get database
        mongo_db = mongo_client[connection_data['database']]
        collection = mongo_db[collection_name]

        # Drop the collection and all indexes with it - there is no way back - good luck homie
        collection.drop()

        # Re-assign collection
        collection = mongo_db[collection_name]

        # Make sure collection is gone
        assert not (collection_name in mongo_db.list_collection_names(
        )), "MONGO_COLLECTION_NAME_CONFLICT"

        # Create new collection
        collection = mongo_db[collection_name]

        # Create unique index for entry_id and descending index on entry_datetime
        # It seems like it is unecessary but there is a weired bug of duplicates
        # -  at least on some collections
        collection.create_index([("entry_id", pymongo.ASCENDING)], unique=True)
        collection.create_index([("entry_id", pymongo.DESCENDING)], unique=True)
        collection.create_index([ ("entry_datetime", pymongo.DESCENDING) ])
        collection.create_index([ ("data_version", pymongo.DESCENDING) ])


        # Create index for well-known datetime and location reference properties
        for property_name in property_names:
            if property_name in INDEX_PROPERTY_NAMES:
                collection.create_index([ (property_name, pymongo.DESCENDING) ])

        return True
    

    @staticmethod
    def delete_collection(connection_data, collection_name):
        mongo_client = MongoClient(host=connection_data["host"], port=connection_data["port"],
                                username=connection_data["username"],
                                password=connection_data["password"],
                                authSource=connection_data["auth_source"],
                                authMechanism=connection_data["auth_mechanism"],
                                connectTimeoutMS=None)
        # Get database
        mongo_db = mongo_client[connection_data['database']]
        collection = mongo_db[collection_name]

        # Drop the collection and all indexes with it - there is no way back - good luck homie
        collection.drop()

    @staticmethod
    def find_collection(df_base=None, connection_data=None):

        # encontrar informações no mongo db
        db = MongoDB.connect(
            host=connection_data['host'],
            port=connection_data['port'],
            username=connection_data['username'],
            password=connection_data['password'],
        )
        # Get database
        mongo_db = db[connection_data['database']]
        collection = mongo_db[connection_data['collection_name']]
        print(collection)

        if len(df_base)>0:

            lista_param = df_base['dataDia'].drop_duplicates().tolist()

            for i in lista_param:
                print(i)
                parametro_mongodb = {'dataDia': {"$regex": i}} #{'_id': ObjectId('656a10f1015952a422aeba03')}
                print(list(collection.find(parametro_mongodb)))

        # nova pesquisa - ulitmos 30dias
        date_atual_param = (date.today()-timedelta(30)).strftime("%Y-%m-%d")

        cursor = collection.aggregate([
                {
            '$match': {"Fonte":{"$eq":"Convencional"},
                    'Submercado':{"$eq":"SE/CO"},
                    'UnidadeValor':{"$eq":"Preço Fixo"},
                    'agrupador': { "$in":['TRIMESTRAL', 'SEMESTRAL', 'ANUAL']}} # Substitua pelo valor desejado para filtrar eq = | gte >  | let < 
            },
            
            {"$group":
            {"_id":"$dataDia",
            "total":{"$sum":"$NumeroDeNegocios"},
            "max_data":{"$max":'$dataDia'}
            }}
            #filtro data
            ,{
                '$match': {
                    '_id': {'$gte': date_atual_param}  # Substitua pelo valor desejado para filtrar eq = | gte >  | let < 
                }
            }
        ])
        list = []
        for document in cursor:
            list.append(document)
            #print(document)
        df_list = pd.DataFrame(list)
        #df_list.sort_values(by=['_id'], ascending=False)

        return df_list