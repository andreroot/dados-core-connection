
import math
import re
import itertools
from datetime import datetime, date

#from pathos.pools import ProcessPool as Pool
from pathos.pools import ThreadPool as Pool
from pymongo import MongoClient
from dados_core.core.models.enums.FilterOperator import FilterOperator

import os

import pymongo


class MongoCollectionMaterializer():

    def __init__(self, connection_data={}):

        self.connection_data = connection_data
        assert 'host' in connection_data, "FALSE_PARAMETERS_FOR_MONGO_MATERIALIZATION"
        
        self.mongo_client = MongoClient(host=connection_data["host"], port=connection_data["port"], username=connection_data["username"], password=connection_data["password"], connectTimeoutMS=800000, serverSelectionTimeoutMS=1200000)
        self.mongo_db = self.mongo_client[connection_data["database"]] 
        self.collection_name = connection_data["collection_name"] 
        


    def _convert_to_datetime(self, input_value):
        """Convert input value to datetime if it's a string."""
        if isinstance(input_value, str):
            try:
                return datetime.strptime(input_value, '%Y-%m-%dT%H:%M:%S.%f')
            except ValueError:
                try:
                    return datetime.strptime(input_value, '%Y-%m-%d')
                except ValueError:
                    raise ValueError("Invalid date string format.")
        elif isinstance(input_value, (datetime, date)):
            return input_value
        else:
            raise ValueError("Input value should be either string or datetime/date object.")

    def sanitize_mongo_query(self, query: dict) -> dict:
        """Sanitizes a MongoDB query to prevent any attack vectors."""
        #!TODO Just an idea i don't know if it rerally works and helps against attacks
        sanitized_query = {}
        for key, value in query.items():
            # Allow certain "$" characters in the query keys, such as those used in query operators.
            if key.startswith('$') or '.' not in key:
                sanitized_key = key
            else:
                sanitized_key = re.sub(r'\$', '', key)
            # If the value is a dictionary, recursively sanitize it.
            if isinstance(value, dict):
                value = self.sanitize_mongo_query(value)
            # If the value is a string, sanitize it by converting any "." characters to "_",
            # except for those that appear in a regex pattern.
            elif isinstance(value, str):
                if key.startswith('$') and key not in ['$regex', '$options']:
                    value = re.sub(r'\.', '_', value)
            sanitized_query[sanitized_key] = value
        return sanitized_query

    def _construct_query(self, query):
        query_dict = {}
        for key, val in query.items():
            key = re.sub("[^\w\-_]+", "", key)
            if isinstance(val, str):
                # Remove everything except word characters and whitespace
                val = re.sub("[^a-zA-Z0-9_\s+\-_\:]+", "", val)
            elif isinstance(val, int):
                val = int(val)
            elif isinstance(val, float):
                val = float(val)
            else:
                continue
            
            query_dict[key] = val
        return query_dict
    

    def _construct_filter_query(self, filters):
        """
        Constructs a filter query from the given filters.
        """
        filter_query = {}
        for filter in filters:
            if not isinstance(filter, dict) or len(filter.keys()) != 3:
                raise ValueError("Filter must be a dictionary with keys 'property', 'value', and 'operator'")
            
            property = filter["property"]
            assert property in self.mongo_db[self.collection_name].find_one().keys(), "property_NOT_FOUND"
            value = filter["value"]
            operator: FilterOperator = filter["operator"]
            assert isinstance(operator, FilterOperator), "WRONG_PARAMETER_FORMAT"

            if operator == FilterOperator.EQUALS:
                query = {property: value}
            elif operator == FilterOperator.CONTAINS:
                query = {property: {"$regex": f".*{re.escape(value)}.*", "$options": "-i"}}
            elif operator == FilterOperator.STARTS_WITH:
                query = {property: {"$regex": f"^{re.escape(value)}", "$options": "-i"}}
            elif operator == FilterOperator.ENDS_WITH:
                query = {property: {"$regex": f"{re.escape(value)}$", "$options": "-i"}}
            else:
                query = {property: {operator.value: value}}
            
            if property in filter_query:
                filter_query[property].append(query)
            else:
                filter_query[property] = [query]
        
        final_query = {"$and": []}
        for property, queries in filter_query.items():
            if len(queries) == 1:
                final_query["$and"].append(queries[0])
            else:
                final_query["$and"].append({"$and": queries})
        
        print("FIAL QUERY ")
        print(final_query)
        return final_query


    def _construct_search_query(self, search_term, regex_pattern=False, case_sensitive=False):
        # escape special regex characters in search term
        # search_term = re.escape(search_term)
        # create a regex pattern to match any field that contains the search term
        regex_pattern_str = f".*{search_term}.*"
        if regex_pattern:
            regex_pattern_str = search_term
        options = "-i" if not case_sensitive else ""
        # create a query that matches the regex pattern for all fields
        query_dict = { "$or": [] }
        for field_name in self.mongo_db[self.collection_name].find_one().keys():
            query_dict["$or"].append({ field_name: { "$regex": regex_pattern_str, "$options": options }})
        return query_dict

    def count(self):
        count = self.mongo_db[self.collection_name].estimated_document_count()
        return count

    def materialize(self, parameters, version_boundary=None):
        assert isinstance(parameters["sort_keys"], list) and isinstance(parameters["sort_order"], list) and len(parameters["sort_keys"]) == len(parameters["sort_order"]), "WRONG_PARAMETER_FORMAT"
        
        parameters["sort_keys"] = [item.strip() for item in parameters["sort_keys"] if item.strip()]
        parameters["sort_order"] = [item.strip() for item in parameters["sort_order"] if item.strip()]
        assert len(parameters["sort_keys"]) == len(parameters["sort_order"]), "WRONG_PARAMETER_FORMAT"
        
        sort_tuples = list(zip(parameters["sort_keys"], parameters["sort_order"]))
        
        query_dict = {}
        version_query = {}
        search_dict = {}

        if "query" in parameters.keys():
            print(parameters["query"])
            query_dict = self._construct_query(parameters["query"])

        if "search" in parameters.keys():
            
            assert "query" in parameters["search"].keys(), "WRONG_PARAMETER_FORMAT"
            assert "regex_pattern" in parameters["search"].keys(), "WRONG_PARAMETER_FORMAT"
            assert "case_sensitive" in parameters["search"].keys(), "WRONG_PARAMETER_FORMAT"

            query_str = parameters["search"]["query"]
            regex_pattern = parameters["search"]["regex_pattern"]
            case_sensitive = parameters["search"]["case_sensitive"]

            search_dict = self._construct_search_query(query_str, regex_pattern, case_sensitive)
        
        if "filters" in parameters:
            filter_query = self._construct_filter_query(parameters["filters"])
            if filter_query:
                query_dict.update(filter_query)

        if version_boundary is not None and isinstance(version_boundary, tuple) and all(version_boundary):  # The last one means that there is no None value in the tuple
            # Get delta between two versions
            if len(version_boundary) > 1:
                start_data = self.mongo_db[self.collection_name].find_one({"data_version":version_boundary[0]})
                end_data = self.mongo_db[self.collection_name].find_one({"data_version":version_boundary[1]})
                if start_data and end_data:
                    start_date = start_data["entry_datetime"]
                    target_start_date = self._convert_to_datetime(start_date)
                    end_date = end_data["entry_datetime"]
                    target_end_date = self._convert_to_datetime(end_date)
                else:
                    print("Data Version not found")

                version_query = {
                    "entry_datetime": {
                        "$lte": target_end_date,
                        "$gt": target_start_date               
                    }
                }
            # Get data of a specific version
            elif len(version_boundary) == 1:
                data = self.mongo_db[self.collection_name].find_one({"data_version":version_boundary[0]})
                if data:
                    date = data["entry_datetime"]
                    target_date = self._convert_to_datetime(date)
                else:
                    print("Data Version not found")
                version_query = {
                    "entry_datetime": {
                        "$gte": target_date             
                    }
                }

            query_dict = {**query_dict, **version_query}
            
            print(query_dict)

        query_dict = {**query_dict, **search_dict}
        # sanitize query to prevent any injection attacks
        query_dict = self.sanitize_mongo_query(query_dict)

        # Build projected fields dictionary
        project_fields_dict = {"_id": 0}  # This is the base, never materialize the mongo internal _id
        if "project_fields" in parameters and parameters["project_fields"] is not None and isinstance(parameters["project_fields"], list) and len(parameters["project_fields"]) > 0:
            # Remove empty string from project fields
            parameters["project_fields"] = list(filter(lambda x: x.strip() != '', parameters["project_fields"]))
            project_fields_dict = {**{field:1 for field in parameters["project_fields"]}, **project_fields_dict}


        ######
        # NOT IN USE, MAYBE LATER, ALLOW LATEST ELEMENT IN GROUP EXPORT
        # aggregation_groups = parameters["aggregation_groups"]
        # aggregation_groups_dict = dict()
        # for agg_group in aggregation_groups:
        #     aggregation_groups_dict = {**aggregation_groups_dict, **{agg_group:f"${agg_group}"}}
        # aggregation_groups_dict = {"_id": aggregation_groups_dict, "last_doc": { "$last": "$$ROOT" }}

        # pipeline = [
        #      {"$sort": {"entry_datetime": 1}}, # ALWAYS SORT BY entry_DATETIME FIRST, OTHERWISE $LAST WILL PRODUCT WRONG RESULTS
        #      aggregation_groups_dict
        # ]
        ####
        
        # Get size of a document in the collection
        try:
            average_document_size = self.mongo_db[self.collection_name].aggregate([
            {
                "$collStats": {
                "storageStats": { },
                },
            },
            {
                "$project": {
                "average_document_size": "$storageStats.avgObjSize",
                },
            },
            ])
            average_document_size = list(average_document_size)[0].get("average_document_size")
        except Exception as e:
            print("ERROR IN AVG DOCUMENT SIZE ", e)
            average_document_size = 1024
            
        
        # Calculate limit based on average document size
        # At one batch we would like to get a max of 1.5 GB
        ONE_AND_A_HALF_GB = 1610612736        
        limit_by_size = int(ONE_AND_A_HALF_GB/average_document_size)    

        # Hard limit of max 150k entries per request  > 150000
        if parameters["limit"] > limit_by_size:
            parameters["limit"] = limit_by_size
            
        print("LIMIT BY SIZE ", limit_by_size)

        # Allow slightly higher number of results via a single request for version updates
        # To properly handly any number of updates this needs to be adjusted later on
        # TODO: Currently only 1.5 GB of the latest data can be exported!
        if any(version_query):
            parameters["limit"] = limit_by_size


        if len(sort_tuples) > 0:
            
            for index, sort_tuple in enumerate(sort_tuples):
                if sort_tuple[1].upper() == 'DESC':
                    sort_tuples[index] = (sort_tuple[0], pymongo.DESCENDING)
                else:
                    sort_tuples[index] = (sort_tuple[0], pymongo.ASCENDING)
                    
            data = self.mongo_db[self.collection_name].find(query_dict, project_fields_dict, limit=parameters["limit"], skip=parameters["skip"]).sort(sort_tuples).batch_size(10000)
            
        elif parameters["limit"] > -1:
            # For some reason, the sorting freezes the cursor in some cases with the query parameter
            if any(query_dict) == False:
                data = self.mongo_db[self.collection_name].find(query_dict, project_fields_dict, limit=parameters["limit"], skip=parameters["skip"]).sort([("_id", pymongo.ASCENDING)]).batch_size(math.ceil(limit_by_size/10))
            else:
                data = self.mongo_db[self.collection_name].find(query_dict, project_fields_dict, limit=parameters["limit"], skip=parameters["skip"]).batch_size(math.ceil(limit_by_size/10))
            # data_count = self.mongo_db[self.collection_name].count_documents(query_dict)
            # print("QUERY ", query_dict, flush=True)

            #data = self.mongo_db[self.collection_name].find(query_dict, project_fields_dict, limit=parameters["limit"], skip=parameters["skip"]).batch_size(10000)
        else:
            # Full dataset!
            # Check how large the dataset is
            collection_count = self.mongo_db[self.collection_name].estimated_document_count()
            # Go Multiprocess if it is larger than 500000 rows
            if collection_count <= 150000:
                data = self.mongo_db[self.collection_name].find(query_dict, project_fields_dict).batch_size(150000)
            else: # Multiprocess from here:: Put it into a dataframe, order, remove duplicates (should be none) and put it back to a dict

                # The maximum number of entries that can be exported at once
                # If more is required, users need to use skip + limit
                # This is specifically for the UI
                if collection_count > 5_000_000:
                    collection_count = 5_000_000

                data = list()
                def chunks(lst, n):
                    """Yield successive n-sized chunks from lst."""
                    for i in range(0, len(lst), n):
                        yield lst[i:i + n]

                chunks = list(chunks(range(0, collection_count+1), 150000))
                
                # Make sure at least 1 CPU is used
                use_cpu_count = os.cpu_count()-4
                if use_cpu_count < 1:
                    use_cpu_count = 1

                with Pool(processes=use_cpu_count) as pool:
                    def get_data(chunk):
                        mongo_client = MongoClient(host=self.connection_data["host"], port=self.connection_data["port"], username=self.connection_data["username"], password=self.connection_data["password"], connectTimeoutMS=800000, serverSelectionTimeoutMS=1200000)
                        mongo_db = mongo_client[self.connection_data["database"]] 
                        collection_name = self.connection_data["collection_name"] 
                        collection = mongo_db[collection_name]
                        cursor = collection.find({}, {'_id': False}).sort([("entry_datetime", pymongo.DESCENDING)]).skip(chunk[0]).limit(len(chunk)).batch_size(150000)
                        return list(cursor)

                    data = list(itertools.chain.from_iterable(pool.map(get_data, chunks)))
                    print(f"-----RESULT::EXPORT::MATERIALIZATION::DONE::RESULT_LEN::{len(data)}----")

                return data

        data = list(data)
        return data