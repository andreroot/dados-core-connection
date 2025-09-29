import copy
import hashlib
import json
import os
import uuid
from datetime import datetime
from typing import ClassVar, List

import numpy as np
import openai
from openai.embeddings_utils import get_embedding
from rich import print
from bson.objectid import ObjectId


# Exceptions
from dados_core.exceptions.EntityDoesNotExist import EntityDoesNotExist
from dados_core.exceptions.EntitySaveFailure import EntitySaveFailure
from dados_core.exceptions.DataTableCreationError import DataTableCreationError

# Models
from dados_core.core.models.Base import Base
from dados_core.core.models.enums.FilterOperator import FilterOperator
from dados_core.core.models.source.Source import Source
from dados_core.core.models.data_table.DataItemCollection import DataItemCollection
from dados_core.core.models.data_table.DataUpdate import DataUpdate
from dados_core.utils.meta.MetaDataItemCollection import MetaDataItemCollection
from dados_core.utils.data_table.Preprocessor import Preprocessor
from dados_core.utils.data_table.MongoCollectionMaterializer import MongoCollectionMaterializer
from dados_core.utils.data_table.SchemaInference import SchemaInference
from dados_core.utils.data_table.MongoDataWriter import MongoDataWriter

from dados_core.database.utils.MongoJsonEncoder import MongoJSONEncoder


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class DataTable(Base):

    COLLECTION: ClassVar[str] = "data_tables"

    keywords: list = []
    unique_label: str | None = None
    data_items_unique: list | None = None
    data_item_collections: list = []
    data_sample: dict | None = None
    source: dict | None
    store: dict | None = None
    store_version: str | None = None
    data_version: str | None = None
    data_updated_at: str | None = None
    data: list | None = None
    update_policy: dict | None = None
    unique_label: str | None = None
    indexing: dict| None = None 



    @classmethod
    def create(cls, table_definition,data: list = []):
        if not isinstance(data, list) or len(data) < 1:
            raise DataTableCreationError(message="Can't create table with empty data.")

        DO_ROLLBACK = False
        data_table = DataTable.model_validate(table_definition)

        try:
            data_table.save()
        except EntitySaveFailure as e:
            raise DataTableCreationError(message=e)

        try:
            data_table.append_data(data=data, store_update=True)
        except Exception as e:
            print("Exception while appending data to table ", e)
            DO_ROLLBACK = True

        # Delete all data item collections that have been created
        # All mongo created collections as well
        if DO_ROLLBACK:
            # Delete all data item collections that potentially have been created
            for dic in data_table.get_data_item_collections():
                dic.self_destruct()

            # Delete the mongo collections that potentially haven been created
            MongoDataWriter.delete_collection(
                data_table.store_data, data_table.store["collection_name"])

            # Delete the table object itself
            data_table.self_destruct()

            raise DataTableCreationError(
                "Can't create table - rollback is true - everything has been rolled back")

        if DO_ROLLBACK == False:
            data_table.save()
        else:
            raise DataTableCreationError("Can't create table - rollback is true")

        return data_table

    @property
    def source_as_object(self) -> Source:
        return Source.load(self.source["id"], connection_data=copy.deepcopy(self.connection_data))

    def _update_allowed(self, update_type):
        return update_type.upper() in self.update_policy.get("permitted_types")

    def resolve_properties(self):
        resolved_data_item_collections = list()
        connection_data = copy.deepcopy(self.connection_data)
        for data_item_collection in self.data_item_collections:
            resolved_data_item_collections.append(DataItemCollection.load(
                data_item_collection.get("_id"), connection_data=connection_data))

        self.data_item_collections = resolved_data_item_collections

    def count(self):
        if self.store.get("type") == 'MONGO_COLLECTION':

            materializer = MongoCollectionMaterializer(
                connection_data={**self.store_data, 'collection_name': self.store['collection_name']})
            self.meta["entry_count"] = materializer.count()
            self.meta["main_property_count"] = len(self.data_item_collections)

        return {"entry_count": self.meta["entry_count"], "main_property_count": self.meta["main_property_count"]}

    def add_version(self, fb_store_version=None, deprecated_store=None, fb_data_version=None, fb_datetime=None, data_entries=0, schema_change=False, update_type="DATA"):
        assert update_type in [
            "DATA", "STORE"], "UNSUPPORTED_UPDATE_TYPE_FOR_DATA_TABLE"
        result = False
        if update_type == 'DATA':
            result = self._add_data_version(fb_store_version=fb_store_version, deprecated_store=deprecated_store,
                                            fb_data_version=fb_data_version, fb_datetime=fb_datetime, data_entries=data_entries, schema_change=schema_change)
        elif update_type == 'STORE':
            result = self._add_store_version(fb_store_version=fb_store_version, deprecated_store=deprecated_store,
                                             fb_data_version=fb_data_version, fb_datetime=fb_datetime, data_entries=data_entries, schema_change=schema_change)
        return result

    def add_data_item_collections(self, data_item_collections):
        # No return type!
        self.data_item_collections.extend(data_item_collections)

    def update_version(self, data_version, store_version, fb_datetime, data_entries, schema_change):

        # Create new data version entry
        update_connection_data = copy.deepcopy(self.connection_data)
        data_update = DataUpdate.model_validate({
            "connection_data": update_connection_data,
            "data_table_id": self.id,
            "type": "DATA",
            "fb_store_version": store_version,
            "fb_data_version": data_version,
            "fb_datetime": fb_datetime,
            "data_entries": data_entries,
            "schema_change": schema_change,
            "deprectated": {
                "store_version": self.store_version,
                "data_version": self.data_version,
                "store": self.store
            }
        })

        #try:
        data_update.save()
        # Set the new data version
        self.data_version = data_version

        # Set the new store version
        self.store_version = store_version
        # except Exception as e:

        #     print("ERROR IN DATA UDATE SHIIISHHHH !!!!!!")

    def count_updates(self, since_date: datetime, update_type: str | None) -> int:
        """
        Count the number of updates in the collection for a given data table, 
        filtered by a date and an optional update type.
        
        :param since_date: The start date to filter updates from.
        :type since_date: datetime
        
        :param update_type: The type of update to filter by. It should be either 'DATA' or 'STORE'
        :type update_type: Optional[str]
        
        :return: The number of updates matching the filters
        :rtype: int
        """

        _update_type_aql_filter = ''
        if update_type is not None:
            assert update_type in ["DATA", "STORE"], "UNSUPPORTED_UPDATE_TYPE"
            _update_type_aql_filter = f' AND dv.type == "{update_type}" '

        aql = f'''
            FOR dv IN data_table_updates
                FILTER dv.data_table_id == @data_table_id AND dv.created_at >= @since_date {_update_type_aql_filter}
                COLLECT WITH COUNT INTO length
                RETURN length
        '''

        cursor = self.db.aql.execute(
            aql,
            bind_vars={"since_date": since_date.isoformat(),
                       'data_table_id': self.id}
        )

        result = list(cursor.batch())
        if len(result) < 1:
            return 0
        return result[0]

    def materialize_data(self, parameters=None, version_boundary=None):
        '''
            This method matrializes the data of the data table
        '''
        if parameters is None:
            parameters = {"sort_keys": ["fb_datetime"], "sort_order": [
                "DESC"], "skip": 0, "limit": 10}

        if not "limit" in parameters.keys():
            parameters["limit"] = 10

        if not "sort_keys" in parameters.keys():
            parameters["sort_keys"] = ["fb_datetime"]
            if not "sort_order" in parameters.keys():
                parameters["sort_order"] = ["DESC"]

        if not "skip" in parameters.keys():
            parameters["skip"] = 0

        if self.store.get("type") == 'MONGO_COLLECTION':
            
            # print("----.-.-SELF.-.-.-.-.-.-.-.-.")
            # print(self)
            # print("----.-.-.-.-.-.-.-.-.-.-.")
            
            # TODO: FIX THIS
            # HACK!
            _store_access_hack = copy.deepcopy(self.store_data["access"] if self.store_data is not None else self.store["access"])
            _store_access_hack["host"] = "localhost"
            _store_access_hack["port"] = 31112

            materializer = MongoCollectionMaterializer(
                connection_data=_store_access_hack)
            self.data = materializer.materialize(
                parameters=parameters, version_boundary=version_boundary)

    def get_data_item_collections(self):
        collection = self.db[self.connection_data["database"]]['data_item_collections']
        cur = collection.find({'data_tables': ObjectId(self.id)})
        result = json.loads(MongoJSONEncoder().encode(list(cur)))
        connection_data = copy.deepcopy(self.connection_data)
        for dic_dict in result:
            dic_dict["connection_data"] = connection_data
            result.append(DataItemCollection.model_validate(dic_dict))
        return result

    def get_data(self, parameters=None, version_boundary=None):
        new_params = {}
        if parameters is not None:
            for p, p_value in parameters.items():
                if p_value is not None and p_value != []:
                    new_params[p] = p_value
            parameters = new_params

        self.materialize_data(parameters=parameters,
                              version_boundary=version_boundary)
        return self.data

    def search(self, query_str: str, parameters: dict = None, version_boundary=None, regex_pattern=False, case_sensitive=False):
        if parameters is None:
            parameters = {}

        parameters["search"] = {
            "query": query_str,
            "regex_pattern": regex_pattern,
            "case_sensitive": case_sensitive
        }

        return self.get_data(parameters=parameters, version_boundary=version_boundary)

    def append_data(self, data, store_update=True, allow_schema_change=False):
        if not self._update_allowed("DATA"):
            raise Exception("This dataset cannot be updated")

        if self.id is None:
            raise Exception("Data table does not have an id. Save it first.")

        # Get current schema
        data_item_collections = self.get_data_item_collections()

        # Not schema information are stored yet, infer new
        schema_change = False
        data_entries = 0

        # Update timestamp
        fb_datetime = datetime.utcnow()

        # define a version for the datasets
        fb_data_version = str(uuid.uuid4())

        # Add Fusionbase specific columns to table
        data = Preprocessor.add_fusionbase_columns(
            data=data, fb_data_version=fb_data_version, fb_datetime=fb_datetime)
        inferred_schema = SchemaInference.infer(data)

        new_data_item_collections = list()
        for schema_item in inferred_schema:
            if not any(data_item_collection.name == schema_item['name'] for data_item_collection in data_item_collections):

                dic_connection_data = copy.deepcopy(self.connection_data)
                data_item_collection = DataItemCollection.load_by_name_and_data_table_id(
                    schema_item.get("name"), self.id, self.db, dic_connection_data)

                # Create new data item collection
                data_item_collection = DataItemCollection.parse_obj({
                    "name": schema_item.get("name"),
                    "definition": {"en": "The description"},
                    "description": {"en": "The definition"},
                    "meta": None,
                    "basic_data_type": schema_item.get("basic_data_type"),
                    "semantic_type": None,
                    "semantic_tags": None,
                    "data_tables": [self.id],
                    "connection_data": dic_connection_data,
                    "created_at": None,
                    "updated_at": None
                })

                dic_meta_data = MetaDataItemCollection.get_meta_data(
                    data_item_collection=data_item_collection, table=self)
                if dic_meta_data is not None:
                    data_item_collection.description = dic_meta_data.get(
                        "description")
                    data_item_collection.definition = dic_meta_data.get(
                        "definition")
                    data_item_collection.semantic_type = dic_meta_data.get(
                        "semantic_type")

                data_item_collection.save()

                # Append newly created item collection so we can delete it later on
                new_data_item_collections.append(data_item_collection)

                # Data item collection does not exist
                # => Schema change
                if data_item_collection is not None:
                    schema_change = True

        # Add the actual data from here
        # This means the data does not yet exists in MongoDB => Initially create it
        if self.store.get("access", {}).get("collection_name") is None:
            self.store["access"]["collection_name"] = str(uuid.uuid4())

        # Write the data entries based on the provided store information
        # HACK!
        _store = copy.deepcopy(self.store)
        host = copy.deepcopy(self.store["access"]["host"])
        port = copy.deepcopy(self.store["access"]["port"])
        _store["access"]["host"] = "localhost"
        _store["access"]["port"] = 31112
        data_entries = MongoDataWriter.insert(
            connection_data=_store.get("access"), data=data)

        if data_entries == 0:
            # Something went wrong, delete the data_item_collections again
            for data_item_collection in new_data_item_collections:
                pass
                #data_item_collection.delete()

            # This means the datatable is initially created
            if len(data_item_collections) == 0:
                #self.delete()
                return None

        # Add new metadata and data item collections to the datatable
        if data_entries > 0:
            # Add new data item collections => new columns / properties have been added
            # Only necessary if there has been a schema change
            if len(new_data_item_collections) > 0:
                self.add_data_item_collections(
                    data_item_collections=new_data_item_collections)

            store_version = self.store_version
            if store_update:
                store_version = str(uuid.uuid4())

            # Update data and store version and create the data update object
            self.update_version(data_version=fb_data_version, store_version=store_version,
                                fb_datetime=fb_datetime, data_entries=data_entries, schema_change=schema_change)

        # HACK TO TEST
        #self.store["access"]["host"] = "fb_core__mongodb_data_store_1__dev"
        #self.store["access"]["port"] = 27019

        self.save()
        return True

    def replace_data(self, data, inplace=False, cascade=True):
        # Check if replacing data is allowed on table
        if not "STORE" in self.update_policy["permitted_types"]:
            raise Exception("It is not allowed!")

        # Wipe current collection of inplace is true
        if inplace:
            # Replace collection inplace, i.e., keep the name. However, this is a full deletion - no way back
            # This method also re-creates the default indices
            # HACK!
            _store = copy.deepcopy(self.store)
            host = copy.deepcopy(self.store["access"]["host"])
            port = copy.deepcopy(self.store["access"]["port"])
            _store["access"]["host"] = "localhost"
            _store["access"]["port"] = 31112

            MongoDataWriter.truncate(
                connection_data=_store["access"], collection_name=self.store['access']['collection_name'], property_names=data[0].keys())
        else:
            # Not inplace, i.e., just push the data into a new storage collection
            self.store["access"]["collection_name"] = str(uuid.uuid4())

        if cascade:
            try:
                self.resolve_properties()
            except Exception as e:
                pass

            for data_item_collection in self.data_item_collections:
                # Remove, removes the actual database entry and totally wipes it
                try:
                    data_item_collection.self_destruct()
                except Exception as e:
                    pass
                pass
            #self.data_item_collections = []

        # Re-Add the data / also create all data item collections and the collection with it
        self.append_data(data=data, store_update=True)
        print("DONE")

    def query(self, query: str = None):
        openai.api_key = os.getenv("OPENAI_API_KEY")
        data_item_collections = self.get_data_item_collections()
        if not query.endswith("?"):
            query += "?"
        description = f'"{self.description["en"].strip()}"' if self.description and self.description["en"] and self.description["en"].strip(
        ) else ""

        filter_operators = list(FilterOperator.__members__.keys())

        system_prompt = f'Today is: {datetime.utcnow().isoformat()}\nGiven a Dataset with the name: "{self.name["en"]}"'
        if description:
            system_prompt += f' and the description: {description},'

        sample_row = self.get_data(parameters={"limit": 1})

        system_prompt += f""" containing the columns: {", ".join([data_item_collection.name for data_item_collection in data_item_collections])}.
        This is a data sample from the dataset: {json.dumps(sample_row, indent=4, cls=CustomEncoder)}

        These are examples, how data is retrieved: data_table.get_data(parameters={{
        "filters":
        [
            {{"property": "age",
            "operator": FilterOperator.LESS_THAN, "value": 49}}
        ], "limit": 10, "project_fields": ['age', 'name', 'email']
        }}
        )
        data_table.get_data(parameters={{\\"sort_keys\\": [\\"fb_datetime\\"], \\"sort_order\\": [
            \\"DESC\\"], \\"skip\\": 0, \\"limit\\": 10}})

        Possible filter operators: {", ".join(filter_operators)}"""

        user_prompt = f'Create a the parameters dict for the following question: \\"{query}\\" \n (Remember that every property is optional)'
        messages = [{"role": "system", "content": f"You transform user queries into a filter operation. Respond only with a valid JSON containing the filter operation. Additional context: {system_prompt}"},    {
            "role": "user", "content": user_prompt}]

        print(system_prompt)
        print(messages)
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            temperature=0,
            max_tokens=600,
            frequency_penalty=0,
            presence_penalty=2,
            messages=messages
        )

        reply = response["choices"][0]["message"]["content"].strip().replace(
            "'", "\"").replace("False", "false").replace("True", "true").replace("None", "null")

        print("REPL----------Y")
        print(reply)
        print("----------")

        start_pos = reply.find('{')
        end_pos = reply.rfind('}') + 1
        dict_string = reply[start_pos:end_pos]
        query_dict = json.loads(dict_string)

        parameters = {}
        filter_objs = []

        for key, value in query_dict.items():
            if key == "filters":
                for f in value:
                    prop = f.get('property')
                    op = FilterOperator[f.get('operator')]
                    val = f.get('value')
                    filter_objs.append(
                        {"property": prop, "operator": op, "value": val})
                parameters["filters"] = filter_objs
            elif value is not None:
                parameters[key] = value

        data = self.get_data(parameters=parameters)
        return {"filters": filter_objs, "data": data}

    def self_destruct(self):
        #return
        result = self.db[DataTable.COLLECTION].delete(self.id)
        print("DELETED ", self.id, " -- STATUS -- ", result)

    # Safe public representation of the table

    def to_public_dict(self, exclude: list = []):
        include = {"id", "key", "name", "description", "meta", "source", "data_item_collections",
                   "store_version", "data_version", "data_updated_at", "updated_at", "data"}
        include = include - set(exclude)
        print(self.model_dump(include=include))
        return self.model_dump(include=include)

    # def BETA__create_structure_conversion_function(self, target_structure: dict):
    #     data_item_collections = self.get_data_item_collections()
    #     prompt_string = f"Create a Python function to format each row of the dataset \"{self.name['en']}\" from \"{self.source.name}\" into the following target structure:\n\n{json.dumps(target_structure, indent=4)}\n\nThe function should take a single argument 'row' which represents a single row of the dataset, and return a dictionary with keys and values corresponding to the target structure.\n\nUse the following columns to help format the data:\n"

    #     for data_item_collection in data_item_collections:
    #         description = data_item_collection.description if data_item_collection.description else "No description available."
    #         prompt_string += f"\n- {data_item_collection.name}: {description}"

    #     sample_row = self.get_data(parameters={"limit": 1})
    #     prompt_string += f"\n\nHere is a sample row of data from the dataset:\n\n{json.dumps(sample_row, indent=4, cls=CustomEncoder)}\n"

    #     return prompt_string
    def resolve_docs(self, api_key="***SECRET CREDENTIALS***", base_uri='https://api.raison-dados.com/', data_table_path=None):
        # Set the default data table path if not provided
        if data_table_path is None:
            data_table_path = f'/api/v1/tables/{self.key}/get?skip=0&limit=10&sort_keys=fb_id&sort_order=desc'

        # Set the headers with the API key
        headers = {"x-api-key": api_key}

        # Build the full data table URL
        data_table_url = base_uri.rstrip('/') + data_table_path

        # cURL snippet
        curl_snippet = f"""
    curl -X GET \\
    {data_table_url} \\
    -H "x-api-key: {api_key}"
    """

        # Python Requests snippet
        python_requests_snippet = f"""
    import requests

    url = "{data_table_url}"
    headers = {{"x-api-key": "{api_key}"}}

    response = requests.get(url, headers=headers)
    data = response.json()
    print(data)
    """

        # Axios snippet
        axios_snippet = f"""
    const axios = require('axios');

    const url = '{data_table_url}';
    const apiKey = '{api_key}';

    axios.get(url, {{
        headers: {{
            'x-api-key': apiKey
        }}
    }})
    .then(response => {{
        const data = response.data;
        console.log(data);
    }})
    .catch(error => {{
        console.error(error);
    }});
    """

        # Java snippet
        java_snippet = f"""
    import okhttp3.*;

    public class FusionbaseDataTableInvoker {{

        public static void main(String[] args) {{
            // Set Fusionbase API endpoint and API key
            String fusionbaseApiUri = "{base_uri}";
            String apiKey = "{api_key}";

            // Create OkHttpClient
            OkHttpClient client = new OkHttpClient().newBuilder().build();

            // Create Request
            Request request = new Request.Builder()
                    .url("{data_table_url}")
                    .method("GET", null)
                    .addHeader("x-api-key", apiKey)
                    .build();

            try {{
                // Execute the request and get the response
                Response response = client.newCall(request).execute();
                String responseBody = response.body().string();
                System.out.println(responseBody);
            }} catch (Exception e) {{
                e.printStackTrace();
            }}
        }}
    }}
    """

        snippets_list = [
            {
                'language': 'cURL',
                'snippet': curl_snippet.strip()
            },
            {
                'language': 'Python Requests',
                'snippet': python_requests_snippet.strip()
            },
            {
                'language': 'Axios',
                'snippet': axios_snippet.strip()
            },
            {
                'language': 'Java',
                'snippet': java_snippet.strip()
            }
        ]

        return snippets_list