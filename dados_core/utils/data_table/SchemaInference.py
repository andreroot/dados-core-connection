class SchemaInference:

    @staticmethod
    def infer(data):
        """
            This method iterates through the list of data dicts and flattens them.
            In a second step, it checks the type of each of the attributes and sets the local variable.
        """
        UPPER_INFER_LIMIT = 100000
        none_counter = dict()
        all_data_item_keys = list()
        data_schema = list()
        schema_seen = set()
        
        infer_counter = 0
        for item in data:

            # Hard limit to break "infinite" type guess loops
            if infer_counter > UPPER_INFER_LIMIT:
                break
        
            for key, value in item.items():                
                re_infer_attribute = False
                if key in schema_seen:
                    for __schema_item_inner in data_schema:
                        if __schema_item_inner["name"] == key:
                            if __schema_item_inner["basic_data_type"] == 'N/A':
                                #print("RE - INFER - NOW")
                                re_infer_attribute = True
                            else:
                                re_infer_attribute = False 


                if not key in schema_seen or re_infer_attribute:

                    all_data_item_keys.append(key)
                    basic_data_type = str(type(value))
                    basic_data_type = basic_data_type.split("'")[1]

                    if basic_data_type == 'dict':
                        basic_data_type = 'Object'
                    elif basic_data_type == 'str':
                        basic_data_type = 'String'
                    elif basic_data_type == 'boolean':
                        basic_data_type = 'bool'
                    elif basic_data_type == 'datetime.datetime':
                        basic_data_type = 'Datetime'
                    elif basic_data_type == 'pandas._libs.tslibs.timestamps.Timestamp':
                        basic_data_type = 'Datetime'
                    elif basic_data_type == 'NoneType' or basic_data_type == None:
                        if key not in none_counter:
                            none_counter[key] = 1
                        else:
                            none_counter[key]
                        basic_data_type = 'N/A'                   

                    # Enforce "reference_date" column as datetime
                    if key == 'reference_date':
                        basic_data_type = 'Date'

                    if key == 'reference_dateteime':
                        basic_data_type = 'Datetime'

                    # Check if attribute is already in schema, otherwise overwrite
                    if not re_infer_attribute:
                        data_schema.append({
                            "basic_data_type": basic_data_type,
                            "name": key
                        })
                    else:
                        for __index, __schema_item in enumerate(data_schema):
                            if __schema_item["name"] == key:
                                data_schema[__index]["basic_data_type"] = basic_data_type

                # Count infer iterations
                infer_counter += 1
                schema_seen.add(key)

                # Hard limit to break "infinite" type guess loops
                # This breaks the inner loop
                if infer_counter > UPPER_INFER_LIMIT:
                    break       

        return data_schema