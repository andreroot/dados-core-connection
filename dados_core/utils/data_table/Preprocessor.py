import hashlib
from datetime import datetime
import uuid

import numpy as np
import pandas as pd

class Preprocessor:

    def _add_meta_to_known_property(self, _property):
        try:
            _property = {**_property, **self.pre_defined_meta_properties[_property["name"]]}
        except KeyError:
            pass
        return _property

    @staticmethod
    def df_to_dict_perserve_py_types(df, orient="dict"):
        if orient == "records":
            columns = df.columns
            return [dict(zip(columns, row)) for row in df.values]
        else:
            return df.to_dict(orient)
    
    @staticmethod
    def get_entry_id(row, version=2):
        '''
        Private util method to construct entry_id by hashing row values.
        '''
        # TODO: Comparison of datetime objects is not supported. Use working comparison method.
        if version == 1:
            concat_str = ''.join([str(x) for x in np.array(row)])
            return hashlib.sha3_256(concat_str.encode('utf-8')).hexdigest()[:32]

        # New comparison algorithm to fix the potential issue where 1 | ab | 3 == 1a | b3
        concat_str = '-'.join([str(x) for x in np.array(row)])
        return hashlib.sha3_256(concat_str.encode('utf-8')).hexdigest()[:32] 


    @staticmethod
    def drop_columns(data):
        '''
        Remove columns from entry_id calculation
        '''
        data.pop('entry_id', None)
        data.pop('entry_datetime', None)
        data.pop('data_version', None)
        return data

    @staticmethod
    def add_columns(data, data_version=None, entry_datetime=None, keep_old=[]):
        df_base = pd.DataFrame(data)

        # entry_id is always recalculated based on input data
        data_values = [list(Preprocessor.drop_columns(d).values()) for d in data]
        entry_ids = np.array([Preprocessor.get_entry_id(row) for row in data_values])
        if not "entry_id" in df_base.columns.tolist():            
            df_base.insert(0, "entry_id", entry_ids)
        elif "entry_id" in df_base.columns.tolist() and "entry_id" in keep_old:
            pass
        else:
            df_base["entry_id"] = entry_ids

        if not "entry_datetime" in df_base.columns.tolist():
            if entry_datetime is None:
                entry_datetime = datetime.utcnow()
            df_base.insert(len(df_base.columns), "entry_datetime", entry_datetime)
            df_base["entry_datetime"] = pd.Series(df_base["entry_datetime"].dt.to_pydatetime(), dtype = object)
        else:
            df_base["entry_datetime"] = pd.to_datetime(df_base["entry_datetime"])
            df_base["entry_datetime"] = pd.Series(df_base["entry_datetime"].dt.to_pydatetime(), dtype = object)

        # When this method is invoked, it is always a data update
        if data_version is None:
            data_version = str(uuid.uuid4())
        if not "data_version" in df_base.columns.tolist():
            df_base.insert(len(df_base.columns), "data_version", data_version)
        elif "data_version" in df_base.columns.tolist() and "data_version" in keep_old:
            pass
        else:
            df_base["data_version"] = data_version

        # NaN are not JSON encodable directly, use generic None
        df_base = df_base.replace({np.nan: None})

        # Drop duplicates
        # 29.01.2021 - This is necessary since Mongo bugs when creating unique indices before bulk writes.
        # IDs are in index but data isn't sometimes - e.g. if connection interrupts
        # This makes sure that unique indices can be created afterwards
        df_base.drop_duplicates(subset=["entry_id"], inplace=True) 

        return Preprocessor.df_to_dict_perserve_py_types(df_base, orient="records")


    @staticmethod
    def preProcessamentoDadosCore(df_base):

        from datetime import datetime

        def unique(concat_str):
            key = hashlib.sha3_256(concat_str.encode('utf-8')).hexdigest()[:32] 
            return key
        
        df_base["unique"] = df_base.apply(lambda row: ', '.join(map(str, row)), axis=1)

        df_base.insert(0, "entry_id", None)
        df_base["entry_id"] = df_base["unique"].apply(lambda row: unique(row))

        entry_datetime = datetime.utcnow()
        df_base.insert(len(df_base.columns), "entry_datetime", entry_datetime)

        df_base["entry_datetime"] = pd.to_datetime(df_base["entry_datetime"])

        #trecho substituido dia 15/12 - devido to_pydatetime esta obsoleto, foi substituido para ser aplica via funçaõ lambda, validado ok
        
        #df_base["entry_datetime"] = pd.Series(df_base["entry_datetime"].dt.to_pydatetime(), dtype = object)
        df_base["entry_datetime"] = df_base["entry_datetime"].apply(lambda x: pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S.%f'))


        data_version = str(uuid.uuid4())
        df_base["data_version"] = data_version

        # NaN are not JSON encodable directly, use generic None
        df_base = df_base.replace({np.nan: None})

        # Drop duplicates
        # 29.01.2021 - This is necessary since Mongo bugs when creating unique indices before bulk writes.
        # IDs are in index but data isn't sometimes - e.g. if connection interrupts
        # This makes sure that unique indices can be created afterwards
        df_base.drop_duplicates(subset=["entry_id"], inplace=True) 

        df_base.pop('unique')

        return Preprocessor.df_to_dict_perserve_py_types(df_base, orient="records")         