from dados_core.core.models.data_table.DataItemCollection import DataItemCollection
from dados_core.utils.meta.constants.DICMeta import DICMeta
from dados_core.utils.ai.gpt.OpenAIGPT import OpenAIGPT

class MetaDataItemCollection:
    @staticmethod
    def get_meta_data(data_item_collection: DataItemCollection, data_table  = None):
        if data_item_collection.name in DICMeta.DIC_META:
            return DICMeta.DIC_META.get(data_item_collection.name)
        
        # Get description and definition from GPT
        try:
            prompt = f'''As a data scientist, please provide a description and definition for the "{data_item_collection.name}" column of a dataset called {data_table.name["en"]}. Your task is to write a concise and accurate description that provides insight into the meaning and purpose of the column, without explicitly naming the dataset or column. Your response should be in the format of a JSON object with the following structure, do not use the words column and dataset. The description must be longer than the definition: {{"description": {{"en": "DESCRIPTION"}}, "definition": {{"en": "DEFINITION"}}}}.'''
            answer = OpenAIGPT.get_json_answer(prompt=prompt)

            return answer
        except Exception as e:
            return None