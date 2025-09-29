DICMeta = {
    "entry_id": {
        "description": {
            "en": "The ID is a unique fingerprint that is calculated based on the values of all specific columns."
        },
        "definition": {
            "en": "Unique record identifier and primary key"
        },
        "semantic_type": "Internal ID"
    },
    "data_version": {
        "description": {
            "en": "Each new set of records that is added to the data stream gets automatically versioned."
        },
        "definition": {
            "en": "Version of the record"
        },
        "semantic_type": ""
    },    
    "entry_datetime": {
        "description": {
            "en": "The ISO-8601 timestamp of when the record was added to the data stream."
        },
        "definition": {
            "en": "Timestamp of the record"
        },
        "semantic_type": "Datetime"
    },  

    # Normal columns
    "reference_date": {
        "description": {
            "en": ""
        },
        "definition": {
            "en": "The reference date of the values in the column"
        },
        "semantic_type": "Date"
    },  

    # Specific_Hack
    "region_id": {
        "description": {
            "en": ""
        },
        "definition": {
            "en": "The location as administrative area key"
        },
        "semantic_type": "Location"
    },
    "region_type": {
        "description": {
            "en": ""
        },
        "definition": {
            "en": "The location-based granularity of the specified administrative area"
        },
        "semantic_type": ""
    }, 
    "statistic_variable": {
        "description": {
            "en": ""
        },
        "definition": {
            "en": "The official variable name defined by the Federal Statistical Office of Germany"
        },
        "semantic_type": ""
    }, 
    "statistic_variable_type": {
        "description": {
            "en": ""
        },
        "definition": {
            "en": "The official variable type, either 'value' or 'factual'"
        },
        "semantic_type": ""
    }, 
    "dimensions": {
        "description": {
            "en": ""
        },
        "definition": {
            "en": "The dimensions of the underlying statistical cube"
        },
        "semantic_type": ""
    }, 
    "dimension_values": {
        "description": {
            "en": ""
        },
        "definition": {
            "en": "The value codes of the referenced dimensions"
        },
        "semantic_type": ""
    }, 
    "multi_dimensional": {
        "description": {
            "en": ""
        },
        "definition": {
            "en": "Indicator if the statistical value is based on multiple dimensions or not"
        },
        "semantic_type": ""
    }, 
    "statistic_value": {
        "description": {
            "en": ""
        },
        "definition": {
            "en": "The statistical value on the reference date for the referenced dimensions"
        },
        "semantic_type": ""
    }, 


}