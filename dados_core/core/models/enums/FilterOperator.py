from enum import Enum


class FilterOperator(Enum):
    EQUALS = "eq"
    CONTAINS = "ct"
    STARTS_WITH = "sw"
    ENDS_WITH = "ew"
    NOT_EQUALS = "$ne"
    GREATER_THAN = "$gt"
    GREATER_THAN_OR_EQUALS = "$gte"
    LESS_THAN = "$lt"
    LESS_THAN_OR_EQUALS = "$lte"
    IN = "$in"