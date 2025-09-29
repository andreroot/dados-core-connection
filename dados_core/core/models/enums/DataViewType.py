from enum import Enum


class DataViewType(str, Enum):
    PowerBI = 'POWER_BI'
    Stremlite = 'STREAMLITE'