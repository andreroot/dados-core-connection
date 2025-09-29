from enum import Enum


class Provision(str, Enum):
    Marketplace = 'MARKETPLACE'
    Private = 'PRIVATE'