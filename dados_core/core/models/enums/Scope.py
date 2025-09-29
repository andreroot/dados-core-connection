from enum import Enum


class Scope(str, Enum):
    Public='PUBLIC'
    Private='PRIVATE'
    Open = 'OPEN'
    PrivateUser = 'PRIVATE_USER'