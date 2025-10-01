class ScriptListEmptyError(Exception):
    def __init__(self, message="Scripts list is empty") -> None:
        super().__init__(message)