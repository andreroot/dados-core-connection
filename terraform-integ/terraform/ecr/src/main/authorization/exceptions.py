class GetTokenError(Exception):
    def __init__(self, message="Unable to get token") -> None:
        super().__init__(message)