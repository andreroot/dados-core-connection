class MissingAWSCredentialsError(Exception):
    def __init__(self, cred: str) -> None:
        super().__init__(f"Missing credential: {cred}")