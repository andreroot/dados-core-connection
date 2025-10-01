class FailedStepError(Exception):
    def __init__(self, status) -> None:
        super().__init__(f"Step failed with status: {status}")