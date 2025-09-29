class DataTableCreationError(Exception):
    """Exception raised when a datastream cannot be created.

    Attributes:
        message -- explanation of the error
    """
    def __init__(self, message="GEHT NICHT!!!!"):
        self.message = message
        super().__init__(self.message)