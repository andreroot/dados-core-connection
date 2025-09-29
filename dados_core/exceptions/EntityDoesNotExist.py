class EntityDoesNotExist(ValueError):
    """
    Exception raised when an entity does not exist.

    Attributes:
        cls -- the entity class that was being searched for
        entity_id -- the ID of the missing entity
        message -- explanation of the error (optional)
    """

    def __init__(self, cls, entity_id, message=None):
        self.cls = cls
        self.entity_id = entity_id
        if message is None:
            message = f"{cls.__name__} with ID {entity_id} does not exist."
        super().__init__(message)