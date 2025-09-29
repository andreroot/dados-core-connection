class EntitySaveFailure(Exception):
    """
    Exception raised when an entity fails to be saved.

    Attributes:
        cls: the entity class that was being saved
        entity_id: the ID of the entity that failed to be saved
        message: explanation of the error (optional)
    """

    def __init__(self, cls, entity_id, message=None):
        self.cls = cls
        self.entity_id = entity_id
        if message is None:
            message = f"\n{cls.__name__} with ID {entity_id} failed to be saved."

        else:
            message = f"\n{cls.__name__}\n{entity_id}\n{message}"
        
        super().__init__(message)