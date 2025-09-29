class MissingIdError(Exception):
    def __init__(self, message = "Missing id. Load or save instance first"):
        self.message = message