class EmailAlreadyExist(Exception):
    def __init__(self, message = "Email already exists"):
        self.message = message