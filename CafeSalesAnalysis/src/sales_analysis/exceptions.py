class FileReadError(Exception):

    def __init__(self, message=None):
        m = f"A general error occured reading the file"
        if message:
            m += f": {message}"
        self.message = m
        super().__init__(self.message)
