class FileReadError(Exception):
    def __init__(self, message=None):
        m = f"A general error occured reading the file"
        if message:
            m += f": {message}"
        self.message = m
        super().__init__(self.message)

class DatabaseConnectionError(Exception):
    def __init__(self, message=None):
        m = f"An error occured connecting to the database"
        if message:
            m += f": {message}"
        self.message = m
        super().__init__(self.message)

class DatabaseExeError(Exception):
    def __init__(self, query, message=None):
        m = f"An error occured executing a query '{query}'"
        if message:
            m += f": {message}"
        self.query = query
        self.message = m
        super().__init__(self.message)

class DatabaseNoTableError(Exception):
    def __init__(self, table):
        self.message = f"Table does not exist: {table}"
        super().__init__(self.message)