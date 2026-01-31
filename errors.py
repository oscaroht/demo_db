
class DBError(Exception):
    """Base error class"""
    rollback=True

class ParserError(DBError):
    """Error in parsing the SQL. """
    rollback=False

class SQLSyntaxError(ParserError):
    pass
class ValidationError(ParserError):
    pass

class TableNotFoundError(ValidationError):
    pass
class NamingError(ValidationError):
    pass
