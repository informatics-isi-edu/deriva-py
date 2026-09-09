
import enum
import sqlite3
import json

def sql_identifier(s):
    """Apply SQL identifier quoting to the given str identifier"""
    # double " to protect from SQL
    return '"%s"' % (s.replace('"', '""'))

def sql_literal(v):
    """Apply SQL literal quoting to the given value"""
    if v is None:
        return 'NULL'
    if type(v) is list:
        s = json.dumps(v)
    # double ' to protect from SQL
    s = '%s' % v
    return "'%s'" % (s.replace("'", "''"))

class SqlConstraint (enum.IntFlag):
    NOTNULL = enum.auto()
    UNIQUE = enum.auto()
    PRIMARYKEY = enum.auto()

class SqlType (enum.StrEnum):
    INT = enum.auto()
    REAL = enum.auto()
    TEXT = enum.auto()
    JSON = enum.auto()
    BOOLEAN = enum.auto()

class Sqlite3JsonSupport:
    """JSON adaption and conversion for sqlite3 JSON columns

    Applies JSON serialization/deserialization to Python lists and
    dicts to store as JSON text in a "JSON" column.

    """
    @classmethod
    def adapt_json(cls, data):
        """Serializes dict/list into a JSON string encoded as bytes"""
        return json.dumps(data).encode("utf-8")

    @classmethod
    def convert_json(cls, blob):
        """Deserializes bytes from the DB back into a Python dict/list"""
        return json.loads(blob.decode("utf-8"))

    @classmethod
    def register(cls):
        """Register JSON support functions with sqlite3 module."""
        sqlite3.register_adapter(dict, cls.adapt_json)
        sqlite3.register_adapter(list, cls.adapt_json)
        sqlite3.register_converter(SqlType.JSON, cls.convert_json)

    @classmethod
    def connect(cls, dbname):
        """Open and return a sqlite3 DB connection with JSON adaptation
        """
        conn = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
        return conn
