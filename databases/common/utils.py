import datetime
import decimal
SQL_TO_PYTHON_TYPES = {
    "INTEGER": int,
    "FLOAT": float,
    "NUMERIC": decimal.Decimal,
    "DATE": datetime.date,
    "CHAR": str,
    "VARCHAR": str,
    "LONG VARCHAR": str,
    "BINARY": bin,
    "VARBINARY": bin,
    "LONG VARBINARY": bin,
    "TIMESTAMP": datetime.datetime,
    "TIME": datetime.time,
    "BOOLEAN": bool
}