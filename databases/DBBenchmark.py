from databases.common.utils import SQL_TO_PYTHON_TYPES

class DBBenchmark():
    def __init__(self, table_name, schema) -> None:
        self.TABLE_NAME = table_name
        self.SCHEMA = schema
        self.schema_type_dict = self._split_str_schema_to_type_dict(self.SCHEMA)

    def insert_bulk_to_table(self, data_bulk):
        raise NotImplemented
    
    def insert_row_to_table(self, data):
        raise NotImplemented
    
    def create_table(self, table_name):
        raise NotImplemented

    def _split_str_schema_to_type_dict(self, schema):
        type_dict = {}
        fields = schema.replace(', ', ',').replace('(', '').replace(')', '').split(',')
        for field in fields:
            field_name, field_type = field.split(' ')
            type_dict[field_name] = self.sql_to_python_type(field_type)
        return type_dict
    
    @staticmethod
    def sql_to_python_type(field_type: str):
        return SQL_TO_PYTHON_TYPES[field_type]
    
    def _get_str_of_value_list(self, data_row):
        res = ""
        for val in self._split_row_to_value_list(data_row):
            res += '\'' + val + '\'' + ','
        return res[:-1]
    