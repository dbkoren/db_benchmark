"""
Running clickhouse:
docker run -d -p 18123:8123 -p19000:9000 --name some-clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server
"""
from clickhouse_driver import Client
from databases.DBBenchmark import DBBenchmark

IP = "127.0.0.1"
PORT = "19000"
class ClickhouseBenchmark(DBBenchmark):
    def __init__(self, table_name, schema):
        self.TABLE_NAME = table_name
        self.SCHEMA = schema
        self.schema_type_dict = self._split_str_schema_to_type_dict(self.SCHEMA)
        self.client = Client(IP, PORT)
    
    def create_table(self, table_name):
        self.client.execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({self.SCHEMA}) ENGINE = MergeTree PARTITION BY toYYYYMMDD(time) ORDER BY (time, matched)')

    def insert_row_to_table(self, data):
        self.client.execute(f'INSERT INTO {self.TABLE_NAME} ({",".join(self.schema_type_dict.keys())}) VALUES', [data])

    def insert_bulk_to_table(self, data_bulk):
        self.client.execute(f'INSERT INTO {self.TABLE_NAME} ({",".join(self.schema_type_dict.keys())}) VALUES', data_bulk)
    
    
        