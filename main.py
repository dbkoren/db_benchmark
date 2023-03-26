from databases.vertica import VerticaBenchmark
from databases.clickhouse import ClickhouseBenchmark
from datetime import datetime
def run_vertica():
    table_name = 'itay_test'
    schema = (
        "src_ip VARCHAR, dst_ip VARCHAR, time TIMESTAMP, matched BOOLEAN" 
    )
    data = [
        f"src_ip 10.1.2.3, dst_ip 10.1.2.6, time {datetime.now()}, matched true",
        f"src_ip 10.1.2.4, dst_ip 10.1.2.7, time {datetime.now()}, matched false",
        f"src_ip 10.1.2.5, dst_ip 10.1.2.8, time {datetime.now()}, matched true"]
    vertica = VerticaBenchmark(table_name, schema)
    vertica.create_table(table_name)
    import pudb; pudb.set_trace()
    vertica.insert_row_to_table(data[0])
    vertica.insert_bulk_to_table(data[1:])

def run_clickhouse():
    table_name = 'itay_test'
    schema = (
        "src_ip VARCHAR, dst_ip VARCHAR, time TIMESTAMP, matched BOOLEAN" 
    )
    data = [
        ["10.1.2.3", "10.1.2.6", datetime.now(), True],
        ["10.1.2.4", "10.1.2.7", datetime.now(), False],
        ["10.1.2.5", "10.1.2.8", datetime.now(), True],
        ]

    vertica = ClickhouseBenchmark(table_name, schema)
    import pudb; pudb.set_trace()
    vertica.create_table(table_name)
    vertica.insert_row_to_table(data[0])
    vertica.insert_bulk_to_table(data[1:])

if __name__ == '__main__':
    print("we started ...")
    run_clickhouse()
    print("we done...")