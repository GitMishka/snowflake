
import snowflake.connector as sf
import pandas as pd
from snowflake.connector import DatabaseError, ProgrammingError
from snowflake.connector import SnowflakeConnection
from snowflake.connector.pandas_loader import PandasWriter
import os
import time

sf_account = os.getenv("SF_ACCOUNT")
sf_user = os.getenv("SF_USER")
sf_password = os.getenv("SF_PASSWORD")
sf_db = os.getenv("SF_DB")
sf_warehouse = os.getenv("SF_WAREHOUSE")
sf_schema = os.getenv("SF_SCHEMA")
sf_table = os.getenv("SF_TABLE")

conn = sf.connect(
    user=sf_user,
    password=sf_password,
    account=sf_account,
    warehouse=sf_warehouse,
    database=sf_db,
    schema=sf_schema,
)

df = pd.DataFrame(
    {
        "id": [1, 2, 3, 4, 5],
        "name": ["John", "Mary", "Peter", "Alice", "Bob"],
        "age": [20, 25, 30, 35, 40],
    }
)

writer = PandasWriter(conn, sf_table)

writer.write(df)

conn.close()
