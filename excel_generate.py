import pandas as pd
import oracledb
from db_connection import get_db_connection

BASE_TABLE_NAME = "ETL_IMPORT_FILE_DATA"

def map_dtype_to_oracle(dtype):
    if "int" in str(dtype):
        return "NUMBER"
    elif "float" in str(dtype):
        return "NUMBER(18,4)"
    elif "datetime" in str(dtype):
        return "DATE"
    else:
        return "VARCHAR2(4000)"

def create_table(cursor, table_name, df):
    columns = []
    for col, dtype in df.dtypes.items():
        oracle_type = map_dtype_to_oracle(dtype)
        columns.append(f"{col.upper()} {oracle_type}")

    ddl = f"""
    CREATE TABLE {table_name} (
        {', '.join(columns)}
    )
    """

    cursor.execute(ddl)
    print(f"Table created: {table_name}")

def load_data(cursor, table_name, df):
    cols = ",".join(df.columns.str.upper())
    binds = ",".join([f":{i+1}" for i in range(len(df.columns))])

    sql = f"INSERT INTO {table_name} ({cols}) VALUES ({binds})"

    data = [tuple(x) for x in df.itertuples(index=False, name=None)]
    cursor.executemany(sql, data)

    print(f"Inserted {len(data)} rows into {table_name}")

def process_excel(file_path):
    conn = get_db_connection()
    cursor = conn.cursor()

    sheets = pd.read_excel(file_path, sheet_name=None)

    for idx, (sheet_name, df) in enumerate(sheets.items()):
        table_name = BASE_TABLE_NAME if idx == 0 else f"{BASE_TABLE_NAME}_{idx}"

        df.columns = df.columns.str.strip().str.upper()

        try:
            create_table(cursor, table_name, df)
        except oracledb.DatabaseError:
            print(f"Table already exists: {table_name}")

        load_data(cursor, table_name, df)

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    process_excel("EMPLOYEE_DATA.xlsx")
