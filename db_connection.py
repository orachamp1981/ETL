import oracledb

# Initialize Oracle client (optional if using Instant Client)
oracledb.init_oracle_client(lib_dir=r"C:\instantclient_23_6")

def get_db_connection():
    try:
        connection = oracledb.connect(
            user="HR",
            password="HR",
            dsn="localhost:1521/XE"
        )
        return connection
    except oracledb.Error as e:
        print("❌ Database connection failed:", e)
        return None