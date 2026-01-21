import oracledb
import pandas as pd
from datetime import datetime
from db_connection import get_db_connection  # ✅ Oracle connection
import pandas as pd
from datetime import datetime

# -----------------------------
# Configuration
# -----------------------------
EXCEL_FILE = "EMPLOYEE_INSURANCE.xlsx"   # You can make this dynamic per schedule
SHEET_NAME = "EMPLOYEES"

# -----------------------------
# Step 1: Read Excel
# -----------------------------
df = pd.read_excel(EXCEL_FILE, sheet_name=SHEET_NAME)

# Standardize column names: strip spaces and uppercase
df.columns = [col.strip().upper() for col in df.columns]

if df.empty:
    raise ValueError("Excel sheet is empty!")

# Optional: Check for duplicate CNICs within file
duplicates = df[df.duplicated(subset=['CNIC'], keep=False)]
if not duplicates.empty:
    print("Warning: Duplicate CNIC found in file:")
    print(duplicates)

# -----------------------------
# Step 2: Get Oracle connection and generate UPLOAD_ID
# -----------------------------
conn = get_db_connection()

with conn.cursor() as cursor:
    cursor.execute("SELECT NVL(MAX(UPLOAD_ID),0)+1 FROM ETL_STG_INSURANCE_UPLOAD")
    upload_id = cursor.fetchone()[0]

print(f"Generated UPLOAD_ID: {upload_id}")

# -----------------------------
# Step 3: Prepare staging data for bulk insert
# -----------------------------
staging_data = []
for _, row in df.iterrows():
    staging_data.append({
        'UPLOAD_ID': upload_id,
        'CLIENT_CODE': row['CLIENT_CODE'],
        'EMP_CODE': row['EMP_CODE'],
        'EMP_NAME': row['EMP_NAME'],
        'CNIC': row['CNIC'],
        'PASSPORT_NO': row.get('PASSPORT_NO'),
        'DOB': pd.to_datetime(row.get('DATE_OF_BIRTH')).date() if row.get('DATE_OF_BIRTH') else None,
        'GENDER': row.get('GENDER'),
        'MOBILE_NO': row.get('MOBILE_NO'),
        'EMAIL': row.get('EMAIL'),
        'JOIN_DATE': pd.to_datetime(row.get('JOIN_DATE')).date() if row.get('JOIN_DATE') else None,
        'INS_START_DATE': pd.to_datetime(row.get('INSURANCE_START_DATE')).date() if row.get('INSURANCE_START_DATE') else None,
        'INS_END_DATE': pd.to_datetime(row.get('INSURANCE_END_DATE')).date() if row.get('INSURANCE_END_DATE') else None
    })

# -----------------------------
# Step 4: Bulk Insert into staging table
# -----------------------------
insert_sql = """
INSERT INTO ETL_STG_INSURANCE_UPLOAD (
    UPLOAD_ID, CLIENT_CODE, EMP_CODE, EMP_NAME, CNIC, PASSPORT_NO,
    DOB, GENDER, MOBILE_NO, EMAIL, JOIN_DATE, INS_START_DATE, INS_END_DATE
) VALUES (
    :UPLOAD_ID, :CLIENT_CODE, :EMP_CODE, :EMP_NAME, :CNIC, :PASSPORT_NO,
    :DOB, :GENDER, :MOBILE_NO, :EMAIL, :JOIN_DATE, :INS_START_DATE, :INS_END_DATE
)
"""

with conn.cursor() as cursor:
    cursor.executemany(insert_sql, staging_data)
    conn.commit()

print(f"Inserted {len(staging_data)} rows into staging table")

# -----------------------------
# Step 5: Call PL/SQL Procedure dynamically
# -----------------------------
# Assume client_code is same for all rows; pick first row
client_code = df['CLIENT_CODE'].iloc[0]

with conn.cursor() as cursor:
    cursor.callproc("ETL_PRC_INSURANCE_LOAD", [upload_id, client_code, EXCEL_FILE])

print("ETL procedure executed successfully")

# -----------------------------
# Step 6: Fetch Audit and Error logs
# -----------------------------
with conn.cursor() as cursor:
    cursor.execute("""
        SELECT * 
        FROM ETL_UPLOAD_AUDIT 
        WHERE UPLOAD_ID = :upload_id
    """, {'upload_id': upload_id})
    audit = cursor.fetchall()
    print("Audit Result:", audit)

    cursor.execute("""
        SELECT * 
        FROM ETL_ERROR_LOG 
        WHERE UPLOAD_ID = :upload_id
    """, {'upload_id': upload_id})
    errors = cursor.fetchall()
    print("Error Rows:", errors)

# -----------------------------
# Step 7: Close connection
# -----------------------------
conn.close()
