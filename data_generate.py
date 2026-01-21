import random
from datetime import date, timedelta

def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end-start).days))

data = []
for i in range(1, 20001):
    dob = random_date(date(1966,1,1), date(2002,1,1))
    join = random_date(dob + timedelta(days=8000), date(2024,1,1))

    ins_start = random_date(join, date(2024,6,1))
    ins_end = ins_start + timedelta(days=365)

    data.append({
        "CLIENT_CODE": "CLNT001",
        "EMP_CODE": f"EMP{i:05}",
        "EMP_NAME": f"Employee_{i}",
        "CNIC": f"35202{random.randint(1000000,9999999)}",
        "PASSPORT_NO": f"P{random.randint(10000000,99999999)}",
        "DOB": dob,
        "JOIN_DATE": join,
        "INS_START_DATE": ins_start,
        "INS_END_DATE": ins_end
    })
