import pandas as pd
import mysql.connector
import logging
from datetime import date
import math

# Setup logging
logging.basicConfig(
    filename="insert_sewadar.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# DB connection
conn = mysql.connector.connect(
    host="localhost",
    user="admin",
    password="admin",
    database="ams"
)
cursor = conn.cursor()

# Read Excel
df = pd.read_excel("Complete_list.xlsx")

# ---- Helpers ----
def normalize_gender(value):
    if pd.isna(value):
        return None
    v = str(value).strip().upper()
    if v in ["M", "MALE"]:
        return "Male"
    elif v in ["F", "FEMALE"]:
        return "Female"
    elif v in ["O", "OTHER"]:
        return "Other"
    return None

def safe_date(value):
    """Convert to YYYY-MM-DD or None if invalid/NaN/NaT"""
    if pd.isna(value):
        return None
    try:
        return pd.to_datetime(value, errors="coerce").date()
    except Exception:
        return None

def safe_str(value):
    return None if pd.isna(value) else str(value).strip()

def safe_int(value):
    return None if pd.isna(value) else int(value)

def safe_float(value):
    return None if pd.isna(value) else float(value)

def calculate_age(dob):
    if pd.isna(dob):
        return None
    try:
        dob = pd.to_datetime(dob, errors="coerce")
        if pd.isna(dob):
            return None
        today = date.today()
        return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    except Exception:
        return None

# ---- Process DataFrame ----
df["gender"] = df["gender"].apply(normalize_gender)
df["dob"] = df["dob"].apply(safe_date)
df["enrolment_date"] = df["enrolment_date"].apply(safe_date)
df["initiation_date"] = df["initiation_date"].apply(safe_date)
df["age"] = df["dob"].apply(calculate_age)

# ---- Insert or Update ----
sql = """
INSERT INTO sewadar
(category, name, father_husband_name, gender, aadhaar_no, address, contact_no, alternate_contact_no,
 badge_no, dob, locality, badge_category, department_id, enrolment_date, initiation_date,
 visit_badge_no, age)
VALUES (%(category)s, %(name)s, %(father_husband_name)s, %(gender)s, %(aadhaar_no)s, %(address)s, %(contact_no)s,
 %(alternate_contact_no)s, %(badge_no)s, %(dob)s, %(locality)s, %(badge_category)s, %(department_id)s,
 %(enrolment_date)s, %(initiation_date)s, %(visit_badge_no)s, %(age)s)
ON DUPLICATE KEY UPDATE
 name = VALUES(name),
 father_husband_name = VALUES(father_husband_name),
 gender = VALUES(gender),
 address = VALUES(address),
 contact_no = VALUES(contact_no),
 alternate_contact_no = VALUES(alternate_contact_no),
 dob = VALUES(dob),
 locality = VALUES(locality),
 badge_category = VALUES(badge_category),
 department_id = VALUES(department_id),
 enrolment_date = VALUES(enrolment_date),
 initiation_date = VALUES(initiation_date),
 visit_badge_no = VALUES(visit_badge_no),
 age = VALUES(age)
"""

for idx, row in df.iterrows():
    try:
        data = {
            "category": safe_str(row.get("category")),
            "name": safe_str(row.get("name")),
            "father_husband_name": safe_str(row.get("father_husband_name")),
            "gender": normalize_gender(row.get("gender")),
            "aadhaar_no": safe_str(row.get("aadhaar_no")),  # keep as string (can be big)
            "address": safe_str(row.get("address")),
            "contact_no": safe_str(row.get("contact_no")),
            "alternate_contact_no": safe_str(row.get("alternate_contact_no")),
            "badge_no": safe_str(row.get("badge_no")),
            "dob": safe_date(row.get("dob")),
            "locality": safe_str(row.get("locality")),
            "badge_category": safe_str(row.get("badge_category")),
            "department_id": None,  # placeholder, adjust if mapping exists
            "enrolment_date": safe_date(row.get("enrolment_date")),
            "initiation_date": safe_date(row.get("initiation_date")),
            "visit_badge_no": safe_str(row.get("visit_badge_no")),
            "age": calculate_age(row.get("dob")),
        }
        cursor.execute(sql, data)
        conn.commit()
    except Exception as e:
        logging.error(f"Row {idx} failed: {e} | Data: {data}")

cursor.close()
conn.close()
