import re
import pandas as pd
import mysql.connector
import logging
from datetime import date

# ---------------- Logging ----------------
logging.basicConfig(
    filename="insert_sewadar.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logging.info("=== Sewadar import started ===")

# ---------------- DB Connection ----------------
conn = mysql.connector.connect(
    host="localhost",
    user="admin",
    password="admin",
    database="ams"
)
cursor = conn.cursor(dictionary=True)

# ---------------- Helpers ----------------
def normalize_gender(value):
    if pd.isna(value):
        return None
    v = str(value).strip().upper()
    if v in ("M", "MALE"):
        return "Male"
    if v in ("F", "FEMALE"):
        return "Female"
    if v in ("O", "OTHER"):
        return "Other"
    return None

def safe_date(value):
    """Return a Python date or None."""
    if pd.isna(value):
        return None
    try:
        dt = pd.to_datetime(value, errors="coerce")
        return None if pd.isna(dt) else dt.date()
    except Exception:
        return None

def safe_str(value):
    if pd.isna(value):
        return None
    s = str(value).strip()
    return s if s != "" else None

def calculate_age(dob):
    if dob is None or pd.isna(dob):
        return None
    try:
        dt = pd.to_datetime(dob, errors="coerce")
        if pd.isna(dt):
            return None
        today = date.today()
        return today.year - dt.year - ((today.month, today.day) < (dt.month, dt.day))
    except Exception:
        return None

_dept_cleanup_re = re.compile(r"[^a-z0-9]+")
def normalize_dept(value):
    """lowercase, strip, drop spaces and non-alphanumerics -> canonical key"""
    if value is None or pd.isna(value):
        return None
    v = str(value).strip().lower()
    v = _dept_cleanup_re.sub("", v)  # remove spaces, dashes, etc.
    return v if v else None

def pick_department_cell(row):
    """Try common column names that might carry department text."""
    for k in (
        "department_name",
        "department",
        "Department Name",
        "Department",
        "dept",
        "Dept"
    ):
        if k in row and not pd.isna(row[k]):
            return row[k]
    return None

# ---------------- Load Department Mapping ----------------
# Try common schemas: department(department_id, department_name) or (id, name)
dept_rows = []
try:
    cursor.execute("SELECT department_id AS id, department_name AS name FROM department")
    dept_rows = cursor.fetchall()
except Exception:
    cursor.execute("SELECT id, name FROM department")
    dept_rows = cursor.fetchall()

dept_mapping = {}
for r in dept_rows:
    key = normalize_dept(r["name"])
    if key:
        dept_mapping[key] = r["id"]

logging.info(f"Department mapping loaded (normalized): {dept_mapping}")

# ---------------- Read Excel ----------------
df = pd.read_excel("Complete_list.xlsx")

# Pre-process columns that we *know* we use
if "gender" in df.columns:
    df["gender"] = df["gender"].apply(normalize_gender)
if "dob" in df.columns:
    df["dob"] = df["dob"].apply(safe_date)
if "enrolment_date" in df.columns:
    df["enrolment_date"] = df["enrolment_date"].apply(safe_date)
if "initiation_date" in df.columns:
    df["initiation_date"] = df["initiation_date"].apply(safe_date)
# Recompute age from (possibly) cleaned dob
df["age"] = df["dob"].apply(calculate_age) if "dob" in df.columns else None

# ---------------- SQL ----------------
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

# ---------------- Insert Loop (Batch Optimized + Dept Logs) ----------------
batch_size = 500
rows_processed, rows_failed = 0, 0
batch = []

for idx, row in df.iterrows():
    # Make sure dept vars exist for logging even if anything fails
    dept_name_raw = pick_department_cell(row)  # may be None
    dept_name_norm = normalize_dept(dept_name_raw)
    dept_id = dept_mapping.get(dept_name_norm) if dept_name_norm else None

    # Always log the lookup (use INFO; change to DEBUG if this is too chatty)
    logging.info(
        f"Row {idx} dept lookup: raw='{dept_name_raw}' | normalized='{dept_name_norm}' | dept_id={dept_id}"
    )

    try:
        data = {
            "category": safe_str(row.get("category")),
            "name": safe_str(row.get("name")),
            "father_husband_name": safe_str(row.get("father_husband_name")),
            "gender": normalize_gender(row.get("gender")),
            "aadhaar_no": safe_str(row.get("aadhaar_no")),  # keep as string to avoid precision issues
            "address": safe_str(row.get("address")),
            "contact_no": safe_str(row.get("contact_no")),  # keep as string (can have leading zeros/+91)
            "alternate_contact_no": safe_str(row.get("alternate_contact_no")),
            "badge_no": safe_str(row.get("badge_no")),
            "dob": safe_date(row.get("dob")),
            "locality": safe_str(row.get("locality")),
            "badge_category": safe_str(row.get("badge_category")),
            "department_id": dept_id,
            "enrolment_date": safe_date(row.get("enrolment_date")),
            "initiation_date": safe_date(row.get("initiation_date")),
            "visit_badge_no": safe_str(row.get("visit_badge_no")),
            "age": calculate_age(row.get("dob")),
        }

        batch.append(data)
        if len(batch) >= batch_size:
            cursor.executemany(sql, batch)
            conn.commit()
            rows_processed += len(batch)
            batch = []

    except Exception as e:
        rows_failed += 1
        logging.error(f"Row {idx} failed: {e} | Data: {row.to_dict()}")

# leftover batch
if batch:
    cursor.executemany(sql, batch)
    conn.commit()
    rows_processed += len(batch)

logging.info(f"✅ Import finished. {rows_processed} rows processed, {rows_failed} failed.")
logging.info("=== Sewadar import ended ===")

cursor.close()
conn.close()
