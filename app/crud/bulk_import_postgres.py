import re
import pandas as pd
import psycopg2
import psycopg2.extras
import logging
from datetime import date

# ---------------- Logging ----------------
logging.basicConfig(
    filename="insert_sewadar.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logging.info("=== Sewadar import started ===")

# ---------------- DB Connection (Neon) ----------------
conn = psycopg2.connect(
    dbname="ams",
    user="neondb_owner",
    password="npg_igo8fBOT3MtP",
    host="ep-orange-fog-a1qfrxr9-pooler.ap-southeast-1.aws.neon.tech",
    port="5432",
    sslmode="require"
)
cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

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
    if value is None or pd.isna(value):
        return None
    v = str(value).strip().lower()
    v = _dept_cleanup_re.sub("", v)
    return v if v else None

def pick_department_cell(row):
    for k in ("department_name", "department", "Department Name", "Department", "dept", "Dept"):
        if k in row and not pd.isna(row[k]):
            return row[k]
    return None

# ---------------- Load Department Mapping ----------------
cursor.execute("SELECT department_id AS id, department_name AS name FROM department")
dept_rows = cursor.fetchall()

dept_mapping = {}
for r in dept_rows:
    key = normalize_dept(r["name"])
    if key:
        dept_mapping[key] = r["id"]

logging.info(f"Department mapping loaded: {dept_mapping}")

# ---------------- Read Excel ----------------
df = pd.read_excel("Complete_list.xlsx")

if "gender" in df.columns:
    df["gender"] = df["gender"].apply(normalize_gender)
if "dob" in df.columns:
    df["dob"] = df["dob"].apply(safe_date)
if "enrolment_date" in df.columns:
    df["enrolment_date"] = df["enrolment_date"].apply(safe_date)
if "initiation_date" in df.columns:
    df["initiation_date"] = df["initiation_date"].apply(safe_date)
df["age"] = df["dob"].apply(calculate_age) if "dob" in df.columns else None

# ---------------- SQL ----------------
sql = """
INSERT INTO sewadaar
(category, name, father_husband_name, gender, aadhaar_no, address, contact_no, alternate_contact_no,
 badge_no, dob, locality, badge_category, department_id, enrolment_date, initiation_date,
 visit_badge_no, age)
VALUES (%(category)s, %(name)s, %(father_husband_name)s, %(gender)s, %(aadhaar_no)s, %(address)s, %(contact_no)s,
 %(alternate_contact_no)s, %(badge_no)s, %(dob)s, %(locality)s, %(badge_category)s, %(department_id)s,
 %(enrolment_date)s, %(initiation_date)s, %(visit_badge_no)s, %(age)s)
ON CONFLICT (badge_no) DO UPDATE SET
 name = EXCLUDED.name,
 father_husband_name = EXCLUDED.father_husband_name,
 gender = EXCLUDED.gender,
 address = EXCLUDED.address,
 contact_no = EXCLUDED.contact_no,
 alternate_contact_no = EXCLUDED.alternate_contact_no,
 dob = EXCLUDED.dob,
 locality = EXCLUDED.locality,
 badge_category = EXCLUDED.badge_category,
 department_id = EXCLUDED.department_id,
 enrolment_date = EXCLUDED.enrolment_date,
 initiation_date = EXCLUDED.initiation_date,
 visit_badge_no = EXCLUDED.visit_badge_no,
 age = EXCLUDED.age
"""

# ---------------- Insert Loop ----------------
rows_processed, rows_failed = 0, 0

for idx, row in df.iterrows():
    dept_name_raw = pick_department_cell(row)
    dept_name_norm = normalize_dept(dept_name_raw)
    dept_id = dept_mapping.get(dept_name_norm) if dept_name_norm else None

    data = {
        "category": safe_str(row.get("category")),
        "name": safe_str(row.get("name")),
        "father_husband_name": safe_str(row.get("father_husband_name")),
        "gender": normalize_gender(row.get("gender")),
        "aadhaar_no": safe_str(row.get("aadhaar_no")),
        "address": safe_str(row.get("address")),
        "contact_no": safe_str(row.get("contact_no")),
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

    try:
        cursor.execute(sql, data)
        conn.commit()
        rows_processed += 1
        logging.info(f"Row {idx} inserted successfully (badge_no={data['badge_no']})")
    except Exception as e:
        conn.rollback()
        rows_failed += 1
        logging.error(f"Row {idx} failed: {e} | Data: {row.to_dict()}")

logging.info(f"✅ Import finished. {rows_processed} rows inserted, {rows_failed} rows skipped.")
logging.info("=== Sewadar import ended ===")

cursor.close()
conn.close()
