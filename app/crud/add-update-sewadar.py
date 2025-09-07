from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import base64
from datetime import date, datetime

app = FastAPI()

# ---------------- Schema Model ----------------
class Sewadar(BaseModel):
    name: str
    father_husband_name: Optional[str] = None
    contact_no: Optional[str] = None
    alternate_contact_no: Optional[str] = None
    address: Optional[str] = None
    gender: Optional[str] = None
    dob: Optional[str] = None   # YYYY-MM-DD
    department_name: Optional[str] = None   # <-- instead of department_id
    enrolment_date: Optional[str] = None
    blood_group: Optional[str] = None
    locality: Optional[str] = None
    badge_no: str
    badge_category: Optional[str] = None
    badge_issue_date: Optional[str] = None
    initiation_date: Optional[str] = None
    visit_badge_no: Optional[str] = None
    education: Optional[str] = None
    occupation: Optional[str] = None
    photo: Optional[str] = None          # base64 encoded string
    aadhaar_photo: Optional[str] = None  # base64 encoded string
    aadhaar_no: Optional[str] = None
    category: Optional[str] = None

# ---------------- DB Connection ----------------
def get_db_connection():
    return psycopg2.connect(
        dbname="ams",
        user="neondb_owner",
        password="npg_igo8fBOT3MtP",
        host="ep-orange-fog-a1qfrxr9-pooler.ap-southeast-1.aws.neon.tech",
        port="5432",
        sslmode="require"
    )

# ---------------- Utility: Age Calculation ----------------
def calculate_age(dob_str: Optional[str]) -> Optional[int]:
    if not dob_str:
        return None
    try:
        dob = datetime.strptime(dob_str, "%Y-%m-%d").date()
        today = date.today()
        return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    except ValueError:
        return None

# ---------------- Utility: Department Lookup ----------------
def get_department_id(conn, department_name: Optional[str]) -> Optional[int]:
    if not department_name:
        return None
    cur = conn.cursor()
    cur.execute("SELECT department_id FROM department WHERE department_name = %s", (department_name,))
    row = cur.fetchone()
    cur.close()
    if row:
        return row[0]
    return None

# ---------------- Add Sewadar ----------------
@app.post("/sewadar")
def add_sewadar(sewadar: Sewadar):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Resolve department_id
    department_id = get_department_id(conn, sewadar.department_name)
    if sewadar.department_name and not department_id:
        conn.close()
        raise HTTPException(status_code=404, detail=f"Department '{sewadar.department_name}' not found")

    sql = """
        INSERT INTO sewadar (
            name, father_husband_name, contact_no, alternate_contact_no,
            address, gender, dob, department_id, enrolment_date, blood_group,
            locality, badge_no, badge_category, badge_issue_date,
            initiation_date, visit_badge_no, education, occupation,
            photo, aadhaar_photo, aadhaar_no, category, age
        )
        VALUES (
            %(name)s, %(father_husband_name)s, %(contact_no)s, %(alternate_contact_no)s,
            %(address)s, %(gender)s, %(dob)s, %(department_id)s, %(enrolment_date)s, %(blood_group)s,
            %(locality)s, %(badge_no)s, %(badge_category)s, %(badge_issue_date)s,
            %(initiation_date)s, %(visit_badge_no)s, %(education)s, %(occupation)s,
            %(photo)s, %(aadhaar_photo)s, %(aadhaar_no)s, %(category)s, %(age)s
        )
        ON CONFLICT (badge_no) DO NOTHING
    """

    data = sewadar.dict()
    data["department_id"] = department_id
    data["age"] = calculate_age(data["dob"])
    if data["photo"]:
        data["photo"] = base64.b64decode(data["photo"])
    if data["aadhaar_photo"]:
        data["aadhaar_photo"] = base64.b64decode(data["aadhaar_photo"])

    cursor.execute(sql, data)
    conn.commit()

    cursor.close()
    conn.close()
    return {"message": f"Sewadar {sewadar.name} added successfully"}

# ---------------- Update Sewadar ----------------
@app.put("/sewadar/{badge_no}")
def update_sewadar(badge_no: str, sewadar: Sewadar):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Resolve department_id
    department_id = get_department_id(conn, sewadar.department_name)
    if sewadar.department_name and not department_id:
        conn.close()
        raise HTTPException(status_code=404, detail=f"Department '{sewadar.department_name}' not found")

    sql = """
        UPDATE sewadar SET
            name = %(name)s,
            father_husband_name = %(father_husband_name)s,
            contact_no = %(contact_no)s,
            alternate_contact_no = %(alternate_contact_no)s,
            address = %(address)s,
            gender = %(gender)s,
            dob = %(dob)s,
            department_id = %(department_id)s,
            enrolment_date = %(enrolment_date)s,
            blood_group = %(blood_group)s,
            locality = %(locality)s,
            badge_category = %(badge_category)s,
            badge_issue_date = %(badge_issue_date)s,
            initiation_date = %(initiation_date)s,
            visit_badge_no = %(visit_badge_no)s,
            education = %(education)s,
            occupation = %(occupation)s,
            photo = %(photo)s,
            aadhaar_photo = %(aadhaar_photo)s,
            aadhaar_no = %(aadhaar_no)s,
            category = %(category)s,
            age = %(age)s
        WHERE badge_no = %(badge_no)s
    """

    data = sewadar.dict()
    data["department_id"] = department_id
    data["badge_no"] = badge_no
    data["age"] = calculate_age(data["dob"])
    if data["photo"]:
        data["photo"] = base64.b64decode(data["photo"])
    if data["aadhaar_photo"]:
        data["aadhaar_photo"] = base64.b64decode(data["aadhaar_photo"])

    cursor.execute(sql, data)
    conn.commit()
    rows_updated = cursor.rowcount

    cursor.close()
    conn.close()

    if rows_updated == 0:
        raise HTTPException(status_code=404, detail="Sewadar not found")

    return {"message": f"Sewadar {badge_no} updated successfully"}
