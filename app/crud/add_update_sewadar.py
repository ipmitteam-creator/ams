# app/crud/add_update_sewadar.py
from fastapi import APIRouter, HTTPException, Query, Response
from pydantic import BaseModel
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import date, datetime
import io
import pandas as pd

router = APIRouter()

# ---------------- Schema Model ----------------
class Sewadar(BaseModel):
    name: str
    father_husband_name: Optional[str] = None
    contact_no: Optional[str] = None
    alternate_contact_no: Optional[str] = None
    address: Optional[str] = None
    gender: Optional[str] = None
    dob: Optional[str] = None
    department_name: Optional[str] = None
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
    photo: Optional[str] = None
    aadhaar_photo: Optional[str] = None
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

# ---------------- Utilities ----------------
def get_department_id(conn, department_name: Optional[str]) -> Optional[int]:
    if not department_name:
        return None
    cur = conn.cursor()
    cur.execute(
        "SELECT department_id FROM department WHERE LOWER(TRIM(department_name)) = LOWER(TRIM(%s))",
        (department_name,)
    )
    row = cur.fetchone()
    cur.close()
    if row:
        print(f"[INFO] Found department_id {row[0]} for department_name '{department_name}'")
        return row[0]
    else:
        print(f"[WARN] Department '{department_name}' not found in DB")
        return None

def get_department_name(conn, department_id: Optional[int]) -> Optional[str]:
    if not department_id:
        return None
    cur = conn.cursor()
    cur.execute("SELECT department_name FROM department WHERE department_id = %s", (department_id,))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None

def calculate_age_from_dob(dob_str: Optional[str]) -> Optional[int]:
    if not dob_str:
        return None
    try:
        dob = datetime.strptime(dob_str, "%Y-%m-%d").date()
        today = date.today()
        return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    except ValueError:
        print(f"[WARN] Invalid DOB format: {dob_str}")
        return None

# ---------------- Add Sewadar API ----------------
@router.post("/sewadar")
def add_sewadar(sewadar: Sewadar):
    conn = get_db_connection()
    cursor = conn.cursor()

    dept_id = get_department_id(conn, sewadar.department_name)
    if sewadar.department_name and not dept_id:
        conn.close()
        raise HTTPException(status_code=404, detail=f"Department '{sewadar.department_name}' not found")

    age = calculate_age_from_dob(sewadar.dob)

    sql = """
        INSERT INTO sewadar (
            name, father_husband_name, contact_no, alternate_contact_no, address,
            gender, dob, department_id, enrolment_date, blood_group, locality,
            badge_no, badge_category, badge_issue_date, initiation_date, visit_badge_no,
            education, occupation, photo, aadhaar_photo, aadhaar_no, category, age
        )
        VALUES (
            %(name)s, %(father_husband_name)s, %(contact_no)s, %(alternate_contact_no)s, %(address)s,
            %(gender)s, %(dob)s, %(department_id)s, %(enrolment_date)s, %(blood_group)s, %(locality)s,
            %(badge_no)s, %(badge_category)s, %(badge_issue_date)s, %(initiation_date)s, %(visit_badge_no)s,
            %(education)s, %(occupation)s, %(photo)s, %(aadhaar_photo)s, %(aadhaar_no)s, %(category)s, %(age)s
        )
        ON CONFLICT (badge_no) DO NOTHING
    """

    data = sewadar.dict()
    data["department_id"] = dept_id
    data["age"] = age

    cursor.execute(sql, data)
    conn.commit()
    cursor.close()
    conn.close()

    print(f"[INFO] Sewadar {sewadar.name} added successfully with badge_no={sewadar.badge_no}")
    return {"message": f"Sewadar {sewadar.name} added successfully"}

# ---------------- Get Sewadar by badge_no and search_sewadars remain unchanged ----------------
# (Paste the previously refactored get_sewadar and search_sewadars functions here)
