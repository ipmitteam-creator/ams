# app/crud/add_update_sewadar.py
from fastapi import APIRouter, HTTPException, Query, Response, Body
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
    permanent_address: Optional[str] = None   # 👈 NEW
    gender: Optional[str] = None
    dob: Optional[str] = None
    department_name: Optional[str] = None
    current_department_name: Optional[str] = None
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
    updated_by: Optional[int] = None

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
    return row[0] if row else None

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
        return None

# ---------------- Add Sewadar ----------------
@router.post("/")
def add_sewadar(sewadar: Sewadar):
    conn = get_db_connection()
    cursor = conn.cursor()

    dept_id = get_department_id(conn, sewadar.department_name)
    current_dept_id = get_department_id(conn, sewadar.current_department_name)
    age = calculate_age_from_dob(sewadar.dob)

    sql = """
        INSERT INTO sewadar (
            name, father_husband_name, contact_no, alternate_contact_no, address, permanent_address,
            gender, dob, department_id, current_department_id, enrolment_date,
            blood_group, locality, badge_no, badge_category, badge_issue_date,
            initiation_date, visit_badge_no, education, occupation, photo,
            aadhaar_photo, aadhaar_no, category, age, updated_by
        )
        VALUES (
            %(name)s, %(father_husband_name)s, %(contact_no)s, %(alternate_contact_no)s, %(address)s, %(permanent_address)s,
            %(gender)s, %(dob)s, %(department_id)s, %(current_department_id)s, %(enrolment_date)s,
            %(blood_group)s, %(locality)s, %(badge_no)s, %(badge_category)s, %(badge_issue_date)s,
            %(initiation_date)s, %(visit_badge_no)s, %(education)s, %(occupation)s, %(photo)s,
            %(aadhaar_photo)s, %(aadhaar_no)s, %(category)s, %(age)s, %(updated_by)s
        )
        ON CONFLICT (badge_no) DO NOTHING
    """

    data = sewadar.dict()
    data["department_id"] = dept_id
    data["current_department_id"] = current_dept_id
    data["age"] = age

    cursor.execute(sql, data)
    conn.commit()
    cursor.close()
    conn.close()

    return {"message": f"Sewadar {sewadar.name} added successfully"}

# ---------------- Update Sewadar ----------------
@router.put("/{badge_no}")
def update_sewadar(badge_no: str, sewadar_update: Sewadar):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute("SELECT * FROM sewadar WHERE badge_no = %s", (badge_no,))
    existing = cursor.fetchone()
    if not existing:
        conn.close()
        raise HTTPException(status_code=404, detail=f"Sewadar with badge_no {badge_no} not found")

    update_data = existing.copy()
    for key, value in sewadar_update.dict(exclude_unset=True).items():
        update_data[key] = value

    dept_id = get_department_id(conn, update_data.get("department_name"))
    current_dept_id = get_department_id(conn, update_data.get("current_department_name"))
    update_data["department_id"] = dept_id if dept_id else existing.get("department_id")
    update_data["current_department_id"] = current_dept_id if current_dept_id else existing.get("current_department_id")
    update_data["age"] = calculate_age_from_dob(update_data.get("dob"))

    fields = [
        "name","father_husband_name","contact_no","alternate_contact_no","address","permanent_address",  # 👈 added
        "gender","dob","department_id","current_department_id","enrolment_date",
        "blood_group","locality","badge_category","badge_issue_date","initiation_date",
        "visit_badge_no","education","occupation","photo","aadhaar_photo",
        "aadhaar_no","category","age","updated_by"
    ]
    set_clause = ", ".join([f"{f} = %({f})s" for f in fields])
    sql = f"UPDATE sewadar SET {set_clause} WHERE badge_no = %(badge_no)s"

    update_data["badge_no"] = badge_no
    cursor.execute(sql, update_data)
    conn.commit()
    cursor.close()
    conn.close()

    return {"message": f"Sewadar with badge_no {badge_no} updated successfully"}

# ---------------- Get Sewadar by badge_no ----------------
@router.get("/{badge_no}")
def get_sewadar(badge_no: str):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute("SELECT * FROM sewadar WHERE badge_no = %s", (badge_no,))
    record = cursor.fetchone()
    if not record:
        conn.close()
        raise HTTPException(status_code=404, detail="Sewadar not found")

    record["department_name"] = get_department_name(conn, record.get("department_id"))
    record["current_department_name"] = get_department_name(conn, record.get("current_department_id"))
    record.pop("department_id", None)
    record.pop("current_department_id", None)

    conn.close()
    return record

# ---------------- Search / Export Sewadars ----------------
@router.get("/")
def search_sewadars(
    department_name: Optional[str] = Query(None),
    current_department_name: Optional[str] = Query(None),
    locality: Optional[str] = Query(None),
    gender: Optional[str] = Query(None),
    badge_no: Optional[str] = Query(None),
    badge_category: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    age: Optional[int] = Query(None),
    format: Optional[str] = Query("json"),
    columns: Optional[str] = Query(None)
):
    ALLOWED_COLUMNS = {
        "sewadar_id","name","father_husband_name","contact_no","alternate_contact_no","address","permanent_address",  # 👈 added
        "gender","dob","department_name","current_department_name","enrolment_date","blood_group",
        "locality","badge_no","badge_category","badge_issue_date","initiation_date",
        "visit_badge_no","education","occupation","photo","aadhaar_photo","aadhaar_no",
        "category","age","updated_by"
    }

    if columns:
        requested_columns = {c.strip() for c in columns.split(",")}
        invalid = requested_columns - ALLOWED_COLUMNS
        if invalid:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid column(s): {', '.join(invalid)}. Allowed: {', '.join(ALLOWED_COLUMNS)}"
            )
        selected_columns = requested_columns
    else:
        selected_columns = ALLOWED_COLUMNS

    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    conditions, values = [], []
    if department_name:
        dept_id = get_department_id(conn, department_name)
        if dept_id: conditions.append("department_id = %s"); values.append(dept_id)
        else: conn.close(); return []

    if current_department_name:
        cur_dept_id = get_department_id(conn, current_department_name)
        if cur_dept_id: conditions.append("current_department_id = %s"); values.append(cur_dept_id)
        else: conn.close(); return []

    if locality: conditions.append("locality ILIKE %s"); values.append(f"%{locality}%")
    if gender: conditions.append("gender = %s"); values.append(gender)
    if badge_no: conditions.append("badge_no = %s"); values.append(badge_no)
    if badge_category: conditions.append("badge_category = %s"); values.append(badge_category)
    if category: conditions.append("category = %s"); values.append(category)
    if age is not None:
        today = date.today()
        dob_cutoff = today.replace(year=today.year - age)
        conditions.append("dob <= %s"); values.append(dob_cutoff)

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    cursor.execute(f"SELECT * FROM sewadar {where_clause}", tuple(values))
    records = cursor.fetchall()

    for r in records:
        r["department_name"] = get_department_name(conn, r.get("department_id"))
        r["current_department_name"] = get_department_name(conn, r.get("current_department_id"))
        r.pop("department_id", None)
        r.pop("current_department_id", None)

    cursor.close()
    conn.close()

    filtered_records = [{col: r.get(col) for col in selected_columns if col in r} for r in records]

    if format == "json":
        return filtered_records
    df = pd.DataFrame(filtered_records)
    if format == "csv":
        output = io.StringIO(); df.to_csv(output, index=False)
        return Response(content=output.getvalue(), media_type="text/csv",
                        headers={"Content-Disposition": "attachment; filename=sewadar_report.csv"})
    if format == "xlsx":
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer: df.to_excel(writer, index=False, sheet_name="Sewadars")
        return Response(content=output.getvalue(), media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        headers={"Content-Disposition": "attachment; filename=sewadar_report.xlsx"})
    raise HTTPException(status_code=400, detail="Invalid format. Use json, csv, or xlsx.")
