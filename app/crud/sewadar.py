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

# ---------------- Add Sewadar ----------------
@router.post("/")
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

# ---------------- Get Sewadar by badge_no ----------------
@router.get("/{badge_no}")
def get_sewadar(badge_no: str):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    print(f"[INFO] Fetching sewadar with badge_no={badge_no}")
    cursor.execute("SELECT * FROM sewadar WHERE badge_no = %s", (badge_no,))
    record = cursor.fetchone()
    cursor.close()
    conn.close()

    if not record:
        print(f"[ERROR] Sewadar with badge_no={badge_no} not found")
        raise HTTPException(status_code=404, detail="Sewadar not found")

    record["department_name"] = get_department_name(get_db_connection(), record.get("department_id"))
    record.pop("department_id", None)

    return record



# ---------------- Update Sewadar ----------------

@router.put("/{badge_no}")
def update_sewadar(
    badge_no: str,
    sewadar_update: Sewadar = Body(...)
):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    # Fetch existing record
    cursor.execute("SELECT * FROM sewadar WHERE badge_no = %s", (badge_no,))
    existing = cursor.fetchone()
    if not existing:
        cursor.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"Sewadar with badge_no {badge_no} not found")

    # Merge existing data with updates (skip None values)
    update_data = existing.copy()
    for key, value in sewadar_update.dict().items():
        if value is not None:  # Only update provided fields
            update_data[key] = value

    # Handle department name → department_id conversion
    dept_id = get_department_id(conn, update_data.get("department_name"))
    update_data["department_id"] = dept_id if dept_id else existing.get("department_id")

    # Recalculate age if dob changed
    update_data["age"] = calculate_age_from_dob(update_data.get("dob"))

    # Build the SQL dynamically
    fields = [
        "name", "father_husband_name", "contact_no", "alternate_contact_no", "address",
        "gender", "dob", "department_id", "enrolment_date", "blood_group", "locality",
        "badge_category", "badge_issue_date", "initiation_date", "visit_badge_no",
        "education", "occupation", "photo", "aadhaar_photo", "aadhaar_no", "category", "age"
    ]
    set_clause = ", ".join([f"{f} = %({f})s" for f in fields])

    sql = f"""
        UPDATE sewadar
        SET {set_clause}
        WHERE badge_no = %(badge_no)s
    """

    update_data["badge_no"] = badge_no
    cursor.execute(sql, update_data)
    conn.commit()
    cursor.close()
    conn.close()

    return {"message": f"Sewadar with badge_no {badge_no} updated successfully"}


# ---------------- Search / Report Sewadars ----------------
@router.get("/")
def search_sewadars(
    department_name: Optional[str] = Query(None),
    locality: Optional[str] = Query(None),
    gender: Optional[str] = Query(None),
    badge_no: Optional[str] = Query(None),
    badge_category: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    age: Optional[int] = Query(None),
    format: Optional[str] = Query("json")
):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    print(f"[INFO] Search params: dept={department_name}, locality={locality}, gender={gender}, "
          f"badge_no={badge_no}, badge_cat={badge_category}, category={category}, age={age}, format={format}")

    # Build dynamic filters
    conditions, values = [], []

    if department_name:
        dept_id = get_department_id(conn, department_name)
        if dept_id:
            conditions.append("department_id = %s")
            values.append(dept_id)
        else:
            conn.close()
            print(f"[WARN] Department '{department_name}' not found, returning empty result")
            return []

    if locality:
        conditions.append("locality ILIKE %s")
        values.append(f"%{locality}%")

    if gender:
        conditions.append("gender = %s")
        values.append(gender)

    if badge_no:
        conditions.append("badge_no = %s")
        values.append(badge_no)

    if badge_category:
        conditions.append("badge_category = %s")
        values.append(badge_category)

    if category:
        conditions.append("category = %s")
        values.append(category)

    if age is not None:
        today = date.today()
        dob_cutoff = today.replace(year=today.year - age)
        conditions.append("dob <= %s")
        values.append(dob_cutoff)

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    query = f"SELECT * FROM sewadar {where_clause}"
    print(f"[DEBUG] Executing query: {query} with values {values}")
    cursor.execute(query, tuple(values))
    records = cursor.fetchall()

    # Replace department_id with department_name
    for r in records:
        r["department_name"] = get_department_name(conn, r.get("department_id"))
        r.pop("department_id", None)

    cursor.close()
    conn.close()

    print(f"[INFO] Found {len(records)} records")

    # Export in desired format
    if format == "json":
        return records

    df = pd.DataFrame(records)

    if format == "csv":
        print("[INFO] Returning CSV export")
        output = io.StringIO()
        df.to_csv(output, index=False)
        return Response(
            content=output.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=sewadar_report.csv"}
        )

    if format == "xlsx":
        print("[INFO] Returning XLSX export")
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Sewadars")
        return Response(
            content=output.getvalue(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=sewadar_report.xlsx"}
        )

    print(f"[ERROR] Invalid format requested: {format}")
    raise HTTPException(status_code=400, detail="Invalid format. Use json, csv, or xlsx.")
