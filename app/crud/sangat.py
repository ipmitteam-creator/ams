# app/crud/sangat.py
from fastapi import APIRouter, HTTPException, Query, Response, Body
from pydantic import BaseModel
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import date, datetime
import io
import pandas as pd

router = APIRouter()

# ---------- DB CONFIG ----------
DB_CONFIG = {
    "dbname": "ams",
    "user": "neondb_owner",
    "password": "npg_igo8fBOT3MtP",
    "host": "ep-orange-fog-a1qfrxr9-pooler.ap-southeast-1.aws.neon.tech",
    "port": "5432",
    "sslmode": "require"
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# ---------- Utils ----------
def calculate_age_from_dob(dob_str: Optional[str]) -> Optional[int]:
    if not dob_str:
        return None
    try:
        dob = datetime.strptime(dob_str, "%Y-%m-%d").date()
        today = date.today()
        return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    except Exception:
        return None

# ---------- Pydantic Model ----------
class Sangat(BaseModel):
    name: str
    father_husband_name: Optional[str] = None
    contact_no: Optional[str] = None
    alternate_contact_no: Optional[str] = None
    address: Optional[str] = None
    permanent_address: Optional[str] = None   # 👈 NEW
    gender: Optional[str] = None
    dob: Optional[str] = None   # YYYY-MM-DD
    blood_group: Optional[str] = None
    locality: Optional[str] = None
    initiation_date: Optional[str] = None
    visit_badge_no: Optional[str] = None
    education: Optional[str] = None
    occupation: Optional[str] = None
    photo: Optional[str] = None
    aadhaar_photo: Optional[str] = None
    aadhaar_no: Optional[str] = None
    updated_by: Optional[int] = None

# ---------- Add Sangat ----------
@router.post("/sangat")
def add_sangat(sangat: Sangat):
    conn = get_db_connection()
    cur = conn.cursor()

    age = calculate_age_from_dob(sangat.dob)

    sql = """
        INSERT INTO sewadar (
            name, father_husband_name, contact_no, alternate_contact_no, address, permanent_address,
            gender, dob, blood_group, locality, initiation_date, visit_badge_no,
            education, occupation, photo, aadhaar_photo, aadhaar_no,
            category, age, updated_by
        )
        VALUES (
            %(name)s, %(father_husband_name)s, %(contact_no)s, %(alternate_contact_no)s, %(address)s, %(permanent_address)s,
            %(gender)s, %(dob)s, %(blood_group)s, %(locality)s, %(initiation_date)s, %(visit_badge_no)s,
            %(education)s, %(occupation)s, %(photo)s, %(aadhaar_photo)s, %(aadhaar_no)s,
            'sangat', %(age)s, %(updated_by)s
        )
        RETURNING sewadar_id
    """

    data = sangat.dict()
    data["age"] = age

    cur.execute(sql, data)
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    return {"message": "Sangat added successfully", "sewadar_id": row[0]}

# ---------- Update Sangat ----------
@router.put("/sangat/{sewadar_id}")
def update_sangat(sewadar_id: int, sangat_update: Sangat = Body(...)):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT * FROM sewadar WHERE sewadar_id = %s AND category = 'sangat'", (sewadar_id,))
    existing = cur.fetchone()
    if not existing:
        conn.close()
        raise HTTPException(status_code=404, detail=f"Sangat with id {sewadar_id} not found")

    update_data = existing.copy()
    for k, v in sangat_update.dict(exclude_unset=True).items():
        update_data[k] = v

    update_data["age"] = calculate_age_from_dob(update_data.get("dob"))

    fields = [
        "name", "father_husband_name", "contact_no", "alternate_contact_no",
        "address", "permanent_address",   # 👈 included here
        "gender", "dob", "blood_group", "locality", "initiation_date", "visit_badge_no",
        "education", "occupation", "photo", "aadhaar_photo", "aadhaar_no", "age", "updated_by"
    ]
    set_clause = ", ".join([f"{f} = %({f})s" for f in fields])
    sql = f"UPDATE sewadar SET {set_clause} WHERE sewadar_id = %(sewadar_id)s AND category = 'sangat'"

    update_data["sewadar_id"] = sewadar_id
    cur.execute(sql, update_data)
    conn.commit()
    cur.close()
    conn.close()

    return {"message": f"Sangat {sewadar_id} updated successfully"}

# ---------- Delete Sangat ----------
@router.delete("/sangat/{sewadar_id}")
def delete_sangat(sewadar_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM sewadar WHERE sewadar_id = %s AND category = 'sangat' RETURNING sewadar_id", (sewadar_id,))
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail=f"Sangat with id {sewadar_id} not found")
    return {"message": f"Sangat {sewadar_id} deleted"}

# ---------- Get Sangat ----------
@router.get("/sangat/{sewadar_id}")
def get_sangat(sewadar_id: int):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM sewadar WHERE sewadar_id = %s AND category = 'sangat'", (sewadar_id,))
    rec = cur.fetchone()
    cur.close()
    conn.close()
    if not rec:
        raise HTTPException(status_code=404, detail="Sangat not found")
    return rec

# ---------- Search Sangat ----------
@router.get("/sangat")
def search_sangat(
    name: Optional[str] = Query(None),
    locality: Optional[str] = Query(None),
    gender: Optional[str] = Query(None),
    aadhaar_no: Optional[str] = Query(None),
    format: Optional[str] = Query("json"),
    columns: Optional[str] = Query(None)
):
    ALLOWED_COLUMNS = {
        "sewadar_id", "name", "father_husband_name", "contact_no", "alternate_contact_no",
        "address", "permanent_address",   # 👈 included here
        "gender", "dob", "blood_group", "locality", "initiation_date",
        "visit_badge_no", "education", "occupation", "photo", "aadhaar_photo",
        "aadhaar_no", "category", "age", "updated_by"
    }

    if columns:
        requested = {c.strip() for c in columns.split(",")}
        invalid = requested - ALLOWED_COLUMNS
        if invalid:
            raise HTTPException(status_code=400, detail=f"Invalid column(s): {', '.join(invalid)}")
        selected = requested
    else:
        selected = ALLOWED_COLUMNS

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    conditions = ["category = 'sangat'"]
    values = []
    if name:
        conditions.append("name ILIKE %s"); values.append(f"%{name}%")
    if locality:
        conditions.append("locality ILIKE %s"); values.append(f"%{locality}%")
    if gender:
        conditions.append("gender = %s"); values.append(gender)
    if aadhaar_no:
        conditions.append("aadhaar_no = %s"); values.append(aadhaar_no)

    where_clause = " AND ".join(conditions)
    cur.execute(f"SELECT * FROM sewadar WHERE {where_clause}", tuple(values))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    filtered = [{col: r.get(col) for col in selected} for r in rows]

    if format == "json":
        return filtered

    df = pd.DataFrame(filtered)
    if format == "csv":
        output = io.StringIO(); df.to_csv(output, index=False)
        return Response(content=output.getvalue(), media_type="text/csv",
                        headers={"Content-Disposition": "attachment; filename=sangat.csv"})
    if format == "xlsx":
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer: df.to_excel(writer, index=False, sheet_name="Sangat")
        return Response(content=output.getvalue(),
                        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        headers={"Content-Disposition": "attachment; filename=sangat.xlsx"})

    raise HTTPException(status_code=400, detail="Invalid format")
