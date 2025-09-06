from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch

app = FastAPI()

# ---------------- Schema Models ----------------
class Department(BaseModel):
    name: str

class DepartmentList(BaseModel):
    departments: List[Department]

class Sewadar(BaseModel):
    category: Optional[str] = None
    name: str
    father_husband_name: Optional[str] = None
    gender: Optional[str] = None
    aadhaar_no: Optional[str] = None
    address: Optional[str] = None
    contact_no: Optional[str] = None
    alternate_contact_no: Optional[str] = None
    badge_no: str
    dob: Optional[str] = None
    locality: Optional[str] = None
    badge_category: Optional[str] = None
    department_id: Optional[int] = None
    enrolment_date: Optional[str] = None
    initiation_date: Optional[str] = None
    visit_badge_no: Optional[str] = None
    age: Optional[int] = None
    education: Optional[str] = None
    occupation: Optional[str] = None

# ---------------- DB Connection ----------------
def get_db_connection():
    return psycopg2.connect(
        dbname="ams",
        user="neondb_owner",
        password="npg_igo8fBOT3MtP",  # ⚠️ replace with your Neon password
        host="ep-orange-fog-a1qfrxr9-pooler.ap-southeast-1.aws.neon.tech",
        port="5432",
        sslmode="require"
    )

# ---------------- Department APIs ----------------
@app.post("/departments")
def add_departments(data: DepartmentList):
    conn = get_db_connection()
    cursor = conn.cursor()

    sql = "INSERT INTO department (department_name) VALUES (%s) ON CONFLICT DO NOTHING"
    values = [(dept.name,) for dept in data.departments]

    execute_batch(cursor, sql, values)
    conn.commit()
    cursor.close()
    conn.close()

    return {"message": f"{len(data.departments)} departments processed successfully"}

# ---------------- Sewadar APIs ----------------
@app.post("/sewadar")
def add_sewadar(sewadar: Sewadar):
    conn = get_db_connection()
    cursor = conn.cursor()

    sql = """
        INSERT INTO sewadar (
            category, name, father_husband_name, gender, aadhaar_no, address,
            contact_no, alternate_contact_no, badge_no, dob, locality,
            badge_category, department_id, enrolment_date, initiation_date,
            visit_badge_no, age, education, occupation
        )
        VALUES (
            %(category)s, %(name)s, %(father_husband_name)s, %(gender)s, %(aadhaar_no)s, %(address)s,
            %(contact_no)s, %(alternate_contact_no)s, %(badge_no)s, %(dob)s, %(locality)s,
            %(badge_category)s, %(department_id)s, %(enrolment_date)s, %(initiation_date)s,
            %(visit_badge_no)s, %(age)s, %(education)s, %(occupation)s
        )
        ON CONFLICT (badge_no) DO NOTHING
    """
    cursor.execute(sql, sewadar.dict())
    conn.commit()

    cursor.close()
    conn.close()
    return {"message": f"Sewadar {sewadar.name} added successfully"}

@app.get("/sewadar/{badge_no}")
def get_sewadar(badge_no: str):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute("SELECT * FROM sewadar WHERE badge_no = %s", (badge_no,))
    record = cursor.fetchone()

    cursor.close()
    conn.close()

    if not record:
        raise HTTPException(status_code=404, detail="Sewadar not found")

    return record

@app.put("/sewadar/{badge_no}")
def update_sewadar(badge_no: str, sewadar: Sewadar):
    conn = get_db_connection()
    cursor = conn.cursor()

    sql = """
        UPDATE sewadar SET
            category = %(category)s,
            name = %(name)s,
            father_husband_name = %(father_husband_name)s,
            gender = %(gender)s,
            aadhaar_no = %(aadhaar_no)s,
            address = %(address)s,
            contact_no = %(contact_no)s,
            alternate_contact_no = %(alternate_contact_no)s,
            dob = %(dob)s,
            locality = %(locality)s,
            badge_category = %(badge_category)s,
            department_id = %(department_id)s,
            enrolment_date = %(enrolment_date)s,
            initiation_date = %(initiation_date)s,
            visit_badge_no = %(visit_badge_no)s,
            age = %(age)s,
            education = %(education)s,
            occupation = %(occupation)s
        WHERE badge_no = %(badge_no)s
    """
    data = sewadar.dict()
    data["badge_no"] = badge_no

    cursor.execute(sql, data)
    conn.commit()
    rows_updated = cursor.rowcount

    cursor.close()
    conn.close()

    if rows_updated == 0:
        raise HTTPException(status_code=404, detail="Sewadar not found")

    return {"message": f"Sewadar {badge_no} updated successfully"}
