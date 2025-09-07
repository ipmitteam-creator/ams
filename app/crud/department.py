from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch

router = APIRouter()

# ---------------- Schema Models ----------------
class Department(BaseModel):
    name: str

class DepartmentList(BaseModel):
    departments: List[Department]

class DepartmentUpdate(BaseModel):
    name: str

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

# ---------------- Add Departments ----------------
@router.post("/")
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

# ---------------- Update Department ----------------
@router.put("/{department_id}")
def update_department(department_id: int, dept: DepartmentUpdate):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        "UPDATE department SET department_name = %s WHERE department_id = %s",
        (dept.name, department_id)
    )
    conn.commit()
    rows_updated = cursor.rowcount

    cursor.close()
    conn.close()

    if rows_updated == 0:
        raise HTTPException(status_code=404, detail=f"Department with id {department_id} not found")

    return {"message": f"Department {department_id} updated successfully"}

# ---------------- Fetch All Departments ----------------
@router.get("/")
def get_all_departments():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute("SELECT * FROM department ORDER BY department_id")
    records = cursor.fetchall()

    cursor.close()
    conn.close()

    return records
