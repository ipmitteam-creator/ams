from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import psycopg2
from psycopg2.extras import execute_batch

app = FastAPI()

# ✅ Define schema for department
class Department(BaseModel):
    name: str

class DepartmentList(BaseModel):
    departments: List[Department]

# ✅ Database connection function
def get_db_connection():
    return psycopg2.connect(
        dbname="ams",
        user="neondb_owner",
        password="npg_igo8fBOT3MtP",
        host="ep-orange-fog-a1qfrxr9-pooler.ap-southeast-1.aws.neon.tech",
        port="5432",
        sslmode="require"
    )

# ✅ API endpoint to insert departments
@app.post("/add_departments")
def add_departments(data: DepartmentList):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Use execute_batch for efficiency
    sql = "INSERT INTO department (department_name) VALUES (%s) ON CONFLICT DO NOTHING"
    values = [(dept.name,) for dept in data.departments]

    execute_batch(cursor, sql, values)
    conn.commit()

    cursor.close()
    conn.close()

    return {"message": f"{len(data.departments)} departments processed successfully"}
