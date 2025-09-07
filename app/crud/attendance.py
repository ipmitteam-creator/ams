from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import date, datetime
import psycopg2

app = FastAPI()

DB_CONFIG = {
    "dbname": "ams",
    "user": "neondb_owner",
    "password": "npg_igo8fBOT3MtP",
    "host": "ep-orange-fog-a1qfrxr9-pooler.ap-southeast-1.aws.neon.tech",
    "port": "5432",
    "sslmode": "require"
}

class ScanAttendance(BaseModel):
    badge_no: str
    attendance_type: str
    check_in_time: str = None
    check_out_time: str = None
    remarks: str = None

VALID_ATTENDANCE_TYPES = [
    "Sunday_Satsang", "Wednesday_Satsang", "WeekDay", "WeekNight", "Bhati", "Beas", "Others"
]

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

@app.post("/attendance/scan")
def scan_attendance(data: ScanAttendance):
    if data.attendance_type not in VALID_ATTENDANCE_TYPES:
        raise HTTPException(status_code=400, detail="Invalid attendance type")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Lookup sewadar_id
    cur.execute("SELECT sewadar_id, name, department_id FROM sewadar WHERE badge_no = %s", (data.badge_no,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Badge not found")
    
    sewadar_id, name, dept_id = row
    
    # Insert attendance
    cur.execute("""
        INSERT INTO attendance (
            sewadar_id, attendance_date, attendance_type, check_in_time, check_out_time, remarks
        ) VALUES (%s, %s, %s, %s, %s, %s)
    """, (sewadar_id, date.today(), data.attendance_type, data.check_in_time, data.check_out_time, data.remarks))
    
    conn.commit()
    conn.close()
    
    return {
        "message": "Attendance recorded",
        "badge_no": data.badge_no,
        "name": name,
        "department_id": dept_id
    }
