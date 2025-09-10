# app/crud/attendance.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from datetime import date
import psycopg2

router = APIRouter()

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
    check_in_time: str | None = None
    check_out_time: str | None = None
    remarks: str | None = None

VALID_ATTENDANCE_TYPES = [
    "Sunday_Satsang", "Wednesday_Satsang", "WeekDay", "WeekNight", "Bhati", "Beas", "Others"
]

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# ---------------- Attendance Scan API ----------------
@router.post("/scan")
def scan_attendance(data: ScanAttendance):
    if data.attendance_type not in VALID_ATTENDANCE_TYPES:
        raise HTTPException(status_code=400, detail="Invalid attendance type")

    conn = get_db_connection()
    cur = conn.cursor()

    # Lookup sewadar_id
    cur.execute(
        """
        SELECT s.sewadar_id, s.name, s.department_id, d.department_name
        FROM sewadar s
        JOIN department d ON s.department_id = d.department_id
        WHERE s.badge_no = %s
        """,
        (data.badge_no,)
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Badge not found")

    sewadar_id, name, dept_id, dept_name = row

    # Insert attendance
    cur.execute(
        """
        INSERT INTO attendance (
            sewadar_id, attendance_date, attendance_type, check_in_time, check_out_time, remarks
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            sewadar_id,
            date.today(),
            data.attendance_type,
            data.check_in_time,
            data.check_out_time,
            data.remarks,
        ),
    )

    conn.commit()
    conn.close()

    return {
    "message": "Attendance recorded",
    "badge_no": data.badge_no,
    "name": name,
    "department_name": dept_name,
    "check_in_time": data.check_in_time,
    "check_out_time": data.check_out_time
}
