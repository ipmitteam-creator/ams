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

JATHA_TYPES = ["Bhati", "Beas", "Others"]

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


@router.post("/scan")
def scan_attendance(data: ScanAttendance):
    if data.attendance_type not in VALID_ATTENDANCE_TYPES:
        raise HTTPException(status_code=400, detail="Invalid attendance type")

    conn = get_db_connection()
    cur = conn.cursor()

    # Lookup sewadar
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
    today = date.today()

    # ----------------- Jatha Type Handling -----------------
    if data.attendance_type in JATHA_TYPES:
        fixed_check_in = "07:00:00"
        fixed_check_out = "19:00:00"

        # Check if row already exists
        cur.execute(
            """
            SELECT attendance_id FROM attendance
            WHERE sewadar_id = %s AND attendance_date = %s AND attendance_type = %s
            """,
            (sewadar_id, today, data.attendance_type)
        )
        existing = cur.fetchone()

        if existing:
            attendance_id = existing[0]
            cur.execute(
                """
                UPDATE attendance
                SET check_in_time=%s, check_out_time=%s, remarks=%s
                WHERE attendance_id=%s
                """,
                (fixed_check_in, fixed_check_out, data.remarks, attendance_id)
            )
        else:
            cur.execute(
                """
                INSERT INTO attendance (
                    sewadar_id, attendance_date, attendance_type, check_in_time, check_out_time, remarks
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (sewadar_id, today, data.attendance_type, fixed_check_in, fixed_check_out, data.remarks)
            )
        conn.commit()
        conn.close()
        return {
            "message": f"{data.attendance_type} attendance recorded",
            "badge_no": data.badge_no,
            "name": name,
            "department_name": dept_name,
            "check_in_time": fixed_check_in,
            "check_out_time": fixed_check_out
        }

    # ----------------- Normal Attendance Handling -----------------
    # Check-In
    if data.check_in_time:
        # Prevent duplicate check-in
        cur.execute(
            """
            SELECT attendance_id FROM attendance
            WHERE sewadar_id=%s AND attendance_date=%s AND attendance_type=%s
            """,
            (sewadar_id, today, data.attendance_type)
        )
        if cur.fetchone():
            conn.close()
            raise HTTPException(status_code=400, detail="❌ Already checked in today!")

        cur.execute(
            """
            INSERT INTO attendance (
                sewadar_id, attendance_date, attendance_type, check_in_time, remarks
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (sewadar_id, today, data.attendance_type, data.check_in_time, data.remarks)
        )
        conn.commit()
        conn.close()
        return {
            "message": "Check-In recorded",
            "badge_no": data.badge_no,
            "name": name,
            "department_name": dept_name,
            "check_in_time": data.check_in_time,
            "check_out_time": None
        }

    # Check-Out
    if data.check_out_time:
        # Find existing check-in
        cur.execute(
            """
            SELECT attendance_id, check_in_time, check_out_time
            FROM attendance
            WHERE sewadar_id=%s AND attendance_date=%s AND attendance_type=%s
            """,
            (sewadar_id, today, data.attendance_type)
        )
        existing = cur.fetchone()
        if not existing:
            conn.close()
            raise HTTPException(status_code=404, detail="❌ No Check-In found for today")

        attendance_id, check_in, existing_check_out = existing
        if existing_check_out:
            conn.close()
            raise HTTPException(status_code=400, detail="❌ Already checked out today!")

        # Update check-out
        cur.execute(
            """
            UPDATE attendance
            SET check_out_time=%s
            WHERE attendance_id=%s
            """,
            (data.check_out_time, attendance_id)
        )
        conn.commit()
        conn.close()
        return {
            "message": "Check-Out recorded",
            "badge_no": data.badge_no,
            "name": name,
            "department_name": dept_name,
            "check_in_time": check_in,
            "check_out_time": data.check_out_time
        }

    conn.close()
    raise HTTPException(status_code=400, detail="No check-in or check-out time provided")
