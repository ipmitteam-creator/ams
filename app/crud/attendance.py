from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from datetime import date, datetime, timedelta
import psycopg2
from typing import Optional, List

router = APIRouter()

DB_CONFIG = {
    "dbname": "ams",
    "user": "neondb_owner",
    "password": "npg_igo8fBOT3MtP",
    "host": "ep-orange-fog-a1qfrxr9-pooler.ap-southeast-1.aws.neon.tech",
    "port": "5432",
    "sslmode": "require"
}

VALID_ATTENDANCE_TYPES = [
    "Sunday_Satsang", "Wednesday_Satsang",
    "WeekDay", "WeekNight",
    "Bhati", "Beas", "Others"
]
JATHA_TYPES = ["Bhati", "Beas", "Others"]

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

class ScanAttendance(BaseModel):
    badge_no: str
    attendance_type: str
    attendance_date: Optional[str] = None  # 👈 new field (YYYY-MM-DD)
    check_in_time: Optional[str] = None
    check_out_time: Optional[str] = None
    remarks: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None

class BulkAttendance(BaseModel):
    records: List[ScanAttendance]

# ----------------- /scan endpoint -----------------
@router.post("/scan")
def scan_attendance(data: ScanAttendance):
    if data.attendance_type not in VALID_ATTENDANCE_TYPES:
        raise HTTPException(status_code=400, detail="Invalid attendance type")

    conn = get_db_connection()
    cur = conn.cursor()

    # Find sewadar
    cur.execute("""
        SELECT s.sewadar_id, s.name, s.current_department_id, d.department_name
        FROM sewadar s
        JOIN department d ON s.current_department_id = d.department_id
        WHERE s.badge_no = %s
    """, (data.badge_no,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Badge not found")

    sewadar_id, name, dept_id, dept_name = row

    # ----- Jatha handling -----
    if data.attendance_type in JATHA_TYPES:
        if not data.start_date or not data.end_date:
            conn.close()
            raise HTTPException(status_code=400, detail="Start and end dates required for Jatha types")

        start_date = datetime.strptime(data.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(data.end_date, "%Y-%m-%d").date()
        if end_date < start_date:
            conn.close()
            raise HTTPException(status_code=400, detail="End date cannot be before start date")

        inserted_dates, skipped_dates = [], []
        fixed_check_in, fixed_check_out = "07:00:00", "19:00:00"

        for n in range((end_date - start_date).days + 1):
            attendance_date = start_date + timedelta(days=n)
            cur.execute("""
                SELECT 1 FROM attendance
                WHERE sewadar_id=%s AND attendance_date=%s
            """, (sewadar_id, attendance_date))
            if cur.fetchone():
                skipped_dates.append(attendance_date.isoformat())
                continue

            cur.execute("""
                INSERT INTO attendance (sewadar_id, attendance_date, attendance_type,
                    check_in_time, check_out_time, remarks)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (sewadar_id, attendance_date, data.attendance_type,
                  fixed_check_in, fixed_check_out, data.remarks))
            inserted_dates.append(attendance_date.isoformat())

        conn.commit()
        conn.close()
        return {
            "message": f"{data.attendance_type} attendance processed",
            "badge_no": data.badge_no,
            "name": name,
            "current_department_name": dept_name,
            "inserted_dates": inserted_dates,
            "skipped_dates": skipped_dates
        }

    # ----- Normal attendance -----
    att_date = date.today() if not data.attendance_date else datetime.strptime(data.attendance_date, "%Y-%m-%d").date()

    if data.check_in_time:
        cur.execute("""
            SELECT 1 FROM attendance
            WHERE sewadar_id=%s AND attendance_date=%s
        """, (sewadar_id, att_date))
        if cur.fetchone():
            conn.close()
            raise HTTPException(status_code=400, detail="❌ Attendance already exists for this date")

        cur.execute("""
            INSERT INTO attendance (sewadar_id, attendance_date, attendance_type, check_in_time, remarks)
            VALUES (%s, %s, %s, %s, %s)
        """, (sewadar_id, att_date, data.attendance_type, data.check_in_time, data.remarks))
        conn.commit()
        conn.close()
        return {"message": "Check-In recorded", "badge_no": data.badge_no,
                "name": name, "current_department_name": dept_name,
                "check_in_time": data.check_in_time, "check_out_time": None, "date": att_date.isoformat()}

    if data.check_out_time:
        cur.execute("""
            SELECT attendance_id, check_in_time, check_out_time
            FROM attendance
            WHERE sewadar_id=%s AND attendance_date=%s AND attendance_type=%s
        """, (sewadar_id, att_date, data.attendance_type))
        existing = cur.fetchone()
        if not existing:
            conn.close()
            raise HTTPException(status_code=404, detail="❌ No Check-In found for this date")
        attendance_id, check_in, existing_out = existing
        if existing_out:
            conn.close()
            raise HTTPException(status_code=400, detail="❌ Already checked out for this date!")

        cur.execute("UPDATE attendance SET check_out_time=%s WHERE attendance_id=%s",
                    (data.check_out_time, attendance_id))
        conn.commit()
        conn.close()
        return {"message": "Check-Out recorded", "badge_no": data.badge_no,
                "name": name, "current_department_name": dept_name,
                "check_in_time": check_in, "check_out_time": data.check_out_time, "date": att_date.isoformat()}

    conn.close()
    raise HTTPException(status_code=400, detail="No check-in or check-out time provided")


# ----------------- Bulk Attendance -----------------
@router.post("/bulk")
def bulk_attendance(payload: BulkAttendance):
    results = []
    conn = get_db_connection()
    cur = conn.cursor()

    for rec in payload.records:
        try:
            if rec.attendance_type not in VALID_ATTENDANCE_TYPES:
                results.append({"badge_no": rec.badge_no, "status": "❌ Invalid type"})
                continue

            cur.execute("""
                SELECT s.sewadar_id, s.name, s.current_department_id, d.department_name
                FROM sewadar s
                JOIN department d ON s.current_department_id = d.department_id
                WHERE s.badge_no = %s
            """, (rec.badge_no,))
            row = cur.fetchone()
            if not row:
                results.append({"badge_no": rec.badge_no, "status": "❌ Badge not found"})
                continue

            sewadar_id, name, dept_id, dept_name = row

            # ----- Jatha -----
            if rec.attendance_type in JATHA_TYPES:
                if not rec.start_date or not rec.end_date:
                    results.append({"badge_no": rec.badge_no, "status": "❌ Dates required"})
                    continue

                start_date = datetime.strptime(rec.start_date, "%Y-%m-%d").date()
                end_date = datetime.strptime(rec.end_date, "%Y-%m-%d").date()
                if end_date < start_date:
                    results.append({"badge_no": rec.badge_no, "status": "❌ End date before start"})
                    continue

                inserted, skipped = [], []
                fixed_in, fixed_out = "07:00:00", "19:00:00"
                for n in range((end_date - start_date).days + 1):
                    dte = start_date + timedelta(days=n)
                    cur.execute("""
                        SELECT 1 FROM attendance
                        WHERE sewadar_id=%s AND attendance_date=%s
                    """, (sewadar_id, dte))
                    if cur.fetchone():
                        skipped.append(dte.isoformat())
                        continue

                    cur.execute("""
                        INSERT INTO attendance (sewadar_id, attendance_date, attendance_type,
                            check_in_time, check_out_time, remarks)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (sewadar_id, dte, rec.attendance_type, fixed_in, fixed_out, rec.remarks))
                    inserted.append(dte.isoformat())

                results.append({
                    "badge_no": rec.badge_no,
                    "name": name,
                    "current_department_name": dept_name,
                    "type": rec.attendance_type,
                    "inserted": inserted,
                    "skipped": skipped,
                    "status": "✅ Jatha processed"
                })
                continue

            # ----- Normal attendance -----
            att_date = date.today() if not rec.attendance_date else datetime.strptime(rec.attendance_date, "%Y-%m-%d").date()

            # Bulk requires both check_in & check_out
            if not rec.check_in_time or not rec.check_out_time:
                results.append({"badge_no": rec.badge_no, "status": "❌ Bulk must include both check-in and check-out"})
                continue

            cur.execute("""
                SELECT 1 FROM attendance
                WHERE sewadar_id=%s AND attendance_date=%s
            """, (sewadar_id, att_date))
            if cur.fetchone():
                results.append({"badge_no": rec.badge_no, "status": f"⚠ Already has attendance on {att_date.isoformat()}"})
                continue

            cur.execute("""
                INSERT INTO attendance (sewadar_id, attendance_date, attendance_type,
                    check_in_time, check_out_time, remarks)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (sewadar_id, att_date, rec.attendance_type,
                  rec.check_in_time, rec.check_out_time, rec.remarks))
            results.append({"badge_no": rec.badge_no, "name": name,
                            "current_department_name": dept_name,
                            "date": att_date.isoformat(),
                            "check_in_time": rec.check_in_time,
                            "check_out_time": rec.check_out_time,
                            "status": "✅ Attendance saved"})

        except Exception as ex:
            results.append({"badge_no": rec.badge_no, "status": f"❌ Error: {ex}"})

    conn.commit()
    conn.close()
    return results
