from fastapi import APIRouter, HTTPException, Query
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

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

@router.get("/report")
def get_attendance_report(
    attendance_type: str = Query(..., description="Attendance type, e.g. Sunday_Satsang")
):
    """
    Returns a report of all sewadaars with Present/Absent status for today's date and the given attendance type.
    """
    today = date.today()

    conn = get_db_connection()
    cur = conn.cursor()

    # Get all sewadaars and their departments
    cur.execute("""
        SELECT s.sewadar_id, s.badge_no, s.name, d.department_name
        FROM sewadar s
        JOIN department d ON s.department_id = d.department_id
    """)
    sewadaars = cur.fetchall()

    if not sewadaars:
        conn.close()
        raise HTTPException(status_code=404, detail="No sewadaars found.")

    # Get all sewadaars who checked in today for this attendance type
    cur.execute("""
        SELECT sewadar_id
        FROM attendance
        WHERE attendance_date = %s
          AND attendance_type = %s
          AND check_in_time IS NOT NULL
    """, (today, attendance_type))
    present_ids = {row[0] for row in cur.fetchall()}

    conn.close()

    # Build the report
    report = []
    for sewadar_id, badge_no, name, dept_name in sewadaars:
        status = "Present" if sewadar_id in present_ids else "Absent"
        report.append({
            "name": name,
            "badge_no": badge_no,
            "department": dept_name,
            "status": status
        })

    return {
        "date": str(today),
        "attendance_type": attendance_type,
        "report": report
    }
