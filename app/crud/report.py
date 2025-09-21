from fastapi import APIRouter, HTTPException, Query
from datetime import date, timedelta
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
    start_date: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: date = Query(None, description="End date (YYYY-MM-DD). Defaults to start_date if not provided"),
    attendance_type: str = Query(None, description="Attendance type filter (optional)"),
    department: str = Query(None, description="Department name filter (optional)"),
    badge_no: str = Query(None, description="Badge number filter (optional)")
):
    """
    Generate attendance report:
    1. For a date or range of dates.
    2. Filter by department (optional).
    3. Filter by badge_no (optional).
    4. Filter by attendance_type (optional).
    """
    if not end_date:
        end_date = start_date

    conn = get_db_connection()
    cur = conn.cursor()

    # Get all sewadaars and their departments
    base_query = """
        SELECT s.sewadar_id, s.badge_no, s.name, d.department_name
        FROM sewadar s
        JOIN department d ON s.department_id = d.department_id
    """
    filters = []
    params = []
    if department:
        filters.append("d.department_name = %s")
        params.append(department)
    if badge_no:
        filters.append("s.badge_no = %s")
        params.append(badge_no)

    if filters:
        base_query += " WHERE " + " AND ".join(filters)

    cur.execute(base_query, tuple(params))
    sewadaars = cur.fetchall()
    if not sewadaars:
        conn.close()
        raise HTTPException(status_code=404, detail="No sewadaars found for given filters.")

    # Get attendance records for the date range and optional filters
    attendance_query = """
        SELECT sewadar_id, attendance_date, attendance_type
        FROM attendance
        WHERE attendance_date BETWEEN %s AND %s
    """
    attendance_params = [start_date, end_date]
    if attendance_type:
        attendance_query += " AND attendance_type = %s"
        attendance_params.append(attendance_type)

    cur.execute(attendance_query, tuple(attendance_params))
    attendance_records = cur.fetchall()
    conn.close()

    # Build a lookup: {(sewadar_id, attendance_date): attendance_type}
    attendance_lookup = {(r[0], r[1]): r[2] for r in attendance_records}

    # Build the report
    report = []
    day_count = (end_date - start_date).days + 1
    for offset in range(day_count):
        day = start_date + timedelta(days=offset)
        for sewadar_id, badge, name, dept_name in sewadaars:
            key = (sewadar_id, day)
            if key in attendance_lookup:
                a_type = attendance_lookup[key]
                status = "Jatha" if a_type in ["Bhati", "Beas", "Others"] else "Present"
            else:
                status = "Absent"
            report.append({
                "date": str(day),
                "name": name,
                "badge_no": badge,
                "department": dept_name,
                "status": status
            })

    return {
        "start_date": str(start_date),
        "end_date": str(end_date),
        "attendance_type_filter": attendance_type or "All",
        "department_filter": department or "All",
        "badge_filter": badge_no or "All",
        "report": report
    }

@router.get("/report/jatha")
def get_jatha_report(
    start_date: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: date = Query(None, description="End date (YYYY-MM-DD). Defaults to start_date if not provided"),
    department: str = Query(None, description="Department name filter (optional)"),
    badge_no: str = Query(None, description="Badge number filter (optional)")
):
    """
    Generate Jatha attendance report:
    - Only includes people who actually attended (Beas, Bhati, Others)
    - Each day is a separate row
    - Columns: Badge Number, Duty Type (J), Date of Seva, Name
    - Sorted by Date, then Badge Number
    """

    valid_jatha_types = ["Beas", "Bhati", "Others"]

    if not end_date:
        end_date = start_date

    if end_date < start_date:
        raise HTTPException(status_code=400, detail="end_date cannot be before start_date.")

    conn = get_db_connection()
    cur = conn.cursor()

    # Fetch attendance records joined with sewadar info
    query = """
        SELECT s.badge_no, s.name, a.attendance_date, a.attendance_type
        FROM attendance a
        JOIN sewadar s ON a.sewadar_id = s.sewadar_id
        JOIN department d ON s.department_id = d.department_id
        WHERE a.attendance_date BETWEEN %s AND %s
        AND a.attendance_type IN %s
    """
    params = [start_date, end_date, tuple(valid_jatha_types)]
    if department:
        query += " AND d.department_name = %s"
        params.append(department)
    if badge_no:
        query += " AND s.badge_no = %s"
        params.append(badge_no)

    cur.execute(query, tuple(params))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return {"report": []}

    # Build report with attendance_date as date object
    report = []
    for badge, name, attendance_date, a_type in rows:
        report.append({
            "badge_no": badge,
            "duty_type": "J",
            "attendance_date": attendance_date,  # keep as date for sorting
            "name": name
        })

    # Sort by attendance_date then badge_no
    report.sort(key=lambda x: (x["attendance_date"], x["badge_no"]))

    # Format date as DD/MM/YYYY for final output
    for row in report:
        row["date_of_seva"] = row.pop("attendance_date").strftime("%d/%m/%Y")

    return {
        "start_date": start_date.strftime("%d/%m/%Y"),
        "end_date": end_date.strftime("%d/%m/%Y"),
        "report": report
    }