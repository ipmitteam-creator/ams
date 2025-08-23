from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import date
import mysql.connector

app = FastAPI()

# ---- Database Connection ----
def get_db():
    return mysql.connector.connect(
        host="localhost",
        user="admin",
        password="admin",
        database="ams"
    )

# ---- Request Models ----
class CheckInRequest(BaseModel):
    sewadar_id: int
    attendance_type: str   # "Daily_Centre", "Bhati_Centre", "Beas_Centre", "Others"
    check_in_time: str     # "HH:MM:SS"
    remarks: str | None = None

class CheckOutRequest(BaseModel):
    sewadar_id: int
    check_out_time: str    # "HH:MM:SS"
    remarks: str | None = None


# ---- API Endpoints ----
@app.post("/attendance/checkin")
def check_in(req: CheckInRequest):
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    today = date.today()

    # Check if sewadar exists
    cursor.execute("SELECT 1 FROM sewadar WHERE sewadar_id = %s", (req.sewadar_id,))
    if not cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail=f"Sewadar ID {req.sewadar_id} not found")

    # Check if already checked in today
    cursor.execute("""
        SELECT * FROM attendance
        WHERE sewadar_id = %s AND attendance_date = %s
    """, (req.sewadar_id, today))
    existing = cursor.fetchone()

    if existing:
        conn.close()
        raise HTTPException(status_code=400, detail="Already checked in today")

    # Insert new attendance record
    sql = """
        INSERT INTO attendance (sewadar_id, attendance_date, attendance_type, check_in_time, remarks)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor.execute(sql, (req.sewadar_id, today, req.attendance_type, req.check_in_time, req.remarks))
    conn.commit()
    conn.close()

    return {"message": "Check-in recorded successfully", "sewadar_id": req.sewadar_id}


@app.post("/attendance/checkout")
def check_out(req: CheckOutRequest):
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    today = date.today()

    # Check if record exists
    cursor.execute("""
        SELECT * FROM attendance
        WHERE sewadar_id = %s AND attendance_date = %s
    """, (req.sewadar_id, today))
    record = cursor.fetchone()

    if not record:
        conn.close()
        raise HTTPException(status_code=404, detail="No check-in found for today")

    if record["check_out_time"]:
        conn.close()
        raise HTTPException(status_code=400, detail="Already checked out today")

    # Update record with checkout time
    sql = """
        UPDATE attendance
        SET check_out_time = %s, remarks = %s
        WHERE attendance_id = %s
    """
    cursor.execute(sql, (req.check_out_time, req.remarks, record["attendance_id"]))
    conn.commit()
    conn.close()

    return {"message": "Check-out recorded successfully", "sewadar_id": req.sewadar_id}
