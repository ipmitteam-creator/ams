# app/crud/add_update_sewadar.py
from fastapi import APIRouter, HTTPException, Query, Response
from pydantic import BaseModel
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import date, datetime
import io
import pandas as pd
from fastapi_mail import FastMail, MessageSchema, ConnectionConfig
from pydantic import EmailStr
import asyncio

router = APIRouter()



EMAIL_CONFIG = ConnectionConfig(
    MAIL_USERNAME="ipmitteam@gmail.com",
    MAIL_PASSWORD="rerw quhk jsjw wlpm",
    MAIL_FROM="ipmitteam@gmail.com",
    MAIL_PORT=587,
    MAIL_SERVER="smtp.gmail.com",
    MAIL_FROM_NAME="IPMIT Team",
    MAIL_STARTTLS=True,   # required
    MAIL_SSL_TLS=False,   # required
    USE_CREDENTIALS=True,
    #TEMPLATE_FOLDER="app/email_templates"  # optional, if you use templates
)


# ---------------- Schema Model ----------------
class Sewadar(BaseModel):
    name: str
    father_husband_name: Optional[str] = None
    contact_no: Optional[str] = None
    alternate_contact_no: Optional[str] = None
    address: Optional[str] = None
    permanent_address: Optional[str] = None
    gender: Optional[str] = None
    dob: Optional[str] = None
    department_name: Optional[str] = None
    current_department_name: Optional[str] = None
    enrolment_date: Optional[str] = None
    enrolment_code: Optional[str] = None     # <-- new
    short_name: Optional[str] = None        # <-- new
    age: Optional[int] = None               # <-- new
    blood_group: Optional[str] = None
    locality: Optional[str] = None
    badge_no: str
    badge_category: Optional[str] = None
    badge_issue_date: Optional[str] = None
    initiation_date: Optional[str] = None
    visit_badge_no: Optional[str] = None
    education: Optional[str] = None
    occupation: Optional[str] = None
    photo: Optional[str] = None
    aadhaar_photo: Optional[str] = None
    aadhaar_no: Optional[str] = None
    category: Optional[str] = None
    updated_by: Optional[int] = None

# ---------------- DB Connection ----------------
async def send_new_sewadar_email(to_email: EmailStr, sewadar_name: str, enrolment_code: str):
    message = MessageSchema(
        subject="New Sewadar Enrolment",
        recipients=[to_email],
        body=f"Hello,\n\nA new Sewadar '{sewadar_name}' has been enrolled successfully.\nEnrolment Code: {enrolment_code}\n\nRegards,\nAMS Team",
        subtype="plain"
    )
    fm = FastMail(EMAIL_CONFIG)
    await fm.send_message(message)

def get_db_connection():
    return psycopg2.connect(
        dbname="ams",
        user="neondb_owner",
        password="npg_igo8fBOT3MtP",
        host="ep-orange-fog-a1qfrxr9-pooler.ap-southeast-1.aws.neon.tech",
        port="5432",
        sslmode="require"
    )

# ---------------- Utilities ----------------
def normalize_strings(data: dict) -> dict:
    """Strip + lowercase all string values in a dict."""
    for k, v in data.items():
        if isinstance(v, str):
            data[k] = v.strip().lower()
    return data

def get_department_id(conn, department_name: Optional[str]) -> Optional[int]:
    if not department_name:
        return None
    cur = conn.cursor()
    cur.execute(
        "SELECT department_id FROM department WHERE LOWER(TRIM(department_name)) = LOWER(TRIM(%s))",
        (department_name,)
    )
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None

def get_department_name(conn, department_id: Optional[int]) -> Optional[str]:
    if not department_id:
        return None
    cur = conn.cursor()
    cur.execute("SELECT department_name FROM department WHERE department_id = %s", (department_id,))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None

def calculate_age_from_dob(dob_str: Optional[str]) -> Optional[int]:
    if not dob_str:
        return None
    try:
        dob = datetime.strptime(dob_str, "%Y-%m-%d").date()
        today = date.today()
        return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    except ValueError:
        return None

def generate_enrolment_code(conn, gender: Optional[str]) -> str:
    """Generate sequential enrolment code NE-G-0001 / NE-L-0001."""
    if not gender:
        gender_prefix = "G"
    elif gender.lower() == "female":
        gender_prefix = "L"
    else:
        gender_prefix = "G"

    cur = conn.cursor()
    cur.execute(
        "SELECT enrolment_code FROM sewadar WHERE enrolment_code LIKE %s ORDER BY enrolment_code DESC LIMIT 1",
        (f"NE-{gender_prefix}-%",)
    )
    last = cur.fetchone()
    cur.close()

    if last and last[0]:
        try:
            num = int(last[0].split("-")[-1])
            next_num = num + 1
        except Exception:
            next_num = 1
    else:
        next_num = 1

    return f"NE-{gender_prefix}-{next_num:04d}"

def parse_enrolment_date(enrolment_date_str: Optional[str]) -> str:
    """Convert dd/mm/yyyy to yyyy-mm-dd or default to today."""
    if not enrolment_date_str:
        return date.today().strftime("%Y-%m-%d")
    try:
        dt = datetime.strptime(enrolment_date_str, "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return date.today().strftime("%Y-%m-%d")

# ---------------- Add Sewadar ----------------
@router.post("/")
def add_sewadar(sewadar: Sewadar):
    conn = get_db_connection()
    cursor = conn.cursor()

    data = normalize_strings(sewadar.dict())

    # ---------------- Departments ----------------
    dept_id = get_department_id(conn, data.get("department_name"))
    current_dept_id = get_department_id(conn, data.get("current_department_name"))

    # ---------------- Age ----------------
    age = calculate_age_from_dob(data.get("dob"))

    # ---------------- Enrolment Date & Code ----------------
    data["enrolment_date"] = parse_enrolment_date(data.get("enrolment_date"))
    data["enrolment_code"] = generate_enrolment_code(conn, data.get("gender"))

    # ---------------- Short Name ----------------
    if not data.get("short_name") or data.get("short_name").strip() == "":
        # Auto-generate short_name
        name_parts = data.get("name", "").split()
        if len(name_parts) == 0:
            raise HTTPException(status_code=400, detail="Name is required for generating short_name")
        first_name = name_parts[0]
        last_name = name_parts[-1] if len(name_parts) > 1 else ""
        base_short_name = (first_name + last_name[:1]).lower().replace(" ", "")

        # Ensure uniqueness
        cursor.execute(
            "SELECT short_name FROM sewadar WHERE short_name ILIKE %s",
            (base_short_name + "%",)
        )
        existing = [row[0] for row in cursor.fetchall()]
        if base_short_name not in existing:
            data["short_name"] = base_short_name
        else:
            # Find next available numeric suffix
            suffix = 2
            while f"{base_short_name}{suffix}" in existing:
                suffix += 1
            data["short_name"] = f"{base_short_name}{suffix}"

    # ---------------- SQL Insert ----------------
    sql = """
        INSERT INTO sewadar (
            name, father_husband_name, contact_no, alternate_contact_no, address, permanent_address,
            gender, dob, department_id, current_department_id, enrolment_date, enrolment_code,
            short_name, blood_group, locality, badge_no, badge_category, badge_issue_date,
            initiation_date, visit_badge_no, education, occupation, photo,
            aadhaar_photo, aadhaar_no, category, age, updated_by
        )
        VALUES (
            %(name)s, %(father_husband_name)s, %(contact_no)s, %(alternate_contact_no)s, %(address)s, %(permanent_address)s,
            %(gender)s, %(dob)s, %(department_id)s, %(current_department_id)s, %(enrolment_date)s, %(enrolment_code)s,
            %(short_name)s, %(blood_group)s, %(locality)s, %(badge_no)s, %(badge_category)s, %(badge_issue_date)s,
            %(initiation_date)s, %(visit_badge_no)s, %(education)s, %(occupation)s, %(photo)s,
            %(aadhaar_photo)s, %(aadhaar_no)s, %(category)s, %(age)s, %(updated_by)s
        )
        ON CONFLICT ((LOWER(badge_no))) DO NOTHING
    """

    data["department_id"] = dept_id
    data["current_department_id"] = current_dept_id
    data["age"] = age

    cursor.execute(sql, data)
    conn.commit()
    cursor.close()
    conn.close()

    # ---------------- Send Email ----------------
    if data.get("contact_no"):  # or you can have email field in Sewadar model
        # Example: send email to admin or secretary
        asyncio.create_task(send_new_sewadar_email("kaushambiit@gmail.com", data["name"], data["enrolment_code"]))


    return {
        "message": f"Sewadar {data['name']} added successfully",
        "enrolment_code": data["enrolment_code"],
        "enrolment_date": data["enrolment_date"],
        "short_name": data["short_name"]
    }

# ---------------- Update Sewadar ----------------
@router.put("/{badge_no}")
def update_sewadar(badge_no: str, sewadar_update: Sewadar):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    # Fetch existing sewadar
    cursor.execute("SELECT * FROM sewadar WHERE LOWER(badge_no) = LOWER(%s)", (badge_no,))
    existing = cursor.fetchone()
    if not existing:
        conn.close()
        raise HTTPException(status_code=404, detail=f"Sewadar with badge_no {badge_no} not found")

    # Merge updates
    update_data = existing.copy()
    for key, value in sewadar_update.dict(exclude_unset=True).items():
        update_data[key] = value
    update_data = normalize_strings(update_data)

    # ---------------- Departments ----------------
    dept_id = get_department_id(conn, update_data.get("department_name"))
    current_dept_id = get_department_id(conn, update_data.get("current_department_name"))
    update_data["department_id"] = dept_id if dept_id else existing.get("department_id")
    update_data["current_department_id"] = current_dept_id if current_dept_id else existing.get("current_department_id")

    # ---------------- Age ----------------
    update_data["age"] = calculate_age_from_dob(update_data.get("dob"))

    # ---------------- Enrolment Date ----------------
    update_data["enrolment_date"] = parse_enrolment_date(update_data.get("enrolment_date"))

    # ---------------- Short Name ----------------
    if not update_data.get("short_name") or update_data.get("short_name").strip() == "":
        name_parts = update_data.get("name", "").split()
        if len(name_parts) == 0:
            conn.close()
            raise HTTPException(status_code=400, detail="Name is required for generating short_name")
        first_name = name_parts[0]
        last_name = name_parts[-1] if len(name_parts) > 1 else ""
        base_short_name = (first_name + last_name[:1]).lower().replace(" ", "")

        # Ensure uniqueness (excluding current sewadar)
        cursor.execute(
            "SELECT short_name FROM sewadar WHERE short_name ILIKE %s AND LOWER(badge_no) != LOWER(%s)",
            (base_short_name + "%", badge_no)
        )
        existing_short_names = [row[0] for row in cursor.fetchall()]
        if base_short_name not in existing_short_names:
            update_data["short_name"] = base_short_name
        else:
            suffix = 2
            while f"{base_short_name}{suffix}" in existing_short_names:
                suffix += 1
            update_data["short_name"] = f"{base_short_name}{suffix}"

    # ---------------- SQL Update ----------------
    fields = [
        "name","father_husband_name","contact_no","alternate_contact_no","address","permanent_address",
        "gender","dob","department_id","current_department_id","enrolment_date","blood_group",
        "locality","short_name","badge_category","badge_issue_date","initiation_date",
        "visit_badge_no","education","occupation","photo","aadhaar_photo",
        "aadhaar_no","category","age","updated_by"
    ]
    set_clause = ", ".join([f"{f} = %({f})s" for f in fields])
    sql = f"UPDATE sewadar SET {set_clause} WHERE LOWER(badge_no) = LOWER(%(badge_no)s)"

    update_data["badge_no"] = badge_no
    cursor.execute(sql, update_data)
    conn.commit()
    cursor.close()
    conn.close()

    return {
        "message": f"Sewadar with badge_no {badge_no} updated successfully",
        "short_name": update_data["short_name"]
    }

@router.put("/update_by_shortname/{short_name}")
def update_sewadar_by_shortname(short_name: str, sewadar_update: Sewadar):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    # Fetch existing sewadar
    cursor.execute("SELECT * FROM sewadar WHERE LOWER(short_name) = LOWER(%s)", (short_name,))
    existing = cursor.fetchone()
    if not existing:
        conn.close()
        raise HTTPException(status_code=404, detail=f"Sewadar with short_name {short_name} not found")

    # Merge updates
    update_data = existing.copy()
    for key, value in sewadar_update.dict(exclude_unset=True).items():
        update_data[key] = value
    update_data = normalize_strings(update_data)

    # ---------------- Departments ----------------
    dept_id = get_department_id(conn, update_data.get("department_name"))
    current_dept_id = get_department_id(conn, update_data.get("current_department_name"))
    update_data["department_id"] = dept_id if dept_id else existing.get("department_id")
    update_data["current_department_id"] = current_dept_id if current_dept_id else existing.get("current_department_id")

    # ---------------- Age ----------------
    update_data["age"] = calculate_age_from_dob(update_data.get("dob"))

    # ---------------- Enrolment Date ----------------
    update_data["enrolment_date"] = parse_enrolment_date(update_data.get("enrolment_date"))

    # ---------------- Short Name ----------------
    if not update_data.get("short_name") or update_data.get("short_name").strip() == "":
        name_parts = update_data.get("name", "").split()
        if len(name_parts) == 0:
            conn.close()
            raise HTTPException(status_code=400, detail="Name is required for generating short_name")
        first_name = name_parts[0]
        last_name = name_parts[-1] if len(name_parts) > 1 else ""
        base_short_name = (first_name + last_name[:1]).lower().replace(" ", "")

        # Ensure uniqueness (excluding current sewadar)
        cursor.execute(
            "SELECT short_name FROM sewadar WHERE short_name ILIKE %s AND sewadar_id != %s",
            (base_short_name + "%", existing["sewadar_id"])
        )
        existing_short_names = [row[0] for row in cursor.fetchall()]
        if base_short_name not in existing_short_names:
            update_data["short_name"] = base_short_name
        else:
            suffix = 2
            while f"{base_short_name}{suffix}" in existing_short_names:
                suffix += 1
            update_data["short_name"] = f"{base_short_name}{suffix}"

    # ---------------- SQL Update ----------------
    fields = [
        "name","father_husband_name","contact_no","alternate_contact_no","address","permanent_address",
        "gender","dob","department_id","current_department_id","enrolment_date","blood_group",
        "locality","short_name","badge_category","badge_issue_date","initiation_date",
        "visit_badge_no","education","occupation","photo","aadhaar_photo",
        "aadhaar_no","category","age","updated_by"
    ]
    set_clause = ", ".join([f"{f} = %({f})s" for f in fields])
    sql = f"UPDATE sewadar SET {set_clause} WHERE sewadar_id = %(sewadar_id)s"

    update_data["sewadar_id"] = existing["sewadar_id"]
    cursor.execute(sql, update_data)
    conn.commit()
    cursor.close()
    conn.close()

    return {
        "message": f"Sewadar with short_name {short_name} updated successfully",
        "short_name": update_data["short_name"]
    }

@router.get("/{badge_no}")
def get_sewadar(badge_no: str):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute("SELECT * FROM sewadar WHERE LOWER(badge_no) = LOWER(%s)", (badge_no,))
    record = cursor.fetchone()
    if not record:
        conn.close()
        raise HTTPException(status_code=404, detail="Sewadar not found")

    # Add department names
    record["department_name"] = get_department_name(conn, record.get("department_id"))
    record["current_department_name"] = get_department_name(conn, record.get("current_department_id"))
    record.pop("department_id", None)
    record.pop("current_department_id", None)

    # Include short_name from table as-is
    # No generation in GET

    conn.close()
    return record


# ---------------- Search / Export Sewadars ----------------
@router.get("/")
def search_sewadars(
    department_name: Optional[str] = Query(None),
    current_department_name: Optional[str] = Query(None),
    locality: Optional[str] = Query(None),
    gender: Optional[str] = Query(None),
    badge_no: Optional[str] = Query(None),
    badge_category: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    age: Optional[int] = Query(None),
    short_name: Optional[str] = Query(None),  # ✅ New filter added
    format: Optional[str] = Query("json"),
    columns: Optional[str] = Query(None)
):
    ALLOWED_COLUMNS = {
        "sewadar_id","name","father_husband_name","contact_no","alternate_contact_no","address","permanent_address",
        "gender","dob","department_name","current_department_name","enrolment_date","enrolment_code","blood_group",
        "locality","badge_no","badge_category","badge_issue_date","initiation_date",
        "visit_badge_no","education","occupation","photo","aadhaar_photo","aadhaar_no",
        "category","age","updated_by","short_name"  # ✅ Add short_name to allowed output
    }

    if columns:
        requested_columns = {c.strip().lower() for c in columns.split(",")}
        invalid = requested_columns - {c.lower() for c in ALLOWED_COLUMNS}
        if invalid:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid column(s): {', '.join(invalid)}. Allowed: {', '.join(ALLOWED_COLUMNS)}"
            )
        selected_columns = [c for c in ALLOWED_COLUMNS if c.lower() in requested_columns]
    else:
        selected_columns = list(ALLOWED_COLUMNS)

    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    conditions, values = [], []
    if department_name:
        dept_id = get_department_id(conn, department_name.strip().lower())
        if dept_id: conditions.append("department_id = %s"); values.append(dept_id)
        else: conn.close(); return []

    if current_department_name:
        cur_dept_id = get_department_id(conn, current_department_name.strip().lower())
        if cur_dept_id: conditions.append("current_department_id = %s"); values.append(cur_dept_id)
        else: conn.close(); return []

    if locality: conditions.append("locality ILIKE %s"); values.append(f"%{locality}%")
    if gender: conditions.append("LOWER(gender) = LOWER(%s)"); values.append(gender)
    if badge_no: conditions.append("LOWER(badge_no) = LOWER(%s)"); values.append(badge_no)
    if badge_category: conditions.append("LOWER(badge_category) = LOWER(%s)"); values.append(badge_category)
    if category: conditions.append("LOWER(category) = LOWER(%s)"); values.append(category)
    if age is not None:
        today = date.today()
        dob_cutoff = today.replace(year=today.year - age)
        conditions.append("dob <= %s"); values.append(dob_cutoff)
    if short_name:  # ✅ Apply short_name filter
        conditions.append("LOWER(short_name) = LOWER(%s)")
        values.append(short_name)

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    cursor.execute(f"SELECT * FROM sewadar {where_clause}", tuple(values))
    records = cursor.fetchall()

    for r in records:
        r["department_name"] = get_department_name(conn, r.get("department_id"))
        r["current_department_name"] = get_department_name(conn, r.get("current_department_id"))
        r.pop("department_id", None)
        r.pop("current_department_id", None)

    cursor.close()
    conn.close()

    filtered_records = [{col: r.get(col) for col in selected_columns if col in r} for r in records]

    if format == "json":
        return filtered_records
    df = pd.DataFrame(filtered_records)
    if format == "csv":
        output = io.StringIO(); df.to_csv(output, index=False)
        return Response(content=output.getvalue(), media_type="text/csv",
                        headers={"Content-Disposition": "attachment; filename=sewadar_report.csv"})
    if format == "xlsx":
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer: df.to_excel(writer, index=False, sheet_name="Sewadars")
        return Response(content=output.getvalue(), media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        headers={"Content-Disposition": "attachment; filename=sewadar_report.xlsx"})
    raise HTTPException(status_code=400, detail="Invalid format. Use json, csv, or xlsx.")
