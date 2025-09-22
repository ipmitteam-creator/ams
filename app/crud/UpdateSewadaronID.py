import pandas as pd
import psycopg2

# === CONFIGURATION ===
EXCEL_FILE = "sewadar_update.xlsx"
SHEET_NAME = "Sheet1"

DB_CONFIG = {
    "dbname": "ams",
    "user": "neondb_owner",
    "password": "npg_igo8fBOT3MtP",
    "host": "ep-orange-fog-a1qfrxr9-pooler.ap-southeast-1.aws.neon.tech",
    "port": 5432,
    "sslmode": "require"
}

AADHAAR_MAX_LENGTH = 12

# === SEWADAR COLUMNS BASED ON SCHEMA ===
SEWADAR_COLUMNS = [
    "dob", "badge_issue_date", "photo", "aadhaar_photo", "department_id",
    "enrolment_date", "age", "updated_by",
    "badge_category", "initiation_date", "visit_badge_no", "education",
    "occupation", "aadhaar_no", "category", "role", "name",
    "father_husband_name", "contact_no", "alternate_contact_no", "address",
    "gender", "blood_group", "locality", "badge_no"
]

# === LOAD EXCEL DATA ===
df = pd.read_excel(EXCEL_FILE, sheet_name=SHEET_NAME)

if "sewadar_id" not in df.columns:
    raise ValueError("Excel file must include 'sewadar_id' column.")

# Clean sewadar_id (convert floats like 1252.0 to int)
def clean_id(value):
    if pd.isnull(value):
        return None
    if isinstance(value, float):
        return int(value)
    return int(str(value).strip())

df["sewadar_id"] = df["sewadar_id"].apply(clean_id)

# === CONNECT TO DB AND LOAD DEPARTMENTS ===
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

cur.execute("SELECT department_name, department_id FROM department")
dept_map = {name.strip().lower(): did for name, did in cur.fetchall()}

updated_count = 0
skipped_count = 0

for _, row in df.iterrows():
    sewadar_id = row["sewadar_id"]
    if sewadar_id is None:
        print("❌ Skipping row: Missing sewadar_id.")
        skipped_count += 1
        continue

    set_clauses = []
    values = []

    # Handle current_department_name lookup
    current_dept_name = row.get("current_department_name")
    if pd.notnull(current_dept_name):
        dept_key = str(current_dept_name).strip().lower()
        current_dept_id = dept_map.get(dept_key)
        if current_dept_id is None:
            print(f"❌ Skipping sewadar_id={sewadar_id}: Department '{current_dept_name}' not found.")
            skipped_count += 1
            continue
        set_clauses.append("current_department_id = %s")
        values.append(current_dept_id)

    for col in SEWADAR_COLUMNS:
        if col in df.columns:
            value = row[col] if pd.notnull(row[col]) else None

            # Aadhaar cleanup
            if col == "aadhaar_no" and value is not None:
                if isinstance(value, float):
                    value = str(int(value))
                else:
                    value = str(value).strip()

                if len(value) > AADHAAR_MAX_LENGTH:
                    print(f"❌ Skipping sewadar_id={sewadar_id}: Aadhaar '{value}' exceeds {AADHAAR_MAX_LENGTH} characters.")
                    skipped_count += 1
                    set_clauses = []  # Prevent update
                    break

            set_clauses.append(f"{col} = %s")
            values.append(value)

    if not set_clauses:
        continue

    values.append(sewadar_id)
    sql = f"""
        UPDATE sewadar
        SET {', '.join(set_clauses)}
        WHERE sewadar_id = %s
    """
    cur.execute(sql, tuple(values))
    updated_count += cur.rowcount

conn.commit()
cur.close()
conn.close()

print(f"✅ Update complete: {updated_count} records updated.")
print(f"⚠️ Skipped records: {skipped_count}")
