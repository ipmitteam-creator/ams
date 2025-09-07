import pandas as pd
import psycopg2

# ---------------- CONFIG ----------------
EXCEL_FILE = "sewadar_department_update.xlsx"  # your Excel file
DB_CONFIG = {
    "dbname": "ams",
    "user": "neondb_owner",
    "password": "npg_igo8fBOT3MtP",
    "host": "ep-orange-fog-a1qfrxr9-pooler.ap-southeast-1.aws.neon.tech",
    "port": "5432",
    "sslmode": "require"
}

# ---------------- DB Connection ----------------
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# ---------------- Utilities ----------------
def get_department_id(conn, department_name):
    cur = conn.cursor()
    cur.execute(
        "SELECT department_id FROM department WHERE LOWER(TRIM(department_name)) = LOWER(TRIM(%s))",
        (department_name,)
    )
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None

def update_sewadar_department(conn, badge_no, department_id):
    cur = conn.cursor()
    cur.execute("UPDATE sewadar SET department_id=%s WHERE badge_no=%s", (department_id, badge_no))
    updated = cur.rowcount
    cur.close()
    return updated

# ---------------- Main ----------------
def main():
    df = pd.read_excel(EXCEL_FILE)
    print(f"Loaded {len(df)} rows from Excel")

    conn = get_db_connection()
    updated_count = 0
    skipped_count = 0

    for idx, row in df.iterrows():
        badge_no = str(row["badge_no"]).strip()
        department_name = str(row["department_name"]).strip()

        dept_id = get_department_id(conn, department_name)
        if not dept_id:
            print(f"[SKIP] Department '{department_name}' not found for badge_no={badge_no}")
            skipped_count += 1
            continue

        updated = update_sewadar_department(conn, badge_no, dept_id)
        if updated:
            updated_count += 1
        else:
            print(f"[SKIP] badge_no={badge_no} not found in sewadar table")
            skipped_count += 1

    conn.commit()
    conn.close()
    print(f"Finished: {updated_count} rows updated, {skipped_count} rows skipped.")

if __name__ == "__main__":
    main()
