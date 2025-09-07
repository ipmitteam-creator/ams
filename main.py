# main.py
from fastapi import FastAPI
from app.crud.sewadar import router as sewadar_router
from app.crud.department import router as department_router
from app.crud.attendance import router as attendance_router   # 👈 NEW

app = FastAPI(
    title="AMS API",
    description="API for managing Sewadars, Departments, and Attendance",
    version="1.0.0"
)

# ---------------- Include Routers ----------------
app.include_router(sewadar_router, prefix="/sewadar", tags=["Sewadar"])
app.include_router(department_router, prefix="/department", tags=["Department"])
app.include_router(attendance_router, prefix="/attendance", tags=["Attendance"])  # 👈 NEW

# ---------------- Root Endpoint ----------------
@app.get("/")
def root():
    return {"message": "Welcome to AMS API. Visit /docs for API documentation."}

