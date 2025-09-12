from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.crud.sewadar import router as sewadar_router
from app.crud.department import router as department_router
from app.crud.attendance import router as attendance_router
from app.crud.report import router as report_router  # 👈 NEW

app = FastAPI(
    title="AMS API",
    description="API for managing Sewadars, Departments, and Attendance",
    version="1.0.0"
)

# ---------------- Enable CORS ----------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],        # ✅ Allow all origins (change to specific domains in production)
    allow_credentials=True,
    allow_methods=["*"],        # ✅ Allow all HTTP methods (GET, POST, PUT, DELETE, etc.)
    allow_headers=["*"],        # ✅ Allow all headers
)

# ---------------- Include Routers ----------------
app.include_router(sewadar_router, prefix="/sewadar", tags=["Sewadar"])
app.include_router(department_router, prefix="/department", tags=["Department"])
app.include_router(attendance_router, prefix="/attendance", tags=["Attendance"])
app.include_router(report_router, prefix="/attendance", tags=["Reports"])  # 👈 NEW

# ---------------- Root Endpoint ----------------
@app.get("/")
def root():
    return {"message": "Welcome to AMS API. Visit /docs for API documentation."}
