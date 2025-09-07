# main.py
from fastapi import FastAPI
from app.crud.sewadar import router as sewadar_router
from app.crud.department import router as department_router

app = FastAPI(
    title="AMS API",
    description="API for managing Sewadars and Departments",
    version="1.0.0"
)

# ---------------- Include Routers ----------------
app.include_router(sewadar_router, prefix="/sewadar", tags=["Sewadar"])
app.include_router(department_router, prefix="/department", tags=["Department"])

# ---------------- Root Endpoint ----------------
@app.get("/")
def root():
    return {"message": "Welcome to AMS API. Visit /docs for API documentation."}
