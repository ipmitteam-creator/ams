# main.py
from fastapi import FastAPI
from app.crud import sewadar, department

app = FastAPI(title="AMS Sewadar Management API", version="1.0")

# Include routers from modules
app.include_router(sewadar.router, prefix="", tags=["Sewadar"])
app.include_router(department.router, prefix="", tags=["Department"])

# Optional root endpoint
@app.get("/")
def root():
    return {"message": "Welcome to AMS Sewadar Management API"}
