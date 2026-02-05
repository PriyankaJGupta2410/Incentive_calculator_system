import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # adjust for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

########################## IMPORT ROUTES #################
from routes.data_ingestion import data_ingestion_router
from routes.calculator import calculator_router
from routes.results import results_router

################### Declar Routes ####################
app.include_router(data_ingestion_router, prefix="/data-ingestion")
app.include_router(calculator_router, prefix="/calculator")
app.include_router(results_router, prefix="/results")

@app.get("/")
async def index():
   return {"message": "Hello World"}

if __name__ == "__main__":
   uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)