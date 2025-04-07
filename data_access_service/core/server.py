from fastapi import FastAPI
from data_access_service.core.routes import router as api_router

app = FastAPI()
app.include_router(api_router)