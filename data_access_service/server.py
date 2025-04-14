from fastapi import FastAPI
from data_access_service.core.api import API
from data_access_service.core.routes import router as api_router

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    app.state.api_instance = API()


app.include_router(api_router)
