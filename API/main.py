from fastapi import FastAPI
from API.routers.dados import router

app = FastAPI(title="PyPAH API")

app.include_router(router, prefix="/api")

@app.get("/health")
def health():
    return {"status": "ok"}