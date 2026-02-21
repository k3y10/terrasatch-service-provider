from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.database import init_db
from app.routes import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="TerraSatch Service Provider",
    description="Central ingestion service for TerraSatch provider data.",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(router)
