#FastAPI app (API endpoints)
from fastapi import FastAPI
app = FastAPI(
    title = "Cafe Sales API",
    description = "API for Cafe Sales Data",
    version = "0.0.1"
)