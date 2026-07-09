from fastapi import APIRouter

from src.auth.router import router as auth_router

v1_router = APIRouter(prefix="/api/v1")

v1_router.include_router(auth_router)
