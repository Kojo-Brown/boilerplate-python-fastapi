from fastapi import APIRouter

from src.api.v1.uploads import router as uploads_router
from src.auth.router import router as auth_router

v1_router = APIRouter(prefix="/api/v1")

v1_router.include_router(auth_router)
v1_router.include_router(uploads_router)
