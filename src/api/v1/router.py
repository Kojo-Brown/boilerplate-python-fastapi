from fastapi import APIRouter

from src.api.v1.events import router as events_router
from src.api.v1.exports import router as exports_router
from src.api.v1.uploads import router as uploads_router
from src.api.v1.users import router as users_router
from src.api.v1.ws import router as ws_router
from src.auth.router import router as auth_router

v1_router = APIRouter(prefix="/api/v1")

v1_router.include_router(auth_router)
v1_router.include_router(events_router)
v1_router.include_router(exports_router)
v1_router.include_router(uploads_router)
v1_router.include_router(users_router)
v1_router.include_router(ws_router)
