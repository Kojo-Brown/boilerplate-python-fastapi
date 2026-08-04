from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.oauth import oauth
from src.auth.schemas import (
    LoginRequest,
    OAuthUserInfo,
    RefreshRequest,
    RegisterRequest,
    TokenResponse,
    UserResponse,
)
from src.auth.service import AuthService
from src.database import get_db
from src.exceptions import BadRequestError
from src.limiter import limiter
from src.tasks import send_welcome_email_task

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post(
    "/register",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new user",
)
@limiter.limit("5/minute")
async def register(
    request: Request,
    data: RegisterRequest,
    db: AsyncSession = Depends(get_db),
) -> UserResponse:
    service = AuthService(db)
    user = await service.register(data)
    send_welcome_email_task.delay(user.email)
    return user


@router.post(
    "/login",
    response_model=TokenResponse,
    summary="Login and receive access + refresh tokens",
)
@limiter.limit("5/minute")
async def login(
    request: Request,
    data: LoginRequest,
    db: AsyncSession = Depends(get_db),
) -> TokenResponse:
    service = AuthService(db)
    return await service.login(data.email, data.password)


@router.post(
    "/refresh",
    response_model=TokenResponse,
    summary="Rotate refresh token and get new access + refresh tokens",
)
@limiter.limit("10/minute")
async def refresh(
    request: Request,
    data: RefreshRequest,
    db: AsyncSession = Depends(get_db),
) -> TokenResponse:
    service = AuthService(db)
    return await service.refresh(data.refresh_token)


@router.post(
    "/logout",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Revoke refresh token",
)
@limiter.limit("10/minute")
async def logout(
    request: Request,
    data: RefreshRequest,
    db: AsyncSession = Depends(get_db),
) -> None:
    service = AuthService(db)
    await service.logout(data.refresh_token)


@router.get(
    "/google",
    summary="Initiate Google OAuth 2.0 login",
)
@limiter.limit("10/minute")
async def google_login(request: Request) -> RedirectResponse:
    """Redirect the browser to Google's OAuth consent screen."""
    redirect_uri = str(request.url_for("google_callback"))
    response: RedirectResponse = await oauth.google.authorize_redirect(
        request, redirect_uri
    )
    return response


@router.get(
    "/google/callback",
    response_model=TokenResponse,
    name="google_callback",
    summary="Handle Google OAuth 2.0 callback and issue JWT tokens",
)
@limiter.limit("10/minute")
async def google_callback(
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> TokenResponse:
    """Exchange Google authorization code for JWT access and refresh tokens.

    Everything above ``AuthService`` here is transport: it turns whatever Google
    sent back into the ``(provider, sub, email)`` triple the service works in.
    Failures in that step genuinely are bad requests — the fault is in the
    callback the browser arrived with, not in the domain — which is why they are
    the only errors this module still raises for itself.
    """
    try:
        token = await oauth.google.authorize_access_token(request)
    except Exception as exc:
        raise BadRequestError(f"OAuth error: {exc}") from exc

    user_info_data = token.get("userinfo")
    if not user_info_data:
        raise BadRequestError("No user info returned from Google")

    try:
        user_info = OAuthUserInfo.model_validate(user_info_data)
    except ValueError as exc:
        raise BadRequestError("Invalid user info from Google") from exc

    service = AuthService(db)
    return await service.oauth_login("google", user_info.sub, str(user_info.email))
