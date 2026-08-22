"""Request and response models for the authentication routes.

Every model here is `frozen=True`. A request model is the *record of what the
client sent*, and a handler that edits one destroys the only copy of that: the
raw body is long gone by the time the handler runs, so a normalisation applied
in place leaves nothing able to say what actually arrived. Rebinding to a new
model via `model_copy(update=...)` says so in the code instead.

Freezing also makes these hashable, which is what lets one be used as a cache
key or put in a set — a mutable model in either is a bug waiting for the first
mutation.
"""

import uuid

from pydantic import BaseModel, ConfigDict, EmailStr, Field


class RegisterRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    email: EmailStr
    password: str = Field(min_length=8, max_length=128)


class LoginRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    email: EmailStr
    password: str


class RefreshRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    refresh_token: str


class TokenResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class UserResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, frozen=True)

    id: uuid.UUID
    email: str
    role: str
    is_active: bool
    is_verified: bool


class OAuthUserInfo(BaseModel):
    model_config = ConfigDict(frozen=True)

    sub: str
    email: EmailStr
    name: str | None = None
    picture: str | None = None
    email_verified: bool = False
