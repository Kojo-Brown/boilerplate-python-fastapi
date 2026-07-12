from pydantic import BaseModel, Field


class PresignedUploadRequest(BaseModel):
    filename: str = Field(..., min_length=1, max_length=255)
    content_type: str = Field(..., min_length=1, max_length=127)
    folder: str = Field(
        default="uploads",
        min_length=1,
        max_length=64,
        pattern=r"^[a-z0-9][a-z0-9_/-]*$",
    )


class PresignedUploadResponse(BaseModel):
    key: str
    url: str
    fields: dict[str, str]
    expires_in: int


class PresignedDownloadRequest(BaseModel):
    key: str = Field(..., min_length=1, max_length=1024)


class PresignedDownloadResponse(BaseModel):
    url: str
    expires_in: int
