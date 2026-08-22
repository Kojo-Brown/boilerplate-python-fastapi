"""Request and response models for the presigned-upload routes.

Frozen, like every other schema in this application — see
`src/auth/schemas.py` for why a request model in particular has no business
being edited after validation.

`PresignedUploadResponse.fields` is where freezing a Pydantic model stops
short on its own, and is worth spelling out because the pattern recurs
wherever a model carries a container. `frozen=True` blocks
`response.fields = {}`; it does nothing about `response.fields["key"] = ...`,
because validation builds an ordinary dict and hands it over. These are the
form fields of an S3 POST policy, and the policy's signature covers them — a
field edited after signing produces an upload S3 rejects, at the browser,
with a message about a signature rather than about the line that changed it.
`FrozenDict` is what makes the field as frozen as the model claiming to hold
it.
"""

from pydantic import BaseModel, ConfigDict, Field

from src.immutable import FrozenDict


class PresignedUploadRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    filename: str = Field(..., min_length=1, max_length=255)
    content_type: str = Field(..., min_length=1, max_length=127)
    folder: str = Field(
        default="uploads",
        min_length=1,
        max_length=64,
        pattern=r"^[a-z0-9][a-z0-9_/-]*$",
    )


class PresignedUploadResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    key: str
    url: str
    fields: FrozenDict[str, str]
    expires_in: int


class PresignedDownloadRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    key: str = Field(..., min_length=1, max_length=1024)


class PresignedDownloadResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    url: str
    expires_in: int
