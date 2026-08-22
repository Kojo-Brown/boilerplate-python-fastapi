import base64
from collections.abc import Callable

from pydantic import BaseModel, ConfigDict, Field


class CursorPage[T](BaseModel):
    """Generic cursor-paginated response envelope.

    `items` is a tuple rather than a list, which is the difference between a
    frozen model and an immutable one: `frozen=True` refuses `page.items = []`
    and has nothing to say about `page.items.append(...)`. A page is a
    *snapshot* — `next_cursor` was computed from its last element — so an
    appended item leaves the envelope describing a page it no longer contains.
    JSON has one array type, so the serialised body is unchanged.
    """

    model_config = ConfigDict(frozen=True)

    items: tuple[T, ...]
    next_cursor: str | None = None
    has_more: bool = False


class CursorParams(BaseModel):
    """Query parameters for cursor-based pagination."""

    model_config = ConfigDict(frozen=True)

    limit: int = Field(default=20, ge=1, le=100)
    cursor: str | None = None


def encode_cursor(value: str) -> str:
    """Base64url-encode an opaque cursor value."""
    return base64.urlsafe_b64encode(value.encode()).decode()


def decode_cursor(cursor: str) -> str:
    """Base64url-decode a cursor value produced by encode_cursor."""
    return base64.urlsafe_b64decode(cursor.encode()).decode()


def build_page[T](
    items: list[T],
    limit: int,
    get_cursor: Callable[[T], str],
) -> CursorPage[T]:
    """Build a CursorPage from a result set fetched with limit+1 rows.

    Callers should query `limit + 1` rows from the database.  This function
    slices the list to `limit`, detects whether a next page exists, and
    encodes the cursor from the last *included* item so the client can pass
    it back as ``cursor`` on the next request.
    """
    has_more = len(items) > limit
    page_items = tuple(items[:limit] if has_more else items)
    next_cursor: str | None = (
        encode_cursor(get_cursor(page_items[-1])) if has_more and page_items else None
    )
    return CursorPage(items=page_items, next_cursor=next_cursor, has_more=has_more)
