import pytest
from pydantic import BaseModel

from src.pagination import (
    CursorPage,
    CursorParams,
    build_page,
    decode_cursor,
    encode_cursor,
)


class Item(BaseModel):
    id: int
    name: str


# ---------------------------------------------------------------------------
# encode / decode
# ---------------------------------------------------------------------------


def test_encode_cursor_roundtrip() -> None:
    value = "2024-01-01T00:00:00Z"
    assert decode_cursor(encode_cursor(value)) == value


def test_encode_cursor_is_url_safe() -> None:
    token = encode_cursor("some/value+with=special chars")
    assert "+" not in token
    assert "/" not in token
    assert "=" not in token or token.replace("=", "") == token.rstrip("=")


def test_decode_cursor_rejects_invalid_base64() -> None:
    with pytest.raises(Exception):
        decode_cursor("!!!not-base64!!!")


# ---------------------------------------------------------------------------
# CursorParams validation
# ---------------------------------------------------------------------------


def test_cursor_params_defaults() -> None:
    params = CursorParams()
    assert params.limit == 20
    assert params.cursor is None


def test_cursor_params_custom_limit() -> None:
    params = CursorParams(limit=50)
    assert params.limit == 50


def test_cursor_params_limit_min() -> None:
    with pytest.raises(Exception):
        CursorParams(limit=0)


def test_cursor_params_limit_max() -> None:
    with pytest.raises(Exception):
        CursorParams(limit=101)


def test_cursor_params_with_cursor() -> None:
    encoded = encode_cursor("42")
    params = CursorParams(limit=10, cursor=encoded)
    assert decode_cursor(params.cursor) == "42"  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# CursorPage model
# ---------------------------------------------------------------------------


def test_cursor_page_serialization() -> None:
    page: CursorPage[Item] = CursorPage(
        items=[Item(id=1, name="a"), Item(id=2, name="b")],
        next_cursor="abc",
        has_more=True,
    )
    data = page.model_dump()
    assert data["items"] == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    assert data["next_cursor"] == "abc"
    assert data["has_more"] is True


def test_cursor_page_empty() -> None:
    page: CursorPage[Item] = CursorPage(items=[])
    assert page.has_more is False
    assert page.next_cursor is None


# ---------------------------------------------------------------------------
# build_page
# ---------------------------------------------------------------------------


def _items(n: int) -> list[Item]:
    return [Item(id=i, name=f"item-{i}") for i in range(1, n + 1)]


def _cursor(item: Item) -> str:
    return str(item.id)


def test_build_page_no_next_page() -> None:
    """When fewer items than limit are returned, has_more is False."""
    rows = _items(5)
    page = build_page(rows, limit=10, get_cursor=_cursor)
    assert page.has_more is False
    assert page.next_cursor is None
    assert len(page.items) == 5


def test_build_page_exact_limit() -> None:
    """Exactly limit rows → no next page (no overflow sentinel)."""
    rows = _items(10)
    page = build_page(rows, limit=10, get_cursor=_cursor)
    assert page.has_more is False
    assert len(page.items) == 10


def test_build_page_has_more() -> None:
    """Caller provides limit+1 rows; build_page should detect the overflow."""
    limit = 5
    rows = _items(limit + 1)  # 6 rows for a page of 5
    page = build_page(rows, limit=limit, get_cursor=_cursor)

    assert page.has_more is True
    assert len(page.items) == limit
    # Cursor encodes the last *included* item (id=5)
    assert decode_cursor(page.next_cursor) == "5"  # type: ignore[arg-type]


def test_build_page_cursor_points_to_last_included_item() -> None:
    limit = 3
    rows = _items(limit + 1)
    page = build_page(rows, limit=limit, get_cursor=_cursor)

    last_id = page.items[-1].id
    assert decode_cursor(page.next_cursor) == str(last_id)  # type: ignore[arg-type]


def test_build_page_empty_input() -> None:
    page = build_page([], limit=10, get_cursor=_cursor)
    assert page.items == []
    assert page.has_more is False
    assert page.next_cursor is None


def test_build_page_generic_with_dict() -> None:
    """build_page is generic — works with plain dicts as well."""
    rows: list[dict[str, int]] = [{"id": i} for i in range(1, 7)]
    page: CursorPage[dict[str, int]] = build_page(
        rows, limit=5, get_cursor=lambda r: str(r["id"])
    )
    assert page.has_more is True
    assert len(page.items) == 5
    assert decode_cursor(page.next_cursor) == "5"  # type: ignore[arg-type]
