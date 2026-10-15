"""Properties of cursor pagination — the ones a page of examples cannot state.

`tests/test_pagination.py` covers the cases somebody thought of: five rows for
a page of ten, exactly ten, eleven, zero. Those are the right examples and they
are still there. What they cannot say is *for every*, and pagination is a place
where the difference is load-bearing in two directions.

**One page is not the contract.** Every assertion in the example suite is about
a single `build_page` call, and the bug that pagination actually has is not
visible from inside one: `>` becoming `>=` on the overflow check, or a cursor
taken from the row *after* the last included one, produces a perfectly
well-formed page whose only fault is the row it skips on the way to the next
one. `test_a_full_traversal_visits_every_row_exactly_once` drives the loop a
client would — fetch `limit + 1`, follow `next_cursor`, repeat — against a
keyset reader, and asserts the whole sequence. That is the requirement; a page
is an implementation detail of it.

**The interesting inputs are the ones nobody types.** The cursor codec is
asserted here over arbitrary text, which is how the empty string, a lone
newline, an astral-plane emoji and a value whose base64 needs two characters of
padding get into the suite without anybody choosing them. Hypothesis shrinks a
counterexample to its smallest form before reporting, so a failure arrives as
the one-character input that reproduces it rather than as the forty-character
one that happened to be generated.

The limit is drawn from `1..100` throughout, which is `CursorParams.limit`'s own
`ge=1, le=100`. `build_page` takes a bare `int` and would do something defensible
with `limit=0` — an empty page claiming `has_more` — but no request can produce
one, so asserting a shape for it would be pinning down behaviour that is
unreachable rather than specified. The property that holds that door shut is
`test_cursor_params_accepts_exactly_the_documented_range`, and it is stated
over *all* integers.
"""

from __future__ import annotations

from collections.abc import Callable
from urllib.parse import parse_qs, urlencode

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import BaseModel, ConfigDict, ValidationError

from src.pagination import (
    CursorPage,
    CursorParams,
    build_page,
    decode_cursor,
    encode_cursor,
)

#: `CursorParams.limit`'s own bounds. A page size outside them cannot reach
#: `build_page` through the API, so every property below is stated inside them.
MIN_LIMIT = 1
MAX_LIMIT = 100

#: The base64url alphabet plus its padding character — RFC 4648 §5. Spelled out
#: rather than imported so the assertion is independent of what `base64` thinks.
_BASE64URL = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_="
)


class Row(BaseModel):
    """A paginated row. Frozen, because `CursorPage.items` is a snapshot."""

    model_config = ConfigDict(frozen=True)

    id: int
    name: str


def _cursor_of(row: Row) -> str:
    return str(row.id)


limits = st.integers(min_value=MIN_LIMIT, max_value=MAX_LIMIT)

#: Page sizes small enough that a traversal of a bounded dataset takes a
#: handful of pages rather than one. A limit of 100 over 60 rows exercises the
#: single-page case and nothing else, and the single-page case is the one the
#: example suite already covers.
traversal_limits = st.integers(min_value=MIN_LIMIT, max_value=10)


@st.composite
def row_lists(draw: st.DrawFn, *, min_size: int = 0, max_size: int = 40) -> list[Row]:
    """Rows with distinct ids, ordered by id, as a keyset query returns them."""
    ids = draw(
        st.lists(
            st.integers(min_value=-(10**6), max_value=10**6),
            unique=True,
            min_size=min_size,
            max_size=max_size,
        )
    )
    return [Row(id=i, name=f"row-{i}") for i in sorted(ids)]


@st.composite
def overflowing_result_sets(
    draw: st.DrawFn, *, max_size: int = 40
) -> tuple[list[Row], int]:
    """Rows and a limit they overflow: `len(rows) > limit`, always.

    Drawn as a pair rather than filtered with `assume(page.has_more)`. The two
    are not independent — most limits overflow almost no result set — so
    filtering would discard the great majority of what it generated, which
    Hypothesis reports as a health check and is right to: a property that
    reaches its own precondition one example in ten is a property with a tenth
    of the examples it claims.
    """
    rows = draw(row_lists(min_size=2, max_size=max_size))
    limit = draw(st.integers(min_value=MIN_LIMIT, max_value=len(rows) - 1))
    return rows, limit


class TestCursorCodec:
    @given(value=st.text())
    def test_a_cursor_round_trips_whatever_it_was_built_from(self, value: str) -> None:
        assert decode_cursor(encode_cursor(value)) == value

    @given(value=st.text())
    def test_an_encoded_cursor_is_base64url_and_nothing_else(self, value: str) -> None:
        # The reason the codec is `urlsafe_b64encode` and not `b64encode`: a
        # `+` in a query string is a space by the time the server reads it, and
        # a `/` in a path segment is a new segment. Either corrupts the cursor
        # on the way back without anything noticing.
        assert set(encode_cursor(value)) <= _BASE64URL

    @given(value=st.text())
    def test_a_cursor_survives_a_round_trip_through_a_query_string(
        self, value: str
    ) -> None:
        token = encode_cursor(value)
        # `keep_blank_values`, because an empty cursor encodes to an empty
        # token and `parse_qs` drops a blank value by default — which is a
        # fact about the parser in this test, not about the codec.
        query = urlencode({"cursor": token})
        echoed = parse_qs(query, keep_blank_values=True)["cursor"][0]
        assert echoed == token
        assert decode_cursor(echoed) == value


class TestCursorParams:
    @given(limit=st.integers())
    def test_cursor_params_accepts_exactly_the_documented_range(
        self, limit: int
    ) -> None:
        if MIN_LIMIT <= limit <= MAX_LIMIT:
            assert CursorParams(limit=limit).limit == limit
        else:
            with pytest.raises(ValidationError):
                CursorParams(limit=limit)

    @given(cursor=st.text())
    def test_an_opaque_cursor_is_carried_through_unexamined(self, cursor: str) -> None:
        # `cursor` is not validated as base64 at the edge, deliberately: the
        # 400 for a corrupt cursor belongs to whatever decodes it, which knows
        # what the decoded value was supposed to mean.
        assert CursorParams(cursor=cursor).cursor == cursor


class TestBuildPage:
    @given(rows=row_lists(), limit=limits)
    def test_a_page_is_the_leading_prefix_of_the_rows_it_was_given(
        self, rows: list[Row], limit: int
    ) -> None:
        # Not just "the right length": the right rows, in the order they
        # arrived. A slice that reorders or takes from the tail would satisfy a
        # length assertion and silently reshuffle a client's pagination.
        assert build_page(rows, limit=limit, get_cursor=_cursor_of).items == tuple(
            rows[:limit]
        )

    @given(rows=row_lists(), limit=limits)
    def test_a_page_holds_as_many_rows_as_are_available_up_to_the_limit(
        self, rows: list[Row], limit: int
    ) -> None:
        page = build_page(rows, limit=limit, get_cursor=_cursor_of)
        assert len(page.items) == min(len(rows), limit)

    @given(rows=row_lists(), limit=limits)
    def test_has_more_is_exactly_the_overflow_sentinel(
        self, rows: list[Row], limit: int
    ) -> None:
        # The caller fetched `limit + 1` rows; the extra one is the sentinel.
        # `>=` here is the off-by-one that reports a next page that does not
        # exist, and a client that follows it gets an empty page back.
        page = build_page(rows, limit=limit, get_cursor=_cursor_of)
        assert page.has_more is (len(rows) > limit)

    @given(rows=row_lists(), limit=limits)
    def test_a_next_cursor_exists_exactly_when_a_next_page_does(
        self, rows: list[Row], limit: int
    ) -> None:
        page = build_page(rows, limit=limit, get_cursor=_cursor_of)
        assert (page.next_cursor is not None) is page.has_more

    @given(result_set=overflowing_result_sets())
    def test_the_next_cursor_names_the_last_included_row(
        self, result_set: tuple[list[Row], int]
    ) -> None:
        rows, limit = result_set
        page = build_page(rows, limit=limit, get_cursor=_cursor_of)
        assert page.next_cursor is not None
        # The last *included* row, not the sentinel. Taking the cursor from the
        # sentinel skips a row on the next page; taking it from `rows[limit]`
        # when `has_more` is false reads past the end.
        assert decode_cursor(page.next_cursor) == _cursor_of(page.items[-1])
        assert _cursor_of(page.items[-1]) == _cursor_of(rows[limit - 1])

    @given(rows=row_lists(), limit=limits)
    def test_the_cursor_is_computed_once_and_only_for_the_row_it_names(
        self, rows: list[Row], limit: int
    ) -> None:
        # A contract about cost, not about output. `get_cursor` is the caller's
        # function — in production it reads a column and formats a timestamp —
        # and a `build_page` that mapped it over the page would charge a page's
        # worth of that per request to produce one string.
        calls: list[Row] = []

        def counting_cursor(row: Row) -> str:
            calls.append(row)
            return _cursor_of(row)

        page = build_page(rows, limit=limit, get_cursor=counting_cursor)
        assert calls == ([page.items[-1]] if page.has_more else [])

    @given(rows=row_lists(), limit=limits)
    def test_a_page_round_trips_through_json_as_an_array(
        self, rows: list[Row], limit: int
    ) -> None:
        built = build_page(rows, limit=limit, get_cursor=_cursor_of)
        page: CursorPage[Row] = CursorPage[Row](
            items=built.items,
            next_cursor=built.next_cursor,
            has_more=built.has_more,
        )
        # `items` is a tuple in Python and an array in JSON, and validating the
        # body back gives a tuple again. A client cannot tell, which is the
        # point: the immutability is this process's, not the protocol's.
        assert '"items":[' in page.model_dump_json()
        assert CursorPage[Row].model_validate_json(page.model_dump_json()) == page


class TestTraversal:
    """The property that spans pages, which is where the real bugs are."""

    @staticmethod
    def _keyset_window(rows: list[Row], after: int | None, limit: int) -> list[Row]:
        """What `WHERE id > :after ORDER BY id LIMIT :limit + 1` returns.

        Keyset rather than `OFFSET`, matching what the cursor is for: the
        cursor names a row, and the next query asks for what sorts after it.
        """
        remaining = [row for row in rows if after is None or row.id > after]
        return remaining[: limit + 1]

    @given(rows=row_lists(), limit=traversal_limits)
    def test_a_full_traversal_visits_every_row_exactly_once(
        self, rows: list[Row], limit: int
    ) -> None:
        seen: list[Row] = []
        cursor: str | None = None
        pages = 0
        # A traversal of n rows at `limit` per page is at most
        # `ceil(n / limit)` pages, plus one for the empty-dataset case. A
        # cursor that fails to advance loops forever instead of failing, so the
        # bound is an assertion rather than a `while` condition: an infinite
        # loop in a property test is a hung CI job with no counterexample in it.
        budget = -(-len(rows) // limit) + 1

        while True:
            after = int(decode_cursor(cursor)) if cursor is not None else None
            window = self._keyset_window(rows, after, limit)
            page = build_page(window, limit=limit, get_cursor=_cursor_of)
            seen.extend(page.items)
            pages += 1
            assert pages <= budget, f"traversal did not terminate within {budget} pages"
            if not page.has_more:
                break
            cursor = page.next_cursor

        # Every row, once, in order — and nothing invented along the way.
        assert seen == rows

    @given(result_set=overflowing_result_sets(max_size=12))
    def test_following_a_cursor_never_returns_a_row_already_seen(
        self, result_set: tuple[list[Row], int]
    ) -> None:
        # Stated separately from the traversal above because it is the half a
        # client notices first: a duplicate row is a duplicate row in a UI,
        # whereas a dropped one is invisible until somebody counts.
        rows, limit = result_set
        first = build_page(
            self._keyset_window(rows, None, limit), limit=limit, get_cursor=_cursor_of
        )
        assert first.next_cursor is not None
        after = int(decode_cursor(first.next_cursor))
        second = build_page(
            self._keyset_window(rows, after, limit), limit=limit, get_cursor=_cursor_of
        )
        assert not set(first.items) & set(second.items)
        # And the join is seamless: the second page starts at the row directly
        # after the first page's last.
        assert second.items[0] == rows[len(first.items)]


class TestGenericity:
    @given(
        values=st.lists(st.integers(), unique=True, max_size=30),
        limit=limits,
    )
    def test_build_page_is_generic_over_anything_with_a_cursor(
        self, values: list[int], limit: int
    ) -> None:
        # `build_page` is declared over a bare `T` with a `Callable[[T], str]`
        # beside it, so the envelope has to hold a type that is not a Pydantic
        # model at all — `CursorPage[T]` validating `items` against an
        # unparameterised `T` is the part that could quietly stop being true.
        get_cursor: Callable[[int], str] = str
        page = build_page(values, limit=limit, get_cursor=get_cursor)
        assert page.items == tuple(values[:limit])
        assert page.has_more is (len(values) > limit)
