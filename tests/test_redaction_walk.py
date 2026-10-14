"""The walk: how deep redaction goes, and what it refuses to rewrite.

The rule these tests are really defending is that a log line with nothing
sensitive in it comes out exactly as it went in. Redaction that reshapes every
event — summarising objects, reordering containers, stringifying numbers — is a
change to every log this service writes, and it would be paid for on every line
in exchange for the handful that carry something.
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import Callable
from typing import Any

import pytest

from src.redaction.keys import KeyPolicy
from src.redaction.values import REDACTED
from src.redaction.walk import CIRCULAR, MAX_DEPTH, TOO_DEEP, UNRENDERABLE, Redactor


def redactor() -> Redactor:
    return Redactor(KeyPolicy())


def _in_a_list(value: Any) -> list[Any]:
    return [value]


def _in_a_dict(value: Any) -> dict[str, Any]:
    # A deliberately insensitive key: this nests, it does not test the policy.
    return {"inner": value}


class Unprintable:
    def __str__(self) -> str:
        raise RuntimeError("this object cannot be rendered")


class TestTopLevelFields:
    def test_a_sensitive_name_redacts_without_looking_at_the_value(self) -> None:
        assert redactor().field("password", {"old": "a", "new": "b"}) == REDACTED

    def test_an_ordinary_name_keeps_its_value(self) -> None:
        assert redactor().field("idempotency_key", "01JAB2") == "01JAB2"

    def test_a_string_value_is_scrubbed_by_shape(self) -> None:
        assert redactor().field("error", "rejected ada@example.com") == (
            f"rejected {REDACTED}"
        )


class TestScalars:
    def test_numbers_and_none_are_untouched(self) -> None:
        walk = redactor()
        assert walk.field("size", 4111111111111111) == 4111111111111111
        assert walk.field("ratio", 0.5) == 0.5
        assert walk.field("ok", True) is True
        assert walk.field("detail", None) is None


class TestContainers:
    def test_a_mapping_is_redacted_by_key_and_by_value(self) -> None:
        payload = {"email": "ada@example.com", "page": 2, "note": "ok"}
        assert redactor().field("request", payload) == {
            "email": REDACTED,
            "page": 2,
            "note": "ok",
        }

    def test_a_non_string_mapping_key_is_still_visited(self) -> None:
        assert redactor().field("by_id", {1: "ada@example.com"}) == {1: REDACTED}

    def test_a_list_stays_a_list_and_a_tuple_stays_a_tuple(self) -> None:
        walk = redactor()
        assert walk.field("a", ["x", "y"]) == ["x", "y"]
        assert walk.field("b", ("x", "y")) == ("x", "y")

    def test_a_set_becomes_a_list_so_collapsed_members_are_not_swallowed(
        self,
    ) -> None:
        result = redactor().field("tokens", {"a@example.com", "b@example.com"})
        assert result == [REDACTED, REDACTED]

    def test_nesting_is_followed(self) -> None:
        event = {"user": {"contact": {"email": "ada@example.com"}}}
        assert redactor().field("payload", event) == {
            "user": {"contact": {"email": REDACTED}}
        }

    def test_depth_is_bounded(self) -> None:
        deepest: Any = "ada@example.com"
        for _ in range(MAX_DEPTH + 2):
            deepest = [deepest]
        rendered = repr(redactor().field("nest", deepest))
        assert TOO_DEEP in rendered
        assert "ada@example.com" not in rendered

    @pytest.mark.parametrize("wrap", [_in_a_list, _in_a_dict], ids=["list", "dict"])
    @pytest.mark.parametrize(
        ("containers", "expected"),
        [(MAX_DEPTH, False), (MAX_DEPTH + 1, True)],
        ids=["at_the_limit", "one_past_it"],
    )
    def test_the_depth_limit_is_where_it_says_it_is(
        self,
        wrap: Callable[[Any], Any],
        containers: int,
        expected: bool,
    ) -> None:
        """Exactly `MAX_DEPTH` containers render; one more is truncated.

        `test_depth_is_bounded` above nests `MAX_DEPTH + 2` deep and only asks
        whether truncation happened *somewhere*, which every off-by-one in the
        descent also satisfies: counting from one instead of zero, comparing
        with `>` instead of `>=`, or stepping the depth by two all still
        truncate eventually, just at the wrong place — and truncating early is
        the expensive direction, because it silently drops the part of a log
        event somebody is reading.

        Pinning both sides of the boundary is what makes the constant mean
        something. Both container kinds are exercised because the descent is
        written twice, once in the mapping comprehension and once for
        sequences, so a wrong step in either is invisible from the other.
        """
        innermost = "plain-and-not-sensitive"
        value: Any = innermost
        for _ in range(containers):
            value = wrap(value)

        rendered = repr(redactor().field("nest", value))

        assert (TOO_DEEP in rendered) is expected
        assert (innermost in rendered) is not expected

    def test_a_sensitive_key_inside_a_container_is_redacted_by_its_name(
        self,
    ) -> None:
        """The value is innocuous, so only the key can explain the marker.

        `test_nesting_is_followed` nests an `email` key over an address, and
        both the key policy and the value policy would redact that on their
        own — so it passes whether or not the walk consults key names below the
        top level. This one puts a harmless value under a sensitive name, where
        the value policy has nothing to say and the key check is the only thing
        that can produce `REDACTED`.
        """
        event = {"user": {"password": "correct-horse-battery-staple"}}
        assert redactor().field("payload", event) == {"user": {"password": REDACTED}}

    def test_a_cycle_is_marked_rather_than_followed(self) -> None:
        looped: list[Any] = []
        looped.append(looped)
        assert redactor().field("loop", looped) == [CIRCULAR]

    def test_a_repeated_sibling_is_not_mistaken_for_a_cycle(self) -> None:
        shared = ["ok"]
        assert redactor().field("pair", [shared, shared]) == [["ok"], ["ok"]]


class TestForeignObjects:
    def test_a_benign_object_is_returned_as_itself(self) -> None:
        value = uuid.UUID("00000000-0000-4000-8000-000000000001")
        assert redactor().field("user_id", value) is value

    def test_a_datetime_is_returned_as_itself(self) -> None:
        value = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
        assert redactor().field("at", value) is value

    def test_an_object_whose_rendering_leaks_is_replaced_by_the_redaction(
        self,
    ) -> None:
        error = ValueError("rejected ada@example.com")
        assert redactor().field("error", error) == f"rejected {REDACTED}"

    def test_bytes_are_scanned_through_their_rendering(self) -> None:
        assert redactor().field("body", b"to=ada@example.com") == (f"b'to={REDACTED}'")

    def test_an_object_that_cannot_be_rendered_is_marked(self) -> None:
        assert redactor().field("thing", Unprintable()) == UNRENDERABLE
