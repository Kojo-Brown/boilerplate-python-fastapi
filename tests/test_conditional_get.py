"""`If-None-Match` parsing and the weak comparison function, per RFC 9110.

`If-Match` already exists in `src/concurrency/etag.py`, and the most expensive
mistake available while adding its sibling is to reuse its comparison
function. The two headers deliberately use different ones (§8.8.3.2):
`If-Match` guards a write and compares strongly, `If-None-Match` guards a read
and compares weakly. A module that compared strongly here would pass every
test a suite full of strong tags could write, and would then re-send an
unchanged representation on every request the moment anything started emitting
a weak tag — a cache that silently stops working, which is the kind of bug that
shows up as a bandwidth bill rather than as an error.

The second expensive mistake is `header.split(",")`: a comma is a legal
`etagc`, so `If-None-Match: "a,b"` is one tag. There is a case for that below
because the scanner it forces is the entire reason this is not a one-liner.
"""

from __future__ import annotations

import pytest

from src.concurrency import EntityTag, MalformedPreconditionError
from src.concurrency.conditional import IfNoneMatch


class TestWeakComparison:
    """§8.8.3.2: values equal, and the weak flag on either side is ignored."""

    def test_matches_equal_strong_tags(self) -> None:
        assert EntityTag("7").weakly_matches(EntityTag("7"))

    @pytest.mark.parametrize(
        ("left", "right"),
        [
            (EntityTag("7", weak=True), EntityTag("7")),
            (EntityTag("7"), EntityTag("7", weak=True)),
            (EntityTag("7", weak=True), EntityTag("7", weak=True)),
        ],
    )
    def test_ignores_weakness_on_either_side(
        self, left: EntityTag, right: EntityTag
    ) -> None:
        """Exactly the three pairs `strongly_matches` refuses."""
        assert left.weakly_matches(right)

    def test_rejects_different_values(self) -> None:
        assert not EntityTag("7").weakly_matches(EntityTag("8"))

    def test_rejects_different_values_even_when_both_are_weak(self) -> None:
        assert not EntityTag("7", weak=True).weakly_matches(EntityTag("8", weak=True))


class TestIfNoneMatchParsing:
    def test_a_missing_header_is_recorded_as_absent(self) -> None:
        parsed = IfNoneMatch.parse(None)

        assert parsed.present is False
        assert parsed.wildcard is False
        assert parsed.tags == ()

    def test_wildcard(self) -> None:
        parsed = IfNoneMatch.parse("*")

        assert parsed.present is True
        assert parsed.wildcard is True

    def test_wildcard_tolerates_surrounding_whitespace(self) -> None:
        assert IfNoneMatch.parse("  *  ").wildcard is True

    def test_a_single_strong_tag(self) -> None:
        assert IfNoneMatch.parse('"7"').tags == (EntityTag("7"),)

    def test_a_list_of_tags(self) -> None:
        assert IfNoneMatch.parse('"7", "8"').tags == (
            EntityTag("7"),
            EntityTag("8"),
        )

    def test_a_weak_tag_keeps_its_weakness(self) -> None:
        """Parsing must not normalise it away: the client said what it said."""
        assert IfNoneMatch.parse('W/"7"').tags == (EntityTag("7", weak=True),)

    def test_a_quoted_comma_is_one_tag_and_not_two(self) -> None:
        """`etagc` admits `%x2C`, so splitting on commas misreads this field."""
        assert IfNoneMatch.parse('"a,b"').tags == (EntityTag("a,b"),)

    def test_tolerates_whitespace_around_list_elements(self) -> None:
        assert IfNoneMatch.parse('  "7" ,  "8"  ').tags == (
            EntityTag("7"),
            EntityTag("8"),
        )

    def test_tolerates_an_empty_list_element(self) -> None:
        """§5.6.1.2: recipients tolerate holes a client left while joining."""
        assert IfNoneMatch.parse('"7", , "8"').tags == (
            EntityTag("7"),
            EntityTag("8"),
        )

    @pytest.mark.parametrize("raw", ["7", "notatag", '"unterminated', 'W"7"'])
    def test_a_malformed_field_is_refused_rather_than_ignored(self, raw: str) -> None:
        with pytest.raises(MalformedPreconditionError):
            IfNoneMatch.parse(raw)

    def test_a_list_of_nothing_is_refused(self) -> None:
        """A field listing zero tags cannot be satisfied and is a client bug."""
        with pytest.raises(MalformedPreconditionError):
            IfNoneMatch.parse(",")


class TestIfNoneMatchMatching:
    def test_a_wildcard_names_any_existing_representation(self) -> None:
        assert IfNoneMatch.parse("*").matches(EntityTag("7"))

    def test_an_absent_field_names_nothing(self) -> None:
        assert not IfNoneMatch.absent().matches(EntityTag("7"))

    def test_a_listed_tag_matches(self) -> None:
        assert IfNoneMatch.parse('"6", "7"').matches(EntityTag("7"))

    def test_an_unlisted_tag_does_not_match(self) -> None:
        assert not IfNoneMatch.parse('"6", "8"').matches(EntityTag("7"))

    def test_a_weak_field_tag_matches_a_strong_current_tag(self) -> None:
        """The whole point of the weak comparison function being the one here."""
        assert IfNoneMatch.parse('W/"7"').matches(EntityTag("7"))
