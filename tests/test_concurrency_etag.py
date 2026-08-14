"""Entity-tag parsing and comparison, against the grammar in RFC 9110.

These are the tests that would have caught every mistake the module's docstring
warns about: splitting the header on commas, comparing weak tags as though they
were strong, and treating an unparseable precondition as no precondition.
"""

from __future__ import annotations

import uuid

import pytest

from src.concurrency import (
    EntityTag,
    IfMatch,
    MalformedPreconditionError,
    resource_version_tag,
)
from src.exceptions import PreconditionFailedError, PreconditionRequiredError


class TestEntityTag:
    def test_serializes_strong_tag_in_quotes(self) -> None:
        assert EntityTag("abc").serialize() == '"abc"'

    def test_serializes_weak_tag_with_prefix(self) -> None:
        assert EntityTag("abc", weak=True).serialize() == 'W/"abc"'

    def test_empty_opaque_value_is_legal(self) -> None:
        # `opaque-tag = DQUOTE *etagc DQUOTE` — zero characters is a valid tag.
        assert EntityTag("").serialize() == '""'

    @pytest.mark.parametrize("value", ['has"quote', "has\nnewline", "has\x7fdel"])
    def test_rejects_values_the_grammar_forbids(self, value: str) -> None:
        with pytest.raises(ValueError, match="RFC 9110 forbids"):
            EntityTag(value)

    def test_strong_comparison_matches_equal_strong_tags(self) -> None:
        assert EntityTag("7").strongly_matches(EntityTag("7"))

    def test_strong_comparison_rejects_different_values(self) -> None:
        assert not EntityTag("7").strongly_matches(EntityTag("8"))

    @pytest.mark.parametrize(
        ("left", "right"),
        [
            (EntityTag("7", weak=True), EntityTag("7")),
            (EntityTag("7"), EntityTag("7", weak=True)),
            (EntityTag("7", weak=True), EntityTag("7", weak=True)),
        ],
    )
    def test_strong_comparison_rejects_any_weak_participant(
        self, left: EntityTag, right: EntityTag
    ) -> None:
        assert not left.strongly_matches(right)


class TestResourceVersionTag:
    def test_combines_identifier_and_version(self) -> None:
        resource_id = uuid.UUID("11111111-1111-4111-8111-111111111111")
        assert resource_version_tag(resource_id, 3).value == f"{resource_id}.3"

    def test_same_version_of_different_rows_does_not_collide(self) -> None:
        """The reason the id is in the tag at all.

        Every row is at version 1 when it is created, so a tag built from the
        version alone would compare equal across users — and `/users/me` is one
        URI naming a different row per bearer token.
        """
        first = resource_version_tag(uuid.uuid4(), 1)
        second = resource_version_tag(uuid.uuid4(), 1)
        assert not first.strongly_matches(second)

    def test_result_is_strong(self) -> None:
        assert resource_version_tag(uuid.uuid4(), 1).weak is False


class TestIfMatchParsing:
    def test_absent_header(self) -> None:
        parsed = IfMatch.parse(None)
        assert parsed.present is False
        assert parsed.wildcard is False
        assert parsed.tags == ()

    def test_wildcard(self) -> None:
        parsed = IfMatch.parse("*")
        assert parsed.present is True
        assert parsed.wildcard is True

    def test_wildcard_tolerates_surrounding_whitespace(self) -> None:
        assert IfMatch.parse("  *  ").wildcard is True

    def test_single_strong_tag(self) -> None:
        assert IfMatch.parse('"7"').tags == (EntityTag("7"),)

    def test_weak_tag_is_parsed_as_weak_rather_than_rejected(self) -> None:
        """Parsing and evaluation are separate concerns.

        `W/"7"` is a syntactically valid entity tag, so it parses; it fails
        later, at comparison time, which is where RFC 9110 puts the rule.
        """
        assert IfMatch.parse('W/"7"').tags == (EntityTag("7", weak=True),)

    def test_list_of_tags(self) -> None:
        assert IfMatch.parse('"a", "b" ,"c"').tags == (
            EntityTag("a"),
            EntityTag("b"),
            EntityTag("c"),
        )

    def test_comma_inside_an_opaque_tag_is_not_a_separator(self) -> None:
        """The bug that `header.split(",")` produces."""
        assert IfMatch.parse('"a,b"').tags == (EntityTag("a,b"),)

    def test_mixed_weak_and_strong_list(self) -> None:
        assert IfMatch.parse('W/"a", "b"').tags == (
            EntityTag("a", weak=True),
            EntityTag("b"),
        )

    @pytest.mark.parametrize("raw", ['"a",,"b"', ', "a"', '"a", '])
    def test_empty_list_elements_are_tolerated(self, raw: str) -> None:
        # RFC 9110 §5.6.1.2: recipients parse and ignore empty list elements.
        assert EntityTag("a") in IfMatch.parse(raw).tags

    def test_obs_text_is_accepted(self) -> None:
        assert IfMatch.parse('"caf\xe9"').tags == (EntityTag("caf\xe9"),)

    @pytest.mark.parametrize(
        "raw",
        [
            "7",  # unquoted
            '"unterminated',
            '"a" "b"',  # missing comma
            "**",
            '*, "a"',  # the grammar is "*" *or* a list, never both
            "",
            ",",
            "  ",
            'w/"a"',  # `weak` is case-sensitive: %s"W/"
        ],
    )
    def test_malformed_headers_are_rejected(self, raw: str) -> None:
        with pytest.raises(MalformedPreconditionError) as excinfo:
            IfMatch.parse(raw)
        assert excinfo.value.status_code == 400
        assert excinfo.value.error_code == "MALFORMED_PRECONDITION"


class TestIfMatchEvaluation:
    current = resource_version_tag("11111111-1111-4111-8111-111111111111", 4)

    def test_matching_tag_passes(self) -> None:
        IfMatch.parse(self.current.serialize()).require_match(self.current)

    def test_wildcard_passes(self) -> None:
        IfMatch.parse("*").require_match(self.current)

    def test_any_tag_in_the_list_may_match(self) -> None:
        header = f'"nope", {self.current.serialize()}'
        IfMatch.parse(header).require_match(self.current)

    def test_absent_header_raises_428(self) -> None:
        with pytest.raises(PreconditionRequiredError) as excinfo:
            IfMatch.absent().require_match(self.current)
        assert excinfo.value.status_code == 428
        assert excinfo.value.error_code == "PRECONDITION_REQUIRED"

    def test_stale_tag_raises_412_carrying_the_current_tag(self) -> None:
        stale = resource_version_tag("11111111-1111-4111-8111-111111111111", 3)
        with pytest.raises(PreconditionFailedError) as excinfo:
            IfMatch.parse(stale.serialize()).require_match(self.current)
        assert excinfo.value.status_code == 412
        assert excinfo.value.headers == {"ETag": self.current.serialize()}

    def test_weak_tag_never_satisfies_if_match(self) -> None:
        """RFC 9110 §13.1.1 mandates strong comparison for If-Match."""
        weak = EntityTag(self.current.value, weak=True)
        with pytest.raises(PreconditionFailedError):
            IfMatch.parse(weak.serialize()).require_match(self.current)

    def test_matches_is_available_without_the_raising_wrapper(self) -> None:
        assert IfMatch.parse(self.current.serialize()).matches(self.current)
        assert not IfMatch.parse('"other"').matches(self.current)
