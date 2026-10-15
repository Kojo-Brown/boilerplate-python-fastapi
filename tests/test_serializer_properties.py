"""Properties of the request and response schemas.

Every schema in `src/` is a Pydantic model, which means two things are true of
it that no individual test says: it is the *published contract* for a body on
the wire, and it is the only place a constraint on that body is written down.
Both are properties over all inputs, and both are what this file asserts.

**A response is a value that survives the wire.** The round-trip property
takes an arbitrary instance of every schema this application serialises and
asserts that encoding it and validating the result back gives the same object.
That is the one assertion a response model actually owes: a field with a
serialiser that loses precision, a `datetime` that comes back naive, a
`FrozenDict` that validates into something else — each is a client reading a
value the server did not send, and each is invisible to a test that only checks
a status code. The companion property over `model_dump()` covers the Python
side, which is the path the streaming export and the event payloads take.

**A constraint is a boundary, and a boundary needs both of its sides.** The
example suites assert that a valid body is accepted and an obviously invalid
one is refused. What they do not say is that the line between them is where the
schema claims it is — and the interesting inputs sit directly on it. So the
constraint properties here are stated as equivalences:
*accepted exactly when* the documented predicate holds. A `max_length` that is
off by one, a `pattern` anchored so that a trailing newline slips through, a
validator whose two clauses are `or` where the prose says `and`: each of those
makes the equivalence false in one direction, and a one-sided test cannot see
either.

The lengths are drawn through `sized_text`, which generates the *length* and
then the text, for a reason worth stating: `st.text(max_size=140)` concentrates
on short strings, so it would reach a 128-character boundary somewhere around
never. Generating the length uniformly puts the boundary in the sample on
roughly one example in a hundred and forty, and shrinking then reports it as
exactly `"a" * 129`.

What this file deliberately does not restate: that the schemas are frozen, or
that `src/` contains no mutable value object. That is
`tests/test_immutability_gate.py`, which asserts it over the module tree rather
than over generated instances, and is the stronger statement of the two — a
model this file never names is still covered there.
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import UTC
from types import SimpleNamespace
from typing import Final

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st
from pydantic import BaseModel, ValidationError

from src.auth.schemas import (
    LoginRequest,
    OAuthUserInfo,
    RefreshRequest,
    RegisterRequest,
    TokenResponse,
    UserResponse,
)
from src.immutable import FrozenDict
from src.notifications.registry import NotificationStrategyRegistry
from src.storage.schemas import (
    PresignedDownloadRequest,
    PresignedDownloadResponse,
    PresignedUploadRequest,
    PresignedUploadResponse,
)
from src.users.export import UserExportRecord
from src.users.schemas import (
    _ALLOWED_WEBHOOK_SCHEMES,
    _WEBHOOK_URL_MAX_LENGTH,
    ProfileUpdateRequest,
    UserProfileResponse,
)

# The bounds the schemas declare, spelled out rather than imported, so that a
# property states the requirement instead of restating the implementation. The
# one private constant imported above is reconciled with its literal in
# `TestTheBoundsAreTheOnesTheSchemasDeclare`.
PASSWORD_MIN: Final = 8
PASSWORD_MAX: Final = 128
FILENAME_MAX: Final = 255
CONTENT_TYPE_MAX: Final = 127
FOLDER_MAX: Final = 64
OBJECT_KEY_MAX: Final = 1024
WEBHOOK_URL_MAX: Final = 2048

#: `PresignedUploadRequest.folder`'s pattern, without its anchors. Checked here
#: with `re.fullmatch`, which is anchored by construction: Python's `$` also
#: matches *before* a trailing newline, so `re.search` with the schema's own
#: `^...$` would call `"uploads\n"` valid and the property would assert the
#: laxness rather than the rule. Pydantic validates with the Rust engine, whose
#: `$` is the end of the haystack and nothing else, and `fullmatch` is what
#: states that independently of which engine is in use.
_FOLDER_BODY = re.compile(r"[a-z0-9][a-z0-9_/-]*")


def sized_text(
    max_chars: int, *, min_chars: int = 0, near: tuple[int, ...] = ()
) -> st.SearchStrategy[str]:
    """Text whose length is drawn first, so the bounds are actually reached.

    See the module docstring: this is the difference between a property that
    tests a `max_length` and one that tests short strings.

    `near` names the lengths a boundary sits on, and the strategy then draws
    either a uniform length or one within a character of them. Uniform alone is
    not enough, and the gap is not theoretical: a `min_length=8` quietly
    changed to `7` is a single length out of 141, which 50 uniform examples
    miss about seven times in ten — and did, when it was tried. Half the
    examples landing within a character of a declared bound turns that into a
    test that fails every run.
    """
    lengths = st.integers(min_value=min_chars, max_value=max_chars)
    if near:
        edges = sorted(
            {
                n + delta
                for n in near
                for delta in (-1, 0, 1)
                if min_chars <= n + delta <= max_chars
            }
        )
        lengths = st.one_of(lengths, st.sampled_from(edges))
    return lengths.flatmap(lambda n: st.text(min_size=n, max_size=n))


#: Addresses `EmailStr` accepts and returns unchanged. Lower-case throughout
#: because `email_validator` normalises the domain to lower case, and a
#: round-trip property wants a fixed point rather than a normalisation to
#: account for.
emails = st.from_regex(
    r"[a-z0-9]{1,16}(\.[a-z0-9]{1,16})?@[a-z]{1,12}\.(com|net|org|io)",
    fullmatch=True,
)

#: Opaque strings — a JWT, a refresh token, a presigned URL. Unconstrained in
#: the schemas, so unconstrained here.
opaque = st.text(max_size=60)

channels = st.sampled_from(NotificationStrategyRegistry.available())

uuids = st.uuids()

#: Timezone-aware, matching what `src/models/user.py` stores and what every
#: `datetime` in this application is. Microseconds survive the ISO-8601
#: round-trip; a naive value would too, but it is not a value this system has.
timestamps = st.datetimes(timezones=st.just(UTC))


@st.composite
def webhook_urls(draw: st.DrawFn) -> str:
    """A webhook address the validator accepts."""
    scheme = draw(st.sampled_from(_ALLOWED_WEBHOOK_SCHEMES))
    return scheme + draw(st.text(max_size=40))


@st.composite
def webhook_url_candidates(draw: st.DrawFn) -> str:
    """Anything a client might send as a webhook address.

    Weighted towards values near the two edges the validator draws — a correct
    scheme, and the length limit — because uniformly random text is rejected
    for the scheme every time and would never reach the second clause at all.
    """
    return draw(
        st.one_of(
            st.text(max_size=60),
            webhook_urls(),
            # Long enough to straddle the limit from both sides. Built from a
            # drawn length so the shrinker reports the boundary rather than
            # whichever oversized value came out first.
            st.integers(
                min_value=WEBHOOK_URL_MAX - 4, max_value=WEBHOOK_URL_MAX + 4
            ).map(
                lambda n: "https://host.test/" + "a" * (n - len("https://host.test/"))
            ),
        )
    )


# --- One instance of every schema this application serialises ---------------
#
# `st.builds` rather than `st.from_type`: the fields are constrained, and the
# strategies above are what satisfy those constraints — an inferred one would
# spend the run being rejected by `EmailStr`.

register_requests = st.builds(
    RegisterRequest,
    email=emails,
    password=sized_text(PASSWORD_MAX, min_chars=PASSWORD_MIN),
)
login_requests = st.builds(
    LoginRequest, email=emails, password=sized_text(PASSWORD_MAX, min_chars=1)
)
refresh_requests = st.builds(RefreshRequest, refresh_token=opaque)
token_responses = st.builds(
    TokenResponse,
    access_token=opaque,
    refresh_token=opaque,
    token_type=st.just("bearer"),
)
user_responses = st.builds(
    UserResponse,
    id=uuids,
    email=emails,
    role=st.text(max_size=16),
    is_active=st.booleans(),
    is_verified=st.booleans(),
)
oauth_user_infos = st.builds(
    OAuthUserInfo,
    sub=st.text(max_size=40),
    email=emails,
    name=st.none() | st.text(max_size=40),
    picture=st.none() | st.text(max_size=60),
    email_verified=st.booleans(),
)
profile_responses = st.builds(
    UserProfileResponse,
    id=uuids,
    email=emails,
    role=st.text(max_size=16),
    is_active=st.booleans(),
    is_verified=st.booleans(),
    notification_channel=channels,
    notification_webhook_url=st.none() | webhook_urls(),
)
profile_updates = st.builds(
    ProfileUpdateRequest,
    notification_channel=channels,
    notification_webhook_url=st.none() | webhook_urls(),
)
export_records = st.builds(
    UserExportRecord,
    id=uuids,
    email=emails,
    role=st.text(max_size=16),
    is_active=st.booleans(),
    is_verified=st.booleans(),
    notification_channel=channels,
    created_at=timestamps,
    updated_at=timestamps,
)
upload_requests = st.builds(
    PresignedUploadRequest,
    filename=sized_text(FILENAME_MAX, min_chars=1),
    content_type=sized_text(CONTENT_TYPE_MAX, min_chars=1),
    folder=st.from_regex(_FOLDER_BODY.pattern, fullmatch=True).filter(
        lambda f: len(f) <= FOLDER_MAX
    ),
)
upload_responses = st.builds(
    PresignedUploadResponse,
    key=st.text(max_size=60),
    url=st.text(max_size=60),
    fields=st.dictionaries(st.text(max_size=16), st.text(max_size=32), max_size=6).map(
        FrozenDict
    ),
    expires_in=st.integers(min_value=1, max_value=604800),
)
download_requests = st.builds(
    PresignedDownloadRequest, key=sized_text(OBJECT_KEY_MAX, min_chars=1)
)
download_responses = st.builds(
    PresignedDownloadResponse,
    url=st.text(max_size=60),
    expires_in=st.integers(min_value=1, max_value=604800),
)

#: Every schema above. A model added to `src/` and not added here is not
#: covered, which is the honest failure direction — the alternative, walking
#: `src/` for `BaseModel` subclasses, would need a strategy per field anyway
#: and would pass vacuously for anything it could not build.
any_schema: st.SearchStrategy[BaseModel] = st.one_of(
    register_requests,
    login_requests,
    refresh_requests,
    token_responses,
    user_responses,
    oauth_user_infos,
    profile_responses,
    profile_updates,
    export_records,
    upload_requests,
    upload_responses,
    download_requests,
    download_responses,
)


def _accepts(model: type[BaseModel], /, **fields: object) -> bool:
    """Whether the schema validates these field values."""
    try:
        model(**fields)
    except ValidationError:
        return False
    return True


class TestEverySchemaIsAValueOnTheWire:
    @given(model=any_schema)
    def test_every_schema_round_trips_through_json(self, model: BaseModel) -> None:
        assert type(model).model_validate_json(model.model_dump_json()) == model

    @given(model=any_schema)
    def test_every_schema_round_trips_through_a_python_dict(
        self, model: BaseModel
    ) -> None:
        # The path the streaming export and the event payloads take, which is
        # not the JSON one: `model_dump()` keeps `UUID`, `datetime` and
        # `FrozenDict` as themselves, and re-validating them has to be a no-op.
        assert type(model).model_validate(model.model_dump()) == model

    @given(model=any_schema)
    def test_the_serialised_body_has_exactly_the_declared_fields(
        self, model: BaseModel
    ) -> None:
        # The published contract: no field silently dropped by an `exclude`,
        # none added by an alias nobody meant to publish. For a response model
        # this is the whole of what a client is promised.
        assert set(json.loads(model.model_dump_json())) == set(type(model).model_fields)


class TestTheBoundsAreTheOnesTheSchemasDeclare:
    def test_the_webhook_limit_in_this_file_is_the_schemas_own(self) -> None:
        # The one constant these properties take from the implementation. The
        # properties below state the *rule*; this states the number, so a
        # change to it is a failing test rather than a silently weaker suite.
        assert _WEBHOOK_URL_MAX_LENGTH == WEBHOOK_URL_MAX
        assert _ALLOWED_WEBHOOK_SCHEMES == ("http://", "https://")

    @given(
        email=emails,
        password=sized_text(PASSWORD_MAX + 12, near=(PASSWORD_MIN, PASSWORD_MAX)),
    )
    def test_registration_accepts_exactly_the_documented_password_lengths(
        self, email: str, password: str
    ) -> None:
        accepted = PASSWORD_MIN <= len(password) <= PASSWORD_MAX
        assert _accepts(RegisterRequest, email=email, password=password) is accepted

    @given(
        email=emails,
        password=sized_text(PASSWORD_MAX + 12, near=(1, PASSWORD_MAX)),
    )
    def test_login_accepts_any_non_empty_password_within_the_bound(
        self, email: str, password: str
    ) -> None:
        # Deliberately *not* `RegisterRequest`'s minimum. A password shorter
        # than eight characters cannot be one this API issued, but refusing it
        # at the edge with a 422 would tell an unauthenticated caller
        # something a 401 does not. The maximum is the opposite case: it bounds
        # the argon2 work an anonymous request can buy, so it is enforced.
        accepted = 1 <= len(password) <= PASSWORD_MAX
        assert _accepts(LoginRequest, email=email, password=password) is accepted

    @given(key=sized_text(OBJECT_KEY_MAX + 12, near=(1, OBJECT_KEY_MAX)))
    def test_a_download_key_accepts_exactly_the_documented_lengths(
        self, key: str
    ) -> None:
        accepted = 1 <= len(key) <= OBJECT_KEY_MAX
        assert _accepts(PresignedDownloadRequest, key=key) is accepted

    @given(
        filename=sized_text(FILENAME_MAX + 6, min_chars=1, near=(FILENAME_MAX,)),
        content_type=sized_text(
            CONTENT_TYPE_MAX + 6, min_chars=1, near=(CONTENT_TYPE_MAX,)
        ),
    )
    def test_an_upload_request_accepts_exactly_the_documented_lengths(
        self, filename: str, content_type: str
    ) -> None:
        accepted = (
            len(filename) <= FILENAME_MAX and len(content_type) <= CONTENT_TYPE_MAX
        )
        assert (
            _accepts(
                PresignedUploadRequest, filename=filename, content_type=content_type
            )
            is accepted
        )

    @given(
        folder=st.one_of(
            st.text(max_size=70),
            st.from_regex(_FOLDER_BODY.pattern, fullmatch=True),
            sized_text(FOLDER_MAX + 6, min_chars=FOLDER_MAX - 2).map(
                lambda s: "a" + s[1:]
            ),
        )
    )
    def test_an_upload_folder_is_accepted_exactly_when_it_matches_the_pattern(
        self, folder: str
    ) -> None:
        accepted = bool(_FOLDER_BODY.fullmatch(folder)) and len(folder) <= FOLDER_MAX
        assert (
            _accepts(
                PresignedUploadRequest,
                filename="report.csv",
                content_type="text/csv",
                folder=folder,
            )
            is accepted
        )


@st.composite
def profile_patches(draw: st.DrawFn) -> dict[str, object]:
    """A PATCH body: any subset of the fields, with values a client sends.

    `notification_webhook_url` may be an explicit `None` — that is how the
    address is cleared — while `notification_channel` may not, which is the
    asymmetry the two validators encode.
    """
    provided = draw(st.sets(st.sampled_from(sorted(ProfileUpdateRequest.model_fields))))
    values: dict[str, object] = {}
    if "notification_channel" in provided:
        values["notification_channel"] = draw(channels)
    if "notification_webhook_url" in provided:
        values["notification_webhook_url"] = draw(st.none() | webhook_urls())
    return values


class TestTheProfilePatch:
    """`ProfileUpdateRequest`, where omitted and null mean different things."""

    @given(patch=profile_patches())
    def test_exclude_unset_reports_exactly_the_fields_the_client_sent(
        self, patch: dict[str, object]
    ) -> None:
        # The whole of PATCH semantics, and the reason the service applies
        # `model_dump(exclude_unset=True)` rather than the full dump: a field
        # the client did not mention must not be written back as its default.
        # An explicit null *is* mentioned, and clears the address.
        request = ProfileUpdateRequest(**patch)
        assert set(request.model_dump(exclude_unset=True)) == set(patch)
        assert request.model_dump(exclude_unset=True) == patch

    @given(channel=st.text(max_size=20))
    def test_a_channel_is_accepted_exactly_when_the_registry_has_it(
        self, channel: str
    ) -> None:
        # The registry is the single source of truth for what channels exist,
        # so this property follows it rather than listing names: a strategy
        # registered tomorrow is accepted by the schema the same day.
        accepted = channel in NotificationStrategyRegistry.available()
        assert _accepts(ProfileUpdateRequest, notification_channel=channel) is accepted

    def test_an_explicitly_null_channel_is_refused(self) -> None:
        # Not a property — there is one null — but it belongs beside the one
        # above, which says nothing about it: `None` is not in the registry and
        # would be refused for the wrong reason without the validator's own
        # first clause, whose message tells the client to omit the field.
        with pytest.raises(ValidationError, match="omit the field"):
            ProfileUpdateRequest(notification_channel=None)

    @given(url=webhook_url_candidates())
    def test_a_webhook_url_is_accepted_exactly_when_both_clauses_hold(
        self, url: str
    ) -> None:
        accepted = url.startswith(_ALLOWED_WEBHOOK_SCHEMES) and len(url) <= (
            WEBHOOK_URL_MAX
        )
        assert _accepts(ProfileUpdateRequest, notification_webhook_url=url) is accepted

    @given(url=webhook_url_candidates())
    def test_an_accepted_webhook_url_is_stored_byte_for_byte(self, url: str) -> None:
        # No normalisation at the edge: whether the host is one this service
        # may call is settled at delivery time, against the address the client
        # actually gave, and a URL silently rewritten here would be a different
        # address by the time that check runs.
        assume(url.startswith(_ALLOWED_WEBHOOK_SCHEMES) and len(url) <= WEBHOOK_URL_MAX)
        assert (
            ProfileUpdateRequest(notification_webhook_url=url).notification_webhook_url
            == url
        )

    @given(name=st.text(min_size=1, max_size=24))
    def test_an_unknown_field_is_refused_rather_than_ignored(self, name: str) -> None:
        # `extra="forbid"`. The alternative is a 200 for `{"emial": "..."}`
        # that changes nothing, which is the worst answer available: the client
        # has no way to learn its update was dropped.
        assume(name not in ProfileUpdateRequest.model_fields)
        with pytest.raises(ValidationError):
            ProfileUpdateRequest.model_validate({name: "whatever"})


class TestOrmAndKeywordConstructionAgree:
    @given(
        id=uuids,
        email=emails,
        role=st.text(max_size=16),
        is_active=st.booleans(),
        is_verified=st.booleans(),
    )
    def test_user_response_reads_a_row_the_same_way_it_reads_keywords(
        self,
        id: uuid.UUID,
        email: str,
        role: str,
        is_active: bool,
        is_verified: bool,
    ) -> None:
        # `from_attributes=True` is how the handlers build these — from a
        # SQLAlchemy row, not from a dict — and it is a second construction
        # path that nothing otherwise compares against the first.
        values = {
            "id": id,
            "email": email,
            "role": role,
            "is_active": is_active,
            "is_verified": is_verified,
        }
        assert UserResponse.model_validate(SimpleNamespace(**values)) == UserResponse(
            **values
        )

    @given(
        id=uuids,
        email=emails,
        role=st.text(max_size=16),
        is_active=st.booleans(),
        is_verified=st.booleans(),
        notification_channel=channels,
        notification_webhook_url=st.none() | webhook_urls(),
    )
    def test_the_profile_reads_a_row_the_same_way_it_reads_keywords(
        self,
        id: uuid.UUID,
        email: str,
        role: str,
        is_active: bool,
        is_verified: bool,
        notification_channel: str,
        notification_webhook_url: str | None,
    ) -> None:
        values = {
            "id": id,
            "email": email,
            "role": role,
            "is_active": is_active,
            "is_verified": is_verified,
            "notification_channel": notification_channel,
            "notification_webhook_url": notification_webhook_url,
        }
        assert UserProfileResponse.model_validate(
            SimpleNamespace(**values)
        ) == UserProfileResponse(**values)
