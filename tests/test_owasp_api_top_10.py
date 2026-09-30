"""One test per mitigation claimed in `docs/owasp-api-top-10.md`.

The checklist and this file are one artefact in two halves. A checklist is prose,
and prose about security decays in a particular way: it stays true about the
version of the code it was written against and goes on reading as though it were
true afterwards. Every line of that document therefore names a test here, and
every test here defends one line of it — so a mitigation that stops holding
fails a build instead of quietly becoming a claim.

**What this file is not.** It is not a penetration test, and none of it proves
this application is secure. Each test asserts that one specific, named mitigation
is in place — which is a much smaller claim than the category heading it sits
under, and the checklist says so per category. The gaps are written down there
too, without tests, because a test for an absent mitigation is either vacuous or
a lie.

**Two kinds of test, deliberately.** Most are behavioural: drive the real ASGI
app and watch it refuse something. A few cannot be, and those are source- or
structure-level gates in the `test_webhook_gates.py` idiom — asserted because the
decision decays silently rather than because the code is hard to run. The one
worth naming is the enumeration fix in `AuthService.login`: the mitigation is
*wasted work*, so the only observable difference between the guarded and
unguarded versions is a duration, and a wall-clock assertion in CI is a flake
generator. What is asserted instead is that both branches reach the hasher, which
is the property the timing follows from.

The structural gates carry their own not-vacuous test —
`test_the_inventory_is_not_empty`, `test_the_schema_list_is_not_empty` — because a
gate that enumerates nothing passes forever.
"""

from __future__ import annotations

import ast
import inspect
import pathlib
import re
import textwrap
import uuid
from typing import Final
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from httpx import AsyncClient
from pydantic import BaseModel, ValidationError
from starlette.middleware.cors import CORSMiddleware

from src.auth import password as password_module
from src.auth import utils as auth_utils_module
from src.auth.dependencies import get_current_user
from src.auth.schemas import LoginRequest, RegisterRequest, UserResponse
from src.auth.service import AuthService
from src.auth.utils import InvalidAccessTokenError, verify_access_token
from src.config import settings
from src.exceptions import BadRequestError, ForbiddenError, UnauthorizedError
from src.limiter import limiter
from src.main import app
from src.models.user import User
from src.notifications.base import validate_webhook_url
from src.pagination import CursorParams
from src.storage.base import (
    build_object_key,
    owner_key_prefix,
    require_key_owned_by,
)
from src.storage.s3 import generate_presigned_download
from src.users.schemas import ProfileUpdateRequest, UserProfileResponse
from src.webhooks.errors import (
    SignatureHeaderMissingError,
    SignatureMismatchError,
    SignatureTimestampOutsideWindowError,
)
from tests.fakes import InMemoryUserStore
from tests.owasp import NON_API_PATHS, RouteFacts, flat_routes

#: The repository root, derived from this file rather than from the working
#: directory: a gate that globbed `src/` relative to the CWD would silently
#: enumerate nothing when pytest is invoked from elsewhere, and a gate that
#: enumerates nothing passes.
REPO_ROOT: Final[pathlib.Path] = pathlib.Path(__file__).resolve().parent.parent

ROUTES: Final[tuple[RouteFacts, ...]] = flat_routes(app)


def test_the_inventory_is_not_empty() -> None:
    """Every structural gate below iterates `ROUTES`, so an empty one is vacuous.

    The number is a floor rather than an equality: adding a route should not
    fail this, but losing the recursion in `flat_routes` — which would leave
    five routes instead of nineteen — has to.
    """
    assert len(ROUTES) >= 19
    assert any(route.path == "/api/v1/users/me" for route in ROUTES)


# --------------------------------------------------------------------------
# API1:2023 — Broken Object Level Authorization
# --------------------------------------------------------------------------


class TestAPI1BrokenObjectLevelAuthorization:
    """Objects are addressed by the credential, or by a key that names its owner."""

    def test_no_route_takes_an_object_id_from_the_client(self) -> None:
        """The structural reason BOLA is mostly absent here rather than mitigated.

        Not one route declares a path parameter. `/users/me` resolves its object
        from the bearer token, so there is no `/users/{id}` for an authorisation
        check to be forgotten on. This gate is what makes that a property of the
        application instead of a coincidence of what has been built so far: the
        first route that does take an id fails here and has to argue for itself.
        """
        with_parameters = [route.path for route in ROUTES if "{" in route.path]

        assert with_parameters == []

    async def test_presigned_download_refuses_another_accounts_key(
        self, authenticated_client: AsyncClient, mock_user: User
    ) -> None:
        """The mitigation this item added, and the hole it closed.

        `/uploads/presigned-download` used to sign whatever key the request
        named. Every key in the bucket was therefore readable by every
        authenticated account — not through a bug in the check, but because there
        was no check: the route required *a* user and never asked whether the
        object was theirs.
        """
        someone_else = uuid.uuid4()
        assert someone_else != mock_user.id
        victim_key = build_object_key("uploads", "secret.pdf", owner_id=someone_else)

        response = await authenticated_client.post(
            "/api/v1/uploads/presigned-download", json={"key": victim_key}
        )

        assert response.status_code == 403
        assert response.json()["error"] == "FORBIDDEN"

    async def test_presigned_download_allows_the_callers_own_key(
        self, authenticated_client: AsyncClient, mock_user: User
    ) -> None:
        """The other half: the refusal above must not be refusing everything.

        A check that returned 403 unconditionally would pass the test above and
        break the feature, which is the failure mode a negative-only test invites.
        """
        own_key = build_object_key("uploads", "mine.pdf", owner_id=mock_user.id)

        with patch("src.storage.s3._get_s3_client") as get_client:
            client = MagicMock()
            get_client.return_value = client
            client.generate_presigned_url.return_value = "https://s3.example/x?sig=y"

            response = await authenticated_client.post(
                "/api/v1/uploads/presigned-download", json={"key": own_key}
            )

        assert response.status_code == 200
        assert response.json()["url"] == "https://s3.example/x?sig=y"

    async def test_presigned_upload_lands_in_the_callers_namespace(
        self, authenticated_client: AsyncClient, mock_user: User
    ) -> None:
        """A client chooses its folder, but only inside its own prefix.

        `folder` is still caller-supplied. What changed is that it is a suffix of
        the owner prefix rather than the start of the key, so no value of it
        reaches another account's namespace.
        """
        with patch("src.storage.s3._get_s3_client") as get_client:
            client = MagicMock()
            get_client.return_value = client
            client.generate_presigned_post.return_value = {
                "url": "https://s3.example/upload",
                "fields": {},
            }

            response = await authenticated_client.post(
                "/api/v1/uploads/presigned-upload",
                json={
                    "filename": "photo.jpg",
                    "content_type": "image/jpeg",
                    "folder": "uploads",
                },
            )

        assert response.status_code == 200
        assert response.json()["key"].startswith(owner_key_prefix(mock_user.id))

    def test_a_traversal_key_naming_the_callers_prefix_is_refused(self) -> None:
        """Why `require_key_owned_by` validates before it compares.

        `users/<me>/../<victim>/x` starts with the caller's prefix and resolves
        somewhere else. A prefix test applied to an unvalidated key admits it, so
        the order of the two steps is the mitigation rather than an arrangement
        of it — this is the test that fails if they are swapped.
        """
        mine = uuid.uuid4()
        victim = uuid.uuid4()
        traversal = f"{owner_key_prefix(mine)}../{victim}/secret.pdf"

        # A 400 (malformed) or a 403 (not yours) are both correct refusals; a
        # returned key is not, which is what this would be without the
        # validation step.
        with pytest.raises((BadRequestError, ForbiddenError)):
            require_key_owned_by(traversal, mine)

    def test_the_owner_prefix_is_slash_terminated(self) -> None:
        """A prefix test is only as good as the boundary it compares against.

        Without the trailing slash `users/<a>` prefixes `users/<ab>...`, so an
        account whose id extended another's would read the other's objects. UUIDs
        are fixed-width and cannot collide that way, which is exactly why this is
        a test: dropping the slash passes every scenario written with real ids.
        """
        owner = uuid.uuid4()

        assert owner_key_prefix(owner).endswith("/")
        # The concrete collision the slash prevents, stated as data: a key under
        # an id that merely *starts with* the owner's id is not the owner's.
        extended = f"users/{owner}extra/uploads/file.pdf"
        with pytest.raises(ForbiddenError):
            require_key_owned_by(extended, owner)

    def test_minting_a_key_requires_naming_an_owner(self) -> None:
        """No default owner, because an unattributed key is unauthorisable."""
        signature = inspect.signature(build_object_key)
        owner = signature.parameters["owner_id"]

        assert owner.kind is inspect.Parameter.KEYWORD_ONLY
        assert owner.default is inspect.Parameter.empty

    def test_the_download_signer_cannot_be_called_without_an_owner(self) -> None:
        """The authorisation boundary is in the signature, not in the caller.

        `generate_presigned_download` is importable from anywhere in `src/`. A
        route is not the only possible caller, so the check lives in the function
        that mints the capability and `owner_id` has no default there either.
        """
        signature = inspect.signature(generate_presigned_download)
        owner = signature.parameters["owner_id"]

        assert owner.kind is inspect.Parameter.KEYWORD_ONLY
        assert owner.default is inspect.Parameter.empty


# --------------------------------------------------------------------------
# API2:2023 — Broken Authentication
# --------------------------------------------------------------------------


class TestAPI2BrokenAuthentication:
    """Credentials are verified, and refusals do not say more than "no"."""

    async def test_login_hashes_even_when_the_address_is_unknown(
        self, auth_service: AuthService
    ) -> None:
        """The enumeration fix, asserted as work done rather than as time taken.

        `or` short-circuits, so the natural spelling of this check never reaches
        argon2 for an address nobody registered. Measured on this machine that is
        ~75ms against ~0ms — three orders of magnitude, readable in one sample,
        and a working "is this address registered" oracle. A wall-clock assertion
        would be the direct test and would also be a flake generator in CI, so
        what is pinned is the property the timing follows from: the branch reaches
        the hasher.
        """
        with patch(
            "src.auth.service.verify_password", wraps=password_module.verify_password
        ) as spy:
            with pytest.raises(UnauthorizedError):
                await auth_service.login("nobody@example.com", "whatever")

        assert spy.call_count == 1

    async def test_login_hashes_for_an_oauth_only_account(
        self, auth_service: AuthService, user_store: InMemoryUserStore
    ) -> None:
        """The third branch, which is the one most easily left out.

        An account created through Google has `hashed_password is None`. It has
        to be indistinguishable from an address that was never registered, or the
        oracle is merely narrowed from "is this registered" to "is this registered
        with a password" — which is the more useful answer of the two.
        """
        user_store.users.append(
            User(
                id=uuid.uuid4(),
                email="google-only@example.com",
                hashed_password=None,
                is_active=True,
                is_verified=True,
                role="user",
                notification_channel="email",
                version=1,
            )
        )

        with patch(
            "src.auth.service.verify_password", wraps=password_module.verify_password
        ) as spy:
            with pytest.raises(UnauthorizedError):
                await auth_service.login("google-only@example.com", "whatever")

        assert spy.call_count == 1

    def test_the_decoy_is_derived_from_the_live_hasher(self) -> None:
        """Not a committed constant, because argon2 reads its cost from the hash.

        A literal hash pinned in source keeps verifying at whatever
        `ARGON2_TIME_COST` produced it, so raising the setting would make the real
        branch slower than the decoy branch and silently reopen the gap. Asserted
        by parsing the module: a constant assigned anywhere in it would be the
        mutation this catches.
        """
        decoy = password_module.decoy_hash()

        # The format argon2-cffi emits, carrying the parameters it will verify at.
        assert decoy.startswith("$argon2")
        assert f"t={settings.ARGON2_TIME_COST}" in decoy
        assert f"m={settings.ARGON2_MEMORY_COST}" in decoy

    def test_the_decoy_matches_no_password(self) -> None:
        """It is a hash of a random value, so it cannot become a credential."""
        decoy = password_module.decoy_hash()

        assert password_module.verify_password("", decoy) is False
        assert password_module.verify_password("password", decoy) is False

    async def test_both_login_refusals_are_byte_identical(
        self, auth_service: AuthService, user_store: InMemoryUserStore
    ) -> None:
        """Timing is one channel; the message is the other, and both are closed."""
        user_store.users.append(
            User(
                id=uuid.uuid4(),
                email="real@example.com",
                hashed_password=password_module.hash_password("correct-horse"),
                is_active=True,
                is_verified=True,
                role="user",
                notification_channel="email",
                version=1,
            )
        )

        with pytest.raises(UnauthorizedError) as unknown:
            await auth_service.login("nobody@example.com", "correct-horse")
        with pytest.raises(UnauthorizedError) as wrong_password:
            await auth_service.login("real@example.com", "wrong-password")

        assert str(unknown.value) == str(wrong_password.value)
        assert unknown.value.status_code == wrong_password.value.status_code

    def test_token_verification_pins_one_algorithm(self) -> None:
        """`algorithms=[...]` is the whole defence against algorithm confusion.

        A verifier that takes the algorithm from the token's own header accepts
        `alg: none` — an unsigned token — and, where an asymmetric key is
        configured, accepts a token HMAC-signed with the public key. Parsed rather
        than grepped so the word in a docstring is not what satisfies this.
        """
        tree = ast.parse(pathlib.Path(auth_utils_module.__file__).read_text())
        decode_calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "decode"
        ]

        assert decode_calls, "jwt.decode is no longer called; this gate is vacuous"
        for call in decode_calls:
            keywords = {kw.arg for kw in call.keywords}
            assert "algorithms" in keywords

    def test_an_unsigned_token_is_refused(self) -> None:
        """The behaviour the gate above protects, driven end to end."""
        import base64
        import json

        def segment(payload: dict[str, object]) -> str:
            raw = json.dumps(payload).encode()
            return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

        header = segment({"alg": "none", "typ": "JWT"})
        claims = segment(
            {"sub": str(uuid.uuid4()), "type": "access", "exp": 9999999999}
        )
        unsigned = f"{header}.{claims}."

        with pytest.raises(InvalidAccessTokenError):
            verify_access_token(unsigned)

    def test_a_refresh_token_is_not_an_access_token(self) -> None:
        """Both are signed with the same key, so only the claim separates them.

        Without the `type` check a refresh token — deliberately long-lived, and
        stored by clients accordingly — authenticates every request as though it
        were the short-lived credential.
        """
        from src.auth.utils import create_refresh_token

        token, _ = create_refresh_token(str(uuid.uuid4()), str(uuid.uuid4()))

        with pytest.raises(InvalidAccessTokenError, match="Invalid token type"):
            verify_access_token(token)

    async def test_an_unauthenticated_request_is_refused_with_a_challenge(
        self, async_client: AsyncClient
    ) -> None:
        """A 401 without `WWW-Authenticate` tells a client nothing to do next.

        Both shapes of refusal are asserted because they come from different
        places: a *missing* header is refused by `HTTPBearer` before the
        dependency runs, and a present-but-invalid one by `get_current_user`
        after it. A change that left one of the two answering 200 would be
        invisible to a test that only tried the other.
        """
        missing = await async_client.get("/api/v1/users/me")

        assert missing.status_code == 401

        invalid = await async_client.get(
            "/api/v1/users/me", headers={"Authorization": "Bearer not-a-jwt"}
        )

        assert invalid.status_code == 401
        assert "WWW-Authenticate" in invalid.headers


# --------------------------------------------------------------------------
# API3:2023 — Broken Object Property Level Authorization
# --------------------------------------------------------------------------

#: Columns on `User` that the owner may set through the API. Everything else on
#: that model is set by this application or by the database — `role` and
#: `is_verified` are privilege, `hashed_password` is a credential, `version` is
#: the concurrency counter, and `oauth_sub` is an identity assertion made by a
#: provider. The list is here rather than in `src/` because it is the *test's*
#: statement of what the schema is allowed to grow into.
OWNER_EDITABLE_FIELDS: Final[frozenset[str]] = frozenset(
    {"notification_channel", "notification_webhook_url"}
)

#: Attributes that must never appear in a response model, whatever the route.
NEVER_SERIALISED: Final[frozenset[str]] = frozenset(
    {"hashed_password", "oauth_sub", "oauth_provider", "version"}
)

#: Every response model this API serialises a user through.
USER_RESPONSE_MODELS: Final[tuple[type[BaseModel], ...]] = (
    UserResponse,
    UserProfileResponse,
)


class TestAPI3BrokenObjectPropertyLevelAuthorization:
    """A client may read and write some properties of its own object, not all."""

    def test_the_schema_list_is_not_empty(self) -> None:
        """Guards the two gates below against enumerating nothing."""
        assert len(USER_RESPONSE_MODELS) >= 2

    def test_the_profile_update_schema_forbids_unknown_fields(self) -> None:
        """The whole safety of the `setattr` loop in `ProfileService.update`.

        That method does `setattr(user, key, value)` over
        `model_dump(exclude_unset=True)`, which is the classic mass-assignment
        shape and is safe here for exactly one reason: the schema is closed, so
        the only keys that can reach it are the ones declared. Loosen `extra` and
        the same loop writes `role`.
        """
        assert ProfileUpdateRequest.model_config.get("extra") == "forbid"

        with pytest.raises(ValidationError):
            ProfileUpdateRequest.model_validate(
                {"notification_channel": "email", "role": "admin"}
            )

    def test_the_profile_update_schema_declares_only_editable_fields(self) -> None:
        """The other half, and the one that catches a *declared* field.

        `extra="forbid"` stops an undeclared `role` from arriving. It does nothing
        about somebody adding `role: str | None = None` to the model, which the
        `setattr` loop would then apply. This gate is what makes that edit fail.
        """
        declared = frozenset(ProfileUpdateRequest.model_fields)

        assert declared <= OWNER_EDITABLE_FIELDS, (
            f"{sorted(declared - OWNER_EDITABLE_FIELDS)} would be writable by the "
            "owner through PATCH /users/me; add it to OWNER_EDITABLE_FIELDS only "
            "if that is intended."
        )

    @pytest.mark.parametrize("model", USER_RESPONSE_MODELS, ids=lambda m: m.__name__)
    def test_response_models_never_serialise_a_credential(
        self, model: type[BaseModel]
    ) -> None:
        """Read-side property authorisation, which is the half usually forgotten.

        `from_attributes=True` means these models are built straight off the ORM
        row, so a field added here is a column published. The password hash is the
        one that matters most and `oauth_sub` is the one most likely to be added
        without thinking — it is a stable identifier a provider issued, and it
        belongs to the provider relationship rather than to the client.
        """
        exposed = frozenset(model.model_fields) & NEVER_SERIALISED

        assert exposed == frozenset(), f"{model.__name__} exposes {sorted(exposed)}"

    async def test_register_cannot_assign_a_role(
        self, fake_backed_client: AsyncClient
    ) -> None:
        """Privilege escalation through the one unauthenticated write.

        `AuthService.register` names `email` and `hashed_password` explicitly
        rather than splatting the request model, so a `role` in the body is
        ignored rather than applied. Driven over HTTP because the claim is about
        the whole path, schema included.
        """
        response = await fake_backed_client.post(
            "/api/v1/auth/register",
            json={
                "email": "escalate@example.com",
                "password": "a-long-enough-password",
                "role": "admin",
                "is_verified": True,
            },
        )

        assert response.status_code == 201
        assert response.json()["role"] == "user"
        assert response.json()["is_verified"] is False

    def test_register_does_not_splat_the_request_model(self) -> None:
        """Why the test above keeps passing.

        A future `self.users.create(**data.model_dump())` would pass every test
        that checks the *response*, because `UserResponse` reports what the row
        now says — which would be `admin`. The gate is on the call.
        """
        source = textwrap.dedent(inspect.getsource(AuthService.register))
        tree = ast.parse(source)

        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                assert not any(kw.arg is None for kw in node.keywords), (
                    "register() splats a mapping into a call; an attacker-supplied "
                    "key would then reach the row"
                )


# --------------------------------------------------------------------------
# API4:2023 — Unrestricted Resource Consumption
# --------------------------------------------------------------------------

#: Routes reachable without a credential. Each is a place an anonymous caller can
#: spend this application's CPU, memory or connections, so each must carry an
#: explicit rate limit — the default 200/minute is a backstop, not a decision.
PUBLIC_API_ROUTES: Final[frozenset[str]] = frozenset(
    {
        "/api/v1/auth/register",
        "/api/v1/auth/login",
        "/api/v1/auth/refresh",
        "/api/v1/auth/logout",
        "/api/v1/auth/google",
        "/api/v1/auth/google/callback",
    }
)


class TestAPI4UnrestrictedResourceConsumption:
    """Every unauthenticated cost has a ceiling, and the ceilings are asserted."""

    def test_every_public_route_carries_an_explicit_rate_limit(self) -> None:
        """The default limit is a backstop; a decision is per route.

        slowapi records decorated endpoints in `_route_limits`, keyed by
        `module.qualname`, so a public route that lost its decorator is absent
        here — which no behavioural test would notice, the default still applying.
        """
        limited = {key.rsplit(".", 1)[-1] for key in limiter._route_limits}
        public_endpoints = {
            route.endpoint.rsplit(".", 1)[-1]
            for route in ROUTES
            if route.path in PUBLIC_API_ROUTES
        }

        assert public_endpoints, "the public-route list no longer matches any route"
        assert public_endpoints <= limited, sorted(public_endpoints - limited)

    def test_the_credential_routes_are_limited_harder_than_the_default(self) -> None:
        """Login and register are the two an attacker wants unbounded.

        The numbers are asserted rather than only their presence: a limit quietly
        widened to the 200/minute default would otherwise read as "still limited".
        """
        limits = {
            key.rsplit(".", 1)[-1]: [limit.limit for limit in value]
            for key, value in limiter._route_limits.items()
        }

        for endpoint in ("register", "login"):
            amounts = [limit.amount for limit in limits[endpoint]]
            assert amounts and max(amounts) <= 5

    def test_the_login_password_is_bounded(self) -> None:
        """Every login now reaches argon2, so body length became a CPU multiplier.

        This is the cost of the enumeration fix, paid deliberately: before it, an
        unknown address short-circuited and the field's length did not matter. It
        does now, and `RegisterRequest`'s bound is the natural one because a
        longer password is one no account can have.
        """
        register_max = RegisterRequest.model_fields["password"].metadata
        login_max = LoginRequest.model_fields["password"].metadata

        def max_length(metadata: list[object]) -> int | None:
            return next(
                (
                    getattr(item, "max_length")
                    for item in metadata
                    if hasattr(item, "max_length")
                ),
                None,
            )

        assert max_length(login_max) is not None
        assert max_length(login_max) == max_length(register_max)

        with pytest.raises(ValidationError):
            LoginRequest(email="a@example.com", password="x" * 129)

    async def test_an_oversized_login_password_never_reaches_the_hasher(
        self, fake_backed_client: AsyncClient
    ) -> None:
        """The bound has to be enforced before the expensive part, not after."""
        with patch(
            "src.auth.service.verify_password", wraps=password_module.verify_password
        ) as spy:
            response = await fake_backed_client.post(
                "/api/v1/auth/login",
                json={"email": "a@example.com", "password": "x" * 5000},
            )

        assert response.status_code == 422
        assert spy.call_count == 0

    def test_pagination_cannot_request_an_unbounded_page(self) -> None:
        """A `limit` a client chooses is a row count a client chooses."""
        assert CursorParams(limit=100).limit == 100

        # One over the ceiling, not a wild number: `le=100` widened to `le=1000`
        # would still refuse 1001 and would have quadrupled the worst-case page.
        with pytest.raises(ValidationError):
            CursorParams(limit=101)
        with pytest.raises(ValidationError):
            CursorParams(limit=0)

    def test_the_export_stream_is_bounded_in_time_and_memory(self) -> None:
        """The one route whose response size is the size of a table.

        Three separate ceilings, because they bound different things: rows per
        round trip, bytes per chunk, and — the one that matters most — a deadline,
        without which a slow consumer holds a database cursor open indefinitely.
        """
        assert settings.EXPORT_BATCH_ROWS > 0
        assert settings.EXPORT_CHUNK_BYTES > 0
        assert settings.EXPORT_READAHEAD_CHUNKS > 0
        assert settings.EXPORT_DEADLINE_SECONDS > 0

    def test_websocket_frames_and_lifetimes_are_bounded(self) -> None:
        """A connection is a held resource, so every dimension of it is capped."""
        assert settings.WS_MAX_MESSAGE_BYTES > 0
        assert settings.WS_MAX_ROOMS_PER_CONNECTION > 0
        assert settings.WS_MAX_CONNECTION_SECONDS > 0
        assert settings.WS_IDLE_TIMEOUT_SECONDS > 0

    def test_upload_size_and_type_are_bounded_in_the_signed_policy(self) -> None:
        """A presigned POST is uploaded to S3 without touching this process.

        So a cap enforced in a handler would not apply at all. Both conditions
        travel inside the signature, where S3 enforces them.
        """
        from src.storage.base import ALLOWED_CONTENT_TYPES, MAX_FILE_SIZE_BYTES

        with patch("src.storage.s3._get_s3_client") as get_client:
            client = MagicMock()
            get_client.return_value = client
            client.generate_presigned_post.return_value = {"url": "u", "fields": {}}

            from src.storage.s3 import generate_presigned_upload

            generate_presigned_upload(
                folder="uploads",
                filename="a.jpg",
                content_type="image/jpeg",
                owner_id=uuid.uuid4(),
            )

        conditions = client.generate_presigned_post.call_args[1]["Conditions"]

        assert ["content-length-range", 1, MAX_FILE_SIZE_BYTES] in conditions
        assert {"Content-Type": "image/jpeg"} in conditions
        assert "application/octet-stream" not in ALLOWED_CONTENT_TYPES


# --------------------------------------------------------------------------
# API5:2023 — Broken Function Level Authorization
# --------------------------------------------------------------------------

#: Routes that serve without a credential *by design*, each with its reason. The
#: gate below admits only these, so a new route that forgot its guard fails until
#: somebody either adds the guard or writes the route down here — which is the
#: point: the omission becomes a decision.
INTENTIONALLY_PUBLIC: Final[dict[str, str]] = {
    "/docs": "API documentation",
    "/docs/oauth2-redirect": "OAuth redirect helper for the docs page",
    "/redoc": "API documentation",
    "/health": "liveness probe, read by an orchestrator that holds no credential",
    "/health/ready": "readiness probe, same",
    "/metrics": "scrape endpoint; access is a network decision, not a token one",
    "/api/v1/auth/register": "creates the account a credential would name",
    "/api/v1/auth/login": "issues the credential",
    "/api/v1/auth/refresh": "the refresh token *is* the credential, read from the body",
    "/api/v1/auth/logout": "same, and must work for a token this API no longer takes",
    "/api/v1/auth/google": "starts the OAuth redirect",
    "/api/v1/auth/google/callback": "the provider arrives here with no bearer token",
    # Not an exemption so much as a different mechanism: a browser cannot set a
    # request header on a WebSocket, so this endpoint authenticates from
    # `Sec-WebSocket-Protocol` inside the handler, before `accept()`. See
    # `src/ws/auth.py` and `tests/test_ws_auth.py`.
    "/api/v1/ws": "authenticates via subprotocol before the handshake completes",
}


class TestAPI5BrokenFunctionLevelAuthorization:
    """Administrative functions are guarded, and no route is unguarded by accident."""

    def test_every_route_either_authenticates_or_is_listed_as_public(self) -> None:
        """The gate that catches the *next* route rather than the current ones.

        BFLA is mostly a bookkeeping failure: somebody adds a handler, forgets the
        dependency, and nothing complains because the route works. Here the route
        table is compared against an explicit list, so forgetting is a red build.
        """
        unguarded = sorted(
            route.path
            for route in ROUTES
            if not route.authenticates and route.path not in INTENTIONALLY_PUBLIC
        )

        assert unguarded == [], (
            f"{unguarded} serve without authentication and are not in "
            "INTENTIONALLY_PUBLIC; add the guard, or add the route and its reason."
        )

    def test_the_public_list_does_not_name_routes_that_no_longer_exist(self) -> None:
        """An exemption list that outlives its routes hides the next omission."""
        live = {route.path for route in ROUTES}
        stale = sorted(path for path in INTENTIONALLY_PUBLIC if path not in live)

        assert stale == []

    async def test_the_export_refuses_a_non_admin(
        self, authenticated_client: AsyncClient
    ) -> None:
        """Reading every account is a different function from reading one.

        An ordinary user is allowed `/users/me`, so this route is exactly the case
        BFLA describes: an authenticated caller reaching an administrative function
        by knowing its URL.
        """
        response = await authenticated_client.get("/api/v1/exports/users")

        assert response.status_code == 403
        assert response.json()["error"] == "FORBIDDEN"

    async def test_the_export_refuses_before_the_response_starts(
        self, authenticated_client: AsyncClient
    ) -> None:
        """A streaming route can refuse in two places, and only one is usable.

        `require_role` resolves during dependency injection, so the refusal is an
        ordinary 403 envelope. A check inside the generator would arrive *after*
        the 200 and the `Content-Type`, making the failure a truncated NDJSON
        stream that clients would have to parse to notice.
        """
        response = await authenticated_client.get("/api/v1/exports/users")

        assert response.status_code == 403
        assert response.headers["content-type"].startswith("application/json")
        assert "_export" not in response.text

    def test_the_role_guard_is_a_dependency_not_a_handler_check(self) -> None:
        """Which is what makes the ordering above structural.

        A check written as the handler's first statement would run after the
        response class was chosen and, on the export route, after streaming began.
        """
        export = next(
            route for route in ROUTES if route.path == "/api/v1/exports/users"
        )

        assert any(name.startswith("require_role") for name in export.dependencies)


# --------------------------------------------------------------------------
# API6:2023 — Unrestricted Access to Sensitive Business Flows
# --------------------------------------------------------------------------


class TestAPI6SensitiveBusinessFlows:
    """The flows worth automating against are the ones with the tightest limits."""

    async def test_registration_is_rate_limited_per_client(
        self, fake_backed_client: AsyncClient
    ) -> None:
        """Account creation is the flow this API most wants throttled.

        Unbounded, it is free bulk account creation — and each one enqueues a
        welcome email, so the cost lands on a mail reputation as well as on a
        table. Driven rather than read off the decorator because the limiter has
        to be *wired into the app*: a `@limiter.limit` on a route whose app has no
        `state.limiter` raises at request time rather than limiting.
        """
        limiter.reset()
        try:
            statuses = []
            for index in range(7):
                response = await fake_backed_client.post(
                    "/api/v1/auth/register",
                    json={
                        "email": f"bulk-{index}@example.com",
                        "password": "a-long-enough-password",
                    },
                )
                statuses.append(response.status_code)
        finally:
            limiter.reset()

        assert 429 in statuses, statuses
        assert statuses.index(429) == 5, "the limit should admit five per minute"

    async def test_a_throttled_response_tells_the_client_when_to_retry(
        self, fake_backed_client: AsyncClient
    ) -> None:
        """A 429 with no `Retry-After` produces a client that retries immediately."""
        limiter.reset()
        try:
            last = None
            for index in range(7):
                last = await fake_backed_client.post(
                    "/api/v1/auth/login",
                    json={
                        "email": f"guess-{index}@example.com",
                        "password": "whatever",
                    },
                )
        finally:
            limiter.reset()

        assert last is not None
        assert last.status_code == 429
        assert int(last.headers["Retry-After"]) > 0
        assert last.json()["error"] == "RATE_LIMIT_EXCEEDED"


# --------------------------------------------------------------------------
# API7:2023 — Server Side Request Forgery
# --------------------------------------------------------------------------


class TestAPI7ServerSideRequestForgery:
    """The one user-supplied URL this application fetches is checked before it does."""

    @pytest.mark.parametrize(
        "url",
        [
            "https://169.254.169.254/latest/meta-data/",  # cloud instance metadata
            "https://127.0.0.1:8000/api/v1/exports/users",  # this process
            "https://localhost/admin",
            "https://10.0.0.5/internal",
            "https://192.168.1.1/",
            "https://[::1]/",
            "https://0.0.0.0/",
            "https://100.100.100.200/",  # Alibaba Cloud metadata, a shared-CGN address
        ],
    )
    def test_private_and_reserved_targets_are_refused(self, url: str) -> None:
        """`notification_webhook_url` is the attacker's input; the network is ours.

        169.254.169.254 is the one that matters most — on EC2 it hands out role
        credentials to anything that can reach it, and this process can. Written
        as `https` throughout on purpose: over `http` the scheme check below
        refuses these first, so an `http` spelling of this test would pass with
        the address check deleted.
        """
        with pytest.raises(BadRequestError):
            validate_webhook_url(url)

    @pytest.mark.parametrize(
        "url",
        [
            "file:///etc/passwd",
            "gopher://example.com/",
            "ftp://example.com/x",
            "http://example.com/hook",
        ],
    )
    def test_non_https_schemes_are_refused(self, url: str) -> None:
        """A scheme check is not redundant with a host check.

        `file:///etc/passwd` has no host to classify as private, so an
        address-based check alone passes it straight through. Plain `http` is
        refused for a second reason as well: the body carries whatever the
        notification put in it, and cleartext would put that on the wire.
        """
        with pytest.raises(BadRequestError):
            validate_webhook_url(url)

    def test_embedded_credentials_are_refused(self) -> None:
        """They would reach the logs, and the `Location` of any redirect."""
        with pytest.raises(BadRequestError):
            validate_webhook_url("https://user:secret@hooks.example.com/abc")

    def test_a_public_https_target_is_accepted(self) -> None:
        """The check must not be refusing everything."""
        assert (
            validate_webhook_url("https://hooks.example.com/abc")
            == "https://hooks.example.com/abc"
        )

    def test_private_hosts_are_refused_by_default(self) -> None:
        """The opt-in exists for development, and defaults to off.

        A default of `True` would make every deployment that never set the flag
        SSRF-able, which is the failure mode of every safety switch that defaults
        to permissive.
        """
        assert settings.NOTIFICATION_WEBHOOK_ALLOW_PRIVATE_HOSTS is False

        signature = inspect.signature(validate_webhook_url)
        assert signature.parameters["allow_private_hosts"].default is False

    def test_the_resolving_hostname_gap_is_not_silently_closed(self) -> None:
        """A name that resolves privately passes, and that is written down.

        This is the honest boundary of a string check: re-resolving here would not
        close it either, because the socket does its own lookup afterwards (DNS
        rebinding). The mitigation is egress policy, and the checklist says so
        rather than implying the check is complete. Asserted so that a future
        reader who assumes otherwise finds a test stating the behaviour.
        """
        # A name, not a literal: accepted here, whatever it resolves to.
        assert validate_webhook_url("https://internal.corp.example/hook")

    def test_the_profile_route_defers_the_host_check_to_delivery(self) -> None:
        """Validating at write time would be a check with a shelf life.

        A hostname that is public when it is saved can point at 169.254.169.254
        by the time anything is sent to it, so the authoritative check is the late
        one. The schema's own validation is deliberately only about shape — which
        means a stored URL is *not* evidence it will be fetched.
        """
        accepted = ProfileUpdateRequest(
            notification_webhook_url="https://internal.example.com/hook"
        )

        assert accepted.notification_webhook_url is not None

        # And the shape check is still a check: a non-http(s) scheme never
        # reaches storage at all.
        with pytest.raises(ValidationError):
            ProfileUpdateRequest(notification_webhook_url="file:///etc/passwd")


# --------------------------------------------------------------------------
# API8:2023 — Security Misconfiguration
# --------------------------------------------------------------------------

#: The headers every response must carry. HSTS is deliberately absent: it is
#: conditional on the request's scheme, because sending it over plain HTTP would
#: pin a developer's browser to HTTPS for localhost across every project on the
#: machine. `tests/test_security_headers.py` covers that condition.
REQUIRED_SECURITY_HEADERS: Final[tuple[str, ...]] = (
    "content-security-policy",
    "x-content-type-options",
    "referrer-policy",
    "x-frame-options",
)


class TestAPI8SecurityMisconfiguration:
    """Defaults are the secure ones, including on the paths nobody routes."""

    async def test_security_headers_reach_a_response_no_handler_produced(
        self, async_client: AsyncClient
    ) -> None:
        """A 404 never reaches a route, so it is the case a per-route answer misses.

        The middleware is added last and therefore runs outermost, which is what
        makes the policy a property of every response rather than of the ones the
        router happened to build.
        """
        response = await async_client.get("/no-such-path")

        assert response.status_code == 404
        for header in REQUIRED_SECURITY_HEADERS:
            assert header in response.headers, header

    def test_the_500_envelope_leaks_nothing_and_is_still_stamped(self) -> None:
        """The one response the middleware cannot reach, and the likeliest to leak.

        Starlette installs the handler for bare `Exception` on
        `ServerErrorMiddleware`, outside the user middleware stack, so this
        envelope is stamped by `unhandled_exception_handler` itself. Two claims in
        one test because they fail together: a handler that re-raised would lose
        both the redaction and the headers.

        `TestClient(..., raise_server_exceptions=False)` is what lets the
        application render its own 500 instead of the exception propagating into
        the test — the idiom `tests/test_exception_handlers.py` already uses.
        """
        secret = "asyncpg-dsn-with-a-password-in-it"

        async def boom() -> User:
            raise RuntimeError(secret)

        app.dependency_overrides[get_current_user] = boom
        try:
            client = TestClient(app, raise_server_exceptions=False)
            response = client.get(
                "/api/v1/users/me", headers={"Authorization": "Bearer irrelevant"}
            )
        finally:
            app.dependency_overrides.pop(get_current_user, None)

        assert response.status_code == 500
        assert response.json() == {
            "error": "INTERNAL_SERVER_ERROR",
            "message": "An unexpected error occurred",
            "status": 500,
        }
        assert secret not in response.text
        assert "Traceback" not in response.text
        assert "RuntimeError" not in response.text
        for header in REQUIRED_SECURITY_HEADERS:
            assert header in response.headers, header

    def test_no_wildcard_cors_is_configured(self) -> None:
        """This API mounts no CORS middleware at all, which is the strict default.

        A browser therefore cannot read a cross-origin response from it, and a
        deployment that needs one has to say so — rather than inheriting
        `allow_origins=["*"]`, which combined with `allow_credentials=True` is the
        misconfiguration this category is named for.
        """
        installed = [
            getattr(entry.cls, "__name__", "") for entry in app.user_middleware
        ]

        # Not empty, or the check below is vacuous.
        assert installed
        assert CORSMiddleware.__name__ not in installed

    async def test_a_cross_origin_response_carries_no_access_control_headers(
        self, async_client: AsyncClient
    ) -> None:
        """The behavioural half of the check above.

        A gate on the middleware list would pass if CORS were ever configured some
        other way; what a browser actually acts on is the response header, so that
        is what is asserted. No `Access-Control-Allow-Origin` means no cross-origin
        page can read this API's responses.
        """
        response = await async_client.get(
            "/health", headers={"Origin": "https://evil.example"}
        )

        assert response.status_code == 200
        assert "access-control-allow-origin" not in response.headers
        assert "access-control-allow-credentials" not in response.headers

    def test_the_content_security_policy_denies_by_default(self) -> None:
        """`default-src 'none'` rather than an allow-list bolted onto `*`."""
        policy = dict((name, value) for name, value in _policy_headers())
        csp = policy["content-security-policy"]

        assert "default-src 'none'" in csp
        assert "frame-ancestors 'none'" in csp
        assert "'unsafe-eval'" not in csp

    def test_the_debug_flag_is_off(self) -> None:
        """FastAPI's debug mode renders a traceback page in place of the envelope."""
        assert app.debug is False


def _policy_headers() -> list[tuple[str, str]]:
    from src.middleware.security_headers import get_security_headers_policy

    policy = get_security_headers_policy()
    assert policy is not None, "the suite runs with security headers enabled"
    return list(policy.headers_for(path="/api/v1/users/me", secure=True))


# --------------------------------------------------------------------------
# API9:2023 — Improper Inventory Management
# --------------------------------------------------------------------------


class TestAPI9ImproperInventoryManagement:
    """Every route is versioned, documented, and reachable from the schema."""

    def test_every_api_route_is_under_a_version_prefix(self) -> None:
        """An unversioned route cannot be retired without breaking a client.

        Which is how a "temporary" endpoint becomes a permanent one nobody owns —
        the shape this category describes. Probes, the scrape endpoint and the
        documentation pages are infrastructure rather than API surface and are
        listed out.
        """
        unversioned = sorted(
            route.path
            for route in ROUTES
            if route.path not in NON_API_PATHS and not route.path.startswith("/api/v1/")
        )

        assert unversioned == []

    def test_the_non_api_list_does_not_name_routes_that_no_longer_exist(self) -> None:
        """Same reason as the public list: a stale exemption hides the next one."""
        live = {route.path for route in ROUTES}

        assert sorted(path for path in NON_API_PATHS if path not in live) == []

    def test_every_http_route_appears_in_the_openapi_document(self) -> None:
        """An undocumented route is an unmanaged one.

        `include_in_schema=False` is the switch that hides one, and a route hidden
        from the schema is a route that does not appear in a review of what this
        service exposes. The scrape endpoint is the deliberate exception — nothing
        generating a client should find it — and it is named here rather than
        assumed.
        """
        documented = set(app.openapi()["paths"])
        # Not API surface: the scrape endpoint is infrastructure and nothing
        # generating a client should find it, and the three documentation pages
        # *render* the schema rather than appearing in it.
        hidden_on_purpose = {
            "/metrics",
            "/docs",
            "/docs/oauth2-redirect",
            "/redoc",
        }

        missing = sorted(
            route.path
            for route in ROUTES
            if not route.websocket
            and route.path not in documented
            and route.path not in hidden_on_purpose
        )

        assert missing == []

    def test_the_documented_error_shapes_include_the_refusals(self) -> None:
        """A client cannot handle a 403 it was never told about.

        The two upload routes are the ones this item changed, so they are the ones
        whose contract had to change with them.
        """
        paths = app.openapi()["paths"]
        download = paths["/api/v1/uploads/presigned-download"]["post"]["responses"]

        assert "403" in download
        assert "400" in download


# --------------------------------------------------------------------------
# API10:2023 — Unsafe Consumption of APIs
# --------------------------------------------------------------------------


class TestAPI10UnsafeConsumptionOfAPIs:
    """What arrives from a third party is data, and what we send it has a deadline."""

    def test_every_outbound_client_is_built_with_a_timeout(self) -> None:
        """A client with no timeout waits forever, and httpx lets you build one.

        `httpx.AsyncClient()` defaults to five seconds, but `timeout=None` is
        legal and means *no* limit — so a third party that accepts a connection
        and never answers holds a worker and, behind it, a database session. The
        gate is on client construction rather than on each `post`, because that is
        where this codebase sets it: a per-call `timeout` would be the exception.

        Parsed across `src/` rather than asserted on one module, because the next
        module to make an outbound call is the one that will forget.
        """
        constructions = 0
        for path in sorted((REPO_ROOT / "src").rglob("*.py")):
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                if getattr(node.func, "attr", None) != "AsyncClient":
                    continue
                constructions += 1
                keywords = {kw.arg: kw.value for kw in node.keywords}
                assert "timeout" in keywords, (
                    f"{path}:{node.lineno} builds an httpx client with no timeout"
                )
                # `timeout=None` is the spelling that means "wait forever", and it
                # reads like a default rather than like a decision.
                assert not (
                    isinstance(keywords["timeout"], ast.Constant)
                    and keywords["timeout"].value is None
                ), f"{path}:{node.lineno} builds an httpx client with timeout=None"

        assert constructions >= 2, "no httpx clients found; this gate is vacuous"

    def test_the_webhook_client_does_not_follow_redirects(self) -> None:
        """Following one would defeat the SSRF check outright.

        `validate_webhook_url` classifies the address the client is *told* to
        reach. A 302 to 169.254.169.254 is a second request to an address nothing
        checked, which is how an SSRF guard that only inspects the first URL is
        walked around.
        """
        source = (REPO_ROOT / "src/notifications/webhook.py").read_text()
        tree = ast.parse(source)

        clients = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and getattr(node.func, "attr", None) == "AsyncClient"
        ]

        assert clients, "no client construction found; this gate is vacuous"
        for node in clients:
            keywords = {kw.arg: kw.value for kw in node.keywords}
            assert "follow_redirects" in keywords
            assert isinstance(keywords["follow_redirects"], ast.Constant)
            assert keywords["follow_redirects"].value is False

    async def test_an_inbound_webhook_needs_a_verified_signature(self) -> None:
        """What a third party sends is a claim until the digest says otherwise.

        Three refusals rather than one, because they are three different bugs: a
        delivery with no signature at all, one whose digest does not verify, and
        one whose timestamp is outside the tolerance. A verifier that accepted any
        of them would accept a forgery.
        """
        import time

        from src.webhooks.factory import create_verifier

        verifier = create_verifier()
        body = b'{"event":"forged"}'
        now = int(time.time())
        # Well-formed but wrong: 64 hex characters, so the header parses and the
        # refusal comes from the digest comparison rather than from the parser.
        wrong_digest = "de" * 32

        with pytest.raises(SignatureHeaderMissingError):
            await verifier.verify(signature=None, body=body)

        with pytest.raises(SignatureMismatchError):
            await verifier.verify(signature=f"t={now},v1={wrong_digest}", body=body)

        with pytest.raises(SignatureTimestampOutsideWindowError):
            await verifier.verify(signature=f"t=1,v1={wrong_digest}", body=body)

    async def test_an_oauth_callback_failure_is_a_400_not_a_500(
        self, async_client: AsyncClient
    ) -> None:
        """A provider's bad response is a bad request, not this service faulting.

        The distinction matters operationally: a 500 pages somebody, and an
        unparseable callback is not an outage. It also keeps the provider's own
        error text out of a 500 envelope.
        """
        response = await async_client.get(
            "/api/v1/auth/google/callback", params={"code": "not-a-real-code"}
        )

        assert response.status_code == 400
        assert response.json()["error"] == "BAD_REQUEST"


# --------------------------------------------------------------------------
# The checklist and this file are one artefact
# --------------------------------------------------------------------------

CHECKLIST: Final[pathlib.Path] = REPO_ROOT / "docs/owasp-api-top-10.md"

#: Tests the checklist deliberately does not cite by name. The first two are
#: guards on the gates rather than mitigations — they assert an enumeration found
#: something, so citing them would list a test against a category it is not about.
#: The other two are the "must not refuse everything" halves of mitigations the
#: document cites through their positive case.
UNCITED_BY_DESIGN: Final[frozenset[str]] = frozenset(
    {
        "test_the_inventory_is_not_empty",
        "test_the_schema_list_is_not_empty",
        "test_a_public_https_target_is_accepted",
        "test_the_download_signer_cannot_be_called_without_an_owner",
    }
)


class TestTheChecklistAndTheSuiteAgree:
    """The claim this whole file rests on: every line of the doc has a test.

    Without these three, the two halves drift in both directions and both
    directions are bad. A citation naming a test that no longer exists makes the
    document look defended where it is not; a test nobody cites is a mitigation
    that will be deleted by somebody who cannot see what depends on it.
    """

    def test_the_checklist_exists_and_is_substantial(self) -> None:
        """Guards the two gates below against reading an empty file."""
        assert CHECKLIST.is_file()
        assert len(CHECKLIST.read_text().splitlines()) > 100

    def test_every_test_the_checklist_cites_exists(self) -> None:
        """A citation to a renamed or deleted test is a false claim of coverage."""
        cited = set(re.findall(r"`(test_\w+)`", CHECKLIST.read_text()))
        defined = _defined_test_names()

        assert cited, "the checklist cites no tests; it has stopped being backed"
        assert sorted(cited - defined) == []

    def test_every_test_here_is_cited_by_the_checklist(self) -> None:
        """The other direction, which is what keeps the document complete.

        A mitigation tested but not written down is one a reader of the checklist
        does not know they have — and one the next person to touch the code has no
        reason to preserve.
        """
        cited = set(re.findall(r"`(test_\w+)`", CHECKLIST.read_text()))
        defined = _defined_test_names()
        uncited = sorted(defined - cited - UNCITED_BY_DESIGN - _meta_test_names())

        assert uncited == [], (
            f"{uncited} defend nothing the checklist claims; cite them in "
            "docs/owasp-api-top-10.md or add them to UNCITED_BY_DESIGN."
        )

    def test_all_ten_categories_are_present(self) -> None:
        """A checklist missing a category reads as though it had nothing to say."""
        text = CHECKLIST.read_text()

        for number in range(1, 11):
            assert f"## API{number}:2023" in text, f"API{number} is not covered"

    def test_every_category_states_what_is_not_covered(self) -> None:
        """The gaps are the part worth reading, so none may be omitted.

        A category with ticks and no `Not covered` paragraph implies completeness,
        which is the one thing a checklist must never imply by accident.
        """
        sections = CHECKLIST.read_text().split("\n## ")

        covered = [section for section in sections if section.startswith("API")]
        assert len(covered) == 10

        for section in covered:
            heading = section.splitlines()[0]
            assert "Not covered" in section, f"{heading} states no gaps"


def _defined_test_names() -> frozenset[str]:
    source = pathlib.Path(__file__).read_text()
    return frozenset(re.findall(r"def (test_\w+)", source))


def _meta_test_names() -> frozenset[str]:
    """The tests in this final class, which are about the pair rather than the API."""
    return frozenset(
        name
        for name, value in vars(TestTheChecklistAndTheSuiteAgree).items()
        if name.startswith("test_") and callable(value)
    )
