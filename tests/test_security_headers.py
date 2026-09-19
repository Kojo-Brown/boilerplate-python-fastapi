"""Unit tests for the policy value and the middleware that applies it."""

import pytest
from starlette.types import Message, Receive, Scope, Send

from src.config import Settings
from src.docs import DOCS_URL, REDOC_URL
from src.middleware.security_headers import (
    CONTENT_TYPE_OPTIONS_HEADER,
    CSP_HEADER,
    FRAME_OPTIONS_HEADER,
    HSTS_HEADER,
    NONCE_PLACEHOLDER,
    PRELOAD_MIN_MAX_AGE_SECONDS,
    REFERRER_POLICY_HEADER,
    HSTSPolicy,
    SecurityHeadersMiddleware,
    SecurityHeadersPolicy,
    build_security_headers_policy,
    get_security_headers_policy,
    merge_headers,
)

API_CSP = "default-src 'none'; frame-ancestors 'none'"


def make_policy(**overrides: object) -> SecurityHeadersPolicy:
    kwargs: dict[str, object] = {"content_security_policy": API_CSP}
    kwargs.update(overrides)
    return SecurityHeadersPolicy(**kwargs)  # type: ignore[arg-type]


def settings_with(**overrides: object) -> Settings:
    return Settings(
        DATABASE_URL="postgresql+asyncpg://fake:fake@localhost/fake",
        SECRET_KEY="mock-secret-key-not-a-real-one",
        **overrides,  # type: ignore[arg-type]
    )


class TestHSTSPolicy:
    def test_default_is_one_year_with_subdomains(self) -> None:
        assert HSTSPolicy().header_value == (
            f"max-age={PRELOAD_MIN_MAX_AGE_SECONDS}; includeSubDomains"
        )

    def test_subdomains_can_be_dropped(self) -> None:
        policy = HSTSPolicy(max_age_seconds=600, include_subdomains=False)
        assert policy.header_value == "max-age=600"

    def test_preload_appends_the_token(self) -> None:
        policy = HSTSPolicy(preload=True)
        assert policy.header_value.endswith("; includeSubDomains; preload")

    def test_zero_max_age_is_allowed_because_it_unsets_the_pin(self) -> None:
        # RFC 6797 §6.1.1: max-age=0 tells the browser to stop treating this
        # host as known-HSTS. Refusing it would remove the only way back out.
        policy = HSTSPolicy(max_age_seconds=0, include_subdomains=False)
        assert policy.header_value == "max-age=0"

    def test_negative_max_age_is_refused(self) -> None:
        with pytest.raises(ValueError, match="must not be negative"):
            HSTSPolicy(max_age_seconds=-1)

    def test_preload_without_subdomains_is_refused(self) -> None:
        with pytest.raises(ValueError, match="includeSubDomains"):
            HSTSPolicy(preload=True, include_subdomains=False)

    def test_preload_under_a_year_is_refused(self) -> None:
        with pytest.raises(ValueError, match="at least"):
            HSTSPolicy(preload=True, max_age_seconds=PRELOAD_MIN_MAX_AGE_SECONDS - 1)


class TestSecurityHeadersPolicy:
    def test_api_path_gets_the_api_policy(self) -> None:
        assert make_policy().csp_for("/api/v1/users", nonce="abc") == API_CSP

    def test_documentation_path_gets_the_documentation_policy(self) -> None:
        csp = make_policy().csp_for(DOCS_URL, nonce="abc")
        assert "'nonce-abc'" in csp
        assert NONCE_PLACEHOLDER not in csp

    def test_documentation_policy_without_a_nonce_leaves_an_empty_source(self) -> None:
        # Reached only when a documentation page fails through
        # ServerErrorMiddleware, which renders its own response and has no
        # nonce to hand. `'nonce-'` matches nothing, so the effect is a policy
        # that refuses every script — the safe direction.
        assert "'nonce-'" in make_policy().csp_for(REDOC_URL, nonce=None)

    def test_needs_nonce_only_for_documentation_paths(self) -> None:
        policy = make_policy()
        assert policy.needs_nonce(DOCS_URL)
        assert not policy.needs_nonce("/api/v1/users")

    def test_needs_nonce_is_false_when_the_policy_has_no_placeholder(self) -> None:
        policy = make_policy(documentation_csp="default-src 'self'")
        assert not policy.needs_nonce(DOCS_URL)

    def test_hsts_is_omitted_on_a_plaintext_request(self) -> None:
        names = dict(make_policy().headers_for(path="/health", secure=False))
        assert HSTS_HEADER not in names

    def test_hsts_is_present_on_a_tls_request(self) -> None:
        names = dict(make_policy().headers_for(path="/health", secure=True))
        assert names[HSTS_HEADER] == HSTSPolicy().header_value

    def test_hsts_is_omitted_when_the_policy_has_none(self) -> None:
        names = dict(make_policy(hsts=None).headers_for(path="/health", secure=True))
        assert HSTS_HEADER not in names

    def test_empty_frame_options_drops_the_header(self) -> None:
        names = dict(
            make_policy(frame_options="").headers_for(path="/health", secure=False)
        )
        assert FRAME_OPTIONS_HEADER not in names
        assert CSP_HEADER in names


class TestBuildFromSettings:
    def test_disabled_settings_produce_no_policy(self) -> None:
        assert (
            build_security_headers_policy(settings_with(SECURITY_HEADERS_ENABLED=False))
            is None
        )

    def test_hsts_can_be_turned_off_on_its_own(self) -> None:
        policy = build_security_headers_policy(
            settings_with(SECURITY_HSTS_ENABLED=False)
        )
        assert policy is not None
        assert policy.hsts is None

    def test_values_come_from_settings(self) -> None:
        policy = build_security_headers_policy(
            settings_with(
                SECURITY_CSP="default-src 'self'",
                SECURITY_REFERRER_POLICY="no-referrer",
                SECURITY_FRAME_OPTIONS="SAMEORIGIN",
                SECURITY_HSTS_MAX_AGE_SECONDS=120,
                SECURITY_HSTS_INCLUDE_SUBDOMAINS=False,
            )
        )
        assert policy is not None
        assert policy.content_security_policy == "default-src 'self'"
        assert policy.referrer_policy == "no-referrer"
        assert policy.frame_options == "SAMEORIGIN"
        assert policy.hsts == HSTSPolicy(max_age_seconds=120, include_subdomains=False)

    def test_a_preload_misconfiguration_fails_at_build_time(self) -> None:
        # The point of validating in HSTSPolicy: this is a failed start-up, not
        # a header that looks right and is rejected by the preload list later.
        with pytest.raises(ValueError, match="includeSubDomains"):
            build_security_headers_policy(
                settings_with(
                    SECURITY_HSTS_PRELOAD=True,
                    SECURITY_HSTS_INCLUDE_SUBDOMAINS=False,
                )
            )

    def test_process_policy_is_cached(self) -> None:
        assert get_security_headers_policy() is get_security_headers_policy()


class TestMergeHeaders:
    def test_additions_are_appended(self) -> None:
        merged = merge_headers([(b"content-type", b"application/json")], [("x-a", "1")])
        assert merged == [(b"content-type", b"application/json"), (b"x-a", b"1")]

    def test_an_existing_header_is_left_alone(self) -> None:
        merged = merge_headers(
            [(b"Content-Security-Policy", b"sandbox")],
            [("content-security-policy", "default-src 'none'")],
        )
        assert merged == [(b"Content-Security-Policy", b"sandbox")]

    def test_a_repeated_addition_is_added_once(self) -> None:
        merged = merge_headers([], [("x-a", "1"), ("x-a", "2")])
        assert merged == [(b"x-a", b"1")]


class TestMiddleware:
    async def test_non_http_scopes_pass_through_untouched(self) -> None:
        sent: list[Message] = []

        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            await send({"type": "websocket.accept"})

        middleware = SecurityHeadersMiddleware(app, policy=make_policy())

        async def send(message: Message) -> None:
            sent.append(message)

        async def receive() -> Message:  # pragma: no cover - never awaited
            return {"type": "websocket.connect"}

        await middleware({"type": "websocket", "path": "/ws"}, receive, send)
        assert sent == [{"type": "websocket.accept"}]

    async def test_a_nonce_is_minted_only_for_documentation_paths(self) -> None:
        seen: list[Scope] = []

        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            seen.append(scope)
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b""})

        middleware = SecurityHeadersMiddleware(app, policy=make_policy())

        async def send(message: Message) -> None:
            return None

        async def receive() -> Message:  # pragma: no cover - never awaited
            return {"type": "http.request"}

        await middleware(
            {"type": "http", "path": DOCS_URL, "scheme": "http"}, receive, send
        )
        await middleware(
            {"type": "http", "path": "/health", "scheme": "http"}, receive, send
        )

        assert "csp_nonce" in seen[0]["state"]
        assert "state" not in seen[1] or "csp_nonce" not in seen[1]["state"]

    async def test_headers_are_added_to_the_response_start_message(self) -> None:
        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            await send({"type": "http.response.start", "status": 204, "headers": []})

        captured: list[Message] = []

        async def send(message: Message) -> None:
            captured.append(message)

        async def receive() -> Message:  # pragma: no cover - never awaited
            return {"type": "http.request"}

        middleware = SecurityHeadersMiddleware(app, policy=make_policy())
        await middleware(
            {"type": "http", "path": "/health", "scheme": "https"}, receive, send
        )

        headers = dict(captured[0]["headers"])
        assert headers[CSP_HEADER.encode()] == API_CSP.encode()
        assert headers[CONTENT_TYPE_OPTIONS_HEADER.encode()] == b"nosniff"
        assert headers[REFERRER_POLICY_HEADER.encode()] == (
            b"strict-origin-when-cross-origin"
        )
        assert headers[FRAME_OPTIONS_HEADER.encode()] == b"DENY"
        assert HSTS_HEADER.encode() in headers
