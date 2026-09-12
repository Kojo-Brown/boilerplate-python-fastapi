"""The endpoint and header parsing that every signal's exporter is built from.

Small surface, and the two mistakes it exists to prevent are both silent: an
OTLP base URL used verbatim (the collector answers 404 per batch and nothing
else looks wrong), and a credential split on the wrong `=`.
"""

import pytest

from src.observability.exporters import (
    SIGNAL_PATHS,
    parse_otlp_headers,
    signal_endpoint,
)


class TestSignalEndpoint:
    @pytest.mark.parametrize(
        ("signal", "expected"),
        [
            ("traces", "http://collector:4318/v1/traces"),
            ("metrics", "http://collector:4318/v1/metrics"),
            ("logs", "http://collector:4318/v1/logs"),
        ],
    )
    def test_the_signal_path_is_appended_to_the_base(
        self, signal: str, expected: str
    ) -> None:
        assert signal_endpoint("http://collector:4318", signal) == expected

    def test_a_trailing_slash_does_not_double_up(self) -> None:
        assert (
            signal_endpoint("http://collector:4318/", "traces")
            == "http://collector:4318/v1/traces"
        )

    def test_surrounding_whitespace_is_ignored(self) -> None:
        assert (
            signal_endpoint("  http://collector:4318  ", "logs")
            == "http://collector:4318/v1/logs"
        )

    def test_a_path_prefix_is_preserved(self) -> None:
        # A collector behind a reverse proxy at /otlp is a normal deployment,
        # and the signal path hangs off whatever prefix it was given.
        assert (
            signal_endpoint("https://gateway.test/otlp", "metrics")
            == "https://gateway.test/otlp/v1/metrics"
        )

    def test_an_unknown_signal_is_refused(self) -> None:
        with pytest.raises(ValueError, match="Unknown OTLP signal"):
            signal_endpoint("http://collector:4318", "profiles")

    def test_an_empty_endpoint_is_refused_rather_than_defaulted(self) -> None:
        # Falling back to the SDK's localhost default would export to whatever
        # happens to be listening on the node.
        with pytest.raises(ValueError, match="OTEL_EXPORTER_OTLP_ENDPOINT is empty"):
            signal_endpoint("   ", "traces")

    def test_every_signal_this_package_builds_has_a_path(self) -> None:
        assert set(SIGNAL_PATHS) == {"traces", "metrics", "logs"}


class TestParseOtlpHeaders:
    def test_a_single_pair(self) -> None:
        assert parse_otlp_headers("api-key=secret") == {"api-key": "secret"}

    def test_several_pairs_with_whitespace(self) -> None:
        assert parse_otlp_headers(" api-key = secret , x-tenant = 7 ") == {
            "api-key": "secret",
            "x-tenant": "7",
        }

    def test_a_value_may_contain_the_separator(self) -> None:
        # Base64 pads with '='. Splitting on every separator instead of the
        # first truncates exactly the credentials that need it.
        assert parse_otlp_headers("authorization=Basic dXNlcjpwYXNz==") == {
            "authorization": "Basic dXNlcjpwYXNz=="
        }

    def test_an_empty_string_is_no_headers(self) -> None:
        assert parse_otlp_headers("") == {}

    def test_a_pair_without_a_separator_is_dropped(self) -> None:
        assert parse_otlp_headers("api-key=secret,nonsense") == {"api-key": "secret"}

    def test_a_pair_with_an_empty_name_is_dropped(self) -> None:
        assert parse_otlp_headers("=orphaned,api-key=secret") == {"api-key": "secret"}

    def test_an_empty_value_is_kept(self) -> None:
        # A header the collector requires to be present but empty is legal;
        # dropping it would be a different request from the one configured.
        assert parse_otlp_headers("x-flag=") == {"x-flag": ""}

    def test_a_later_pair_wins(self) -> None:
        assert parse_otlp_headers("k=first,k=second") == {"k": "second"}
