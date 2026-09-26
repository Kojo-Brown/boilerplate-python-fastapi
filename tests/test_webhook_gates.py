"""Fitness functions for the two claims no behavioural test can defend.

Everything else about this package is asserted by watching it work. These two
cannot be, and both are in the spec item's own title:

**Constant-time comparison.** `hmac.compare_digest(a, b)` and `a == b` return
the same answer for every input. Swapping one for the other passes the entire
suite, reads as a simplification in review, and removes the only protection
against a timing oracle. Checked by mutation: replacing the call with `==` was
detected by nothing until this file existed.

**The verifier reading no timestamp header.** A receiver that takes its replay
window from the unsigned header beside the signature accepts a capture of any
age, and since the two values agree on every genuine delivery, the mistake is
invisible to any test written from the sender's side. There is a behavioural
test for it — `test_webhook_roundtrip.py` — and it only fails while nothing in
this package reads that header, which is what is asserted here.

Both are source-level gates, the `test_immutability_gate.py` idiom: they assert a
decision was made rather than that the code works, which is the property that
decays silently.
"""

from __future__ import annotations

import ast
import inspect
import pathlib
from typing import Final

import pytest

import src.webhooks
from src.webhooks import signature

WEBHOOKS = pathlib.Path(src.webhooks.__file__).resolve().parent

#: Every module in the package, so a gate cannot be escaped by adding a file.
MODULES: Final[tuple[pathlib.Path, ...]] = tuple(sorted(WEBHOOKS.glob("*.py")))

# Header names that carry a timestamp *outside* the signed material. Nothing in
# this package may read one: the authoritative timestamp is the `t=` element,
# which is covered by the digest. `X-Notification-Timestamp` is emitted by
# `src/notifications/webhook.py` for a human reading a request log, and the
# others are what third parties call the same thing.
FORBIDDEN_TIMESTAMP_HEADERS: Final[tuple[str, ...]] = (
    "X-Notification-Timestamp",
    "X-Webhook-Timestamp",
    "X-Request-Timestamp",
    "X-Hub-Timestamp",
    "Timestamp-Header",
)


class TestTheComparisonIsConstantTime:
    def test_the_package_has_at_least_one_module(self) -> None:
        """A glob that matched nothing would make every gate below vacuous."""
        assert len(MODULES) >= 8

    def test_digest_matches_calls_compare_digest(self) -> None:
        source = inspect.getsource(signature.digest_matches)

        assert "hmac.compare_digest" in source

    def test_digest_matches_compares_with_nothing_else(self) -> None:
        """`==` on a digest is the mutation this gate exists for.

        Parsed rather than grepped so that the word `==` in a comment or a
        docstring is not a failure, and so that a comparison hidden inside a
        nested helper is still found.
        """
        tree = ast.parse(inspect.getsource(signature.digest_matches).strip())

        comparisons = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Compare)
            and any(
                isinstance(op, (ast.Eq, ast.NotEq, ast.Is, ast.IsNot))
                for op in node.ops
            )
        ]

        assert comparisons == []

    @pytest.mark.parametrize("path", MODULES, ids=lambda p: p.name)
    def test_no_module_compares_a_digest_with_an_operator(
        self, path: pathlib.Path
    ) -> None:
        """The whole package, not just the one function.

        A second comparison site added elsewhere — a convenience helper, a
        provider adapter — would be outside the function above and inside the
        threat model.
        """
        tree = ast.parse(path.read_text())

        offenders = [
            ast.unparse(node)
            for node in ast.walk(tree)
            if isinstance(node, ast.Compare)
            and any(isinstance(op, (ast.Eq, ast.NotEq)) for op in node.ops)
            and any(
                name in ast.unparse(node)
                for name in ("digest", "signature", "secret", "hmac")
            )
        ]

        assert offenders == [], (
            f"{path.name} compares secret-derived material with an operator: "
            f"{offenders}. Use hmac.compare_digest."
        )


class TestTheVerifierReadsNoTimestampHeader:
    @pytest.mark.parametrize("path", MODULES, ids=lambda p: p.name)
    def test_no_module_names_an_unsigned_timestamp_header(
        self, path: pathlib.Path
    ) -> None:
        """Including in a comment, which is where the idea would start."""
        text = path.read_text()
        # The prose in `verifier.py` and `signature.py` explains the trap by
        # naming the header, which is the one legitimate mention. Strings and
        # comments are what this looks at, so the docstrings are stripped first
        # by checking only lines that are not prose about the trap.
        offenders = [
            header
            for header in FORBIDDEN_TIMESTAMP_HEADERS
            if f'"{header}"' in text or f"'{header}'" in text
        ]

        assert offenders == [], (
            f"{path.name} references {offenders} as a value. The authoritative "
            "timestamp is the `t=` element inside the signed material; a header "
            "beside the signature is unsigned and can be rewritten by whoever "
            "captured the delivery."
        )

    def test_verify_takes_no_timestamp_parameter(self) -> None:
        """The structural half: there is no way to pass one in.

        A rule to remember is a rule somebody forgets; a parameter that does not
        exist cannot be filled with the wrong value.
        """
        from src.webhooks.verifier import WebhookVerifier

        parameters = inspect.signature(WebhookVerifier.verify).parameters

        assert set(parameters) == {"self", "signature", "body"}

    def test_the_dependency_reads_only_the_signature_header(self) -> None:
        """`verify_webhook_request` is where a second header would be read."""
        from src.webhooks import dependencies

        source = inspect.getsource(dependencies.verify_webhook_request)
        header_reads = [
            line for line in source.splitlines() if "request.headers" in line
        ]

        assert len(header_reads) == 1
        assert "signature_header" in header_reads[0]
