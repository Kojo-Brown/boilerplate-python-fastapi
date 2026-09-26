#!/usr/bin/env python
"""Print a fresh shared secret as a `WEBHOOK_SIGNING_SECRETS` entry.

A webhook secret is agreed with a counterparty rather than generated inside one
system, so it usually gets typed — which is how a production endpoint ends up
authenticated by something memorable. This emits 32 bytes from the OS CSPRNG as
URL-safe base64, which matters beyond taste: `WEBHOOK_SIGNING_SECRETS` separates
its entries with commas and standard base64's alphabet includes `+` and `/`,
neither of which is a problem, while a hand-picked passphrase containing a comma
would silently be read as two entries.

Usage::

    uv run python scripts/generate_webhook_secret.py
    uv run python scripts/generate_webhook_secret.py --key-id 2026-09

The output is a secret. It goes into a secret store, into the deployment's
environment and to the counterparty over a channel that is not email — never
into a file in this repository, and never into a shell history, which is why it
is printed rather than written anywhere.

Rotation is: add the new entry alongside the old one, have the sender switch (or
sign under both, which the scheme allows), confirm from the `key_id` on the
`webhook.verified` log line that nothing is still arriving under the old secret,
then drop it. `docs/webhook-signatures.md` has the detail.
"""

from __future__ import annotations

import argparse
import datetime as dt
import secrets
import sys

from src.webhooks.secrets import SigningSecret

#: 256 bits. The HMAC is SHA-256, so more key than that buys nothing.
SECRET_BYTES = 32


def _default_key_id() -> str:
    """Today's month. A secret's id should say when it started, not what it is."""
    return dt.datetime.now(dt.UTC).strftime("%Y-%m")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--key-id",
        default=_default_key_id(),
        help="Identifier for the new secret (default: the current year and month).",
    )
    args = parser.parse_args(argv)

    # Construct the secret rather than only formatting it: `SigningSecret` is
    # where the id and the length are validated, so a value this script would
    # happily print but the application would refuse fails here instead.
    secret = SigningSecret(
        key_id=args.key_id, secret=secrets.token_urlsafe(SECRET_BYTES)
    )
    print(f"{secret.key_id}:{secret.secret}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
