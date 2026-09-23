#!/usr/bin/env python
"""Print a fresh AES-256 key as an `ENCRYPTION_KEYS` entry.

The alternative to a script is an operator reaching for whatever is on hand,
and the things on hand are mostly wrong: `openssl rand -hex 32` produces 64
characters that base64-decode to 48 bytes of nonsense, a password manager's
"generate" gives a printable string whose entropy is well under 256 bits, and
`uuid4()` gives 122 bits dressed up as 128. This emits 32 bytes from the OS
CSPRNG, standard-base64 encoded, in exactly the format `ENCRYPTION_KEYS` parses.

Usage::

    uv run python scripts/generate_encryption_key.py
    uv run python scripts/generate_encryption_key.py --key-id 2026-09

The output is a secret. It goes into a secret store and into the deployment's
environment, never into a file in this repository — and never into a shell
history, which is why it is printed rather than written anywhere.

Rotation is three steps and the order matters; `docs/field-encryption.md` has
the reasons. In short: publish the new key alongside the old one, then move
`ENCRYPTION_ACTIVE_KEY_ID`, then re-encrypt and only then drop the old key.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import secrets
import sys

from src.encryption.keys import KEY_BYTES, DataKey


def _default_key_id() -> str:
    """Today's month. A key's id should say when it started, not what it is."""
    return dt.datetime.now(dt.UTC).strftime("%Y-%m")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--key-id",
        default=_default_key_id(),
        help="Identifier for the new key (default: the current year and month).",
    )
    args = parser.parse_args(argv)

    material = secrets.token_bytes(KEY_BYTES)
    # Construct the key rather than only formatting it: `DataKey` is where the
    # id is validated, so an id this script would happily print but the
    # application would refuse at start-up fails here instead.
    key = DataKey(key_id=args.key_id, material=material)
    print(f"{key.key_id}:{base64.b64encode(material).decode('ascii')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
