"""Sensitive *values*, found by shape, and only where the shape can be checked.

The key policy next door covers the fields somebody chose to log. It cannot
cover the ones nobody chose, and those are how secrets actually reach a log:

    logger.warning("payment.rejected", error=str(exc))

where `exc` is an upstream 401 quoting the `Authorization` header it refused, or
a validation error quoting the body it rejected. `error` must never become a
sensitive key — it is the field an incident is read through — so the string
under it is examined instead.

**Every detector here validates before it redacts.** A JWT's header is
base64url-decoded and has to be a JSON object naming an algorithm; a card number
has to pass Luhn; an IBAN has to pass the ISO 13616 mod-97 check. That is what
keeps a sixteen-digit order id, an epoch-millis timestamp and a request id out
of the marker. Detectors with nothing to check — "looks like a phone number",
"looks like a name" — are deliberately absent: their false positives land on
exactly the numbers an incident is being read for, and a marker that shows up
where nothing was ever sensitive teaches people that `[redacted]` means "ignore
this", which is the belief that makes the real ones invisible.

Matches are replaced **in place** rather than swallowing the whole string. A
message that reads

    upstream rejected token <a JWT> for user@example.com

becomes `upstream rejected token [redacted] for [redacted]`, which still says
what happened. Replacing the value wholesale would remove the sentence along
with the secret, and the field would stop being worth reading.

One detector is not about shape at all. `k=v` pairs are scanned with the *key*
policy, because `src/middleware/request_id.py` logs `query=` on every single
request and a query string is a sequence of named fields that happens to be
spelled as one value. `?token=...&email=...` is not a string that looks
dangerous; it is two fields wearing a disguise.
"""

from __future__ import annotations

import base64
import binascii
import json
import re
from collections.abc import Callable
from typing import Final

from src.redaction.keys import KeyPolicy

#: What replaces a redacted span. One spelling everywhere, so that a log can be
#: grepped for it and an alert can count it.
REDACTED: Final[str] = "[redacted]"

# --- validators ---------------------------------------------------------------


def _luhn_ok(digits: str) -> bool:
    """The Luhn check digit, as ISO/IEC 7812-1 defines it."""
    total = 0
    for index, character in enumerate(reversed(digits)):
        value = ord(character) - 48
        if index % 2:
            value *= 2
            if value > 9:
                value -= 9
        total += value
    return total % 10 == 0


def _iban_ok(candidate: str) -> bool:
    """The ISO 13616 mod-97 check: move the country prefix to the end, and the
    whole thing read as a base-36-ish integer must be congruent to 1 mod 97."""
    rearranged = candidate[4:] + candidate[:4]
    digits = "".join(
        str(ord(character) - 55) if character.isalpha() else character
        for character in rearranged
    )
    return int(digits) % 97 == 1


def _jwt_header_ok(segment: str) -> bool:
    """Whether a JWT's first segment really is a JOSE header.

    `eyJ` is just the base64url of `{"`, which any base64url-encoded JSON object
    starts with — including a perfectly innocuous encoded payload. Decoding it
    and insisting on an `alg` is the difference between "starts like a token"
    and "is one".
    """
    padded = segment + "=" * (-len(segment) % 4)
    try:
        header = json.loads(base64.urlsafe_b64decode(padded))
    except (binascii.Error, UnicodeDecodeError, ValueError):
        return False
    return isinstance(header, dict) and "alg" in header


# --- detectors ----------------------------------------------------------------

#: A PEM private key, replaced whole. The armour is matched non-greedily so
#: that two keys in one string do not collapse into a single span with whatever
#: sat between them.
_PEM = re.compile(
    r"-----BEGIN (?:[A-Z0-9 ]+ )?PRIVATE KEY-----.*?"
    r"-----END (?:[A-Z0-9 ]+ )?PRIVATE KEY-----",
    re.DOTALL,
)

_JWT = re.compile(r"\beyJ[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*")

#: `Authorization: Bearer ...`, and the two other schemes that carry the
#: credential inline. The scheme is kept: knowing the caller sent Basic where
#: Bearer was expected is most of a 401 diagnosis.
_AUTH_SCHEME = re.compile(
    r"\b(Bearer|Basic|Token|ApiKey)\s+([A-Za-z0-9._~+/=-]{8,})",
    re.IGNORECASE,
)

#: The password in a URL's userinfo, which is how a connection string leaks.
#: Only the password is replaced — the scheme, user and host are what make the
#: line worth having.
_URL_PASSWORD = re.compile(r"(?<![\w.-])([a-zA-Z][\w+.-]*://[^\s:/?#@]+):([^\s/?#@]+)@")

#: `name=value`, anywhere in a string. The name decides, not the value.
_PAIR = re.compile(r"(?<![\w.%-])([A-Za-z0-9_.\[\]-]+)=([^&\s\"']+)")

_EMAIL = re.compile(
    r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}\b"
)

_IBAN = re.compile(r"\b[A-Z]{2}[0-9]{2}[A-Z0-9]{11,30}\b")

#: 13 to 19 digits, optionally grouped by single spaces or hyphens, not glued to
#: another digit on either side. Luhn does the rest.
_PAN = re.compile(r"(?<![\d-])(?:\d[ -]?){12,18}\d(?![\d-])")


def _sub_jwt(match: re.Match[str]) -> str:
    text = match.group(0)
    return REDACTED if _jwt_header_ok(text.split(".", 1)[0]) else text


def _sub_iban(match: re.Match[str]) -> str:
    text = match.group(0)
    return REDACTED if _iban_ok(text) else text


def _sub_pan(match: re.Match[str]) -> str:
    text = match.group(0)
    digits = text.replace(" ", "").replace("-", "")
    return REDACTED if _luhn_ok(digits) else text


class ValuePolicy:
    """The value detectors, bound to the key policy the `name=value` pass needs.

    Order is not arbitrary. The armoured block goes first, because a PEM body
    contains base64 that the narrower patterns would otherwise pick at; the
    `name=value` pass runs before the shape detectors so that `token=1234` is
    redacted for its name even though its value is shapeless; and every
    replacement writes `[redacted]`, which contains no digit, no `@` and no dot,
    so a later pass cannot find a second match inside the first one's output.
    """

    def __init__(self, keys: KeyPolicy) -> None:
        self._keys = keys
        self._passes: tuple[
            tuple[re.Pattern[str], str | Callable[[re.Match[str]], str]], ...
        ] = (
            (_PEM, REDACTED),
            (_URL_PASSWORD, rf"\1:{REDACTED}@"),
            (_AUTH_SCHEME, rf"\1 {REDACTED}"),
            (_PAIR, self._sub_pair),
            (_JWT, _sub_jwt),
            (_EMAIL, REDACTED),
            (_IBAN, _sub_iban),
            (_PAN, _sub_pan),
        )

    def _sub_pair(self, match: re.Match[str]) -> str:
        name = match.group(1)
        if not self._keys.is_sensitive(name):
            return match.group(0)
        return f"{name}={REDACTED}"

    def scrub(self, text: str) -> str:
        """`text` with every validated sensitive span replaced."""
        for pattern, replacement in self._passes:
            text = pattern.sub(replacement, text)
        return text


__all__ = ["REDACTED", "ValuePolicy"]
