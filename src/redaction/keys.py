"""Which field *names* are sensitive, and how a name is matched.

Matching is on contiguous **word runs**, not on equality and not on substrings,
because the two obvious strategies each fail in a way that gets redaction
switched off.

*Equality* misses every field that exists. Nobody logs `password`; they log
`password_hash`, `user_email`, `stripe_api_key`. A deny list of bare nouns
matches none of them and produces a redactor that passes its own unit tests and
redacts nothing in production.

*Substring* over-matches, and over-matching is not the safe direction it looks
like. `secret` eats `secretary_id`, `token` eats `tokenizer` and `tokens_used`,
`key` eats `keyspace`, `keep_alive` and every `idempotency_key` this codebase
already logs. A log where a third of the fields read `[redacted]` for no reason
is a log nobody can read during an incident, and the fix that gets applied at
3am is not a better matcher.

So a key is split into words — on underscores, on camelCase humps, and on the
digit boundaries that produce `addr1` — and a phrase matches when its words
appear as a *contiguous run* of the key's words. `user_email` and `emailAddress`
both match `email`; `passengers` does not match `pass`, because `passengers` is
one word. Multi-word phrases are what make the policy usable here: `key` is not
sensitive — this codebase logs storage keys, idempotency keys and lock names
under it — while `api key`, `secret key` and `private key` are.

Four names are deliberately **absent**, each because of a field it would have
hollowed out:

`name`
    `event_name`, `provider`/`backend` names, `consumer` and `stream` names.
    The specific names that are personal are on the list instead: `full name`,
    `first name`, `last name`, `surname`, `maiden name`.

`address`
    `ip_address` is logged on purpose (see `src/middleware/request_id.py`), and
    an email address is already caught by `email`. The postal senses are listed
    explicitly: `street address`, `postal address`, `billing address`,
    `mailing address`, `home address`.

`id`
    The join key of every log line this service writes. Redacting it does not
    protect a user, it removes the ability to follow one request.

`signature`
    The value you need in front of you when a webhook is being rejected, and a
    signature is a keyed *digest* — publishing one does not disclose the key.

The list is a floor, not a ceiling: `LOG_REDACTION_EXTRA_KEYS` widens it per
deployment. There is deliberately no setting that narrows it and none that
turns redaction off, because that switch is one hurried incident away from
being set by somebody who will not be the person who notices, a month later,
that it was never set back.
"""

from __future__ import annotations

import re
from typing import Final

#: Word boundaries inside a field name: underscores and dashes, the hump in
#: `emailAddress`, and the letter/digit seam in `addr1` or `x2Token`.
_WORD = re.compile(r"[A-Z]+(?![a-z])|[A-Z][a-z]+|[a-z]+|[0-9]+")


def words(key: str) -> tuple[str, ...]:
    """The lower-cased words of a field name, in order.

    >>> words("userEmailAddress")
    ('user', 'email', 'address')
    >>> words("STRIPE_API_KEY")
    ('stripe', 'api', 'key')
    >>> words("passengers")
    ('passengers',)
    """
    return tuple(match.group(0).lower() for match in _WORD.finditer(key))


#: Phrases whose presence as a contiguous word run makes a field sensitive.
#: Read the module docstring before adding a single-word entry: a bare noun
#: common enough to be useful is usually common enough to over-match.
SENSITIVE_PHRASES: Final[frozenset[tuple[str, ...]]] = frozenset(
    {
        # Credentials the caller chose to put in a field.
        ("password",),
        ("passwd",),
        ("passphrase",),
        ("secret",),
        ("token",),
        ("credential",),
        ("credentials",),
        ("authorization",),
        ("cookie",),
        ("session", "key"),
        ("api", "key"),
        ("apikey",),
        ("access", "key"),
        ("secret", "key"),
        ("private", "key"),
        ("signing", "key"),
        ("encryption", "key"),
        ("connection", "string"),
        ("dsn",),
        # Second factors. Short-lived, but a log is not.
        ("otp",),
        ("mfa", "code"),
        ("one", "time", "code"),
        ("recovery", "code"),
        # Identity.
        ("email",),
        ("ssn",),
        ("social", "security", "number"),
        ("national", "id", "number"),
        ("passport", "number"),
        ("drivers", "license"),
        ("driver", "license"),
        ("full", "name"),
        ("first", "name"),
        ("last", "name"),
        ("middle", "name"),
        ("maiden", "name"),
        ("surname",),
        ("given", "name"),
        ("date", "of", "birth"),
        ("birth", "date"),
        ("dob",),
        ("phone",),
        ("msisdn",),
        # Postal senses of "address", which on its own is `ip_address`.
        ("street", "address"),
        ("postal", "address"),
        ("mailing", "address"),
        ("billing", "address"),
        ("home", "address"),
        ("post", "code"),
        ("postcode",),
        ("zip", "code"),
        # Money.
        ("card", "number"),
        ("cardnumber",),
        ("pan",),
        ("cvv",),
        ("cvc",),
        ("iban",),
        ("bic",),
        ("account", "number"),
        ("routing", "number"),
        ("sort", "code"),
    }
)


class KeyPolicy:
    """The compiled deny list: "is this field name sensitive?".

    Built once per process from `SENSITIVE_PHRASES` plus whatever
    `LOG_REDACTION_EXTRA_KEYS` adds, then asked a few times per log line. The
    answers are cached on the instance because field names repeat: a service
    emits the same twenty or so keys for its whole life, so the second request
    pays a dict lookup rather than a scan.
    """

    def __init__(self, phrases: frozenset[tuple[str, ...]] = SENSITIVE_PHRASES) -> None:
        self._phrases = phrases
        self._longest = max((len(phrase) for phrase in phrases), default=0)
        self._cache: dict[str, bool] = {}

    def widened_with(self, extra: str) -> KeyPolicy:
        """This policy plus the comma-separated names in `extra`.

        Each entry is split the same way a field name is, so a deployment can
        write `LOG_REDACTION_EXTRA_KEYS=employeeNumber,badge_id` and have both
        spellings behave like the entries above. Empty entries are dropped so
        that a trailing comma is not a phrase matching everything.
        """
        added = {
            parsed for entry in extra.split(",") if (parsed := words(entry.strip()))
        }
        if not added:
            return self
        return KeyPolicy(self._phrases | added)

    def is_sensitive(self, key: str) -> bool:
        """Whether `key` contains a sensitive phrase as a contiguous word run."""
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        verdict = self._scan(key)
        self._cache[key] = verdict
        return verdict

    def _scan(self, key: str) -> bool:
        parts = words(key)
        for start in range(len(parts)):
            for length in range(1, min(self._longest, len(parts) - start) + 1):
                if parts[start : start + length] in self._phrases:
                    return True
        return False


__all__ = ["SENSITIVE_PHRASES", "KeyPolicy", "words"]
