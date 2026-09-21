# Refresh-token reuse detection

A refresh token is a bearer credential with a long life. That is the point of
it — the access token expires in thirty minutes so that a leaked one is worth
little, and the refresh token exists so the user does not have to type a
password every thirty minutes. It also means a stolen refresh token is worth a
great deal, and that nothing about the request presenting it distinguishes the
thief from the owner. Both copies are the same bytes.

So the question this code answers is not "was this token stolen?", which is
unanswerable, but a narrower one that is: **has this token been used twice?**

## Rotation is what makes the question askable

`POST /api/v1/auth/refresh` does not hand back the token it was given. It
revokes it and issues a successor. A refresh token is therefore single-use, and
a login leaves behind a *chain*:

```
login ─→ T1 ──refresh──→ T2 ──refresh──→ T3
         (rotated)       (rotated)       (live)
```

Exactly one link is live. That is the invariant everything below rests on,
because it makes a second use of `T2` a detectable event rather than an
ordinary request: the only way anyone can still be holding `T2` after the
rotation is if it was copied before it was spent.

Rotation alone, though, produces an unhelpful outcome. Suppose an attacker
steals `T2`:

| | victim | attacker |
|---|---|---|
| attacker refreshes first | still holds `T2` | gets `T3`, and `T4`, and `T5`… |
| victim refreshes | **401**, signed out | unaffected, session continues |

The theft is *visible* at the moment the victim's request arrives — the server
knows a spent token just came back — and rotation on its own responds by
refusing the victim and leaving the attacker with a working session that
refreshes itself indefinitely. It has detected the breach and then acted
against the wrong party.

## Families

The server cannot tell which of the two requests came from the account owner.
The attacker's copy is byte-identical, arrives over the same TLS, and may well
arrive from a similar address. Any rule that picks a winner picks wrong half the
time, and it picks wrong in the direction the attacker controls: whoever
refreshes *first* looks legitimate, and an attacker polling every thirty seconds
always refreshes first.

[RFC 9700 §4.14.2][rfc] resolves this by refusing to guess. When a spent token
is replayed, revoke the entire authorization grant and make both parties
authenticate again. The owner loses a session and types a password. The
attacker loses everything, having no password to type.

`refresh_tokens.family_id` is that grant. A login mints a fresh one; every
rotation copies its parent's unchanged. The chain above is one family, and it
is the unit the mitigation acts on:

```
replay of T2 ─→ revoke every live token where family_id = F ─→ 401
```

The column is not derived from anything on the token, and it is deliberately
**not** a claim in the JWT — a grant id a client can read is one an attacker can
correlate across stolen tokens, and it buys nothing, since the server looks the
row up by token string on every refresh anyway. It is also not a foreign key to
the first token in the chain, though that is what it identifies:
`delete_expired` hard-deletes rows, and a family whose root has been swept is
still a family.

## What counts, and what does not

A detector that fires on every failed refresh is not a detector. It is a way for
anyone holding one long-expired token to sign a user out of a live session, and
the noise buries the real signal. Only one condition triggers revocation:

| presented token | response | family revoked? |
|---|---|---|
| live, unexpired | 200, rotated | no |
| **revoked** (rotated or logged out) | 401 | **yes** |
| expired | 401 | no |
| never issued / forged | 401 | no |
| an *access* token | 401 | no |

An expired token is not evidence of anything: it was never redeemed, so nothing
about it suggests a second holder, and revoking on it would end a live session
because a user came back from holiday. A token that was never issued names no
family to revoke. An access token is rejected on its `type` claim before any
lookup happens.

**Logout is the interesting asymmetry.** Logging out revokes the family — not
just the presented row, which is what it used to do and which could leave a
successor alive if a refresh raced the logout. A logout presented with an
already-revoked token is *not* treated as reuse: a client retrying a request it
was not sure landed is the expected shape of that call. But *refreshing* with a
logged-out token is reuse, and is treated as such. The distinction is between
asking to end a session that has already ended, and asking to extend one with a
credential that was spent.

## The 401 says nothing

A replay and a token that was never issued get the same message. Distinguishing
them would tell an attacker whether a guessed token is real and whether their
replay landed. The detail goes to the event and the logs, where the account
owner's side of it can be acted on.

## The revocation is committed before the refusal

This is the part that is easy to get wrong and impossible to notice, so it is
worth stating plainly. `get_db` closes its session **without committing**.
A revocation still pending when `UnauthorizedError` propagates out of
`refresh()` is rolled back on the way out of the request — and the response is a
401 either way. The mitigation would appear to work, in every test that asserts
a status code, while doing nothing at all.

`AuthService._handle_reuse` therefore commits before it raises, and
`test_reuse_commits_the_revocation_before_refusing` is there to keep it that
way. (The tests for this file are mutation-checked: removing that commit, or
revoking the single token instead of the family, or letting rotation mint a
fresh family, each fails the suite.)

## The event

`RefreshTokenReuseDetected` is published — into the transactional outbox, before
the commit, so the alert and the revocation land together or not at all. It
carries `user_id`, `family_id` and `sessions_revoked`.

It is not a `UserEvent`, though it names a user. That base requires an `email`,
and `_handle_reuse` has a token row rather than an account; inheriting would
mean adding a database round trip to the one path whose rate an attacker
chooses, to fill a field that [`src/events/catalog.py`][catalog] already says
belongs on the subscriber's side of the outbox. A subscriber that wants to mail
the owner ("you have been signed out everywhere, and here is why") loads the
account itself, in its own session.

`sessions_revoked` is usually 1. A larger number means tokens were being issued
in parallel on one grant and is worth alerting on by itself. Zero means the
family was already dead — a second replay, or one after a logout — which is
still an incident worth recording, and says plainly that nothing was cut off
this time.

## Provenance

`revoked` is the flag that decides whether a token works. `revoked_at` and
`revoked_reason` are evidence, never the check. The reason is one of
`"rotated"`, `"logout"` or `"reuse_detected"`, and a family revocation leaves
already-revoked members exactly as they are — restamping them would erase the
ordinary rotation that a replay is only recognisable *against*, which is the one
fact an incident review needs from those rows.

Rows revoked before migration 0006 carry a NULL reason. The reason is not
recoverable for them, and inventing one would put fabricated provenance in the
table an incident is read from.

## What this does not do

- **It does not detect theft that is never used twice.** An attacker who steals
  a token and refreshes with it *instead of* the victim — because the victim's
  device is off, or because the attacker also stole the session — rotates
  cleanly and is never seen. Reuse detection is a tripwire, not a lock.
- **It does not bound the damage before the second use.** Everything between the
  theft and the replay is the attacker's. Short access-token lifetimes are what
  limit that window, and they are a separate setting.
- **It is per-grant, not per-account.** A theft on a laptop does not sign the
  same person out on their phone; that is a different login with a different
  family, and nothing about the replay implicates it. Revoking every session an
  account has is `revoke_all_for_user`, which is a deliberate administrative
  act.

## Where the code is

| | |
|---|---|
| Policy | `src/auth/service.py` — `refresh`, `_handle_reuse`, `logout` |
| Storage | `src/repositories/refresh_token.py` — `revoke_family` |
| Port | `src/repositories/protocols.py` — `RefreshTokenStore` |
| Columns | `src/models/refresh_token.py`, migration `0006` |
| Event | `src/events/catalog.py` — `RefreshTokenReuseDetected` |
| Tests | `tests/test_refresh_token_reuse.py` |

[rfc]: https://www.rfc-editor.org/rfc/rfc9700.html#section-4.14.2
[catalog]: ../src/events/catalog.py
