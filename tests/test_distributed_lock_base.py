"""The parts of the contract that are pure: leases, names, and the fence check."""

from __future__ import annotations

import pytest

from src.distributed_lock.base import (
    MAX_NAME_LENGTH,
    Lease,
    LockBackendUnavailableError,
    LockLostError,
    LockNameInvalidError,
    LockState,
    LockUnavailableError,
    ReleaseOutcome,
    StaleFencingTokenError,
    fence_is_current,
    require_fence,
    validate_lock_name,
)


def a_lease(**overrides: object) -> Lease:
    defaults: dict[str, object] = {
        "name": "resource",
        "token": 7,
        "owner": "owner-a",
        "ttl_seconds": 30.0,
        "expires_at": 1000.0,
    }
    return Lease(**{**defaults, **overrides})  # type: ignore[arg-type]


class TestLeaseDeadline:
    def test_remaining_counts_down(self) -> None:
        assert a_lease().remaining(now=970.0) == 30.0

    def test_remaining_is_floored_at_zero(self) -> None:
        """Negative time left is still no time left, and a caller that
        multiplied it by a backoff factor would sleep backwards."""
        assert a_lease().remaining(now=1100.0) == 0.0

    def test_a_lease_is_expired_at_its_deadline(self) -> None:
        assert a_lease().is_expired(now=1000.0) is True

    def test_a_lease_is_live_before_its_deadline(self) -> None:
        assert a_lease().is_expired(now=999.999) is False


class TestRenewal:
    def test_renewal_moves_the_deadline(self) -> None:
        renewed = a_lease().renewed_for(ttl_seconds=60.0, expires_at=1060.0)

        assert renewed.expires_at == 1060.0
        assert renewed.ttl_seconds == 60.0

    def test_renewal_keeps_the_token(self) -> None:
        """A token that changed per renewal would fence out the very holder it
        belongs to: the resource has already accepted writes under the old one,
        and the new one is not the token those writes established."""
        original = a_lease()
        renewed = original.renewed_for(ttl_seconds=60.0, expires_at=1060.0)

        assert renewed.token == original.token
        assert renewed.owner == original.owner
        assert renewed.name == original.name

    def test_renewal_does_not_mutate_the_original(self) -> None:
        original = a_lease()
        original.renewed_for(ttl_seconds=60.0, expires_at=1060.0)

        assert original.expires_at == 1000.0

    def test_a_lease_is_frozen(self) -> None:
        with pytest.raises(AttributeError):
            a_lease().expires_at = 2000.0  # type: ignore[misc]


class TestLockNames:
    def test_an_ordinary_hierarchical_name_is_accepted(self) -> None:
        assert validate_lock_name("invoice:8f2c:settle") == "invoice:8f2c:settle"

    def test_an_empty_name_is_refused(self) -> None:
        with pytest.raises(LockNameInvalidError):
            validate_lock_name("")

    @pytest.mark.parametrize("name", ["has space", "has\nnewline", "has\ttab", "é"])
    def test_whitespace_and_non_ascii_are_refused(self, name: str) -> None:
        """Names reach a store key, a log line and an error body. Refusing them
        once beats escaping them in three places."""
        with pytest.raises(LockNameInvalidError):
            validate_lock_name(name)

    def test_a_name_at_the_limit_is_accepted(self) -> None:
        assert validate_lock_name("x" * MAX_NAME_LENGTH)

    def test_a_name_over_the_limit_is_refused(self) -> None:
        with pytest.raises(LockNameInvalidError):
            validate_lock_name("x" * (MAX_NAME_LENGTH + 1))

    def test_an_invalid_name_is_a_value_error(self) -> None:
        """Lock names are chosen by this codebase, not by a client, so a bad
        one is a defect that should surface as a 500 with a stack trace — not
        as a 4xx telling a user to fix something they never sent."""
        assert issubclass(LockNameInvalidError, ValueError)


class TestFencing:
    def test_any_token_is_current_for_an_untouched_resource(self) -> None:
        assert fence_is_current(1, None) is True

    def test_a_higher_token_is_current(self) -> None:
        assert fence_is_current(9, 8) is True

    def test_a_lower_token_is_stale(self) -> None:
        assert fence_is_current(7, 8) is False

    def test_the_same_token_twice_is_stale(self) -> None:
        """A replay of a write the resource has already applied. Applying it
        again is the double execution the lock was taken to prevent."""
        assert fence_is_current(8, 8) is False

    def test_require_fence_passes_a_current_token(self) -> None:
        require_fence(9, 8, resource="ledger")

    def test_require_fence_rejects_a_stale_token(self) -> None:
        with pytest.raises(StaleFencingTokenError) as raised:
            require_fence(7, 8, resource="ledger")

        assert raised.value.details == {
            "resource": "ledger",
            "token": 7,
            "last_accepted": 8,
        }


class TestErrorContract:
    def test_an_unavailable_lock_is_a_409(self) -> None:
        assert LockUnavailableError("held").status_code == 409

    def test_an_unavailable_lock_asks_the_client_to_retry(self) -> None:
        """Distinct from the durable 409s: a duplicate email fails identically
        forever, and a busy lock will not."""
        assert LockUnavailableError("held").headers == {"Retry-After": "1"}
        assert LockUnavailableError.error_code == "DISTRIBUTED_LOCK_UNAVAILABLE"

    def test_a_lost_lease_is_a_409(self) -> None:
        assert LockLostError("lost").status_code == 409
        assert LockLostError.error_code == "DISTRIBUTED_LOCK_LOST"

    def test_a_stale_token_is_a_409(self) -> None:
        assert StaleFencingTokenError("stale").status_code == 409

    def test_an_unreachable_backend_is_a_503(self) -> None:
        """Never downgraded to 'carry on without the lock': a section that runs
        when its coordination is unreachable is the concurrent execution the
        lock exists to prevent."""
        error = LockBackendUnavailableError()
        assert error.status_code == 503
        assert error.headers == {"Retry-After": "1"}

    def test_release_outcomes_are_readable_in_a_log(self) -> None:
        assert ReleaseOutcome.RELEASED.value == "released"
        assert ReleaseOutcome.EXPIRED.value == "expired"
        assert ReleaseOutcome.NOT_OWNER.value == "not_owner"


class TestLockState:
    def test_a_state_records_what_the_store_reported(self) -> None:
        state = LockState(name="n", token=3, owner="o", ttl_seconds=1.5)

        assert (state.name, state.token, state.owner, state.ttl_seconds) == (
            "n",
            3,
            "o",
            1.5,
        )
