import os

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")

from argon2 import PasswordHasher

from src.auth.password import hash_password, needs_rehash, verify_password


def test_hash_password_is_not_plaintext() -> None:
    plain = "hunter2"
    hashed = hash_password(plain)
    assert hashed != plain


def test_hash_password_starts_with_argon2id() -> None:
    hashed = hash_password("any-password")
    assert hashed.startswith("$argon2id$")


def test_hash_password_is_unique_per_call() -> None:
    plain = "same-password"
    assert hash_password(plain) != hash_password(plain)


def test_verify_password_correct_returns_true() -> None:
    plain = "correct-horse-battery-staple"
    hashed = hash_password(plain)
    assert verify_password(plain, hashed) is True


def test_verify_password_wrong_returns_false() -> None:
    hashed = hash_password("correct-password")
    assert verify_password("wrong-password", hashed) is False


def test_verify_password_invalid_hash_returns_false() -> None:
    assert verify_password("anything", "not-a-valid-hash") is False


def test_verify_password_empty_plain_against_valid_hash() -> None:
    hashed = hash_password("nonempty")
    assert verify_password("", hashed) is False


def test_verify_password_empty_plain_hashed() -> None:
    hashed = hash_password("")
    assert verify_password("", hashed) is True


def test_needs_rehash_fresh_hash_returns_false() -> None:
    hashed = hash_password("password")
    assert needs_rehash(hashed) is False


def test_needs_rehash_weak_params_returns_true() -> None:
    # Hash with weaker parameters than the configured defaults
    weak_ph = PasswordHasher(time_cost=1, memory_cost=8, parallelism=1)
    weak_hash = weak_ph.hash("password")
    assert needs_rehash(weak_hash) is True


def test_hash_length_is_reasonable() -> None:
    hashed = hash_password("test")
    # Argon2id hashes are typically 95+ chars with standard params
    assert len(hashed) >= 50


def test_verify_unicode_password() -> None:
    plain = "pässwörد123"
    hashed = hash_password(plain)
    assert verify_password(plain, hashed) is True
    assert verify_password("passw0rd123", hashed) is False
