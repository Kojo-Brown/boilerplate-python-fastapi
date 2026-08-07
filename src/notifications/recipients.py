"""The one place that knows both the ORM model and the notification contract.

Keeping this mapping in its own module is what lets `base.py` and every
strategy stay free of SQLAlchemy: they take a `Recipient`, and only this
function knows a `User` exists.
"""

from __future__ import annotations

from src.models.user import User
from src.notifications.base import Recipient


def recipient_from_user(user: User) -> Recipient:
    """Project a `User` row onto the fields a strategy is allowed to see.

    Everything else on the row — the password hash, the OAuth subject, the role
    — is deliberately not carried across. A strategy has no use for it, and a
    webhook body assembled from a wider object is one refactor away from
    shipping a credential to a third-party URL.
    """
    return Recipient(
        id=str(user.id),
        channel=user.notification_channel,
        email=user.email,
        webhook_url=user.notification_webhook_url,
    )
