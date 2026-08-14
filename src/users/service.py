"""Profile updates, guarded by the client's `If-Match`.

The interesting part of this module is that it checks the precondition twice,
in two different places, and both are necessary:

1. `require_match` compares the client's tag against the row that was just
   loaded. This is the check that produces a *useful* 412 — it knows the
   current tag and can hand it back — and it is the one that fires in the
   overwhelmingly common case, where the client is simply working from an old
   copy.

2. The `UPDATE ... WHERE version = :loaded` that SQLAlchemy emits because
   `User` declares a `version_id_col`. This is the check that is actually
   *sound*. Between step 1 and the write there is a window, and under
   concurrency something will eventually land in it; the database resolving it
   is the only version of this that does not depend on timing. It surfaces as
   `StaleDataError`, which is the same failure as step 1 and gets the same 412.

Dropping either one is tempting and wrong. Without the version column, the
route has a lost-update race that no test running requests one at a time will
ever show. Without `require_match`, every stale write costs a database round
trip to discover, and the 412 cannot name the current tag.
"""

from __future__ import annotations

import structlog
from sqlalchemy.orm.exc import StaleDataError

from src.concurrency import EntityTag, IfMatch, resource_version_tag
from src.exceptions import PreconditionFailedError, UnprocessableEntityError
from src.models.user import User
from src.unit_of_work import UnitOfWork
from src.users.schemas import ProfileUpdateRequest

logger = structlog.get_logger(__name__)


def profile_etag(user: User) -> EntityTag:
    """The entity tag for a user's profile representation."""
    return resource_version_tag(user.id, user.version)


class ProfileService:
    """Reads and conditional writes of the authenticated user's own profile.

    Takes a `UnitOfWork` and nothing else. There is no `UserStore` here because
    there is no lookup to do: `get_current_user` has already loaded the row
    from this request's session in order to authenticate it, so asking a store
    for it again would issue a second query for an object we are holding —
    and, worse, could return a *different* instance whose version was read at a
    different moment.
    """

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def update(
        self,
        user: User,
        changes: ProfileUpdateRequest,
        precondition: IfMatch,
    ) -> User:
        """Apply `changes` to `user` if `precondition` still holds.

        Returns the updated row. Raises 412 if the client's tag is stale, 428
        if it sent no tag at all, and 422 for a patch that asks for nothing.
        """
        precondition.require_match(profile_etag(user))

        fields = changes.model_dump(exclude_unset=True)
        if not fields:
            raise UnprocessableEntityError(
                "Request body must contain at least one field to update"
            )

        for key, value in fields.items():
            setattr(user, key, value)

        # Read before the write, and used only in the failure path below. A
        # failed flush expires the instance, so touching `user.version`
        # afterwards asks the session to reload it — and the session is exactly
        # what is broken, so the attempt raises `PendingRollbackError` and the
        # 412 turns into a 500. Logging is not worth a live database call in an
        # error handler in any case.
        attempted_id = str(user.id)
        attempted_version = user.version

        try:
            await self._uow.commit()
        except StaleDataError as exc:
            # Someone committed between our read and our write. No ETag header
            # on this one: the session is unusable after a failed flush, so any
            # tag we could name here would be the one we already know is wrong.
            # The client re-reads, which is what it has to do anyway.
            logger.info(
                "profile.update_conflict",
                user_id=attempted_id,
                attempted_version=attempted_version,
            )
            raise PreconditionFailedError(
                "The resource was modified by another request while this "
                "update was being applied"
            ) from exc

        logger.info(
            "profile.updated",
            user_id=str(user.id),
            fields=sorted(fields),
            version=user.version,
        )
        return user
