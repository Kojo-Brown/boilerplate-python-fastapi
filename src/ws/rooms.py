"""Rooms: who receives a broadcast, and what a slow member costs everyone else.

A room is a named set of connections. Publishing to one delivers to its
members; that is the whole data structure, and every decision below is about
the failure modes rather than the lookup.

## Broadcasting never blocks and never fails

`broadcast` is a plain `def` with no awaits, the same invariant
`src/sse/hub.py` holds and for a sharper reason: here the publisher is *another
connection's* receive loop. If delivering to a member could suspend on that
member's socket, then one participant on hotel wifi would pace the room — and
every other member's inbound handling with it, because the sender's receive
loop is where the broadcast runs. A room would degrade to the speed of its
slowest reader, which is the thing chat systems are famous for getting wrong.

So delivery is an offer into the recipient's own bounded queue, and a member
with no room left is **removed from every room and closed** rather than waited
for or silently skipped. `src/sse/hub.py` argues the case against the two drop
policies at length; it applies unchanged. What is different here is that the
consequence is visible to third parties — the room's member count drops — which
is one more reason it must not be "drop the oldest and say nothing".

## Membership is a set, and joining twice is not an error

A client that sends `join` for a room it is already in gets the same `joined`
frame it got the first time and the room is unchanged. The alternative — an
error — makes every client implement "have I already joined?" state that the
server is already keeping, and gets it wrong across a reconnect. Idempotent
join is what lets a client's reconnect handler simply re-join everything it
believes it should be in.

## Room names are validated, not because of injection but because of confusion

The name is echoed to every member of the room and appears in logs. A name
containing a newline is a forged log line; a name differing from another only
in case, or in a zero-width character, is two rooms that every human involved
believes are one. `validate_room_name` admits a deliberately boring subset and
`is` the authorisation boundary's junior partner — it decides that a name is
*well-formed*, never that this caller may join it.

## Who may join a room is not decided here

`RoomRegistry` has no notion of a user. That is not an omission to be fixed by
adding one: this application's rooms are public to any authenticated caller,
and a deployment where they are not needs a policy keyed on whatever its rooms
actually mean — an organisation id, a document's ACL, a subscription tier —
which is a question about the domain and not about fan-out. The seam is the
endpoint, which calls `validate_room_name` and then `join`; a policy check goes
between those two lines, and nothing in this module has to learn about it.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Final, Protocol, runtime_checkable

import structlog

logger = structlog.get_logger(__name__)

#: Room names are lowercase alphanumerics plus `.`, `-` and `_`, one to
#: sixty-four characters, starting with an alphanumeric. Lowercase-only because
#: a case-sensitive room name is two rooms whenever one client title-cases it;
#: a leading alphanumeric because a name starting with a separator reads as a
#: namespace prefix that this application does not implement.
ROOM_NAME_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")

#: Ceiling on the name a client may send, applied *before* the pattern. The
#: pattern already bounds a valid name, but matching a megabyte-long candidate
#: against it is work an invalid message should not be able to buy.
MAX_ROOM_NAME_LENGTH: Final[int] = 64


class InvalidRoomName(ValueError):
    """The name is not one this endpoint will accept."""


@runtime_checkable
class RoomMember(Protocol):
    """What a room needs from a connection, and nothing else.

    A protocol rather than the concrete `Connection` so that a test can put a
    list-backed member in a room — which is what makes the overflow path
    testable without a socket — and so that this module has no import edge back
    to the one that uses it.

    Members are identified by object identity: two connections from the same
    account are two members and both receive a broadcast, because they are two
    browser tabs and both are showing the room.
    """

    @property
    def id(self) -> str:
        """Stable identifier for logs. Not an authorisation subject."""

    def offer(self, payload: str) -> bool:
        """Queue an encoded frame for delivery. Must not block and must not raise.

        A `str` rather than a mapping, so a broadcast serialises once for the
        room instead of once per member — and so that a payload which cannot be
        serialised fails in the sender's own loop rather than in each
        recipient's writer, where one bad message would cost every member of
        the room their connection.

        Returns `False` when the member cannot accept it and should be removed
        from every room — a terminal condition, never a transient one to retry.
        """


def validate_room_name(name: str) -> str:
    """Return `name` if this endpoint will accept it.

    Raises:
        InvalidRoomName: the name is empty, over `MAX_ROOM_NAME_LENGTH`, or
            contains anything outside `ROOM_NAME_PATTERN`.
    """
    if not name:
        raise InvalidRoomName("Room name must not be empty.")
    if len(name) > MAX_ROOM_NAME_LENGTH:
        raise InvalidRoomName(
            f"Room name may be at most {MAX_ROOM_NAME_LENGTH} characters, "
            f"got {len(name)}."
        )
    if not ROOM_NAME_PATTERN.match(name):
        raise InvalidRoomName(
            "Room name may contain only lowercase letters, digits, '.', '-' "
            "and '_', and must start with a letter or digit."
        )
    return name


class RoomRegistry:
    """Named sets of members, with a reverse index so leaving is cheap.

    The reverse index is not an optimisation. Without it, dropping a member on
    disconnect is a scan of every room in the process — which happens once per
    disconnect, and disconnects arrive in bursts exactly when a deploy or a
    network partition has made the process busiest.

    One registry per process is the normal case, and its reach is one process:
    a second replica has its own rooms, so a message published here is
    delivered to the members connected *here*. Making that cross-process is a
    broker's job — the seam is `broadcast`, which a subscriber on each replica
    can call without any connection knowing. See `docs/websockets.md`.
    """

    def __init__(self) -> None:
        self._rooms: dict[str, list[RoomMember]] = {}
        self._joined: dict[RoomMember, set[str]] = {}

    @property
    def rooms(self) -> tuple[str, ...]:
        """Rooms with at least one member."""
        return tuple(self._rooms)

    def member_count(self, room: str) -> int:
        """Members currently in `room`."""
        return len(self._rooms.get(room, ()))

    def rooms_of(self, member: RoomMember) -> frozenset[str]:
        """Rooms `member` is currently in."""
        return frozenset(self._joined.get(member, ()))

    def contains(self, room: str, member: RoomMember) -> bool:
        """Whether `member` is in `room`."""
        return room in self._joined.get(member, ())

    def join(self, room: str, member: RoomMember) -> int:
        """Add `member` to `room`. Returns the room's member count.

        Idempotent: joining a room already joined changes nothing and returns
        the same count.
        """
        joined = self._joined.setdefault(member, set())
        if room not in joined:
            joined.add(room)
            self._rooms.setdefault(room, []).append(member)
            logger.debug(
                "ws.room_joined",
                room=room,
                member=member.id,
                members=self.member_count(room),
            )
        return self.member_count(room)

    def leave(self, room: str, member: RoomMember) -> bool:
        """Remove `member` from `room`. Returns whether it was in it."""
        joined = self._joined.get(member)
        if joined is None or room not in joined:
            return False
        joined.discard(room)
        if not joined:
            del self._joined[member]
        self._remove_from_room(room, member)
        logger.debug(
            "ws.room_left", room=room, member=member.id, members=self.member_count(room)
        )
        return True

    def leave_all(self, member: RoomMember) -> frozenset[str]:
        """Remove `member` from every room. Returns the rooms it was in.

        The disconnect path. A member left in a room after its connection is
        gone is broadcast to on every publish for the life of the process, and
        each of those offers fills a queue nothing is draining.
        """
        rooms = frozenset(self._joined.pop(member, ()))
        for room in rooms:
            self._remove_from_room(room, member)
        if rooms:
            logger.debug("ws.member_removed", member=member.id, rooms=sorted(rooms))
        return rooms

    def broadcast(
        self,
        room: str,
        payload: str,
        *,
        exclude: RoomMember | None = None,
    ) -> int:
        """Offer `payload` to every member of `room`. Returns how many took it.

        Synchronous and total: no awaits, no exceptions. A count below
        `member_count(room)` means the difference were dropped for falling
        behind, which is already logged — the caller is a receive loop handling
        somebody else's message and has nothing useful to do about it.

        Args:
            exclude: A member not to deliver to, normally the sender. Excluded
                rather than filtered by the client because the sender already
                has the message, and a client that has to recognise its own
                messages needs an identity for itself that survives reconnects.
        """
        members = self._rooms.get(room)
        if not members:
            return 0
        delivered = 0
        # Over a copy: a failed offer removes the member from this very list,
        # and mutating what you are iterating skips the member after it — which
        # would silently not deliver to somebody who was perfectly healthy.
        for member in tuple(members):
            if member is exclude:
                continue
            if member.offer(payload):
                delivered += 1
            else:
                logger.warning("ws.member_overflowed", room=room, member=member.id)
                self.leave_all(member)
        return delivered

    def _remove_from_room(self, room: str, member: RoomMember) -> None:
        """Drop `member` from `room`'s list, tidying the room if it empties."""
        members = self._rooms.get(room)
        if members is None:  # pragma: no cover - the reverse index is authoritative
            # Unreachable while `_joined` and `_rooms` agree, which every
            # mutation here maintains. Kept because the cost of being wrong
            # about that in a later refactor is a `KeyError` on a disconnect
            # path, which is the worst place in this module to raise from.
            return
        if member in members:
            members.remove(member)
        # An empty list left behind is a slow leak: one entry per room that has
        # ever been used, for the life of the process.
        if not members:
            del self._rooms[room]

    def close(self, members: Iterable[RoomMember] | None = None) -> int:
        """Remove `members` — or everyone — from every room. Returns how many.

        For shutdown. The connections themselves are closed by the endpoint;
        this is what stops anything still publishing from fanning out into
        queues that will never be drained.
        """
        targets = tuple(members) if members is not None else tuple(self._joined)
        for member in targets:
            self.leave_all(member)
        return len(targets)


#: The process-wide registry the endpoint uses. Provided through a dependency
#: (`src/dependencies.py`) rather than imported into the route, so a test gets
#: its own rather than sharing one across the suite.
room_registry: Final[RoomRegistry] = RoomRegistry()


__all__ = [
    "MAX_ROOM_NAME_LENGTH",
    "ROOM_NAME_PATTERN",
    "InvalidRoomName",
    "RoomMember",
    "RoomRegistry",
    "room_registry",
    "validate_room_name",
]
