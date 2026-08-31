"""The room registry: fan-out, membership bookkeeping, and the slow member.

The member here is a list, not a socket. That is what makes the overflow path
— the one a healthy integration test never reaches — an ordinary assertion:
`RefusingMember` simply says no, and the question under test is what the
registry does about it.
"""

from __future__ import annotations

import pytest

from src.ws.rooms import (
    MAX_ROOM_NAME_LENGTH,
    InvalidRoomName,
    RoomMember,
    RoomRegistry,
    validate_room_name,
)


class RecordingMember:
    """A member that keeps everything offered to it."""

    def __init__(self, name: str) -> None:
        self._name = name
        self.received: list[str] = []

    @property
    def id(self) -> str:
        return self._name

    def offer(self, payload: str) -> bool:
        self.received.append(payload)
        return True


class RefusingMember(RecordingMember):
    """A member that has run out of room. The condition, without a socket."""

    def offer(self, payload: str) -> bool:
        return False


@pytest.fixture
def registry() -> RoomRegistry:
    return RoomRegistry()


@pytest.fixture
def alice() -> RecordingMember:
    return RecordingMember("alice")


@pytest.fixture
def bob() -> RecordingMember:
    return RecordingMember("bob")


class TestRoomNames:
    @pytest.mark.parametrize(
        "name", ["lobby", "a", "room-1", "team.eng", "a_b", "0", "x" * 64]
    )
    def test_a_boring_name_is_accepted(self, name: str) -> None:
        assert validate_room_name(name) == name

    def test_an_empty_name_is_refused(self) -> None:
        with pytest.raises(InvalidRoomName, match="must not be empty"):
            validate_room_name("")

    def test_a_name_over_the_ceiling_is_refused(self) -> None:
        with pytest.raises(InvalidRoomName, match="at most"):
            validate_room_name("x" * (MAX_ROOM_NAME_LENGTH + 1))

    def test_a_newline_in_a_name_would_forge_a_log_line(self) -> None:
        """The name reaches structlog and every member of the room."""
        with pytest.raises(InvalidRoomName):
            validate_room_name("lobby\nlevel=critical")

    @pytest.mark.parametrize(
        "name",
        [
            "Lobby",  # two rooms every human involved believes are one
            "lobby room",
            "lobby/../admin",
            "-leading",
            ".leading",
            "emoji🎉",
            "zero​width",
            "nul\x00",
        ],
    )
    def test_a_confusable_or_structured_name_is_refused(self, name: str) -> None:
        with pytest.raises(InvalidRoomName):
            validate_room_name(name)


class TestMembership:
    def test_joining_puts_a_member_in_a_room(
        self, registry: RoomRegistry, alice: RecordingMember
    ) -> None:
        assert registry.join("lobby", alice) == 1
        assert registry.contains("lobby", alice)
        assert registry.rooms == ("lobby",)

    def test_joining_twice_changes_nothing(
        self, registry: RoomRegistry, alice: RecordingMember
    ) -> None:
        """Idempotent, so a client's reconnect handler can just re-join.

        An error instead would make every client keep the membership state the
        server is already keeping — and get it wrong across a reconnect.
        """
        registry.join("lobby", alice)

        assert registry.join("lobby", alice) == 1
        assert registry.member_count("lobby") == 1

    def test_two_connections_from_one_account_are_two_members(
        self, registry: RoomRegistry
    ) -> None:
        """Two tabs, both showing the room, both wanting the message."""
        tab_one, tab_two = RecordingMember("u"), RecordingMember("u")
        registry.join("lobby", tab_one)
        registry.join("lobby", tab_two)

        assert registry.member_count("lobby") == 2

    def test_leaving_removes_the_member_and_reports_it_did(
        self, registry: RoomRegistry, alice: RecordingMember
    ) -> None:
        registry.join("lobby", alice)

        assert registry.leave("lobby", alice) is True
        assert not registry.contains("lobby", alice)

    def test_leaving_a_room_never_joined_reports_so(
        self, registry: RoomRegistry, alice: RecordingMember
    ) -> None:
        assert registry.leave("lobby", alice) is False

    def test_an_emptied_room_is_forgotten(
        self, registry: RoomRegistry, alice: RecordingMember
    ) -> None:
        """An empty list left behind is one entry per room ever used, forever."""
        registry.join("lobby", alice)
        registry.leave("lobby", alice)

        assert registry.rooms == ()

    def test_a_member_with_no_rooms_is_forgotten(
        self, registry: RoomRegistry, alice: RecordingMember
    ) -> None:
        """The reverse index leaks the same way if it is not tidied."""
        registry.join("lobby", alice)
        registry.leave("lobby", alice)

        assert registry.rooms_of(alice) == frozenset()

    def test_rooms_of_reports_current_membership(
        self, registry: RoomRegistry, alice: RecordingMember
    ) -> None:
        registry.join("a", alice)
        registry.join("b", alice)

        assert registry.rooms_of(alice) == frozenset({"a", "b"})

    def test_leave_all_is_the_disconnect_path(
        self, registry: RoomRegistry, alice: RecordingMember, bob: RecordingMember
    ) -> None:
        """A member left behind is broadcast to for the life of the process."""
        registry.join("a", alice)
        registry.join("b", alice)
        registry.join("a", bob)

        assert registry.leave_all(alice) == frozenset({"a", "b"})
        assert registry.rooms == ("a",)
        assert registry.member_count("a") == 1

    def test_leave_all_on_a_member_in_no_rooms_is_harmless(
        self, registry: RoomRegistry, alice: RecordingMember
    ) -> None:
        assert registry.leave_all(alice) == frozenset()


class TestBroadcast:
    def test_every_member_receives_it(
        self, registry: RoomRegistry, alice: RecordingMember, bob: RecordingMember
    ) -> None:
        registry.join("lobby", alice)
        registry.join("lobby", bob)

        assert registry.broadcast("lobby", '{"n":1}') == 2
        assert alice.received == bob.received == ['{"n":1}']

    def test_the_sender_can_be_excluded(
        self, registry: RoomRegistry, alice: RecordingMember, bob: RecordingMember
    ) -> None:
        registry.join("lobby", alice)
        registry.join("lobby", bob)

        assert registry.broadcast("lobby", "x", exclude=alice) == 1
        assert alice.received == []
        assert bob.received == ["x"]

    def test_another_rooms_members_do_not_receive_it(
        self, registry: RoomRegistry, alice: RecordingMember, bob: RecordingMember
    ) -> None:
        registry.join("a", alice)
        registry.join("b", bob)

        registry.broadcast("a", "for-a")

        assert bob.received == []

    def test_broadcasting_to_an_empty_room_is_not_an_error(
        self, registry: RoomRegistry
    ) -> None:
        assert registry.broadcast("nobody-here", "x") == 0

    def test_a_member_that_cannot_keep_up_is_removed_from_every_room(
        self, registry: RoomRegistry, alice: RecordingMember
    ) -> None:
        """Not skipped, not dropped-oldest: removed, so the state is visible.

        A silently skipped member is rendering a view that is wrong until a
        reload it has no reason to perform. Removal is what makes its
        connection close, which its reconnect handler already knows how to
        answer.
        """
        slow = RefusingMember("slow")
        registry.join("lobby", slow)
        registry.join("other", slow)
        registry.join("lobby", alice)

        assert registry.broadcast("lobby", "x") == 1
        assert registry.rooms_of(slow) == frozenset()
        assert registry.member_count("lobby") == 1
        assert registry.rooms == ("lobby",)

    def test_a_failed_offer_does_not_skip_the_member_after_it(
        self, registry: RoomRegistry
    ) -> None:
        """The removal mutates the list being iterated.

        Iterating it directly would step past the next member — who is
        perfectly healthy and would simply not receive the message, with
        nothing anywhere saying so.
        """
        first = RefusingMember("first")
        rest = [RecordingMember(f"m{i}") for i in range(3)]
        registry.join("lobby", first)
        for member in rest:
            registry.join("lobby", member)

        assert registry.broadcast("lobby", "x") == 3
        assert all(member.received == ["x"] for member in rest)

    def test_broadcast_neither_awaits_nor_raises(self, registry: RoomRegistry) -> None:
        """The invariant: it runs inside another client's receive loop.

        A `publish` from one member calls this synchronously, so anything that
        could suspend here would let one participant pace the whole room.
        """
        registry.join("lobby", RefusingMember("a"))
        registry.join("lobby", RecordingMember("b"))

        result = registry.broadcast("lobby", "x")

        assert result == 1
        assert not hasattr(result, "__await__")


class TestShutdown:
    def test_close_empties_the_registry(
        self, registry: RoomRegistry, alice: RecordingMember, bob: RecordingMember
    ) -> None:
        registry.join("a", alice)
        registry.join("b", bob)

        assert registry.close() == 2
        assert registry.rooms == ()

    def test_close_can_target_named_members(
        self, registry: RoomRegistry, alice: RecordingMember, bob: RecordingMember
    ) -> None:
        registry.join("lobby", alice)
        registry.join("lobby", bob)

        assert registry.close([alice]) == 1
        assert registry.member_count("lobby") == 1


class TestTheProtocol:
    def test_a_recording_member_satisfies_it(self, alice: RecordingMember) -> None:
        """The fakes in this file are the protocol, not an approximation of it."""
        assert isinstance(alice, RoomMember)
