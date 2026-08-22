"""`FrozenDict`: the properties the rest of the codebase relies on.

Grouped by the reason each property exists rather than by method, because the
interesting question about this class is never "does `__len__` work" — it is
whether it can be dropped into the three places a mapping is used here without
any of them noticing, while making the writes that were never meant to happen
impossible.
"""

from __future__ import annotations

import pickle
from collections.abc import Mapping

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel, ConfigDict, ValidationError

from src.immutable import EMPTY_MAPPING, FrozenDict, freeze_mapping


class TestItIsAMapping:
    """A drop-in for a read-only dict, so adopting it changes no call site."""

    def test_reads_like_a_dict(self) -> None:
        frozen = FrozenDict({"a": 1, "b": 2})

        assert frozen["a"] == 1
        assert len(frozen) == 2
        assert "b" in frozen
        assert frozen.get("missing", 0) == 0
        assert sorted(frozen) == ["a", "b"]
        assert sorted(frozen.items()) == [("a", 1), ("b", 2)]
        assert sorted(frozen.values()) == [1, 2]

    def test_is_a_mapping_but_not_a_dict(self) -> None:
        frozen = FrozenDict({"a": 1})

        assert isinstance(frozen, Mapping)
        # Deliberately not a `dict` subclass: inheriting from `dict` would
        # bring `__setitem__`, `update`, `pop` and `clear` along with it, and
        # each would have to be overridden to raise. Anything that misses one
        # is a silent mutation, and `dict.update` in C bypasses `__setitem__`
        # anyway, so the overrides would not even be sufficient.
        assert not isinstance(frozen, dict)

    def test_compares_equal_to_a_plain_dict(self) -> None:
        assert FrozenDict({"a": 1}) == {"a": 1}
        assert {"a": 1} == FrozenDict({"a": 1})
        assert FrozenDict({"a": 1}) != {"a": 2}
        assert FrozenDict({"a": 1}) != FrozenDict({"a": 1, "b": 2})

    def test_builds_from_pairs_and_keywords(self) -> None:
        assert FrozenDict([("a", 1), ("b", 2)]) == {"a": 1, "b": 2}
        assert FrozenDict(a=1, b=2) == {"a": 1, "b": 2}
        assert FrozenDict({"a": 1}, b=2) == {"a": 1, "b": 2}
        assert FrozenDict() == {}

    def test_repr_round_trips(self) -> None:
        assert repr(FrozenDict({"a": 1})) == "FrozenDict({'a': 1})"


class TestItCannotBeWritten:
    def test_item_assignment_is_refused(self) -> None:
        frozen = FrozenDict({"a": 1})

        with pytest.raises(TypeError):
            frozen["a"] = 2  # type: ignore[index]

    def test_it_has_none_of_the_dict_mutators(self) -> None:
        frozen = FrozenDict({"a": 1})

        for mutator in ("update", "pop", "popitem", "clear", "setdefault"):
            assert not hasattr(frozen, mutator)

    def test_construction_copies_away_from_the_caller(self) -> None:
        source = {"a": 1}
        frozen = FrozenDict(source)

        source["a"] = 99

        # The failure this class exists to prevent. Holding the caller's
        # mapping would leave every "frozen" field writable from wherever it
        # was built.
        assert frozen["a"] == 1

    def test_iterating_does_not_expose_the_internal_dict(self) -> None:
        frozen = FrozenDict({"a": 1})
        assert dict(frozen) is not frozen._data


class TestFunctionalUpdate:
    def test_or_returns_a_new_frozen_mapping(self) -> None:
        original = FrozenDict({"a": 1})

        merged = original | {"b": 2}

        assert merged == {"a": 1, "b": 2}
        assert isinstance(merged, FrozenDict)
        assert original == {"a": 1}

    def test_or_takes_the_right_hand_side(self) -> None:
        assert FrozenDict({"a": 1}) | {"a": 2} == {"a": 2}

    def test_reflected_or_takes_the_frozen_side(self) -> None:
        merged = {"a": 1, "b": 0} | FrozenDict({"b": 2})

        assert merged == {"a": 1, "b": 2}
        assert isinstance(merged, FrozenDict)


class TestHashing:
    """What makes a value object holding one usable as a key or cache argument."""

    def test_equal_mappings_hash_alike_regardless_of_insertion_order(self) -> None:
        assert hash(FrozenDict({"a": 1, "b": 2})) == hash(FrozenDict({"b": 2, "a": 1}))

    def test_it_can_be_a_set_member_and_a_dict_key(self) -> None:
        first = FrozenDict({"a": 1})
        second = FrozenDict({"a": 1})

        assert len({first, second}) == 1
        assert {first: "value"}[second] == "value"

    def test_unhashable_values_raise_rather_than_hashing_the_container(self) -> None:
        # The same rule a tuple follows. A mapping of lists cannot promise its
        # hash will not move, so it says so instead of returning one that will.
        with pytest.raises(TypeError, match="unhashable"):
            hash(FrozenDict({"a": [1, 2]}))

    def test_the_hash_is_cached(self) -> None:
        frozen = FrozenDict({"a": 1})

        assert frozen._hash is None
        first = hash(frozen)
        assert frozen._hash == first
        assert hash(frozen) == first


class TestPickling:
    """Why this is not `MappingProxyType`.

    `Notification` goes onto a Celery queue and `src/parallel` sends arguments
    into worker processes; both are pickle round trips, and `pickle` refuses a
    `mappingproxy` outright. A frozen mapping that cannot cross a process
    boundary is not usable as a field type here.
    """

    def test_round_trips_through_pickle(self) -> None:
        frozen = FrozenDict({"a": 1, "b": 2})

        restored = pickle.loads(pickle.dumps(frozen))

        assert restored == frozen
        assert isinstance(restored, FrozenDict)
        assert restored is not frozen

    def test_mappingproxy_does_not(self) -> None:
        from types import MappingProxyType

        with pytest.raises(TypeError):
            pickle.dumps(MappingProxyType({"a": 1}))


class TestPydanticIntegration:
    class Model(BaseModel):
        model_config = ConfigDict(frozen=True)

        fields: FrozenDict[str, str]

    def test_validates_a_dict_into_a_frozen_mapping(self) -> None:
        model = self.Model(fields={"a": "b"})

        assert isinstance(model.fields, FrozenDict)
        with pytest.raises(TypeError):
            model.fields["a"] = "c"  # type: ignore[index]

    def test_the_parameters_are_enforced(self) -> None:
        with pytest.raises(ValidationError):
            self.Model(fields={"a": object()})  # type: ignore[dict-item]

    def test_serialises_as_an_ordinary_object(self) -> None:
        model = self.Model(fields={"a": "b"})

        assert model.model_dump() == {"fields": {"a": "b"}}
        assert model.model_dump_json() == '{"fields":{"a":"b"}}'

    def test_an_unparameterised_field_still_works(self) -> None:
        """`FrozenDict` with no parameters must not fail schema generation.

        Nothing in `src/` declares one, and a model that does should get an
        unconstrained object rather than an error at import time — a schema
        hook that only handles the parameterised case breaks at the point a
        model is *defined*, which is start-up, not a request.
        """

        class Loose(BaseModel):
            model_config = ConfigDict(frozen=True)

            anything: FrozenDict  # type: ignore[type-arg]

        model = Loose(anything={"a": 1, 2: "b"})

        assert isinstance(model.anything, FrozenDict)
        assert model.anything == {"a": 1, 2: "b"}
        assert Loose.model_json_schema()["properties"]["anything"]["type"] == "object"

    def test_json_round_trips(self) -> None:
        model = self.Model.model_validate_json('{"fields":{"k":"v"}}')

        assert model.fields == {"k": "v"}

    def test_the_openapi_schema_is_a_plain_object(self) -> None:
        """The client contract must not leak the Python type.

        A wrapper that produced `{"type": "string"}` or an empty schema would
        change every generated client for an implementation detail.
        """
        schema = self.Model.model_json_schema()

        assert schema["properties"]["fields"] == {
            "additionalProperties": {"type": "string"},
            "title": "Fields",
            "type": "object",
        }

    def test_a_route_returning_one_serialises_and_documents(self) -> None:
        app = FastAPI()

        @app.get("/thing", response_model=self.Model)
        async def thing() -> TestPydanticIntegration.Model:
            return TestPydanticIntegration.Model(fields={"a": "b"})

        client = TestClient(app)

        assert client.get("/thing").json() == {"fields": {"a": "b"}}
        documented = client.get("/openapi.json").json()
        assert (
            documented["components"]["schemas"]["Model"]["properties"]["fields"]["type"]
            == "object"
        )


class TestFreezeMapping:
    def test_wraps_a_plain_mapping(self) -> None:
        frozen = freeze_mapping({"a": 1})

        assert isinstance(frozen, FrozenDict)
        assert frozen == {"a": 1}

    def test_returns_an_existing_frozen_mapping_unchanged(self) -> None:
        """The normaliser runs on every construction of every value object.

        Copying one that is already immutable would be a fresh dict per
        instance for no benefit — and would break identity for the shared
        `EMPTY_MAPPING` default.
        """
        original = FrozenDict({"a": 1})

        assert freeze_mapping(original) is original


class TestEmptyMapping:
    def test_it_is_empty_and_unwritable(self) -> None:
        assert EMPTY_MAPPING == {}
        assert len(EMPTY_MAPPING) == 0

        with pytest.raises(TypeError):
            EMPTY_MAPPING["a"] = 1  # type: ignore[index]

    def test_sharing_one_instance_as_a_default_is_safe(self) -> None:
        """Why the value objects use it instead of `default_factory=dict`.

        A shared mutable default is how one instance's metadata turns up on
        another's. A shared *immutable* default cannot, so the factory — and
        the per-instance allocation it implies — is not needed.
        """
        assert freeze_mapping({}) == EMPTY_MAPPING
