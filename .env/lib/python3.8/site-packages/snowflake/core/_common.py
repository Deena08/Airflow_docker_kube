# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

from abc import ABC, abstractmethod
from enum import Enum, EnumMeta
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    ItemsView,
    Iterable,
    Iterator,
    KeysView,
    Protocol,
    Type,
    TypeVar,
    ValuesView,
)

from public import public

from snowflake.connector import SnowflakeConnection


if TYPE_CHECKING:
    from snowflake.core import Root
    from snowflake.core.database import DatabaseResource
    from snowflake.core.schema import SchemaResource

from snowflake.snowpark import Session


T = TypeVar("T")

UNALTERABLE_PARAMETERS = {
    "name",
    "tag"
}


def _is_an_update(parameters: Iterable[str]) -> bool:
    """Decide whether the parameters contain update-relevant values.

    Parameters:
        parameters: an iterable of parameters in the request
    """
    return len(set(parameters) - UNALTERABLE_PARAMETERS) > 0


@public
class ObjectCollectionBC(ABC):
    @property
    @abstractmethod
    def _session(self) -> Session:
        ...

    @property
    @abstractmethod
    def _connection(self) -> SnowflakeConnection:
        ...

    @property
    @abstractmethod
    def root(self) -> "Root":
        ...


@public
class ObjectCollection(ObjectCollectionBC, Generic[T]):
    def __init__(self, ref_class: Type[T]) -> None:
        self._items: Dict[str, T] = {}
        self._ref_class = ref_class

    def __getitem__(self, item: str) -> T:
        if item not in self._items:
            # Protocol doesn't support restricting __init__
            self._items[item] = self._ref_class(item, self)  # type: ignore[call-arg]
        return self._items[item]

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    def keys(self) -> KeysView[str]:
        return self._items.keys()

    def items(self) -> ItemsView[str, T]:
        return self._items.items()

    def values(self) -> ValuesView[T]:
        return self._items.values()


@public
class AccountObjectCollectionParent(ObjectCollection[T], Generic[T]):
    def __init__(self, root: "Root", ref_class: Type[T]) -> None:
        super().__init__(ref_class)
        self._root = root

    @property
    def _session(self) -> Session:
        return self._root.session

    @property
    def _connection(self) -> SnowflakeConnection:
        return self._root.connection

    @property
    def root(self) -> "Root":
        return self._root


@public
class SchemaObjectCollectionParent(ObjectCollection[T], Generic[T]):
    def __init__(self, schema: "SchemaResource", ref_class: Type[T]) -> None:
        super().__init__(ref_class)
        self._schema = schema

    @property
    def _session(self) -> Session:
        return self.schema._session

    @property
    def _connection(self) -> SnowflakeConnection:
        return self.schema._connection

    @property
    def schema(self) -> "SchemaResource":
        return self._schema

    @property
    def database(self) -> "DatabaseResource":
        return self.schema.collection.database

    @property
    def root(self) -> "Root":
        return self.database.collection.root


@public
class ObjectReferenceProtocol(Protocol[T]):
    @property
    def collection(self) -> ObjectCollection[T]:
        ...

    @property
    def root(self) -> "Root":
        ...


@public
class SchemaObjectReferenceProtocol(Protocol[T]):
    @property
    def collection(self) -> SchemaObjectCollectionParent[T]:
        ...

    @property
    def name(self) -> str:
        ...

    @property
    def database(self) -> "DatabaseResource":
        ...

    @property
    def schema(self) -> "SchemaResource":
        ...


@public
class ObjectReferenceMixin(Generic[T]):

    # Default on/off switch for whether a resource supports the rest API
    _supports_rest_api = False

    @property
    def _session(self: ObjectReferenceProtocol[T]) -> Session:
        return self.collection._session

    @property
    def _connection(self: ObjectReferenceProtocol[T]) -> SnowflakeConnection:
        return self.collection._connection

    @property
    def root(self: ObjectReferenceProtocol[T]) -> "Root":
        return self.collection.root


@public
class SchemaObjectReferenceMixin(Generic[T], ObjectReferenceMixin[SchemaObjectCollectionParent[T]]):

    @property
    def schema(self: SchemaObjectReferenceProtocol[T]) -> "SchemaResource":
        return self.collection.schema

    @property
    def database(self: SchemaObjectReferenceProtocol[T]) -> "DatabaseResource":
        return self.collection.schema.database

    @property
    def fully_qualified_name(self: SchemaObjectReferenceProtocol[T]) -> str:
        return f"{self.database.name}.{self.schema.name}.{self.name}"


class CaseInsensitiveEnumMeta(EnumMeta):
    def __init__(cls, *args, **kws) -> None:  # type: ignore
        super().__init__(*args, **kws)

        class lookup(dict):  # type: ignore
            def get(self, key, default=None):  # type: ignore
                return super().get(key.lower(), key.lower())
        cls._legacy_mode_map_ = lookup({item.value.lower(): item.name for item in cls})  # type: ignore

    def __getitem__(cls, name: str) -> Any:
        converted_name = cls._legacy_mode_map_.get(name)
        return super().__getitem__(converted_name)


@public
class CreateMode(str, Enum, metaclass=CaseInsensitiveEnumMeta):
    error_if_exists = "errorIfExists"
    or_replace = "orReplace"
    if_not_exists = "ifNotExists"
