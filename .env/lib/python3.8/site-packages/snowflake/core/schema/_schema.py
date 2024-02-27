from functools import cached_property
from typing import TYPE_CHECKING, Optional, Union

from snowflake.connector import SnowflakeConnection
from snowflake.core._internal.pydantic_compatibility import StrictStr
from snowflake.core.schema._generated.api_client import BridgeApiClient
from snowflake.snowpark import Session

from .._common import CreateMode, ObjectCollection, ObjectReferenceMixin
from .._internal.telemetry import api_telemetry
from ..image_repository import ImageRepositoryCollection
from ..paging import PagedIter
from ..service import ServiceCollection
from ..table import TableCollection
from ..task import TaskCollection
from ._generated.api.schema_api import SchemaApi
from ._generated.models.clone import CloneModel as Clone
from ._generated.models.model_schema import ModelSchemaModel as Schema


if TYPE_CHECKING:
    from .. import Root
    from ..database import DatabaseResource


class SchemaCollection(ObjectCollection["SchemaResource"]):
    def __init__(self, database: "DatabaseResource") -> None:
        super().__init__(SchemaResource)
        self._database = database
        self._api = SchemaApi(
            root=self.root,
            supports_rest_api=self._ref_class._supports_rest_api,
            bridge_client=BridgeApiClient(snowflake_connection=database.root.connection),
        )

    @property
    def database(self) -> "DatabaseResource":
        return self._database

    @property
    def root(self) -> "Root":
        return self.database.collection.root

    @api_telemetry
    def create(
        self,
        schema: Schema,
        *,
        clone: Optional[Union[str, Clone]] = None,
        mode: CreateMode = CreateMode.error_if_exists,
        kind: str = "",
    ) -> "SchemaResource":
        """Create a schema in Snowflake.

        Args:
            schema: an instance of :class:`Schema`.
            mode: One of the following strings.
                CreateMode.error_if_exists: Throw an :class:`snowflake.core.exceptions.ConflictError`
                if the schem already exists in Snowflake. Equivalent to SQL ``create schema <name> ...``.

                CreateMode.or_replace: Replace if the schema already exists in Snowflake. Equivalent to SQL
                ``create or replace schema <name> ...``.

                CreateMode.if_not_exists: Do nothing if the schema already exists in Snowflake. Equivalent to SQL
                ``create schema <name> if not exists...``

                Default value is CreateMode.error_if_exists.
        """
        real_mode = CreateMode[mode].value
        real_clone = Clone(source=clone) if isinstance(clone, str) else clone
        self._api.create_schema(
            self.database.name,
            schema._to_model(),
            StrictStr(real_mode),
            with_managed_access=False,  # TODO: decide what to do with this
            kind=kind,
            clone=real_clone._to_model() if real_clone else None,
            async_req=False,
        )
        return self[schema.name]

    @api_telemetry
    def iter(
        self,
        *,
        like: Optional[str] = None,
        starts_with: Optional[str] = None,
        limit: Optional[int] = None,
        from_name: Optional[str] = None
    ) -> PagedIter[Schema]:
        """Look up schemas in Snowflake."""
        return PagedIter(
            data=self._api.list_schemas(
                self.database.name,
                StrictStr(like) if like is not None else None,
                StrictStr(starts_with) if starts_with else None,
                limit,
                from_name=from_name,
                async_req=False,
            ), map_=Schema._from_model
        )

    @property
    def _session(self) -> Session:
        return self.database._session

    @property
    def _connection(self) -> SnowflakeConnection:
        return self.database._connection


class SchemaResource(ObjectReferenceMixin[SchemaCollection]):

    def __init__(self, name: str, collection: SchemaCollection) -> None:
        self.name: str = name
        self.collection: SchemaCollection = collection

    @property
    def database(self) -> "DatabaseResource":
        return self.collection.database

    @property
    def _api(self) -> SchemaApi:
        return self.collection._api

    @api_telemetry
    def create_or_update(self, schema: Schema) -> "SchemaResource":
        """Create or update a schema in Snowflake."""
        self._api.create_or_update_schema(
            self.database.name,
            schema.name,
            schema._to_model(),
            async_req=False,
        )
        return self

    @api_telemetry
    def fetch(self) -> Schema:
        return Schema._from_model(self.collection._api.fetch_schema(
            self.database.name,
            self.name,
            async_req=False,
        ))

    @api_telemetry
    def delete(self) -> None:
        """Delete this Schema."""
        self.collection._api.delete_schema(
            self.database.name,
            name=self.name,
            async_req=False,
        )

    @cached_property
    def tasks(self) -> TaskCollection:
        return TaskCollection(self)

    @cached_property
    def services(self) -> ServiceCollection:
        return ServiceCollection(self)

    @cached_property
    def image_repositories(self) -> ImageRepositoryCollection:
        return ImageRepositoryCollection(self)

    @cached_property
    def tables(self) -> TableCollection:
        return TableCollection(self)
