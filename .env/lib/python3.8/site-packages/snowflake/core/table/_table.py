from typing import TYPE_CHECKING, Optional, Union

from snowflake.core._common import CreateMode, SchemaObjectCollectionParent, SchemaObjectReferenceMixin
from snowflake.core._internal.pydantic_compatibility import StrictStr

from .._internal.telemetry import api_telemetry
from ..paging import PagedIter
from ._generated.api import TableApi
from ._generated.api_client import BridgeApiClient
from ._generated.models import TableModel as Table


if TYPE_CHECKING:
    from snowflake.core.schema import SchemaResource


class TableCollection(SchemaObjectCollectionParent["TableResource"]):
    def __init__(self, schema: "SchemaResource"):
        super().__init__(schema, TableResource)
        self._api = TableApi(
            root=self.root,
            supports_rest_api=self._ref_class._supports_rest_api,
            bridge_client=BridgeApiClient(
                snowflake_connection=self._connection or self._session._conn._conn
            )
        )

    @api_telemetry
    def create(
        self, table: Union[Table, str],
        *,
        as_select: Optional[str] = None,
        template: Optional[str] = None,
        like_table: Optional[str] = None,
        clone_table: Optional[str] = None,
        copy_grants: Optional[bool] = False,
        mode: CreateMode=CreateMode.error_if_exists,
    ) -> "TableResource":
        """Create a table.

        Args:
            table: The table object, together with the table's properties, object parameters, columns, and constraints.
                It can either be a table name or a ``Table`` object when it's used together with `as_select`,
                `template`, `like_table`, `clone_table`. It must be a ``Table`` when it's not used with these clauses.
            as_select: The `as select` clause.
            template: The `using template` clause.
            like_table: The `like` clause.
            clone_table: The `clone` clause.
            copy_grants: copy grants when `clone_table` is provided.
            mode: One of the following strings.

                CreateMode.error_if_exists: Throw an :class:`snowflake.core.exceptions.ConflictError`
                if the table already exists in Snowflake.  Equivalent to SQL ``create table <name> ...``.

                CreateMode.or_replace: Replace if the task already exists in Snowflake. Equivalent to SQL
                ``create or replace table <name> ...``.

                CreateMode.if_not_exists: Do nothing if the task already exists in Snowflake.
                Equivalent to SQL ``create table <name> if not exists...``

                Default value is CreateMode.error_if_exists.

        Not currently implemented:
            - Row access policy
            - Column masking policy
            - Search optimization
            - Tags
            - Stage file format and copy options
        """
        if isinstance(table, str):
            if not as_select and not template and not clone_table and not like_table:
                raise ValueError(
                    "When `table` is a str, any one of the `as_select`, `template`, `clone_table`, "
                    "or `like_table` must not be empty."
                )
            table = Table(name=table)
        real_mode = CreateMode[mode].value
        self._api.create_table(
            self.database.name, self.schema.name, table._to_model(), create_mode=StrictStr(real_mode),
            as_select=as_select, template_query=template, like_table=like_table, clone_table=clone_table,
            copy_grants=copy_grants,
            async_req=False
        )
        return TableResource(table.name, self)

    @api_telemetry
    def iter(
        self,
        *,
        like: Optional[str] = None,
        starts_with: Optional[str] = None,
        limit: Optional[int] = None,
        from_name: Optional[str] = None,
        history: bool = False,
        deep: bool = False,
    ) -> PagedIter[Table]:
        """Search ``Table`` objects from Snowflake.

        Args:
            like: The pattern of the Table name. Use ``%`` to represent any number of characters and ``?`` for a
                single character.
            startswith: The table name starts with this string.
            limit: limits the number of objects returned.
            from_name: enables fetching the specified number of rows following the first row whose object name matches
                the specified string.
            deep: fetch the sub-resources columns and constraints of every table if it's ``True``. Default ``False``.
            history: includes dropped tables that have not yet been purged.
        """
        return PagedIter(
            self._api.list_tables(
                database=self.database.name, var_schema=self.schema.name, like=like,
                starts_with=starts_with, show_limit=limit, from_name=from_name, history=history, deep=deep,
                async_req=False
            ), Table._from_model
        )


class TableResource(SchemaObjectReferenceMixin[TableCollection]):
    """Represents a reference to a Snowflake Table resource."""

    def __init__(self, name: str, collection: TableCollection) -> None:
        self.collection = collection
        self.name = name

    @api_telemetry
    def create_or_update(
        self, table: Table,
    ) -> None:
        """Create or update a table.

        Args:
            table: The ``Table`` object, including the table's properties, object parameters, columns, and constraints.

        Notes:
            - Not currently implemented:
                - Row access policy
                - Column masking policy
                - Search optimization
                - Tags
                - Stage file format and copy options
                - Foreign keys.
                - Rename the table.
                - If the name and table's name don't match, an error will be thrown.
                - Rename or drop a column.
            - New columns can only be added to the back of the column list.
        """
        self.collection._api.create_or_update_table(self.database.name, self.schema.name, self.name, table._to_model())

    def fetch(self, *, deep: bool = False) -> Table:
        """Fetch the details of a table.

        Args:
            deep: Columns and constraints the Table are not fetched when ``deep`` is False.
              Use ``deep=True`` if you want to fetch a Table object and use create_or_update to update the table later.
              If you use ``deep=False``, then create_or_update() later will raise an exception.
              Default ``False``.

        Notes:
            Inline constraints will become Outofline constraints because Snowflake database doesn't tell whether a
            constraint is inline or out of line from Snowflake database.
        """
        return Table._from_model(self.collection._api.fetch_table(
            self.database.name, self.schema.name, self.name, deep=deep, async_req=False))

    @api_telemetry
    def delete(self) -> None:
        """Delete the table."""
        self.collection._api.delete_table(self.database.name, self.schema.name, self.name, async_req=False)

    @api_telemetry
    def undelete(self) -> None:
        """Undelete the previously deleted table."""
        self.collection._api.undelete_table(self.database.name, self.schema.name, self.name, async_req=False)

    @api_telemetry
    def swap_with(self, to_swap_table_name: str) -> None:
        """Swap the name with another table."""
        self.collection._api.swap_with(
            self.database.name, self.schema.name, self.name, to_swap_table_name, async_req=False)

    @api_telemetry
    def suspend_recluster(self) -> None:
        self.collection._api.suspend_recluster(self.database.name, self.schema.name, self.name, async_req=False)

    @api_telemetry
    def resume_recluster(self) -> None:
        self.collection._api.resume_recluster(self.database.name, self.schema.name, self.name, async_req=False)
