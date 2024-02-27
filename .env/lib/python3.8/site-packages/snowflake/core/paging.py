"""Defines an Iterator to represent a certain resource instances fetched from the Snowflake database."""

from typing import Callable, Generic, Iterable, Iterator, Optional, TypeVar, Union, overload

from public import public


T = TypeVar("T")
S = TypeVar("S")


@public
class PagedIter(Iterator[T], Generic[T]):
    """A page-by-page iterator.

    Data fetched from the server is iterated over page by page, yielding items one by
    one.  For PrPr, we won't have real paging. More for future use.

    Example:
        >>> from snowflake.core import Root
        >>> root = Root(connection)
        >>> tasks: TaskCollection = root.databases["mydb"].schemas["myschema"].tasks
        >>> task_iter = tasks.iter(like="my%")  # returns a PagedIter[Task]
        >>> for task_obj in task_iter:
        ...     print(task_obj.name)
    """

    @overload
    def __init__(self, data: Iterable[T]) -> None:
        ...

    @overload
    def __init__(self, data: Iterable[T], map_: None) -> None:
        ...

    @overload
    def __init__(self, data: Iterable[S], map_: Callable[[S], T]) -> None:
        ...

    def __init__(
        self,
        data: Union[Iterable[T], Iterable[S]],
        map_: Optional[Callable[[S], T]] = None,
    ) -> None:
        self._data = data
        if map_ is None:
            self._map = lambda e: e
        else:
            self._map = map_
        iterator = iter(self._data)
        self._iter: Iterator[T] = map(self._map, iterator)

    def __iter__(self) -> Iterator[T]:
        return self._iter

    def __next__(self) -> T:
        return next(self._iter)
