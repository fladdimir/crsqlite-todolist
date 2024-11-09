from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from typing import Generic, TypeVar

from syncstore.syncstore import SyncStore


T = TypeVar("T")
ID = TypeVar("ID")


@dataclass
class EntityStore(Generic[T, ID], metaclass=ABCMeta):
    name: str

    @abstractmethod
    def save(self, entity: T) -> None: ...
    @abstractmethod
    def load(self, entity_id: str) -> T | None: ...

    @abstractmethod
    def get_tables(self) -> list[str]: ...


@dataclass
class TodoItem:
    item_id: str
    content: str = ""


@dataclass
class TodoList:
    list_id: str
    title: str = "title"
    todos: list[TodoItem] = field(default_factory=list)


@dataclass
class TodoStore(EntityStore[TodoList, str]): ...


@dataclass
class TodoSyncStore(TodoStore, SyncStore): ...
