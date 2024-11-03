from abc import ABC
from dataclasses import dataclass, field
from typing import Generic, TypeVar

from syncstore import SyncStore


T = TypeVar("T")
ID = TypeVar("ID")


@dataclass
class EntityStore(ABC, Generic[T, ID]):
    name: str

    def save(self, entity: T) -> None: ...
    def load(self, entity_id: str) -> T | None: ...

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
