from dataclasses import dataclass, field
from typing import Any, Callable

from sqlalchemy import Engine

from entity_change_checking.entity_change_checker import (
    EntityChangeChecker,
    EntityChangeCheckerImpl,
)
from syncstore.crsqlite_syncstore import CrSqliteSyncStore
from syncstore.syncstore import SyncResult, SyncStore
from syncstore.versioned_changes_syncstore import Tables, VersionedChangesSyncStore
from todostore.sql_todostore import SqlTodoStore
from todostore.todostore import TodoList, TodoStore


@dataclass
class CrSqliteTodoSyncStore(TodoStore, SyncStore, EntityChangeChecker[TodoList, str]):
    # store for Todo entities with the capability to sync changes with another syncstore

    name: str
    engine: Engine
    remote_syncstore: VersionedChangesSyncStore | None

    todostore: TodoStore = field(init=False)
    syncstore: VersionedChangesSyncStore = field(init=False)
    change_checker: EntityChangeCheckerImpl[TodoList, str] = field(
        default_factory=EntityChangeCheckerImpl[TodoList, str]
    )

    def __post_init__(self) -> None:
        self.todostore = SqlTodoStore(self.name, self.engine)
        self.syncstore = CrSqliteSyncStore(
            self.name, self.remote_syncstore, self.engine
        )
        self.syncstore.setup_table_change_tracking(Tables(self.get_tables()))

    def save(self, entity: TodoList) -> None:
        self.todostore.save(entity)

    def load(self, entity_id: str) -> TodoList | None:
        return self.todostore.load(entity_id)

    def get_tables(self) -> list[str]:
        return self.todostore.get_tables()

    def sync(self) -> SyncResult:
        return self.syncstore.sync()

    # tbd: do not expose, but use internally on save / load / sync
    def track(
        self,
        entity: TodoList,
        id_fn: Callable[[TodoList], Any],
        value_retriever_fn: Callable[[], TodoList | None],
        on_change_callback: Callable[[TodoList | None], None],
    ) -> None:
        self.change_checker.track(entity, id_fn, value_retriever_fn, on_change_callback)

    def check_all(self) -> None:
        self.change_checker.check_all()
