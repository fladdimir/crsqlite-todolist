from dataclasses import dataclass, field

from sqlalchemy import Engine

from syncstore import SyncResult, SyncStore
from syncstore_impl.crsqlite_syncstore import CrSqliteSyncStore
from syncstore_impl.versioned_changes_syncstore import VersionedChangesSyncStore
from todostore import TodoList, TodoStore
from todostore_impl import SqlTodoStore


@dataclass
class CrSqliteTodoSyncStore(TodoStore, SyncStore):

    name: str
    engine: Engine
    remote_syncstore: VersionedChangesSyncStore | None

    todostore: TodoStore = field(init=False)
    syncstore: VersionedChangesSyncStore = field(init=False)

    def __post_init__(self):
        self.todostore = SqlTodoStore(self.name, self.engine)
        self.syncstore = CrSqliteSyncStore(
            self.name, self.remote_syncstore, self.engine
        )
        self.syncstore.setup_table_change_tracking(self.todostore.get_tables())

    def save(self, entity: TodoList) -> None:
        self.todostore.save(entity)

    def load(self, entity_id: str) -> TodoList | None:
        return self.todostore.load(entity_id)

    def get_tables(self) -> list[str]:
        return self.todostore.get_tables()

    def sync(self) -> SyncResult:
        return self.syncstore.sync()
