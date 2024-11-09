from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum

from .syncstore import SyncResult, SyncStore


class ValueType(Enum):
    # tbd: extend
    NONE = "none"
    STRING = "string"
    BYTES = "bytes"


@dataclass
class Value:
    value_type: ValueType
    value: str


@dataclass
class Change:
    """
    serializable object corresponding to a row of the virtual crsql_changes table:
    https://vlcn.io/docs/cr-sqlite/api-methods/crsql_changes
    [table] TEXT NOT NULL,
    [pk] TEXT NOT NULL,
    [cid] TEXT NOT NULL,
    [val] ANY,
    [col_version] INTEGER NOT NULL,
    [db_version] INTEGER NOT NULL,
    [site_id] BLOB, -- id of the peer database that made the change
    [cl] INTEGER NOT NULL,
    [seq] INTEGER NOT NULL
    """

    table: str
    pk: Value
    cid: str
    val: Value
    col_version: int
    db_version: int
    site_id: Value
    cl: int
    seq: int


@dataclass
class Changes:
    changes: list[Change]  # list of all changes
    version: int  # version at which these changes were created
    from_site_id: str  # site_id which created these changes


@dataclass
class ChangesQuery:
    since_version: int = -1
    from_site_id: str | None = None
    not_from_site_id: str | None = None


@dataclass
class Tables:
    table_names: list[str]


@dataclass
class VersionedChangesSyncStore(SyncStore):
    """
    abstract algorithm to sync data changes between two stores,
    comprised of functionality which may be implemented e.g. via local or remote operations
    """

    remote_syncstore: "VersionedChangesSyncStore | None"

    @abstractmethod
    def setup_table_change_tracking(self, tables: Tables) -> None: ...

    @abstractmethod
    def get_site_id(self) -> str: ...

    @abstractmethod
    def get_last_received_version(self, from_site_id: str) -> int: ...

    @abstractmethod
    def get_changes(self, changes_query: ChangesQuery) -> Changes: ...

    @abstractmethod
    def apply_changes(self, changes: Changes) -> None: ...

    def sync(self) -> SyncResult:
        if self.remote_syncstore is None:
            raise Exception(f"no remote_syncstore specified for {self.name}")

        site_id = self.get_site_id()

        # tbd: only first time, then from local changes table?
        remote_site_id = self.remote_syncstore.get_site_id()

        # tbd: potential message re-ordering (-> lost changes)

        # pull
        last_received_version = self.get_last_received_version(remote_site_id)
        remote_changes = self.remote_syncstore.get_changes(
            ChangesQuery(since_version=last_received_version, not_from_site_id=site_id)
        )
        self.apply_changes(remote_changes)

        # push
        remote_last_received_version = self.remote_syncstore.get_last_received_version(
            site_id
        )
        changes = self.get_changes(
            ChangesQuery(remote_last_received_version, from_site_id=site_id)
        )
        self.remote_syncstore.apply_changes(changes)

        return SyncResult(
            n_pulled_changes=len(remote_changes.changes),
            n_pushed_changes=len(changes.changes),
        )
