from dataclasses import dataclass
from typing import Any

from sqlalchemy import TEXT
from sqlalchemy import Engine, text
from sqlalchemy import select
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    Session,
    mapped_column,
)

from syncstore.versioned_changes_syncstore import (
    Change,
    Changes,
    ChangesQuery,
    Tables,
    Value,
    ValueType,
    VersionedChangesSyncStore,
)


class PCrsqliteBase(DeclarativeBase, MappedAsDataclass):
    pass


class PTrackedPeer(PCrsqliteBase):
    __tablename__ = "crsql_tracked_peers"
    site_id: Mapped[bytes] = mapped_column(primary_key=True)
    version: Mapped[int]
    tag: Mapped[int] = mapped_column(primary_key=True, default=0)  # 0=WHOLE_DB
    event: Mapped[int] = mapped_column(primary_key=True, default=0)  # 0=RECEIVED


class PChange(PCrsqliteBase):
    """
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

    __tablename__ = "crsql_changes"

    table: Mapped[str] = mapped_column(primary_key=True)
    pk: Mapped[Any] = mapped_column(TEXT, primary_key=True)
    cid: Mapped[str] = mapped_column(primary_key=True)
    val: Mapped[Any] = mapped_column(TEXT)
    col_version: Mapped[int] = mapped_column(primary_key=True)
    db_version: Mapped[int] = mapped_column(primary_key=True)
    site_id: Mapped[bytes]
    cl: Mapped[int]
    seq: Mapped[int]


def from_value(val: Value) -> Any:
    if ValueType.NONE == val.value_type:
        return None
    if ValueType.BYTES == val.value_type:
        return bytes.fromhex(val.value)
    if ValueType.STRING == val.value_type:
        return val.value
    raise NotImplementedError()


def to_value(val: Any) -> Value:
    if val is None:
        return Value(ValueType.NONE, "")
    if isinstance(val, bytes):
        return Value(ValueType.BYTES, val.hex())
    if isinstance(val, str):
        return Value(ValueType.STRING, val)
    raise NotImplementedError(f"type: {type(val)} - val: {val}")


def to_pchange(c: Change) -> PChange:
    return PChange(
        table=c.table,
        pk=from_value(c.pk),
        cid=c.cid,
        val=from_value(c.val),
        col_version=c.col_version,
        db_version=c.db_version,
        site_id=from_value(c.site_id),
        cl=c.cl,
        seq=c.seq,
    )


def to_change(pc: PChange) -> Change:
    return Change(
        table=pc.table,
        pk=to_value(pc.pk),
        cid=pc.cid,
        val=to_value(pc.val),
        col_version=pc.col_version,
        db_version=pc.db_version,
        site_id=to_value(pc.site_id),
        cl=pc.cl,
        seq=pc.seq,
    )


@dataclass
class CrSqliteSyncStore(VersionedChangesSyncStore):
    # crsqlite for change tracking + sync operations

    engine: Engine

    def setup_table_change_tracking(self, tables: Tables) -> None:
        with self.engine.connect() as c:
            for t in tables.table_names:
                c.execute(text(f"SELECT crsql_as_crr('{t}');"))
            c.commit()

    def get_site_id(self) -> str:
        with self.engine.connect() as c:
            site_id_bytes: bytes = (
                c.execute(text("SELECT crsql_site_id()")).all()[0]._tuple()[0]
            )
        return site_id_bytes.hex()

    def get_current_version(self) -> int:
        with self.engine.connect() as c:
            return c.execute(text("SELECT crsql_db_version()")).all()[0]._tuple()[0]

    def get_last_received_version(self, from_site_id: str) -> int:
        with Session(self.engine) as session:
            tp: PTrackedPeer | None = session.scalar(
                select(PTrackedPeer).where(
                    (PTrackedPeer.site_id == bytes.fromhex(from_site_id))
                    & (PTrackedPeer.tag == 0)
                    & (PTrackedPeer.event == 0)
                )
            )
            if tp is not None:
                return tp.version
            else:
                tp = PTrackedPeer(
                    site_id=bytes.fromhex(from_site_id), version=-1, tag=0, event=0
                )
                session.merge(tp)
                session.commit()
                return -1

    def get_changes(self, changes_query: ChangesQuery) -> Changes:
        since_version = changes_query.since_version
        from_site_id = changes_query.from_site_id
        not_from_site_id = changes_query.not_from_site_id
        if not (bool(from_site_id) ^ bool(not_from_site_id)):
            raise Exception("exactly one of the site_id params must be set")
        with Session(self.engine) as session:
            # all reads within a read-tx are guaranteed to only see writes commited before the begin of the read-tx
            # (snapshot-isolation) https://www.sqlite.org/isolation.html
            stmt = select(PChange).where((PChange.db_version > since_version))
            if from_site_id:
                stmt = stmt.where(PChange.site_id == bytes.fromhex(from_site_id))
            elif not_from_site_id:
                stmt = stmt.where(PChange.site_id != bytes.fromhex(not_from_site_id))
            else:
                raise Exception()
            pchanges = session.scalars(stmt).all()
            changes = [to_change(pc) for pc in pchanges]
            return Changes(changes, self.get_current_version(), self.get_site_id())

    def apply_changes(self, changes: Changes) -> None:
        with Session(self.engine) as session:
            pchanges = [to_pchange(c) for c in changes.changes]
            session.add_all(pchanges)
            ptp = PTrackedPeer(
                site_id=bytes.fromhex(changes.from_site_id),
                version=changes.version,
                tag=0,
                event=0,
            )
            session.merge(ptp)
            session.commit()
