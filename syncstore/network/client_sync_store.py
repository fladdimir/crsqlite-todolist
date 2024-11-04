from dataclasses import dataclass, field

import requests

from syncstore.syncstore import SyncResult
from syncstore.versioned_changes_syncstore import Changes, VersionedChangesSyncStore


@dataclass
class HttpClientVersionedChangesSyncstore(VersionedChangesSyncStore):
    # proxy which implements sync-operations by forwarding calls to an http-server

    host: str
    port: int
    syncstore_server: str = field(init=False)  # host:port, e.g. localhost:5000

    def __post_init__(self):
        self.syncstore_server = f"http://{self.host}:{self.port}"

    def setup_table_change_tracking(self, tables: list[str]) -> None:
        r = requests.post(
            self.syncstore_server + "/setup-table-change-tracking", json=tables
        )
        assert r.status_code == 204

    def get_site_id(self) -> str:
        r = requests.get(self.syncstore_server + "/site-id")
        assert r.status_code == 200
        return r.text

    def get_last_received_version(self, from_site_id: str) -> int:
        r = requests.get(
            self.syncstore_server + "/last-received-version",
            params={"from_site_id": from_site_id},
        )
        assert r.status_code == 200
        return int(r.text)

    def get_changes(
        self,
        since_version: int,
        from_site_id: str | None = None,
        not_from_site_id: str | None = None,
    ) -> Changes:
        r = requests.get(
            self.syncstore_server + "/changes",
            params={
                "since_version": since_version,
                "from_site_id": from_site_id,
                "not_from_site_id": not_from_site_id,
            },
        )
        assert r.status_code == 200
        return Changes.from_json(r.text)

    def apply_changes(self, changes: Changes) -> None:
        # TODO: compress
        r = requests.post(
            self.syncstore_server + "/apply-changes",
            data=changes.to_json(),  # , headers=
        )
        assert r.status_code == 204

    def sync(self) -> SyncResult:
        raise NotImplementedError()
