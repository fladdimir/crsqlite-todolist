from dataclasses import dataclass, field

import requests

from syncstore.network.server_sync_store import (
    LastReceivedVersionRequest,
    LastReceivedVersionResponse,
    SiteInfo,
    changes_query_schema,
    changes_schema,
    last_received_version_request_schema,
    last_received_version_response_schema,
    site_info_schema,
    tables_schema,
)
from syncstore.syncstore import SyncResult
from syncstore.versioned_changes_syncstore import (
    Changes,
    ChangesQuery,
    Tables,
    VersionedChangesSyncStore,
)

# TODO: generate client from openapi ?


@dataclass
class HttpClientVersionedChangesSyncstore(VersionedChangesSyncStore):
    # proxy which implements sync-operations by forwarding calls to an http-server

    host: str
    port: int
    syncstore_server: str = field(init=False)  # host:port, e.g. localhost:5000

    def __post_init__(self):
        self.syncstore_server = f"http://{self.host}:{self.port}"

    def setup_table_change_tracking(self, tables: Tables) -> None:
        r = requests.post(
            self.syncstore_server + "/setup-table-change-tracking",
            json=tables_schema.dump(tables),
        )
        assert r.status_code == 204

    def get_site_id(self) -> str:
        r = requests.get(self.syncstore_server + "/site-id")
        assert r.status_code == 200
        info: SiteInfo = site_info_schema.load(r.json())  # type: ignore
        return info.site_id

    def get_last_received_version(self, from_site_id: str) -> int:
        r = requests.get(
            self.syncstore_server + "/last-received-version",
            params=last_received_version_request_schema.dump(
                LastReceivedVersionRequest(from_site_id)
            ),
        )
        assert r.status_code == 200
        lrv: LastReceivedVersionResponse = last_received_version_response_schema.loads(
            r.text
        )  # type: ignore
        return lrv.version

    def get_changes(self, changes_query: ChangesQuery) -> Changes:
        r = requests.get(
            self.syncstore_server + "/changes",
            params=changes_query_schema.dump(changes_query),
        )
        assert r.status_code == 200
        return changes_schema.loads(r.text)  # type: ignore

    def apply_changes(self, changes: Changes) -> None:
        # tbd: compress
        r = requests.post(
            self.syncstore_server + "/changes",
            json=changes_schema.dump(changes),
        )
        assert r.status_code == 204

    def sync(self) -> SyncResult:
        raise NotImplementedError()
