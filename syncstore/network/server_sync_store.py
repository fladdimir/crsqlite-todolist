from dataclasses import dataclass
from typing import Callable

from apiflask import APIFlask
from marshmallow import Schema
from marshmallow_dataclass import class_schema

from syncstore.versioned_changes_syncstore import (
    Changes,
    ChangesQuery,
    Tables,
    VersionedChangesSyncStore,
)


# expose the syncstore functionality via http-endpoints


@dataclass
class LastReceivedVersionRequest:
    from_site_id: str


@dataclass
class LastReceivedVersionResponse:
    version: int


@dataclass
class SiteInfo:
    site_id: str


changes_schema: Schema = class_schema(Changes)()
changes_query_schema: Schema = class_schema(ChangesQuery)()
tables_schema: Schema = class_schema(Tables)()
last_received_version_request_schema = class_schema(LastReceivedVersionRequest)()
last_received_version_response_schema = class_schema(LastReceivedVersionResponse)()
site_info_schema: Schema = class_schema(SiteInfo)()


def run_sync_store_server(
    syncstore: VersionedChangesSyncStore, host: str, port: int, debug=False
):
    app = APIFlask(syncstore.name)

    @app.get("/")
    def index() -> str:
        return f"syncstore: {syncstore.name}"

    @app.post("/setup-table-change-tracking")
    @app.input(tables_schema, arg_name="tables")  # type: ignore
    @app.output({}, status_code=204)
    def setup_table_change_tracking(tables: Tables) -> None:
        syncstore.setup_table_change_tracking(tables)

    @app.get("/site-id")
    @app.output(site_info_schema)  # type: ignore
    def get_site_id() -> SiteInfo:
        return SiteInfo(syncstore.get_site_id())

    @app.get("/last-received-version")
    @app.input(last_received_version_request_schema, location="query")  # type: ignore
    @app.output(last_received_version_response_schema)  # type: ignore
    def get_last_received_version(
        query_data: LastReceivedVersionRequest,
    ) -> LastReceivedVersionResponse:
        v = syncstore.get_last_received_version(query_data.from_site_id)
        return LastReceivedVersionResponse(v)

    @app.get("/changes")
    @app.input(changes_query_schema, location="query")  # type: ignore
    @app.output(changes_schema, status_code=200)  # type: ignore
    def get_changes(query_data: ChangesQuery) -> Changes:  # tbd: compress
        changes = syncstore.get_changes(query_data)
        return changes

    @app.post("/changes")
    @app.input(changes_schema, arg_name="changes")  # type: ignore
    @app.output({}, status_code=204)
    def apply_changes(changes: Changes) -> None:
        syncstore.apply_changes(changes)

    app.run(host, port, debug=debug, threaded=False, use_reloader=False)
    # reloader can lead to running the same test debug session multiple times


def run_sync_store_server_callable(
    syncstore_provider: Callable[[], VersionedChangesSyncStore],
    host: str,
    port: int,
    debug=False,
) -> Callable:
    def run():
        run_sync_store_server(syncstore_provider(), host, port, debug=debug)

    return run
