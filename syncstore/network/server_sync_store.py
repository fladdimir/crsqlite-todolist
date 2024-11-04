from typing import Callable

from flask import Flask, Response, request

from syncstore.versioned_changes_syncstore import Changes, VersionedChangesSyncStore


# expose the syncstore functionality via http-endpoints


def run_sync_store_server(
    syncstore: VersionedChangesSyncStore, host: str, port: int, debug=False
):

    app = Flask(syncstore.name)

    @app.route("/")
    def index():
        return f"syncstore: {syncstore.name}"

    @app.route("/setup-table-change-tracking", methods=["POST"])
    def setup_table_change_tracking_endpoint() -> Response:
        tables: list[str] | None = request.json
        assert tables is not None
        syncstore.setup_table_change_tracking(tables)
        return Response(status=204)

    @app.route("/site-id")
    def get_site_id() -> str:
        return syncstore.get_site_id()

    @app.route("/last-received-version")
    def get_last_received_version() -> Response:
        from_site_id: str | None = request.args.get("from_site_id")
        assert from_site_id is not None
        v = syncstore.get_last_received_version(from_site_id)
        return Response(str(v), 200)

    @app.route("/changes")
    def get_changes() -> Response:  # TODO: compress
        since_version: int | None = request.args.get("since_version", type=int)
        assert since_version is not None
        from_site_id: str | None = request.args.get("from_site_id")
        not_from_site_id: str | None = request.args.get("not_from_site_id")
        changes = syncstore.get_changes(
            since_version, from_site_id=from_site_id, not_from_site_id=not_from_site_id
        )
        return Response(changes.to_json(), 200)

    @app.route("/apply-changes", methods=["POST"])
    def apply_changes() -> Response:
        data = request.get_data(as_text=True)
        changes: Changes = Changes.from_json(data)
        syncstore.apply_changes(changes)
        return Response("", 204)

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
