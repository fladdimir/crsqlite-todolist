from multiprocessing import Process
import time
from typing import Callable

import pytest

from crsqlite_todo_sync_store import CrSqliteTodoSyncStore as StoreImpl
from entity_change_checking.entity_change_checker import E
from sqlite_setup import get_engine
from syncstore.network.client_sync_store import HttpClientVersionedChangesSyncstore
from syncstore.network.server_sync_store import run_sync_store_server_callable
from syncstore.syncstore import SyncResult
from syncstore.versioned_changes_syncstore import VersionedChangesSyncStore
from todostore.todostore import TodoItem, TodoList, TodoSyncStore

TEST_DB_DIR = "./db"

HOST = "127.0.0.1"
PORT = 5000

SQL_ECHO = False


@pytest.fixture
def s0(clean_test_db_dir):  # run after cleanup
    def store_provider() -> VersionedChangesSyncStore:
        store = StoreImpl(
            "s0",
            remote_syncstore=None,
            engine=get_engine(db_file=f"{TEST_DB_DIR}/s0.db", echo=SQL_ECHO),
        )
        return store.syncstore

    run_server_in_separate_process(store_provider)
    time.sleep(0.2)  # tbd: properly wait until server started
    return "s0 server started"


server_process: Process | None = None


def run_server_in_separate_process(
    syncstore_provider: Callable[[], VersionedChangesSyncStore],
):
    global server_process
    server_process = Process(
        target=run_sync_store_server_callable(
            syncstore_provider, HOST, PORT, debug=True
        )
    )
    server_process.daemon = True
    server_process.start()


@pytest.fixture
def s1(s0: StoreImpl):
    remote_syncstore = HttpClientVersionedChangesSyncstore(
        "s1_remote_client_s0", None, HOST, PORT
    )

    return StoreImpl(
        "s1",
        remote_syncstore=remote_syncstore,
        engine=get_engine(db_file=f"{TEST_DB_DIR}/s1.db", echo=SQL_ECHO),
    )


@pytest.fixture
def s2(s0):
    remote_syncstore = HttpClientVersionedChangesSyncstore(
        "s2_remote_client_s0", None, HOST, PORT
    )

    return StoreImpl(
        "s2",
        remote_syncstore=remote_syncstore,
        engine=get_engine(db_file=f"{TEST_DB_DIR}/s2.db", echo=SQL_ECHO),
    )


@pytest.fixture(autouse=True)
def cleanup_server_process():
    yield
    if server_process is not None:
        if server_process.is_alive():
            server_process.terminate()
            server_process.join()


def test_sync(s1: TodoSyncStore, s2: TodoSyncStore):
    list_id = "todolist_1"

    # c1: create and sync
    list_s1 = TodoList(list_id=list_id, title="title_1")
    s1.save(list_s1)
    sync_result = s1.sync()
    assert sync_result == SyncResult(n_pulled_changes=0, n_pushed_changes=1)
    list_s1 = s1.load(list_id)
    assert list_s1 is not None
    assert list_s1.title == "title_1"

    # c2: pull
    sync_result = s2.sync()
    assert sync_result == SyncResult(n_pulled_changes=1, n_pushed_changes=0)
    list_s2 = s2.load(list_id)
    assert list_s2 is not None
    assert list_s2 == list_s1

    # concurrent, non-conflicting changes
    # c1: add item
    item_from_s1 = TodoItem("item_1_s1", "from_s1")
    list_s1.todos.append(item_from_s1)
    s1.save(list_s1)

    # c2: add item
    item_from_s2 = TodoItem("item_1_s2", "from_s2")
    list_s2.todos.append(item_from_s2)
    s2.save(list_s2)

    # c1: sync
    sync_result = s1.sync()
    assert sync_result == SyncResult(n_pulled_changes=0, n_pushed_changes=2)
    list_s1 = s1.load(list_id)
    # c2: sync
    sync_result = s2.sync()
    assert sync_result == SyncResult(n_pulled_changes=2, n_pushed_changes=2)
    list_s2 = s2.load(list_id)
    assert list_s2 is not None
    # (c1: sync again)
    sync_result = s1.sync()
    assert sync_result == SyncResult(n_pulled_changes=2, n_pushed_changes=0)
    list_s1 = s1.load(list_id)
    assert list_s1 is not None

    assert list_s1.list_id == list_s2.list_id
    assert list_s1.title == list_s2.title
    assert len(list_s1.todos) == len(list_s2.todos)
    assert len(list_s1.todos) == 2
    assert TodoItem("item_1_s1", "from_s1") in list_s1.todos
    assert TodoItem("item_1_s2", "from_s2") in list_s1.todos

    # concurrent conflicting changes to the same attribute
    # lww
    list_s1.title = "list_s1_new_title"
    s1.save(list_s1)

    list_s2.title = "list_s2_new_title"
    s2.save(list_s2)

    sync_result = s1.sync()
    assert sync_result == SyncResult(n_pulled_changes=0, n_pushed_changes=1)
    sync_result = s2.sync()
    assert sync_result == SyncResult(n_pulled_changes=1, n_pushed_changes=1)
    sync_result = s1.sync()
    assert sync_result == SyncResult(n_pulled_changes=1, n_pushed_changes=0)

    list_s1 = s1.load(list_id)
    assert list_s1 is not None
    list_s2 = s2.load(list_id)
    assert list_s2 is not None

    assert list_s1.title == list_s2.title
    assert list_s1.title == "list_s2_new_title"

    # nothing synced
    sync_result = s1.sync()
    assert sync_result == SyncResult(n_pulled_changes=0, n_pushed_changes=0)
    sync_result = s2.sync()
    assert sync_result == SyncResult(n_pulled_changes=0, n_pushed_changes=0)
    sync_result = s1.sync()
    assert sync_result == SyncResult(n_pulled_changes=0, n_pushed_changes=0)

    # concurrent modification and deletion
    list_s2.todos.pop(0)
    list_s2.todos.pop(0)
    s2.save(list_s2)  # cascade orphan-removal
    list_s2 = s2.load(list_id)
    assert list_s2 is not None
    assert list_s2.todos == []

    list_s1.todos[0].content = "updated_item_content"
    s1.save(list_s1)  # cascade save
    list_s1 = s1.load(list_id)
    assert list_s1 is not None
    assert list_s1.todos[0].content == "updated_item_content"

    sync_result = s1.sync()
    assert sync_result == SyncResult(n_pulled_changes=0, n_pushed_changes=1)
    sync_result = s2.sync()
    assert sync_result == SyncResult(n_pulled_changes=1, n_pushed_changes=2)
    sync_result = s1.sync()
    assert sync_result == SyncResult(n_pulled_changes=2, n_pushed_changes=0)

    list_s1 = s1.load(list_id)
    assert list_s1 is not None
    list_s2 = s2.load(list_id)
    assert list_s2 is not None

    assert list_s1 == list_s2
    assert list_s1.todos == []
