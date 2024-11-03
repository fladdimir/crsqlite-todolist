import pytest

from crsqlite_todo_sync_store import CrSqliteTodoSyncStore as StoreImpl
from sqlite_setup import get_engine
from syncstore import SyncResult
from todostore import TodoItem, TodoList, TodoSyncStore


TEST_DB_DIR = "./db"


@pytest.fixture
def s0(clean_test_db_dir):  # run after cleanup
    return StoreImpl(
        "s0", remote_syncstore=None, engine=get_engine(db_file=f"{TEST_DB_DIR}/s0.db")
    )
    # TODO: run web-server for s0 in parallel thread
    # return only address for ref by HttpClientRemoteSyncStore


# TODO: https://github.com/rqlite/sqlalchemy-rqlite


@pytest.fixture
def s1(s0: StoreImpl):
    # TODO: create and provide HttpClientRemoteSyncStore (s0_address)
    return StoreImpl(
        "s1",
        remote_syncstore=s0.syncstore,
        engine=get_engine(db_file=f"{TEST_DB_DIR}/s1.db"),
    )


@pytest.fixture
def s2(s0: StoreImpl):
    # TODO: create and provide HttpClientRemoteSyncStore (s0_address)
    return StoreImpl(
        "s2",
        remote_syncstore=s0.syncstore,
        engine=get_engine(db_file=f"{TEST_DB_DIR}/s2.db"),
    )


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
    # c1: change
    item_from_s1 = TodoItem("item_1_s1", "from_s1")
    list_s1.todos.append(item_from_s1)
    s1.save(list_s1)

    # c2: change
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

    sync_result = s1.sync()
    assert sync_result == SyncResult(n_pulled_changes=0, n_pushed_changes=0)
    sync_result = s2.sync()
    assert sync_result == SyncResult(n_pulled_changes=0, n_pushed_changes=0)
    sync_result = s1.sync()
    assert sync_result == SyncResult(n_pulled_changes=0, n_pushed_changes=0)
