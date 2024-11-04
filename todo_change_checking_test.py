import pytest

from crsqlite_todo_sync_store import CrSqliteTodoSyncStore
from sqlite_setup import get_engine
from todostore.todostore import TodoItem, TodoList


@pytest.fixture
def s(clean_test_db_dir) -> CrSqliteTodoSyncStore:
    engine = get_engine(db_file="./db/crsqlite_change_checker_test.db")
    return CrSqliteTodoSyncStore("s", engine=engine, remote_syncstore=None)


def test_on_change_hook(s: CrSqliteTodoSyncStore):
    list_id = "todolist_1"

    list_1 = TodoList(list_id=list_id, title="title_1")
    list_1.todos.append(TodoItem("item_1", "item_content_1"))
    s.save(list_1)

    list_1 = s.load(list_id)
    assert list_1 is not None
    assert list_1.title == "title_1"
    assert list_1.todos == [TodoItem("item_1", "item_content_1")]

    updated_instances: list[TodoList | None] = []

    def id_fn(tl: TodoList) -> str:
        return tl.list_id

    s.track(list_1, id_fn, lambda: s.load(list_id), updated_instances.append)

    s.check_all()
    assert updated_instances == []

    list_1.title = "updated_title"

    s.check_all()
    assert len(updated_instances) == 1
    assert updated_instances[0] is not None
    assert updated_instances[0].title == "title_1"

    s.save(list_1)
    updated_instances = []

    s.check_all()
    assert updated_instances == []

    # test weak refs:
    assert len(s.change_checker.on_change_callbacks) == 1
    assert len(s.change_checker.value_retrieval_fns) == 1
    del list_1
    assert len(s.change_checker.on_change_callbacks) == 0
    assert len(s.change_checker.value_retrieval_fns) == 0
