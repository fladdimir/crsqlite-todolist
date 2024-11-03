import pytest

from sqlite_setup import get_engine
from todostore import TodoItem, TodoList, TodoStore
from todostore_impl import SqlTodoStore


@pytest.fixture
def s(clean_test_db_dir) -> TodoStore:
    engine = get_engine(db_file="./db/crsqlite_syncstore_test.db")
    return SqlTodoStore("s", engine=engine)


def test_crsqlite_store(s: TodoStore):
    list_id = "todolist_1"

    list_1 = TodoList(list_id=list_id, title="title_1")
    list_1.todos.append(TodoItem("item_1", "item_content_1"))
    s.save(list_1)

    list_1 = s.load(list_id)
    assert list_1 is not None
    assert list_1.title == "title_1"
    assert list_1.todos == [TodoItem("item_1", "item_content_1")]
