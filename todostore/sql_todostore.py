from dataclasses import dataclass

from sqlalchemy import Engine, select
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    Session,
    mapped_column,
    relationship,
)

from .todostore import TodoItem, TodoList, TodoStore


class TodoBase(DeclarativeBase, MappedAsDataclass):
    pass


@dataclass
class PTodoItem(TodoBase):
    __tablename__ = "todo_item"

    item_id: Mapped[str] = mapped_column(
        primary_key=True, nullable=False, server_default="default_item_id"
    )
    content: Mapped[str] = mapped_column(nullable=True)
    list_id: Mapped[str] = mapped_column(nullable=True)


@dataclass
class PTodoList(TodoBase):
    __tablename__ = "todo_list"

    list_id: Mapped[str] = mapped_column(
        primary_key=True, nullable=False, server_default="default_list_id"
    )
    title: Mapped[str] = mapped_column(nullable=True, default=None)
    todos: Mapped[list[PTodoItem]] = relationship(
        cascade="all, delete-orphan",
        default_factory=list,
        foreign_keys="PTodoItem.list_id",
        primaryjoin="PTodoItem.list_id == PTodoList.list_id",
        order_by="PTodoItem.item_id",
    )


def to_p_item(item: TodoItem, list_id: str) -> PTodoItem:
    return PTodoItem(item_id=item.item_id, content=item.content, list_id=list_id)


def from_p_item(p_item: PTodoItem) -> TodoItem:
    return TodoItem(p_item.item_id, p_item.content)


def to_p_todo_list(todo_list: TodoList) -> PTodoList:
    return PTodoList(
        list_id=todo_list.list_id,
        title=todo_list.title,
        todos=[to_p_item(i, todo_list.list_id) for i in todo_list.todos],
    )


def from_p_todo_list(p_todo_list: PTodoList) -> TodoList:
    return TodoList(
        p_todo_list.list_id,
        p_todo_list.title,
        [from_p_item(pi) for pi in p_todo_list.todos],
    )


def create_all(engine: Engine) -> None:
    TodoBase.metadata.create_all(engine)


@dataclass
class SqlTodoStore(TodoStore):
    engine: Engine

    def __post_init__(self):
        create_all(self.engine)

    def get_tables(self) -> list[str]:
        return [PTodoItem.__tablename__, PTodoList.__tablename__]

    def save(self, entity: TodoList) -> None:
        p_todo_list = to_p_todo_list(entity)
        with Session(self.engine) as session, session.begin():
            session.merge(p_todo_list)

    def load(self, entity_id: str) -> TodoList | None:
        with Session(self.engine) as session:
            p_todo_list = session.scalar(
                select(PTodoList).where(PTodoList.list_id == entity_id)
            )
            return from_p_todo_list(p_todo_list) if p_todo_list else None
