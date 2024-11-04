from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, MutableMapping, TypeVar
from weakref import WeakValueDictionary, finalize

E = TypeVar("E")
EID = TypeVar("EID")


class EntityChangeChecker(Generic[E, EID], metaclass=ABCMeta):
    @abstractmethod
    def track(
        self,
        entity: E,
        id_fn: Callable[[E], EID],
        value_retriever_fn: Callable[[], E | None],
        on_change_callback: Callable[[E | None], None],
    ) -> None: ...
    @abstractmethod
    def check_all(self) -> None: ...


@dataclass
class EntityChangeCheckerImpl(EntityChangeChecker[E, EID]):

    tracked_instances: WeakValueDictionary[EID, E] = field(
        default_factory=WeakValueDictionary[EID, E]
    )
    value_retrieval_fns: MutableMapping[EID, Callable[[], E | None]] = field(
        default_factory=dict
    )
    on_change_callbacks: MutableMapping[EID, Callable[[E | None], None]] = field(
        default_factory=dict
    )

    def track(
        self,
        entity: E,
        id_fn: Callable[[E], Any],
        value_retriever_fn: Callable[[], E | None],
        on_change_callback: Callable[[E | None], None],
    ) -> None:
        eid = id_fn(entity)
        self.tracked_instances[eid] = entity
        self.value_retrieval_fns[eid] = value_retriever_fn
        self.on_change_callbacks[eid] = on_change_callback
        finalize(entity, lambda: self.value_retrieval_fns.pop(eid))
        finalize(entity, lambda: self.on_change_callbacks.pop(eid))

    def check_all(self) -> None:
        for eid, tracked_instance in self.tracked_instances.items():
            current_value = self.value_retrieval_fns[eid]()
            if tracked_instance != current_value:
                self.on_change_callbacks[eid](current_value)
