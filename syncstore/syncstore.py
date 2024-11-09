from abc import ABCMeta, abstractmethod
from dataclasses import dataclass


@dataclass
class SyncResult:
    n_pulled_changes: int
    n_pushed_changes: int


@dataclass
class SyncStore(metaclass=ABCMeta):
    name: str

    @abstractmethod
    def sync(self) -> SyncResult: ...
