from abc import ABC
from dataclasses import dataclass


@dataclass
class SyncResult:
    n_pulled_changes: int
    n_pushed_changes: int


@dataclass
class SyncStore(ABC):
    name: str

    def sync(self) -> SyncResult: ...
