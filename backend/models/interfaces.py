from abc import ABC, abstractmethod
from typing import Any, List, Optional


class Database(ABC):
    @abstractmethod
    def scan(self, table_name: str) -> List[Any]:
        pass

    @abstractmethod
    def get(self, table_name: str, id_: int) -> Optional[Any]:
        pass

    @abstractmethod
    def put(self, table_name: str, item: Any) -> Any:
        pass
