import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Type

from pydantic import BaseModel

from backend.models import Row, TransactionRow, UserRow
from backend.models.interfaces import Database


@dataclass
class Table:
    schema: Type[Row]
    data: List[Row]


class InMemoryDB(Database):
    def __init__(self):
        self.users: List[UserRow] = []
        self.transactions: List[TransactionRow] = []
        self._tables: Dict[str, Table] = {
            "transactions": Table(schema=TransactionRow, data=self.transactions),
            "users": Table(schema=UserRow, data=self.users),
        }
        self.load("backend/db/tables.json")

    def load(self, filename: str) -> None:
        """Loads data from a JSON file to initiate the database"""
        with open(filename, encoding="utf-8") as file:
            raw_data = json.load(file)
        for table_name, table_data in raw_data.items():
            if table_name in self._tables:
                schema = self._tables[table_name].schema
                for row in table_data:
                    self._tables[table_name].data.append(schema(**row))

    def scan(self, table_name: str) -> List[Row]:
        """Returns all rows in a table"""
        if table_name not in self._tables:
            raise KeyError(f"Table {table_name} does not exist")
        return self._tables[table_name].data

    def get(self, table_name: str, id_: int) -> Optional[Row]:
        """Returns the first row in the table with the given id or None if not found"""
        if table_name not in self._tables:
            raise KeyError(f"Table {table_name} does not exist")
        return next((t for t in self._tables[table_name].data if t.id == id_), None)

    def put(self, table_name: str, item: BaseModel) -> Row:
        """Adds a new item to the table and returns it

        Note: the item's original `id` field is ignored and overwritten with the next available id"""
        if table_name not in self._tables:
            raise KeyError(f"Table {table_name} does not exist")
        schema = self._tables[table_name].schema
        if not isinstance(item, schema):
            raise ValueError(f"Invalid item type {type(item)} for table {table_name}")
        id_ = len(self._tables[table_name].data) + 1
        item.id = id_
        self._tables[table_name].data.append(item)
        return item
