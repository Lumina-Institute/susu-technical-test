from datetime import date
from enum import Enum

from pydantic import BaseModel


class TransactionType(str, Enum):
    DEPOSIT: str = "deposit"
    SCHEDULED_WITHDRAWAL: str = "scheduled_withdrawal"
    REFUND: str = "refund"


class TransactionState(str, Enum):
    SCHEDULED: str = "scheduled"
    PENDING: str = "pending"
    COMPLETED: str = "completed"
    FAILED: str = "failed"


class Row(BaseModel):
    id: int = 0  # id is overwritten by the database upon insertion


class User(BaseModel):
    name: str
    email: str


class UserRow(Row, User):
    pass


class Transaction(BaseModel):
    amount: float
    date: date
    type: TransactionType


class TransactionRow(Row, Transaction):
    user_id: int
    state: TransactionState
