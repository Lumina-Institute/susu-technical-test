from typing import List

from backend.models import (
    Transaction,
    TransactionRow,
    TransactionState,
    TransactionType,
)
from backend.models.interfaces import Database


def transactions(db: Database, user_id: int) -> List[TransactionRow]:
    """
    Returns all transactions of a user.
    """
    return [
        transaction
        for transaction in db.scan("transactions")
        if transaction.user_id == user_id
    ]


def transaction(db: Database, user_id: int, transaction_id: int) -> TransactionRow:
    """
    Returns a given transaction of the user
    """
    transaction = db.get("transactions", transaction_id)
    return transaction if transaction and transaction.user_id == user_id else None


def create_transaction(
    db: Database, user_id: int, transaction: Transaction
) -> TransactionRow:
    """
    Creates a new transaction (adds an ID) and returns it.
    """
    if transaction.type in (TransactionType.DEPOSIT, TransactionType.REFUND):
        initial_state = TransactionState.PENDING
    elif transaction.type == TransactionType.SCHEDULED_WITHDRAWAL:
        initial_state = TransactionState.SCHEDULED
    else:
        raise ValueError(f"Invalid transaction type {transaction.type}")
    transaction_row = TransactionRow(
        user_id=user_id, **transaction.dict(), state=initial_state
    )
    return db.put("transactions", transaction_row)
