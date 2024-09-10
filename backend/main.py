from typing import Any, List

from fastapi import FastAPI, HTTPException

from backend.db import InMemoryDB
from backend.logic import transactions, users
from backend.models import Transaction, TransactionRow

app = FastAPI()

db = InMemoryDB()


@app.get("/")
async def root():
    """Simple endpoint to test if the server is up and running"""
    return {"message": "Hello World"}


@app.get("/users/{user_id}/transactions", response_model=List[TransactionRow])
async def get_transactions(user_id: int) -> List[TransactionRow]:
    """Returns all transactions for a user."""
    return transactions.transactions(db, user_id)


@app.get(
    "/users/{user_id}/transactions/{transaction_id}", response_model=TransactionRow
)
async def get_transaction(user_id: int, transaction_id: int) -> TransactionRow:
    """Returns a given transaction of the user."""
    if users.user(db, user_id) is None:
        raise HTTPException(status_code=404, detail="User not found")
    transaction = transactions.transaction(db, user_id, transaction_id)
    if transaction is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return transaction


@app.post("/users/{user_id}/transactions", response_model=TransactionRow)
async def create_transaction(user_id: int, transaction: Transaction) -> TransactionRow:
    """Adds a new transaction to the list of user transactions."""
    return transactions.create_transaction(db, user_id, transaction)


@app.get("/users/{user_id}/transactions/balance/")
async def get_balance(user_id: int) -> Any:  # pylint: disable=unused-argument
    """Computes the balance of payments for a user subscription."""
    # We expect you to write this function
    if users.user(db, user_id) is None:
        raise HTTPException(status_code=404, detail="User not found")

    return transactions.calculate_user_current_balance(db=db, user_id=user_id)
