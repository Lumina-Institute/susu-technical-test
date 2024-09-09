from typing import List

from backend.models import (
    Transaction,
    TransactionRow,
    TransactionState,
    TransactionType,
)
from backend.models.interfaces import Database


def data_cleaning_dict_obj(obj, params: tuple):
    return {
        key: val for key, val in obj if key not in params
    }


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


def total_amount_deposit(db: Database, user_id: int) -> int:
    """
    Calcule du montant total tous les versement du compte
    """
    return sum(
        [
            transaction.amount for transaction in db.scan("transactions")
            if transaction.user_id == user_id
               and transaction.type == 'deposit'
               and transaction.state == 'completed'
        ]
    )


def total_amount_refund_withdrawal(db: Database, user_id: int) -> int:
    """
    Calcule du montant total tous les prelevement et remboursement du compte

    """
    return sum(
        [
            transaction.amount for transaction in db.scan("transactions")
            if transaction.user_id == user_id
               and transaction.type in ('refund', 'scheduled_withdrawal')
               and transaction.state in ('completed', 'pending')
        ]
    )


def get_user_scheduled_withdrawal(db: Database, user_id: int):
    """
    1) Recuperer tous les transactions du type scheduled_withdrawal en attente de
        prevelement et enlever les propriétes inutiles

    2) Trier toutes les dates de transaction du type scheduled_withdrawal en attente de
        prevelement du plus proche au plus au lointain

    """
    return sorted(
        [
            data_cleaning_dict_obj(obj=transaction, params=('user_id', 'id')) for transaction in db.scan("transactions")
            if transaction.user_id == user_id
               and transaction.type in ('scheduled_withdrawal')
               and transaction.state in ('scheduled')
        ],
        key=lambda i: i['date'], reverse=False
    )


def calculate_user_current_balance(db: Database, user_id: int):
    """
    amount_covered: montant couvert
    coverage_rate: taux de couverture

    1) On calcule le solde courent du compte
    2) On retire a chaque fois le montant du prelevement et on calcule le taux de
        couverture jusqu'a trouvé un solde courent inferieur au montant de prevelement
        prochain.
    """
    item_transaction, tmp = [], {}
    current_balance = total_amount_deposit(db=db, user_id=user_id) - total_amount_refund_withdrawal(db=db,
                                                                                                    user_id=user_id)
    scheduled_withdrawal = get_user_scheduled_withdrawal(db=db, user_id=user_id)
    item = 0
    while item < len(scheduled_withdrawal):
        for i in scheduled_withdrawal:
            tmp_balance = current_balance
            current_balance, operation_flag = (
                tmp_balance - i.get('amount') if tmp_balance > 0 and tmp_balance > i.get('amount')
                else item_transaction[-1]['current_balance'] if len(item_transaction) != 0 else 0,
                True if tmp_balance - i.get('amount') > 0 else False
            )
            tmp['current_balance'] = current_balance
            tmp['amount_covered'] = i.get('amount') if operation_flag is True else item_transaction[-1][
                'current_balance']

            tmp['coverage_rate'] = '100%' if operation_flag is True else str(int(
                # (i.get('amount') - current_balance) / i.get('amount')
                (current_balance / i.get('amount') * 100)
            )) + '%'

            tmp['amount'] = i.get('amount')
            tmp['date'] = i.get('date')
            tmp['type'] = i.get('type')
            tmp['state'] = i.get('state')
            tmp['status'] = 1 if operation_flag is True else 0
            if tmp not in item_transaction:
                item_transaction.append(tmp)
                tmp = {}
            item += 1
    get_transaction_status_schedule = [i for i in item_transaction if i['status'] == 0]

    if len(get_transaction_status_schedule) == 0:
        transaction_schedule = (
                [j for j in item_transaction if j['status'] == 1] +
                [
                    {**k, **{"current_balance": 0, "amount_covered": 0, "coverage_rate": "0%", }}
                    for k in get_transaction_status_schedule[1::]
                ]
        )
        return transaction_schedule

    transaction_schedule = (
            [j for j in item_transaction if j['status'] == 1] +
            [get_transaction_status_schedule[0]] +
            [
                {**k, **{"current_balance": 0, "amount_covered": 0, "coverage_rate": "0%", }}
                for k in get_transaction_status_schedule[1::]
            ]
    )

    return transaction_schedule
