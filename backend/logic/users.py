from typing import Optional

from backend.models import UserRow
from backend.models.interfaces import Database


def user(db: Database, user_id: int) -> Optional[UserRow]:
    return db.get("users", user_id)
