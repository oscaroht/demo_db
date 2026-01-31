from enum import StrEnum, auto

class TransactionStatus(StrEnum):
    OPEN = auto()
    CLOSED = auto()
