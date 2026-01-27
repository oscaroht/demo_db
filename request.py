
from dataclasses import dataclass
from typing import Optional, Dict, Any

@dataclass(frozen=True)
class QueryRequest:
    sql: str
    transaction_id: int = -1
    params: Optional[Dict[str, Any]] = None
