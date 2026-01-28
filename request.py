
from dataclasses import dataclass
from typing import Optional, Dict, Any

@dataclass(frozen=True)
class QueryRequest:
    sql: str
    transaction_id: int = -1
    auto_commit: bool = True
    params: Optional[Dict[str, Any]] = None
