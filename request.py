
from dataclasses import dataclass
from typing import Optional, Dict, Any

@dataclass(frozen=True)
class QueryRequest:
    sql: str
    params: Optional[Dict[str, Any]] = None
