
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class QueryResult:
    columns: List[str]
    rows: List[tuple]
    sql: str
    rowcount: Optional[int] = None
    error: Optional[str] = None
