from syntax_tree import ASTNode
from operators import Operator


from dataclasses import dataclass
from typing import List, Optional

@dataclass
class QueryResult:
    columns: List[str]
    rows: List[tuple]
    sql: str
    tokens: List[str]
    ast: ASTNode
    query_plan: Operator
    transaction_id: int
    rowcount: Optional[int] = None
    error: Optional[str] = None
