from dataclasses import dataclass
from typing import List, Optional

@dataclass
class ColumnInfo:
    full_name: str  # e.g., "users.id"
    alias: None | str = None
    is_aggregate: bool = False

    @property
    def display_name(self) -> str:
        return self.alias if self.alias else self.full_name

class Schema:
    def __init__(self, columns: List[ColumnInfo]):
        self.columns = columns

    def resolve(self, table: Optional[str], name: str) -> int:
        matches = []
        for i, col in enumerate(self.columns):
            col_table = col.full_name.split('.')[0] if '.' in col.full_name else None
            col_name = col.full_name.split('.')[-1]
            
            # Match 1: Explicit FQN (e.city == e.city)
            if table and col.full_name == f"{table}.{name}":
                matches.append(i)
                continue
            
            # Match 2: Name or Alias match without table prefix
            # This is key for picking up "city" from the Aggregate output
            if not table and (col_name == name or col.alias == name):
                matches.append(i)
                continue

            # Match 3: User provided 'e.city' but the schema only has 'city'
            if table and not col_table and col_name == name:
                matches.append(i)

        if not matches:
            raise ValueError(f"Column '{name}' not found in {[c.full_name for c in self.columns]}.")
        if len(matches) > 1:
            raise ValueError(f"Ambiguous column '{name}'. Matches: {[self.columns[i].full_name for i in matches]}")
        return matches[0]

    def __add__(self, other: 'Schema'):
        return Schema(self.columns + other.columns)

    def get_names(self) -> List[str]:
        return [c.display_name for c in self.columns]
