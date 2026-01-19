from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class ColumnIdentifier:
    name: str
    qualifier: Optional[str] = None # Renamed from 'table' to 'qualifier'
    alias: Optional[str] = None
    is_aggregate: bool = False

    def matches_search(self, search_qualifier: Optional[str], search_name: str) -> bool:
        """
        Logic for the Schema.resolve method.
        Matches a user's search string (e.g. 'e', 'city') against this ID.
        """
        if search_qualifier is None and self.alias == search_name:
            return True
        
        qualifier_match = (search_qualifier is None or self.qualifier == search_qualifier)
        name_match = (self.name == search_name)
        
        return qualifier_match and name_match
    def get_full_name(self) -> str:
        prefix = self.qualifier + '.' if self.qualifier else ''
        return prefix + self.name

    @property
    def display_name(self) -> str:
        return self.alias if self.alias else self.name

class Schema:
    def __init__(self, columns: List[ColumnIdentifier]):
        self.columns = columns

    def resolve(self, search_qualifier: Optional[str], search_name: str) -> int:
        matches = [
            i for i, col in enumerate(self.columns) 
            if col.matches_search(search_qualifier, search_name)
        ]

        if not matches:
            qual_str = f"{search_qualifier}." if search_qualifier else ""
            raise ValueError(f"Column '{qual_str}{search_name}' not found in {[c.get_full_name() for c in self.columns]}.")
        
        if len(matches) > 1:
            match_names = [f"{c.qualifier or ''}.{c.name}" for i in matches for c in [self.columns[i]]]
            raise ValueError(f"Ambiguous column '{search_name}'. Matches: {match_names}")
            
        return matches[0]

    def __add__(self, other: 'Schema'):
        return Schema(self.columns + other.columns)    
    def get_names(self) -> List[str]:
        return [c.display_name for c in self.columns]
