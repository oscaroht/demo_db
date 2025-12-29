from typing import List

class Catalog:
    """Mock database schema information."""
    def __init__(self, schema):
        self.schema = schema # {table_name: [column_name, ..], ..}

    def get_all_column_names(self, table_name) -> list[str]:
        """Returns the list of column names for a table, sorted by their index."""
        column_names: List[str] = self.schema.get(table_name.lower())
        if not column_names:
            raise ValueError(f"Table '{table_name}' not found in catalog.")
        return column_names

