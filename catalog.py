
class Catalog:
    """Mock database schema information."""
    def __init__(self, schema):
        self.schema = schema # {table_name: [column_name, ..], ..}

    def get_all_column_names(self, table_name) -> list[str]:
        """Returns the list of column names for a table, sorted by their index."""
        table_info = self.schema.get(table_name.lower())
        if not table_info:
            raise ValueError(f"Table '{table_name}' not found in catalog.")
        return self.schema

