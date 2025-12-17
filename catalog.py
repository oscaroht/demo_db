
class Catalog:
    """Mock database schema information."""
    def __init__(self, schema):
        self.schema = schema # {table_name: {column_name: index}}

    def get_column_index(self, table_name, column_name):
        """Looks up the zero-based index for a column."""
        table_info = self.schema.get(table_name.lower())
        if not table_info:
            raise ValueError(f"Table '{table_name}' not found in catalog.")
        
        col_index = table_info.get(column_name.lower())
        if col_index is None:
            # Check for aggregate argument 'COUNT(*)' where argument is '*'
            if column_name == '*':
                return '*'
            raise ValueError(f"Column '{column_name}' not found in table '{table_name}'.")
        return col_index

    def get_all_column_names(self, table_name) -> list[str]:
        """Returns the list of column names for a table, sorted by their index."""
        table_info = self.schema.get(table_name.lower())
        if not table_info:
            raise ValueError(f"Table '{table_name}' not found in catalog.")
        
        # Invert the dictionary to map index -> name
        index_to_name = {index: name for name, index in table_info.items()}
        
        # Get the highest index and create a list of names in index order
        max_index = max(index_to_name.keys())
        
        # Ensure the list is sorted by index (0, 1, 2, ...)
        sorted_names = [index_to_name[i] for i in range(max_index + 1)]
        
        return sorted_names

