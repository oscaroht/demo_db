
class Catalog:
    """Mock database schema information."""
    def __init__(self, schema):
        self.schema = schema # {table_name: {column_name: index}}

    def get_column_index(self, table_name, column_name):
        """Looks up the zero-based index for a column."""
        table_info = self.schema.get(table_name.upper())
        if not table_info:
            raise ValueError(f"Table '{table_name}' not found in catalog.")
        
        col_index = table_info.get(column_name.upper())
        if col_index is None:
            # Check for aggregate argument 'COUNT(*)' where argument is '*'
            if column_name == '*':
                return '*'
            raise ValueError(f"Column '{column_name}' not found in table '{table_name}'.")
        return col_index

