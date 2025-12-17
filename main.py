from catalog import Catalog
from request import QueryRequest
from result import QueryResult
from engine import DatabaseEngine
from cli import repl, render_result
import readline

table_data = {"users":[
    (1, 'Alice', 30, 'NY', 60000),
    (2, 'Bob', 22, 'SF', 45000),
    (3, 'Charlie', 25, 'NY', 55000),
    (4, 'Dave', 40, 'LA', 70000),
    (5, 'Eve', 19, 'BOS', 30000),
    (6, 'Fay', 22, 'SF', 45000),
    (7, 'Grace', 30, 'NY', 80000),
]}

class MockBufferManager:
    def __init__(self, table_data_map: dict):
        """Initializes the mock with in-memory data for all tables."""
        self.table_data_map = table_data_map

    def get_data_generator(self, table_name: str):
        """
        Simulates the Buffer Manager fetching pages and returning a stream 
        of rows (tuples) for a specific table scan.
        """
        # Logic remains the same: Look up table name and return a row generator
        table_name_lower = table_name.lower()
        # ... (rest of the generator logic) ...
        # Return the generator function
        def data_generator():
            yield from self.table_data_map[table_name_lower]
        return data_generator

mock_schema = {
    'users': {
        'id': 0,
        'name': 1,
        'age': 2,
        'city': 3,
        'salary': 4
    }
}
mock_catalog = Catalog(mock_schema)
mock_buffer_manager = MockBufferManager(table_data)

engine = DatabaseEngine(mock_catalog, mock_buffer_manager)
repl(engine)
