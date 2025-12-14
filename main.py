
import buffermanager
from metaclasses import PageInfo, TableInfo, TableInfoCollection
from diskmanager import DiskManager
from buffermanager import BufferManager
from queryplanner import QueryPlanner
from catalog import Catalog
from sql_interpreter import tokenize, Parser, TokenStream
from tests.test_e2e import mock_buffer_manager

# user_input = input("Press enter to continue...")

def display_results(query_string: str, columns: list[str], results: list[tuple]):
    """
    Formats and prints query results in an ASCII table.
    """
    print("\n" + "="*80)
    print(f"QUERY: {query_string}")
    print("="*80)

    if not results:
        print("RESULT: (Empty set)")
        print("="*80)
        return

    # 1. Convert all data to strings (including column headers)
    string_results = [tuple(str(x) for x in row) for row in results]
    string_columns = [str(c) for c in columns]

    # 2. Determine max width for each column
    num_cols = len(string_columns)
    max_widths = [len(header) for header in string_columns]

    for row in string_results:
        for i in range(num_cols):
            max_widths[i] = max(max_widths[i], len(row[i]))

    # Add a small padding
    col_widths = [w + 2 for w in max_widths]

    # 3. Print the header row
    header_line = ""
    for i in range(num_cols):
        # Center-align the header text
        header_line += f"| {string_columns[i].center(col_widths[i] - 2)} "
    header_line += "|"
    
    # 4. Print separator lines
    separator = "+" + "+".join("-" * w for w in col_widths) + "+"

    print(separator)
    print(header_line)
    print(separator)

    # 5. Print data rows
    for row in string_results:
        row_line = ""
        for i in range(num_cols):
            # Left-align the data
            row_line += f"| {row[i].ljust(col_widths[i] - 2)} "
        row_line += "|"
        print(row_line)

    print(separator)
    print(f"({len(results)} rows in set)")
    print("="*80)

page_info = PageInfo(
    page_id=1,
    page_offset=0,
    page_size=4096,
    table_info=None
)

tbl_info = TableInfo(
    table_name='tbl',
    column_names=['id', 'num', 'letter'],
    column_datatypes=[int, int, str],
    page_info=[page_info]
)

page_info.table_info = tbl_info

table_collection = TableInfoCollection({tbl_info.table_name: tbl_info})

table_data = {"USERS":[
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
        table_name_upper = table_name.upper()
        # ... (rest of the generator logic) ...
        # Return the generator function
        def data_generator():
            yield from self.table_data_map[table_name_upper]
        return data_generator

mock_schema = {
    'USERS': {
        'ID': 0,
        'NAME': 1,
        'AGE': 2,
        'CITY': 3,
        'SALARY': 4
    }
}
mock_catalog = Catalog(mock_schema)
mock_buffer_manager = MockBufferManager(table_data)
# Re-run the AST generation for the complex query
query = "SELECT name, salary FROM users WHERE (age >= 25 AND city = 'NY') OR salary > 65000 ORDER BY SALARY DESC limit 2;"
query = "SELECT city, avg(salary) FROM users GROUP BY city;"
# query = "SELECT DISTINCT city FROM users;"
# Tokens and Parser are assumed to be available from the previous step
tokens = tokenize(query)
stream = TokenStream(tokens)
print(f"--TOKENS--")
print(stream.tokens)
parser = Parser(stream) 
print("\n" + "="*50)
ast_root = parser.parse()

print(f"--ABSTRACT SYNTAX TREE (AST)--")
print(ast_root.display())
print("\n" + "="*50)

planner = QueryPlanner(mock_catalog, mock_buffer_manager)
query_plan_root = planner.plan_query(ast_root)
print("--QUERY PLAN--")
print(query_plan_root.display_plan())


result = list(query_plan_root.next())

display_results(query, query_plan_root.column_names, result)
