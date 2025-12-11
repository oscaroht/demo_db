
from metaclasses import PageInfo, TableInfo, TableInfoCollection
from diskmanager import DiskManager
from buffermanager import BufferManager
from queryplanner import QueryPlanner
from catalog import Catalog
from sql_interpreter import tokenize, Parser, TokenStream

# user_input = input("Press enter to continue...")

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

# Re-run the AST generation for the complex query
query = "SELECT name, salary FROM users WHERE (age >= 25 AND city = 'NY') OR salary > 65000 ORDER BY SALARY DESC limit 2;"
# Tokens and Parser are assumed to be available from the previous step
tokens = tokenize(query)
print(tokens)
stream = TokenStream(tokens)
print(stream.tokens)
parser = Parser(stream) 
ast_root = parser.parse()

print(f"--- 1. AST Generated ---")
print(ast_root.display())
print("\n" + "="*50 + "\n")

planner = QueryPlanner(mock_catalog)
query_plan_root = planner.plan_query(ast_root)

print("--- Human-Readable Query Plan (Top-Down Execution Flow) ---")
print(query_plan_root.display_plan())

print("--- 2. Executable Query Plan (Root Operator) ---")

print("\n--- 3. Execution Results (Running query_plan_root.next()) ---")
for row in query_plan_root.next():
    print(row)

