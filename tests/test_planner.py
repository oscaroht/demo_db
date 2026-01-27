import pytest
from queryplanner import QueryPlanner
from sql_interpreter import tokenize, TokenStream, Parser
from catalog import Table
from schema import Schema, ColumnIdentifier

class MockTransaction:
    def __init__(self, table_schemas):
        # table_schemas = {"name": ["col1", "col2"]}
        self.tables = {
            name: Table(name, cols, [str] * len(cols)) 
            for name, cols in table_schemas.items()
        }
    def get_table_by_name(self, name):
        return self.tables.get(name.lower())
    def get_page_generator_from_table_by_name(self, name):
        return iter([])

def plan(query, schemas):
    ast = Parser(TokenStream(tokenize(query))).parse()
    planner = QueryPlanner(MockTransaction(schemas))
    return planner.plan_query(ast)

def get_op_name(op):
    return op.__class__.__name__

# -------------------- FIXED ORIGINAL TESTS --------------------

def test_plan_simple_projection():
    root = plan("SELECT name FROM users;", {"users": ["id", "name"]})
    assert get_op_name(root) == "Projection"
    assert get_op_name(root.parent) == "ScanOperator"

def test_plan_distinct_sorter():
    root = plan("SELECT DISTINCT name FROM users ORDER BY name;", {"users": ["name"]})
    # For DISTINCT + ORDER BY, the planner should insert a Sorter and Distinct operator
    chain = []
    curr = root
    while curr:
        chain.append(get_op_name(curr))
        curr = getattr(curr, 'parent', None)
    assert "Distinct" in chain
    assert "Sorter" in chain

# -------------------- 10 NEW FEATURE TESTS --------------------

def test_plan_where_filter_placement():
    root = plan("SELECT name FROM users WHERE id = 1;", {"users": ["id", "name"]})
    # Expected: Projection -> Filter -> Scan
    assert get_op_name(root.parent) == "Filter"
    assert get_op_name(root.parent.parent) == "ScanOperator"

def test_plan_limit_operator():
    root = plan("SELECT * FROM users LIMIT 5;", {"users": ["id"]})
    # The limit should be near the top of the plan
    assert get_op_name(root.parent) == "Limit"
    assert root.parent.count == 5

def test_plan_aggregate_count():
    root = plan("SELECT COUNT(*) FROM users;", {"users": ["id"]})
    # Check if Aggregate operator is present
    curr = root
    found_agg = False
    while curr:
        if get_op_name(curr) == "Aggregate": found_agg = True
        curr = getattr(curr, 'parent', None)
    assert found_agg

def test_plan_join_structure():
    schemas = {"t1": ["id"], "t2": ["id"]}
    root = plan("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id;", schemas)
    # Projection -> NestedLoopJoin
    assert get_op_name(root.parent) == "NestedLoopJoin"

def test_plan_insert_statement():
    # Use the manual planner call since plan() returns Select-based plans
    ast = Parser(TokenStream(tokenize("INSERT INTO users VALUES (1);"))).parse()
    planner = QueryPlanner(MockTransaction({"users": ["id"]}))
    root = planner.plan_query(ast)
    assert get_op_name(root) == "Insert"

def test_plan_create_table():
    ast = Parser(TokenStream(tokenize("CREATE TABLE test (a INT);"))).parse()
    planner = QueryPlanner(MockTransaction({}))
    root = planner.plan_query(ast)
    assert get_op_name(root) == "StatusOperator"

def test_plan_multi_column_order_by():
    root = plan("SELECT name FROM users ORDER BY name, id;", {"users": ["id", "name"]})
    # Find Sorter and check sort_keys length
    curr = root
    while curr and get_op_name(curr) != "Sorter":
        curr = getattr(curr, 'parent', None)
    assert len(curr.sort_keys) == 2

def test_plan_aliased_projection():
    root = plan("SELECT id AS user_id FROM users;", {"users": ["id"]})
    schema = root.get_output_schema()
    assert schema.get_names() == ["user_id"]

def test_plan_arithmetic_in_projection():
    root = plan("SELECT id + 1 FROM users;", {"users": ["id"]})
    assert get_op_name(root) == "Projection"
    # Verifies the planner doesn't crash compiling the BinaryOp lambda

def test_plan_complex_group_by():
    root = plan("SELECT city, AVG(salary) FROM users GROUP BY city;", {"users": ["city", "salary"]})
    # Aggregate should be configured with groups
    curr = root
    while curr and get_op_name(curr) != "Aggregate":
        curr = getattr(curr, 'parent', None)
    assert curr is not None
