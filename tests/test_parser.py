import pytest
from sql_interpreter import tokenize, TokenStream, Parser
from syntax_tree import (
    SelectStatement, CreateStatement, InsertStatement, DropStatement,
    BinaryOp, AggregateCall, OrderByClause, GroupByClause, LimitClause,
    TableRef, Join, Star
)

def parse(query: str):
    tokens = tokenize(query)
    ts = TokenStream(tokens)
    parser = Parser(ts)
    return parser.parse()

# -------------------- FIXED ORIGINAL TESTS --------------------

def test_simple_select_columns():
    ast = parse("SELECT id, name FROM users;")
    assert isinstance(ast, SelectStatement)
    assert isinstance(ast.from_clause, TableRef)
    assert ast.from_clause.name == "users"
    assert len(ast.columns) == 2
    assert ast.columns[0].name == "id"
    assert ast.columns[1].name == "name"

def test_select_star():
    ast = parse("SELECT * FROM users;")
    assert isinstance(ast.columns[0], Star)

def test_join_syntax():
    ast = parse("SELECT e.name, c.id FROM employee AS e JOIN contract AS c ON e.id = c.employee_id;")
    assert isinstance(ast.from_clause, Join)
    assert ast.from_clause.left.alias == "e"
    assert ast.from_clause.right.alias == "c"
    # In your syntax_tree, Join condition is a BinaryOp
    assert isinstance(ast.from_clause.condition, BinaryOp)
    assert ast.from_clause.condition.op == "="

def test_where_simple_comparison():
    ast = parse("SELECT id FROM users WHERE age > 18;")
    assert isinstance(ast.where_clause, BinaryOp)
    assert ast.where_clause.op == ">"
    assert ast.where_clause.left.name == "age"
    assert ast.where_clause.right.value == 18

# -------------------- 10 NEW FEATURE TESTS --------------------

def test_create_table_statement():
    ast = parse("CREATE TABLE staff (id INT, name TEXT);")
    assert isinstance(ast, CreateStatement)
    assert ast.table_name == "staff"
    assert ast.column_names == ["id", "name"]
    assert ast.column_types == ["INT", "TEXT"]

def test_insert_values_statement():
    ast = parse("INSERT INTO staff (id, name) VALUES (1, 'Alice');")
    assert isinstance(ast, InsertStatement)
    assert ast.table_name == "staff"
    assert ast.columns[0].name == "id"
    # values is a list of rows (lists of literals)
    assert ast.values[0][0].value == 1
    assert ast.values[0][1].value == "Alice"

def test_insert_from_select():
    ast = parse("INSERT INTO archive SELECT * FROM staff;")
    assert isinstance(ast, InsertStatement)
    assert ast.table_name == "archive"
    assert isinstance(ast.select, SelectStatement)
    assert ast.values is None

def test_drop_table_statement():
    # Note: Ensure your Parser handles DROP; if not, this verifies the AST structure
    ast = parse("DROP TABLE staff;")
    assert isinstance(ast, DropStatement)
    assert ast.table_name == "staff"

def test_logical_and_precedence():
    ast = parse("SELECT * FROM t WHERE a=1 AND b=2;")
    # Logical AND is a BinaryOp in your tree
    assert isinstance(ast.where_clause, BinaryOp)
    assert ast.where_clause.op == "AND"
    assert isinstance(ast.where_clause.left, BinaryOp)

def test_arithmetic_precedence():
    ast = parse("SELECT 1 + 2 * 3 FROM t;")
    expr = ast.columns[0]
    assert isinstance(expr, BinaryOp)
    assert expr.op == "+"
    # 2*3 should be the right-side BinaryOp if precedence is correct
    assert expr.right.op == "*"
    assert expr.right.left.value == 2

def test_aggregate_min_max():
    ast = parse("SELECT MIN(age), MAX(salary) FROM employee;")
    assert ast.columns[0].function_name == "MIN"
    assert ast.columns[1].function_name == "MAX"

def test_order_by_multiple_columns():
    ast = parse("SELECT name FROM employee ORDER BY city DESC, age ASC;")
    items = ast.order_by_clause.sort_items
    assert items[0].column.name == "city"
    assert items[0].direction == "DESC"
    assert items[1].direction == "ASC"

def test_group_by_parsing():
    ast = parse("SELECT city, COUNT(*) FROM employee GROUP BY city;")
    assert isinstance(ast.group_by_clause, GroupByClause)
    assert ast.group_by_clause.columns[0].name == "city"

def test_distinct_parsing():
    ast = parse("SELECT DISTINCT name FROM employee;")
    assert ast.is_distinct is True
