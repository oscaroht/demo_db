import pytest
from sql_interpreter import tokenize, TokenStream, Parser
from syntax_tree import (
    SelectStatement, ColumnRef, Literal, Comparison, LogicalExpression,
    AggregateCall, OrderByClause, SortItem, GroupByClause, LimitClause,
    TableRef, Join, Star
)

def parse(query: str):
    tokens = tokenize(query)
    ts = TokenStream(tokens)
    parser = Parser(ts)
    return parser.parse()

# -------------------- BASIC SELECT & FROM --------------------

def test_simple_select_columns():
    ast = parse("SELECT id, name FROM users;")

    assert isinstance(ast, SelectStatement)
    # Refactored: from_clause is now a TableRef object
    assert isinstance(ast.from_clause, TableRef)
    assert ast.from_clause.name == "users"

    assert len(ast.columns) == 2
    assert isinstance(ast.columns[0], ColumnRef)
    assert ast.columns[0].name == "id"
    assert ast.columns[1].name == "name"

def test_select_star():
    ast = parse("SELECT * FROM users;")

    assert len(ast.columns) == 1
    # Refactored: * is represented by a Star object or a specific ColumnRef name
    assert isinstance(ast.columns[0], Star) or ast.columns[0].name == "*"

# -------------------- JOINS --------------------

def test_join_syntax():
    ast = parse("SELECT e.name, c.id FROM employee AS e JOIN contract AS c ON e.id = c.employee_id;")

    assert isinstance(ast.from_clause, Join)
    assert ast.from_clause.left.name == "employee"
    assert ast.from_clause.left.alias == "e"
    assert ast.from_clause.right.name == "contract"
    assert ast.from_clause.right.alias == "c"
    
    # Verify Join Condition
    assert isinstance(ast.from_clause.condition, Comparison)
    assert ast.from_clause.condition.op == "="
    assert ast.from_clause.condition.left.table == "e"
    assert ast.from_clause.condition.left.name == "id"

# -------------------- WHERE CLAUSE --------------------

def test_where_simple_comparison():
    ast = parse("SELECT id FROM users WHERE age > 18;")

    where = ast.where_clause
    assert isinstance(where, Comparison)
    assert where.op == ">"
    assert where.left.name == "age"
    assert where.right.value == 18

# -------------------- AGGREGATES --------------------

def test_select_aggregate_count_star():
    ast = parse("SELECT COUNT(*) FROM users;")

    agg = ast.columns[0]
    assert isinstance(agg, AggregateCall)
    assert agg.function_name == "COUNT"
    # Refactored: argument is now a Star object
    assert isinstance(agg.argument, Star)

def test_select_aggregate_with_column():
    ast = parse("SELECT MAX(age) FROM users;")

    agg = ast.columns[0]
    assert agg.function_name == "MAX"
    # Refactored: argument is now a ColumnRef object
    assert isinstance(agg.argument, ColumnRef)
    assert agg.argument.name == "age"

# -------------------- GROUP BY --------------------

def test_group_by_single_column():
    ast = parse("SELECT age, COUNT(*) FROM users GROUP BY age;")

    group = ast.group_by_clause
    assert isinstance(group, GroupByClause)
    assert len(group.columns) == 1
    assert isinstance(group.columns[0], ColumnRef)
    assert group.columns[0].name == "age"

# -------------------- ORDER BY --------------------

def test_order_by_default_direction():
    ast = parse("SELECT id FROM users ORDER BY age;")

    order = ast.order_by_clause
    assert isinstance(order, OrderByClause)
    item = order.sort_items[0]
    assert item.column.name == "age"
    assert item.direction == "ASC"

# -------------------- LIMIT --------------------

def test_limit_clause():
    ast = parse("SELECT id FROM users LIMIT 10;")

    assert ast.limit_clause.count == 10

# -------------------- LITERALS --------------------

def test_literals():
    ast_str = parse("SELECT id FROM users WHERE name = 'Alice';")
    assert ast_str.where_clause.right.value == "Alice"

    ast_float = parse("SELECT id FROM products WHERE price >= 19.99;")
    assert ast_float.where_clause.right.value == 19.99    


def test_order_by_descending():
    ast = parse("SELECT id FROM users ORDER BY age DESC, name ASC;")

    items = ast.order_by_clause.sort_items

    assert items[0].column.name == "age"
    assert items[0].direction == "DESC"

    assert items[1].column.name == "name"
    assert items[1].direction == "ASC"


# -------------------- LIMIT --------------------

def test_limit_clause():
    ast = parse("SELECT id FROM users LIMIT 10;")

    limit = ast.limit_clause
    assert isinstance(limit, LimitClause)
    assert limit.count == 10


# -------------------- STRING & FLOAT LITERALS --------------------

def test_where_string_literal():
    ast = parse("SELECT id FROM users WHERE name = 'Alice';")

    where = ast.where_clause
    assert where.right.value == "Alice"


def test_where_float_literal():
    ast = parse("SELECT id FROM products WHERE price >= 19.99;")

    where = ast.where_clause
    assert where.right.value == 19.99
