from sql_interpreter import tokenize, TokenStream, Parser
from syntax_tree import (
    SelectStatement,
    ColumnRef,
    Literal,
    Comparison,
    LogicalExpression,
    AggregateCall,
    OrderByClause,
    SortItem,
    GroupByClause,
    LimitClause,
)


def parse(query: str):
    tokens = tokenize(query)
    ts = TokenStream(tokens)
    parser = Parser(ts)
    return parser.parse()


# -------------------- BASIC SELECT --------------------

def test_simple_select_columns():
    ast = parse("SELECT id, name FROM users;")

    assert isinstance(ast, SelectStatement)
    assert ast.table == "users"

    assert len(ast.columns) == 2
    assert isinstance(ast.columns[0], ColumnRef)
    assert ast.columns[0].name == "id"
    assert ast.columns[1].name == "name"

    assert ast.where_clause is None
    assert ast.order_by_clause is None
    assert ast.limit_clause is None


def test_select_star():
    ast = parse("SELECT * FROM users;")

    assert len(ast.columns) == 1
    assert ast.columns[0].name == "*"


# -------------------- DISTINCT --------------------

def test_select_distinct():
    ast = parse("SELECT DISTINCT name FROM users;")

    assert ast.is_distinct is True
    assert ast.columns[0].name == "name"


# -------------------- WHERE CLAUSE --------------------

def test_where_simple_comparison():
    ast = parse("SELECT id FROM users WHERE age > 18;")

    where = ast.where_clause
    assert isinstance(where, Comparison)
    assert where.op == ">"

    assert isinstance(where.left, ColumnRef)
    assert where.left.name == "age"

    assert isinstance(where.right, Literal)
    assert where.right.value == 18


def test_where_and_or_precedence():
    ast = parse(
        "SELECT id FROM users WHERE age > 18 AND active = 1 OR admin = 1;"
    )

    # OR should be the top-level operator
    assert isinstance(ast.where_clause, LogicalExpression)
    assert ast.where_clause.op == "OR"

    left = ast.where_clause.left
    right = ast.where_clause.right

    assert isinstance(left, LogicalExpression)
    assert left.op == "AND"

    assert isinstance(right, Comparison)
    assert right.left.name == "admin"


def test_where_parentheses_override_precedence():
    ast = parse(
        "SELECT id FROM users WHERE age > 18 AND (active = 1 OR admin = 1);"
    )

    where = ast.where_clause
    assert where.op == "AND"

    assert isinstance(where.right, LogicalExpression)
    assert where.right.op == "OR"


# -------------------- AGGREGATES --------------------

def test_select_aggregate_count_star():
    ast = parse("SELECT COUNT(*) FROM users;")

    agg = ast.columns[0]
    assert isinstance(agg, AggregateCall)
    assert agg.function_name == "COUNT"
    assert agg.argument == "*"


def test_select_aggregate_with_column():
    ast = parse("SELECT MAX(age) FROM users;")

    agg = ast.columns[0]
    assert agg.function_name == "MAX"
    assert agg.argument == "age"


# -------------------- GROUP BY --------------------

def test_group_by_single_column():
    ast = parse("SELECT age, COUNT(*) FROM users GROUP BY age;")

    group = ast.group_by_clause
    assert isinstance(group, GroupByClause)
    assert len(group.columns) == 1
    assert group.columns[0].name == "age"


def test_group_by_multiple_columns():
    ast = parse(
        "SELECT country, city, COUNT(*) FROM users GROUP BY country, city;"
    )

    group = ast.group_by_clause
    assert len(group.columns) == 2
    assert [c.name for c in group.columns] == ["country", "city"]


# -------------------- ORDER BY --------------------

def test_order_by_default_direction():
    ast = parse("SELECT id FROM users ORDER BY age;")

    order = ast.order_by_clause
    assert isinstance(order, OrderByClause)

    item = order.sort_items[0]
    assert isinstance(item, SortItem)
    assert item.column.name == "age"
    assert item.direction == "ASC"


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


