from sql_interpreter import tokenize, TokenStream, Parser
from queryplanner import QueryPlanner
from syntax_tree import ColumnRef, AggregateCall


# -------------------- TEST INFRA --------------------

class FakeCatalog:
    def __init__(self, schema):
        self.schema = schema

    def get_column_index(self, table, col):
        return self.schema.index(col)

    def get_all_column_names(self, table):
        return self.schema


class FakeBufferManager:
    def get_data_generator(self, table):
        return []


class CaptureSorter:
    """Helper to extract sort keys from a planned query tree."""
    def __init__(self, root):
        self.sort_keys = None
        self._walk(root)

    def _walk(self, node):
        if node.__class__.__name__ == 'Sorter':
            self.sort_keys = node.sort_keys
        if hasattr(node, 'parent') and node.parent:
            self._walk(node.parent)


# -------------------- HELPERS --------------------

def plan(query, schema):
    ast = Parser(TokenStream(tokenize(query))).parse()
    planner = QueryPlanner(FakeCatalog(schema), FakeBufferManager())
    return planner.plan_query(ast)


# -------------------- BUG: DISTINCT + ORDER BY --------------------

# ❌ BUG: _get_trivial_sort_keys assumes ORDER BY columns appear
#         in the same order as SELECT columns


def test_distinct_order_by_wrong_column_sorted():
    plan_root = plan(
        "SELECT DISTINCT name, age FROM users ORDER BY age;",
        schema=["id", "name", "age"],
    )

    sorter = CaptureSorter(plan_root)

    # EXPECTED: age is column index 1 in the projection [name, age]
    # ACTUAL (BUG): age sorted using index 0
    assert sorter.sort_keys == [(1, False)]


# -------------------- BUG: DISTINCT + MULTI ORDER BY --------------------


def test_distinct_multi_order_by_column_mismatch():
    plan_root = plan(
        "SELECT DISTINCT name, age FROM users ORDER BY age, name;",
        schema=["id", "name", "age"],
    )

    sorter = CaptureSorter(plan_root)

    # EXPECTED order: age (1), name (0)
    assert sorter.sort_keys == [(1, False), (0, False)]


# -------------------- CONTROL TEST (NO DISTINCT) --------------------


def test_non_distinct_order_by_is_correct():
    plan_root = plan(
        "SELECT name, age FROM users ORDER BY age;",
        schema=["id", "name", "age"],
    )

    sorter = CaptureSorter(plan_root)

    # age index from catalog is 2
    assert sorter.sort_keys == [(2, False)]


# -------------------- BUG: DISTINCT + AGGREGATE ORDER BY --------------------


def test_distinct_order_by_aggregate_column():
    plan_root = plan(
        "SELECT DISTINCT city, COUNT(*) FROM users GROUP BY city ORDER BY COUNT(*);",
        schema=["id", "city"],
    )

    sorter = CaptureSorter(plan_root)

    # EXPECTED: COUNT(*) is index 1 in [city, COUNT(*)]
    assert sorter.sort_keys == [(1, False)]
