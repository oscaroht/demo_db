import pytest
from unittest.mock import MagicMock
# Assume your planner, operators, and AST are imported from shared modules
from queryplanner import QueryPlanner 
from sql_interpreter import tokenize, TokenStream, Parser 

# --- FIXTURE SETUP ---

class MockCatalog:
    """Mocks the Catalog to provide column indices for the 'users' table."""
    # Schema: (0: id, 1: name, 2: age, 3: city, 4: salary)
    SCHEMA = {'id': 0, 'name': 1, 'age': 2, 'city': 3, 'salary': 4}
    
    def get_column_index(self, table_name, column_name):
        return self.SCHEMA.get(column_name)

@pytest.fixture
def mock_catalog():
    """Provides a mocked version of the Catalog."""
    return MockCatalog()

@pytest.fixture
def planner(mock_catalog):
    """Provides an instance of the QueryPlanner initialized with the mock catalog."""
    return QueryPlanner(mock_catalog)

# --- CORE EXECUTION HELPER ---

def parse_sql(query_string):
    """Uses the provided parser logic to convert a SQL string into an AST."""
    tokens = tokenize(query_string)
    stream = TokenStream(tokens)
    return Parser(stream).parse()

def execute_query_and_get_results(planner, query_string):
    """
    1. Parses the query string to AST.
    2. Plans the AST to a root operator.
    3. Executes the plan and returns all results.
    """
    # 1. Parse
    ast_root = parse_sql(query_string) 
    
    # 2. Plan
    plan_root = planner.plan_query(ast_root)
    
    # 3. Execute
    results = list(plan_root.next())
    return results

# --- E2E FUNCTIONAL TESTS (Aggregation) ---

def test_e2e_global_avg_salary(planner):
    """Tests global aggregation and verifies index mapping to [0] for AVG(salary)."""
    query = "SELECT AVG(salary) FROM users;"
    
    # Expected: (60k + 45k + 55k + 70k + 30k) / 5 = 52000.0
    expected = [(52000.0,)]
    results = execute_query_and_get_results(planner, query)
    
    assert results == expected

def test_e2e_grouped_avg_only(planner):
    """Tests grouped aggregation, SELECT list only contains the aggregate."""
    query = "SELECT AVG(salary) FROM users GROUP BY city;"
    
    # Expected results (order is not guaranteed, so we check the set)
    # NY: 57500.0, SF: 45000.0, LA: 70000.0, BOS: 30000.0
    expected_set = {
        (57500.0,), (45000.0,), (70000.0,), (30000.0,)
    }
    
    results = execute_query_and_get_results(planner, query)
    
    assert set(results) == expected_set
    assert len(results) == 4

def test_e2e_grouped_select_key_and_agg(planner):
    """Tests grouped aggregation where the SELECT list includes the key and the aggregate. 
    Verifies index mapping to [0, 1] is correct."""
    query = "SELECT city, AVG(salary) FROM users GROUP BY city;"
    
    expected_set = {
        ('NY', 57500.0), 
        ('SF', 45000.0), 
        ('LA', 70000.0), 
        ('BOS', 30000.0)
    }
    
    results = execute_query_and_get_results(planner, query)
    
    assert set(results) == expected_set
    assert len(results) == 4

def test_e2e_multiple_aggregates_and_keys(planner):
    """Tests multiple aggregates and verifies correct sequencing of output indices."""
    # Output schema: [city (0), COUNT(*) (1), MAX(salary) (2)]
    query = "SELECT city, COUNT(*), MAX(salary) FROM users GROUP BY city;"
    
    expected_set = {
        ('NY', 2, 60000),   # Alice (60k), Charlie (55k)
        ('SF', 1, 45000),   # Bob
        ('LA', 1, 70000),   # Dave
        ('BOS', 1, 30000),  # Eve
    }
    
    results = execute_query_and_get_results(planner, query)
    
    assert set(results) == expected_set
    assert len(results) == 4
