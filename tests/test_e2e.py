import pytest
from queryplanner import QueryPlanner 
from sql_interpreter import tokenize, TokenStream, Parser 
from engine import DatabaseEngine
from result import QueryResult
from request import QueryRequest

@pytest.fixture
def mock_table_data() -> dict:
    """
    Provides the fixed, iterable mock data for the 'USERS' table.
    Schema: (0: ID, 1: NAME, 2: AGE, 3: CITY, 4: SALARY)
    """
    return {"USERS":[
        (1, 'Alice', 30, 'NY', 60000),  # NY-60k
        (2, 'Bob', 22, 'SF', 45000),   # SF-45k
        (3, 'Charlie', 25, 'NY', 55000), # NY-55k (NY is duplicate city)
        (4, 'Dave', 40, 'LA', 70000),   # LA-70k
        (5, 'Eve', 19, 'BOS', 30000),  # BOS-30k
        (6, 'Fay', 22, 'SF', 45000),   # SF-45k (Duplicate SF city, duplicate 45k salary, duplicate 22 age)
        (7, 'Grace', 30, 'NY', 80000),  # NY-80k (NY is duplicate city, 30 is duplicate age)
        (8, 'Hank', 22, 'LA', 70000),   # LA-70k (Duplicate LA city, duplicate 70k salary, duplicate 22 age)
        (9, 'Ivy', 25, 'NY', 55000),    # NY-55k (Duplicate NY city, duplicate 55k salary, duplicate 25 age)
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
        table_name_upper = table_name.upper()
        def data_generator():
            yield from self.table_data_map[table_name_upper]
        return data_generator


class MockCatalog:
    """Mocks the Catalog to provide column indices for the 'users' table."""
    SCHEMA = {'id': 0, 'name': 1, 'age': 2, 'city': 3, 'salary': 4}
    
    def get_column_index(self, table_name, column_name):
        return self.SCHEMA.get(column_name)

    def get_all_column_names(self, table_name):
        """Returns the list of column names in schema order for SELECT *."""
        # Note: Dictionaries preserve insertion order from Python 3.7+
        # We assume the order is based on the keys in the SCHEMA dict.
        return list(self.SCHEMA.keys())

@pytest.fixture
def mock_catalog():
    """Provides a mocked version of the Catalog."""
    return MockCatalog()

@pytest.fixture
def mock_buffer_manager(mock_table_data):
    """Creates the MockBufferManager object from the raw data map."""
    return MockBufferManager(mock_table_data)

@pytest.fixture
def planner(mock_catalog, mock_buffer_manager):
    """Planner is initialized with the Catalog and the Mock Buffer Manager."""
    return QueryPlanner(mock_catalog, mock_buffer_manager)

def execute_query_and_get_results(engine, query_string):
    return engine.execute(QueryRequest(sql=query_string)).rows

@pytest.fixture
def engine(mock_table_data):
    catalog = MockCatalog()
    buffer_mgr = MockBufferManager(mock_table_data)
    return DatabaseEngine(catalog, buffer_mgr)


def test_select_all_with_filter(engine):
    """Test SELECT * with a WHERE clause."""
    query = "SELECT * FROM users WHERE age > 25;"
    results = execute_query_and_get_results(engine, query)
    
    # Expected rows: Alice (30), Dave (40), Grace (30)
    assert len(results) == 3
    assert ('Alice' in [r[1] for r in results])
    assert ('Bob' not in [r[1] for r in results])
    
def test_select_projection_with_filter(engine):
    """Test SELECT specific columns with a WHERE clause."""
    query = "SELECT name, salary FROM users WHERE city = 'NY';"
    results = execute_query_and_get_results(engine, query)
    
    # Expected results: ('Alice', 60000), ('Charlie', 55000), ('Grace', 80000), ('Ivy', 55000)
    assert len(results) == 4
    assert set(results) == {('Alice', 60000), ('Charlie', 55000), ('Grace', 80000), ('Ivy', 55000)}

# --- DISTINCT Tests ---

def test_select_distinct_single_column(engine):
    """Test SELECT DISTINCT on a column with duplicates (CITY)."""
    query = "SELECT DISTINCT city FROM users;"
    results = execute_query_and_get_results(engine, query)
    
    # Expected unique cities: NY, SF, LA, BOS
    assert len(results) == 4
    assert set(r[0] for r in results) == {'NY', 'SF', 'LA', 'BOS'}
    
def test_select_distinct_multiple_columns(engine):
    """Test SELECT DISTINCT on a combination of columns (CITY, SALARY)."""
    query = "SELECT DISTINCT city, salary FROM users;"
    results = execute_query_and_get_results(engine, query)
    
    # Duplicate sets: (SF, 45000), (LA, 70000), (NY, 55000)
    # Total unique combinations: 9 total rows - 3 duplicates = 6 unique combinations.
    assert len(results) == 6
    expected_combos = {
        ('NY', 60000), ('SF', 45000), ('NY', 55000), 
        ('LA', 70000), ('BOS', 30000), ('NY', 80000)
    }
    assert set(results) == expected_combos

def test_select_distinct_with_filter(engine):
    """Test SELECT DISTINCT combined with a WHERE clause."""
    query = "SELECT DISTINCT age FROM users WHERE city != 'NY';"
    results = execute_query_and_get_results(engine, query)
    
    # Rows not in NY: Bob(22), Dave(40), Eve(19), Fay(22), Hank(22, 70k)
    # Unique ages: 22, 40, 19
    assert len(results) == 3
    assert set(r[0] for r in results) == {22, 40, 19}

# --- AGGREGATE Tests (Assuming your Aggregate operator is fully functional) ---

def test_global_aggregate_count_star(engine):
    """Test COUNT(*) without GROUP BY."""
    query = "SELECT COUNT(*) FROM users;"
    results = execute_query_and_get_results(engine, query)
    
    # Total rows: 9
    assert results == [(9,)]
 
def test_count_distinct(engine):
    """Test SELECT COUNT(DISTINCT age) with a WHERE clause."""
    query = "SELECT COUNT(DISTINCT age) FROM users;"
    results = execute_query_and_get_results(engine, query)
    
    assert len(results) == 1
    assert (5 in [r[0] for r in results])

   
def test_global_aggregate_sum(engine):
    """Test SUM(SALARY) without GROUP BY."""
    query = "SELECT SUM(salary) FROM users;"
    results = execute_query_and_get_results(engine, query)
    
    # Sum: 60k + 45k + 55k + 70k + 30k + 45k + 80k + 70k + 55k = 510,000
    assert results == [(510000,)]

def test_group_by_with_count_and_avg(engine):
    """Test GROUP BY CITY, counting rows and averaging salary per city."""
    query = "SELECT city, COUNT(*), AVG(salary) FROM users GROUP BY city;"
    results = execute_query_and_get_results(engine, query)
    
    # NY: 4 rows (60k, 55k, 80k, 55k). Avg: (250k / 4) = 62500.
    # SF: 2 rows (45k, 45k). Avg: 45000.
    # LA: 2 rows (70k, 70k). Avg: 70000.
    # BOS: 1 row (30k). Avg: 30000.
    
    expected_results = {
        ('NY', 4, 62500.0), 
        ('SF', 2, 45000.0), 
        ('LA', 2, 70000.0), 
        ('BOS', 1, 30000.0)
    }
    assert len(results) == 4
    assert set(results) == expected_results

def test_distinct_with_order_by(engine):
    """Test SELECT DISTINCT combined with ORDER BY (requires Pre-Projection)."""
    query = "SELECT DISTINCT age FROM users ORDER BY age DESC;"
    results = execute_query_and_get_results(engine, query)
    
    # Unique ages: 19, 22, 25, 30, 40
    # Expected: (40), (30), (25)
    assert results == [(40,), (30,), (25,), (22,), (19,)]


def test_select_order_by_desc(engine):
    """Test SELECT with ORDER BY (requires Pre-Projection)."""
    query = "SELECT age FROM users ORDER BY age DESC;"
    results = execute_query_and_get_results(engine, query)

        # (1, 'Alice', 30, 'NY', 60000),  # NY-60k
        # (2, 'Bob', 22, 'SF', 45000),   # SF-45k
        # (3, 'Charlie', 25, 'NY', 55000), # NY-55k (NY is duplicate city)
        # (4, 'Dave', 40, 'LA', 70000),   # LA-70k
        # (5, 'Eve', 19, 'BOS', 30000),  # BOS-30k
        # (6, 'Fay', 22, 'SF', 45000),   # SF-45k (Duplicate SF city, duplicate 45k salary, duplicate 22 age)
        # (7, 'Grace', 30, 'NY', 80000),  # NY-80k (NY is duplicate city, 30 is duplicate age)
        # (8, 'Hank', 22, 'LA', 70000),   # LA-70k (Duplicate LA city, duplicate 70k salary, duplicate 22 age)
        # (9, 'Ivy', 25, 'NY', 55000),    # NY-55k (Duplicate NY city, duplicate 55k salary, duplicate 25 age)
        #
    assert results == [(40,), (30,), (30, ), (25,), (25,), (22,), (22,), (22,), (19,)]

def test_select_order_by_asc(engine):
    """Test SELECT with ORDER BY (requires Pre-Projection)."""
    query = "SELECT age FROM users ORDER BY age ASC;"
    results = execute_query_and_get_results(engine, query)

        # (1, 'Alice', 30, 'NY', 60000),  # NY-60k
        # (2, 'Bob', 22, 'SF', 45000),   # SF-45k
        # (3, 'Charlie', 25, 'NY', 55000), # NY-55k (NY is duplicate city)
        # (4, 'Dave', 40, 'LA', 70000),   # LA-70k
        # (5, 'Eve', 19, 'BOS', 30000),  # BOS-30k
        # (6, 'Fay', 22, 'SF', 45000),   # SF-45k (Duplicate SF city, duplicate 45k salary, duplicate 22 age)
        # (7, 'Grace', 30, 'NY', 80000),  # NY-80k (NY is duplicate city, 30 is duplicate age)
        # (8, 'Hank', 22, 'LA', 70000),   # LA-70k (Duplicate LA city, duplicate 70k salary, duplicate 22 age)
        # (9, 'Ivy', 25, 'NY', 55000),    # NY-55k (Duplicate NY city, duplicate 55k salary, duplicate 25 age)
        #
    assert results == [(19,), (22,), (22,), (22,), (25,), (25,), (30,), (30,), (40,)]



# --- COMBINATION Tests ---

def test_distinct_with_order_by_limit(engine):
    """Test SELECT DISTINCT combined with ORDER BY (requires Pre-Projection)."""
    query = "SELECT DISTINCT age FROM users ORDER BY age DESC LIMIT 3;"
    results = execute_query_and_get_results(engine, query)
    
    # Unique ages: 19, 22, 25, 30, 40
    # Expected: (40), (30), (25)
    assert results == [(40,), (30,), (25,)]

def test_aggregate_with_filter_and_order_by(engine):
    """Test Group By with a filter and an ordering."""
    query = "SELECT city, COUNT(*) FROM users WHERE salary > 40000 GROUP BY city ORDER BY COUNT(*) DESC;"
    results = execute_query_and_get_results(engine, query)
    
    # Filtered rows (salary > 40k): Alice(60k), Bob(45k), Charlie(55k), Dave(70k), Fay(45k), Grace(80k), Hank(70k), Ivy(55k)
    # NY: 4, SF: 2, LA: 2
    # Expected: NY (4), SF (2), LA (2) - Order must be NY first, SF/LA tie-break depends on internal sort stability, but the counts are correct.
    assert len(results) == 3
    assert ('NY', 4) in results
    assert ('SF', 2) in results
    assert ('LA', 2) in results
    assert results[0] == ('NY', 4)

def test_select_distinct_on_all_columns_returns_full_dataset(engine):
    """
    Test SELECT DISTINCT * (or all columns). 
    Since the ID column is unique, the total row count should be 9 (the entire dataset).
    """
    query = "SELECT DISTINCT id, name, city, salary FROM users;"
    results = execute_query_and_get_results(engine, query)
    
    # Since ID is unique, no rows are eliminated.
    assert len(results) == 9


def test_order_by_multi_key_asc(engine):
    """
    Test ORDER BY on two keys, both ASC.
    Query: ORDER BY age ASC, name ASC
    Expected: Should group by age, then order by name within each group.
    """
    query = "SELECT age, name FROM users ORDER BY age ASC, name ASC LIMIT 6;"
    results = execute_query_and_get_results(engine, query)
    
    # Age Groups: 19(Eve), 22(Bob, Fay, Hank), 25(Charlie, Ivy), 30(Alice, Grace), 40(Dave)
    # Expected order (first 6):
    # 1. 19, Eve
    # 2. 22, Bob (B < F < H)
    # 3. 22, Fay
    # 4. 22, Hank
    # 5. 25, Charlie (C < I)
    # 6. 25, Ivy
    
    expected = [
        (19, 'Eve'),
        (22, 'Bob'),
        (22, 'Fay'),
        (22, 'Hank'),
        (25, 'Charlie'),
        (25, 'Ivy'),
    ]
    
    assert results == expected


def test_order_by_multi_key_mixed_direction(engine):
    """
    Test ORDER BY on two keys with mixed directions.
    Query: ORDER BY city ASC, salary DESC
    Expected: Should group by CITY (ASC), then order salaries within each city group (DESC).
    """
    query = "SELECT city, salary, name FROM users ORDER BY city ASC, salary DESC;"
    results = execute_query_and_get_results(engine, query)

    # 1. BOS (only Eve)
    # 2. LA (Dave, Hank - both 70k) -> Order is arbitrary tie-break, but salary is sorted DESC.
    # 3. NY (Grace 80k, Alice 60k, Charlie 55k, Ivy 55k) -> Should be 80k, 60k, 55k, 55k
    # 4. SF (Bob 45k, Fay 45k) -> Order is arbitrary tie-break, but salary is sorted DESC.

    expected = [
        # BOS (1 entry)
        ('BOS', 30000, 'Eve'),

        # LA (2 entries, same salary)
        ('LA', 70000, 'Dave'), # Or Hank, depends on stable sort/internal ID tie-break
        ('LA', 70000, 'Hank'), # Or Dave

        # NY (4 entries, Salary DESC)
        ('NY', 80000, 'Grace'), # Highest
        ('NY', 60000, 'Alice'),
        ('NY', 55000, 'Charlie'), # Charlie/Ivy tie is internal
        ('NY', 55000, 'Ivy'),     # Ivy/Charlie tie is internal

        # SF (2 entries, same salary)
        ('SF', 45000, 'Bob'),  # Or Fay
        ('SF', 45000, 'Fay'),  # Or Bob
    ]

    # Since the tie-break order for rows with identical key values (e.g., Dave/Hank) is non-deterministic 
    # without an implicit primary key, we check the set of results for each city, 
    # but the primary sort order MUST be preserved.

    # Check primary sort: City (ASC)
    city_order = [r[0] for r in results]
    assert city_order[:1] == ['BOS']
    assert city_order[1:3] == ['LA', 'LA']
    assert city_order[3:7] == ['NY', 'NY', 'NY', 'NY']
    assert city_order[7:] == ['SF', 'SF']

    # Check secondary sort: Salary within NY (DESC)
    ny_results = [r[1] for r in results if r[0] == 'NY']
    assert ny_results == [80000, 60000, 55000, 55000] # Checks descending order
