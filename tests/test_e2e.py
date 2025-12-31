import pytest
from queryplanner import QueryPlanner 
from engine import DatabaseEngine
from result import QueryResult
from request import QueryRequest
import schema

@pytest.fixture
def mock_table_data() -> dict:
    """
    Provides the fixed, iterable mock data for the 'employee' table.
    Schema: (0: ID, 1: NAME, 2: AGE, 3: CITY, 4: SALARY)
    """
    return {"employee":[
        (1, 'Alice', 30, 'NY', 60000),  # NY-60k
        (2, 'Bob', 22, 'SF', 45000),   # SF-45k
        (3, 'Charlie', 25, 'NY', 55000), # NY-55k (NY is duplicate city)
        (4, 'Dave', 40, 'LA', 70000),   # LA-70k
        (5, 'Eve', 19, 'BOS', 30000),  # BOS-30k
        (6, 'Fay', 22, 'SF', 45000),   # SF-45k (Duplicate SF city, duplicate 45k salary, duplicate 22 age)
        (7, 'Grace', 30, 'NY', 80000),  # NY-80k (NY is duplicate city, 30 is duplicate age)
        (8, 'Hank', 22, 'LA', 70000),   # LA-70k (Duplicate LA city, duplicate 70k salary, duplicate 22 age)
        (9, 'Ivy', 25, 'NY', 55000),    # NY-55k (Duplicate NY city, duplicate 55k salary, duplicate 25 age)
    ],
    "contract": [(1, 1, '2025-01-01', '2027-12-31'),
                  (2, 1, '2023-01-01', '2024-12-31'),
                  (3, 2, '2024-01-01', '2027-07-01'),
                  (4, 2, '2023-01-01', '2023-12-31'),
                  (5, 3, '2026-01-01', '2026-03-31')],
            }


class MockBufferManager:
    def __init__(self, table_data_map: dict):
        """Initializes the mock with in-memory data for all tables."""
        self.table_data_map = table_data_map

    def get_data_generator(self, table_name: str):
        """
        Simulates the Buffer Manager fetching pages and returning a stream 
        of rows (tuples) for a specific table scan.
        """
        table_name_upper = table_name.lower()
        def data_generator():
            yield from self.table_data_map[table_name_upper]
        return data_generator


class MockCatalog:
    """Mocks the Catalog to provide column indices for the 'employee' table."""
    schema = {
        'employee': ['id', 'name', 'age', 'city', 'salary'],
        'contract': ['id', 'employee_id', 'start_date', 'end_date']
    }
    
    def get_all_column_names(self, table_name):
        """Returns the list of column names in schema order for SELECT *."""
        return self.schema[table_name]

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
    request = QueryRequest(query_string)
    result: QueryResult = engine.execute(request)
    return result.rows

@pytest.fixture
def engine(mock_table_data):
    catalog = MockCatalog()
    buffer_mgr = MockBufferManager(mock_table_data)
    return DatabaseEngine(catalog, buffer_mgr)


def test_select_mixed_order(engine):
    """Test different column order"""
    query = "SELECT salary, city, name, id FROM employee"
    results = execute_query_and_get_results(engine, query)

    assert (60000, 'NY', 'Alice', 1) in results  # test on one row is fine

def test_select_literal(engine):
    """Test different column order"""
    query = "SELECT 1 FROM employee"
    results = execute_query_and_get_results(engine, query)

    assert results == [(1,)]*9

def test_select_mixed_column_literal(engine):
    """Test different column order"""
    query = "SELECT 1, id FROM employee"
    results = execute_query_and_get_results(engine, query)

    assert set(results) == {(1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (1, 6), (1, 7), (1, 8), (1, 9)}

def test_select_mixed_column_literal_mixed_type(engine):
    """Test different column order"""
    query = "SELECT 1, id, 'a' FROM employee"
    results = execute_query_and_get_results(engine, query)

    assert set(results) == {(1, 1, 'a'), (1, 2, 'a'), (1, 3, 'a'), (1, 4, 'a'), (1, 5, 'a'), (1, 6, 'a'), (1, 7, 'a'), (1, 8, 'a'), (1, 9, 'a')}

def test_select_mixed_column_literal_mixed_type_alias(engine):
    """Test different column order"""
    query = "SELECT 1 as a, id as b, 'a' as c FROM employee"
    results = execute_query_and_get_results(engine, query)

    assert set(results) == {(1, 1, 'a'), (1, 2, 'a'), (1, 3, 'a'), (1, 4, 'a'), (1, 5, 'a'), (1, 6, 'a'), (1, 7, 'a'), (1, 8, 'a'), (1, 9, 'a')}


def test_select_mixed_column_literal_star_mixed_type_alias(engine):
    """Test different column order"""
    query = "SELECT 1 as a, *, 'a' as b, id as d FROM employee"
    results = execute_query_and_get_results(engine, query)

    expected = {
        (1, 1, 'Alice', 30, 'NY', 60000, 'a', 1),
        (1, 2, 'Bob', 22, 'SF', 45000, 'a', 2),
        (1, 3, 'Charlie', 25, 'NY', 55000, 'a', 3),
        (1, 4, 'Dave', 40, 'LA', 70000, 'a', 4),
        (1, 5, 'Eve', 19, 'BOS', 30000, 'a', 5),
        (1, 6, 'Fay', 22, 'SF', 45000, 'a', 6),
        (1, 7, 'Grace', 30, 'NY', 80000, 'a', 7),
        (1, 8, 'Hank', 22, 'LA', 70000, 'a', 8),
        (1, 9, 'Ivy', 25, 'NY', 55000, 'a', 9),
    }
    assert set(results) == expected


def test_select_all_with_filter(engine):
    """Test SELECT * with a WHERE clause."""
    query = "SELECT * FROM employee WHERE age > 25;"
    results = execute_query_and_get_results(engine, query)
    
    assert len(results) == 3
    assert ('Alice' in [r[1] for r in results])
    assert ('Bob' not in [r[1] for r in results])
    
def test_select_projection_with_filter(engine):
    """Test SELECT specific columns with a WHERE clause."""
    query = "SELECT name, salary FROM employee WHERE city = 'NY';"
    results = execute_query_and_get_results(engine, query)
    
    assert len(results) == 4
    assert set(results) == {('Alice', 60000), ('Charlie', 55000), ('Grace', 80000), ('Ivy', 55000)}

# --- DISTINCT Tests ---

def test_select_distinct_single_column(engine):
    """Test SELECT DISTINCT on a column with duplicates (CITY)."""
    query = "SELECT DISTINCT city FROM employee;"
    results = execute_query_and_get_results(engine, query)
    
    # Expected unique cities: NY, SF, LA, BOS
    assert len(results) == 4
    assert set(r[0] for r in results) == {'NY', 'SF', 'LA', 'BOS'}
    
def test_select_distinct_multiple_columns(engine):
    """Test SELECT DISTINCT on a combination of columns (CITY, SALARY)."""
    query = "SELECT DISTINCT city, salary FROM employee;"
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
    query = "SELECT DISTINCT age FROM employee WHERE city != 'NY';"
    results = execute_query_and_get_results(engine, query)
    
    # Rows not in NY: Bob(22), Dave(40), Eve(19), Fay(22), Hank(22, 70k)
    # Unique ages: 22, 40, 19
    assert len(results) == 3
    assert set(r[0] for r in results) == {22, 40, 19}

# --- AGGREGATE Tests (Assuming your Aggregate operator is fully functional) ---

def test_global_aggregate_count_star(engine):
    """Test COUNT(*) without GROUP BY."""
    query = "SELECT COUNT(*) FROM employee;"
    results = execute_query_and_get_results(engine, query)
    
    # Total rows: 9
    assert results == [(9,)]
 
def test_count_distinct(engine):
    """Test SELECT COUNT(DISTINCT age) with a WHERE clause."""
    query = "SELECT COUNT(DISTINCT age) FROM employee;"
    results = execute_query_and_get_results(engine, query)
    
    assert len(results) == 1
    assert (5 in [r[0] for r in results])

   
def test_global_aggregate_sum(engine):
    """Test SUM(SALARY) without GROUP BY."""
    query = "SELECT SUM(salary) FROM employee;"
    results = execute_query_and_get_results(engine, query)
    
    # Sum: 60k + 45k + 55k + 70k + 30k + 45k + 80k + 70k + 55k = 510,000
    assert results == [(510000,)]

def test_group_by_with_count_and_avg(engine):
    """Test GROUP BY CITY, counting rows and averaging salary per city."""
    query = "SELECT city, COUNT(*), AVG(salary) FROM employee GROUP BY city;"
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
    query = "SELECT DISTINCT age FROM employee ORDER BY age DESC;"
    results = execute_query_and_get_results(engine, query)
    
    # Unique ages: 19, 22, 25, 30, 40
    # Expected: (40), (30), (25)
    assert results == [(40,), (30,), (25,), (22,), (19,)]


def test_select_order_by_desc(engine):
    """Test SELECT with ORDER BY (requires Pre-Projection)."""
    query = "SELECT age FROM employee ORDER BY age DESC;"
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
    query = "SELECT age FROM employee ORDER BY age ASC;"
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
    query = "SELECT DISTINCT age FROM employee ORDER BY age DESC LIMIT 3;"
    results = execute_query_and_get_results(engine, query)
    
    # Unique ages: 19, 22, 25, 30, 40
    # Expected: (40), (30), (25)
    assert results == [(40,), (30,), (25,)]

def test_aggregate_with_filter_and_order_by(engine):
    """Test Group By with a filter and an ordering."""
    query = "SELECT city, COUNT(*) FROM employee WHERE salary > 40000 GROUP BY city ORDER BY COUNT(*) DESC;"
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
    query = "SELECT DISTINCT id, name, city, salary FROM employee;"
    results = execute_query_and_get_results(engine, query)
    
    # Since ID is unique, no rows are eliminated.
    assert len(results) == 9


def test_order_by_multi_key_asc(engine):
    """
    Test ORDER BY on two keys, both ASC.
    Query: ORDER BY age ASC, name ASC
    Expected: Should group by age, then order by name within each group.
    """
    query = "SELECT age, name FROM employee ORDER BY age ASC, name ASC LIMIT 6;"
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
    query = "SELECT city, salary, name FROM employee ORDER BY city ASC, salary DESC;"
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

    assert results == expected


def test_join_basic_equivalence(engine):
    """Test standard Join: Alice(1) has contracts 1,2. Bob(2) has 3,4. Charlie(3) has 5."""
    query = "SELECT employee.id, contract.id FROM employee JOIN contract ON employee.id = contract.employee_id"
    results = execute_query_and_get_results(engine, query)
    
    # Format: (employee.id, contract.id)
    expected = {
        (1, 1), (1, 2), # Alice
        (2, 3), (2, 4), # Bob
        (3, 5)          # Charlie
    }
    assert set(results) == expected

def test_join_with_literals(engine):
    """Test mixing literals with joined columns."""
    query = "SELECT 'active', employee.name, contract.id FROM employee JOIN contract ON employee.id = contract.employee_id WHERE employee.id = 3"
    results = execute_query_and_get_results(engine, query)
    
    expected = {('active', 'Charlie', 5)}
    assert set(results) == expected

def test_join_filter_on_right_table(engine):
    """Test filtering based on a column in the second table."""
    query = "SELECT employee.name FROM employee JOIN contract ON employee.id = contract.employee_id WHERE contract.id = 4"
    results = execute_query_and_get_results(engine, query)
    
    expected = {('Bob',)}
    assert set(results) == expected

def test_join_filter_on_left_table(engine):
    """Test filtering based on a column in the first table."""
    query = "SELECT contract.id FROM employee JOIN contract ON employee.id = contract.employee_id WHERE employee.city = 'SF'"
    results = execute_query_and_get_results(engine, query)
    
    # Bob is the only SF resident with contracts (IDs 3 and 4)
    expected = {(3,), (4,)}
    assert set(results) == expected

def test_join_with_aliases_full_set(engine):
    """Test that aliases resolve correctly across the join schema."""
    query = "SELECT e.name, c.employee_id FROM employee AS e JOIN contract AS c ON e.id = c.employee_id"
    results = execute_query_and_get_results(engine, query)
    
    expected = {
        ('Alice', 1), ('Alice', 1),
        ('Bob', 2), ('Bob', 2),
        ('Charlie', 3)
    }
    assert set(results) == expected

def test_join_on_non_id_column(engine):
    """Test joining where a value matches an ID (e.g., employee age 22 matches contract ID 2... no, let's use a matching value)."""
    # Contract IDs are 1, 2, 3, 4, 5. 
    # Alice is 30, Bob is 22, Charlie 25, Dave 40, Eve 19, Fay 22, Grace 30, Hank 22, Ivy 25.
    # No ages match IDs 1-5. 
    # Let's test a join where contract.id = employee.id
    query = "SELECT employee.name, contract.id FROM employee JOIN contract ON employee.id = contract.id"
    results = execute_query_and_get_results(engine, query)
    
    expected = {
        ('Alice', 1), 
        ('Bob', 2), 
        ('Charlie', 3), 
        ('Dave', 4), 
        ('Eve', 5)
    }
    assert set(results) == expected

def test_join_cross_product_logic(engine):
    """Test a condition that creates a small cross product (e.g., specific cities)."""
    # 3 employees from NY, 5 contracts total. Join where NY employees match any contract.
    query = "SELECT e.name, c.id FROM employee as e JOIN contract as c ON 1=1 WHERE e.city = 'BOS'"
    results = execute_query_and_get_results(engine, query)
    
    # Eve is from BOS. She should be paired with every single contract (1,2,3,4,5)
    expected = {
        ('Eve', 1), ('Eve', 2), ('Eve', 3), ('Eve', 4), ('Eve', 5)
    }
    assert set(results) == expected

def test_join_multiple_filters(engine):
    """Test filtering both tables after the join."""
    query = """
        SELECT e.name, c.id 
        FROM employee as e JOIN contract as c ON e.id = c.employee_id 
        WHERE e.salary > 50000 AND c.id % 2 = 1
    """
    results = execute_query_and_get_results(engine, query)
    
    # Alice (60k) has contracts 1,2 -> 1 is odd.
    # Charlie (55k) has contract 5 -> 5 is odd.
    expected = {('Alice', 1), ('Charlie', 5)}
    assert set(results) == expected

def test_join_no_matches(engine):
    """Ensure a failed join condition returns an empty set."""
    query = "SELECT e.name FROM employee AS e JOIN contract AS c ON e.id = c.id WHERE e.id > 100"
    results = execute_query_and_get_results(engine, query)
    
    assert set(results) == set()

def test_join_select_star_equivalence(engine):
    """Test SELECT * from join and verify the concatenated row width."""
    query = "SELECT * FROM employee AS e JOIN contract AS c ON e.id = c.employee_id WHERE e.id = 3"
    results = execute_query_and_get_results(engine, query)
    
    # Charlie (ID 3) has Contract (ID 5)
    # Emp cols: (3, 'Charlie', 25, 'NY', 55000)
    # Con cols: (5, 3, '2026-01-01', '2026-03-31')
    expected = {(3, 'Charlie', 25, 'NY', 55000, 5, 3, '2026-01-01', '2026-03-31')}
    assert set(results) == expected

# -- JOIN AGGREGATE --
def test_join_count_aggregate(engine):
    """Test COUNT of joined rows. Total matches: Alice(2), Bob(2), Charlie(1)."""
    query = "SELECT COUNT(*) FROM employee  AS e JOIN contract AS c ON e.id = c.employee_id"
    results = execute_query_and_get_results(engine, query)
    
    assert set(results) == {(5,)}

def test_join_group_by_count(engine):
    """Test grouping by a column from the left table and counting rows from the right."""
    query = """
        SELECT e.name, COUNT(c.id) 
        FROM employee AS e JOIN contract AS c ON e.id = c.employee_id 
        GROUP BY e.name
    """
    results = execute_query_and_get_results(engine, query)
    
    expected = {
        ('Alice', 2),
        ('Bob', 2),
        ('Charlie', 1)
    }
    assert set(results) == expected

def test_join_sum_aggregate(engine):
    """Test summing a column from the left table across joined rows."""
    # Alice (60k) appears twice, Bob (45k) twice, Charlie (55k) once.
    # (60*2) + (45*2) + 55 = 120 + 90 + 55 = 265,000
    query = "SELECT SUM(e.salary) FROM employee AS e JOIN contract AS c ON e.id = c.employee_id"
    results = execute_query_and_get_results(engine, query)
    
    assert set(results) == {(265000,)}

def test_join_group_by_with_filter(engine):
    """Test filtering joined rows before aggregation."""
    query = """
        SELECT e.city, COUNT(c.id) 
        FROM employee AS e JOIN contract AS c ON e.id = c.employee_id 
        WHERE e.salary > 50000 
        GROUP BY e.city
    """
    results = execute_query_and_get_results(engine, query)
    
    # Alice (NY, 60k) has 2 contracts.
    # Charlie (NY, 55k) has 1 contract.
    # Total for NY = 3. Bob (SF, 45k) is filtered out.
    expected = {('NY', 3)}
    assert set(results) == expected

def test_join_aggregate_on_right_table(engine):
    """Test aggregating a column that belongs to the right-hand table."""
    query = "SELECT e.name, SUM(c.id) FROM employee AS e JOIN contract AS c ON e.id = c.employee_id GROUP BY e.name"
    results = execute_query_and_get_results(engine, query)
    
    # Alice: Contracts 1+2 = 3
    # Bob: Contracts 3+4 = 7
    # Charlie: Contract 5 = 5
    expected = {
        ('Alice', 3),
        ('Bob', 7),
        ('Charlie', 5)
    }
    assert set(results) == expected

def test_join_multiple_aggregates(engine):
    """Test multiple aggregate functions in a single joined query."""
    query = """
            SELECT COUNT(c.id), SUM(e.age) FROM employee AS e 
            JOIN contract AS c ON e.id = c.employee_id
            """
    results = execute_query_and_get_results(engine, query)
    
    # Counts: 5
    # Ages: Alice(30)*2 + Bob(22)*2 + Charlie(25)*1 = 60 + 44 + 25 = 129
    assert set(results) == {(5, 129)}

def test_join_group_by_non_id(engine):
    """Test grouping by a non-unique column (City) across a join."""
    query = "SELECT e.city, COUNT(*) FROM employee AS e JOIN contract AS c ON e.id = c.employee_id GROUP BY e.city"
    results = execute_query_and_get_results(engine, query)
    
    # NY (Alice+Charlie): 2+1 = 3 rows
    # SF (Bob): 2 rows
    expected = {('NY', 3), ('SF', 2)}
    assert set(results) == expected

def test_join_distinct_aggregate(engine):
    """Test COUNT(DISTINCT) on a joined column."""
    query = "SELECT COUNT(DISTINCT e.city) FROM employee AS e JOIN contract AS c ON e.id = c.employee_id"
    results = execute_query_and_get_results(engine, query)
    
    # Cities with contracts are NY and SF. Count = 2.
    assert set(results) == {(2,)}

def test_join_aggregate_with_order_by(engine):
    """Test joining, grouping, and then ordering the final results."""
    query = """
        SELECT e.name, COUNT(c.id) as total 
        FROM employee AS e JOIN contract AS c ON e.id = c.employee_id 
        GROUP BY e.name 
        ORDER BY total DESC
    """
    results = execute_query_and_get_results(engine, query)
    
    expected = [
        ('Alice', 2),
        ('Bob', 2),
        ('Charlie', 1)
    ]
    assert results == expected

def test_join_aggregate_complex_alias(engine):
    """Test that aliases in joins work correctly within the aggregate functions."""
    query = "SELECT COUNT(contract_alias.id) FROM employee AS emp_alias JOIN contract AS contract_alias ON emp_alias.id = contract_alias.employee_id"
    results = execute_query_and_get_results(engine, query)
    
    assert set(results) == {(5,)}
