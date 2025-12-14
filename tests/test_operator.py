from operators import BaseIterator, Filter, LogicalFilter, Sorter, Aggregate, Limit, Distinct

class MockParent:
    """A dummy iterator to feed fixed data into the operators."""
    def __init__(self, data):
        self._data = data
        
    def next(self):
        for row in self._data:
            yield row

TEST_DATA_FULL_SCHEMA = [
    (1, 'Alice', 30, 'NY', 60000),  # 0
    (2, 'Bob', 22, 'SF', 45000),    # 1
    (3, 'Charlie', 25, 'NY', 55000),# 2
    (4, 'Dave', 40, 'LA', 70000),   # 3
    (5, 'Eve', 19, 'BOS', 30000),   # 4
    (6, 'Fay', 22, 'SF', 45000),    # 5
    (7, 'Grace', 30, 'NY', 80000),  # 6
    (8, 'Hank', 22, 'LA', 70000),   # 7
    (9, 'Ivy', 25, 'NY', 55000),    # 8
]


def test_filter_age_greater_than_30():
    """Test Filter on AGE > 30."""
    parent = MockParent(TEST_DATA_FULL_SCHEMA)
    
    # Filter: AGE (index 2) > 30 (literal)
    filter_op = Filter(
        comparison='>',
        parent=parent,
        val1=None,
        val2=30,
        col_idx1=2,
        col_idx2=None 
    )
    
    results = list(filter_op.next())
    
    # Expected: Dave (40)
    expected = [
        (4, 'Dave', 40, 'LA', 70000),
    ]
    
    assert len(results) == 1
    assert results == expected

def test_filter_city_equals_SF():
    """Test Filter on CITY = 'SF'."""
    parent = MockParent(TEST_DATA_FULL_SCHEMA)
    
    # Filter: CITY (index 3) = 'SF' (literal)
    filter_op = Filter(
        comparison='=',
        parent=parent,
        val1=None,
        val2='SF',
        col_idx1=3,
        col_idx2=None 
    )
    
    results = list(filter_op.next())
    
    # Expected: Bob (SF), Fay (SF)
    expected = [
        (2, 'Bob', 22, 'SF', 45000),
        (6, 'Fay', 22, 'SF', 45000),
    ]
    
    assert len(results) == 2
    assert results == expected


def test_filter_age_less_than_or_equal_22():
    """Test Filter on AGE <= 22."""
    parent = MockParent(TEST_DATA_FULL_SCHEMA)
    
    # Filter: AGE (index 2) <= 22 (literal)
    filter_op = Filter(
        comparison='<=',
        parent=parent,
        val1=None,
        val2=22,
        col_idx1=2,
        col_idx2=None 
    )
    
    results = list(filter_op.next())
    
    # Expected: Bob (22), Eve (19), Fay (22), Hank (22)
    expected = [
        (2, 'Bob', 22, 'SF', 45000),
        (5, 'Eve', 19, 'BOS', 30000),
        (6, 'Fay', 22, 'SF', 45000),
        (8, 'Hank', 22, 'LA', 70000),
    ]
    
    # Order matters for iteration, so we check for exact match
    assert len(results) == 4
    assert results == expected


def test_filter_column_to_column_comparison():
    """Test Filter on ID > AGE."""
    parent = MockParent(TEST_DATA_FULL_SCHEMA + [(50, 'Dude', 25, 'NY', 55000)])
    
    # Filter: ID (index 0) < AGE (index 2)
    filter_op = Filter(
        comparison='>',
        parent=parent,
        val1=None,
        val2=None,
        col_idx1=0,  # ID
        col_idx2=2   # AGE
    )
    
    results = list(filter_op.next())
    
    expected = [
        (50, 'Dude', 25, 'NY', 55000),
    ]
    
    assert len(results) == 1
    assert results == expected


def test_filter_empty_result_set():
    """Test Filter that produces zero results."""
    parent = MockParent(TEST_DATA_FULL_SCHEMA)
    
    # Filter: SALARY (index 4) > 100000 (literal)
    filter_op = Filter(
        comparison='>',
        parent=parent,
        val1=None,
        val2=100000,
        col_idx1=4,
        col_idx2=None 
    )
    
    results = list(filter_op.next())
    
    assert len(results) == 0
    assert results == []



def test_logical_filter_age_and_salary():
    """Test LogicalFilter using AND: (AGE > 25) AND (SALARY < 70000)."""
    parent = MockParent(TEST_DATA_FULL_SCHEMA)
    
    # Filter 1 (Left Child): AGE > 25 (Alice, Dave, Grace)
    filter_age = Filter('>', parent, val2=25, col_idx1=2, col_idx2=None)
    
    # Filter 2 (Right Child): SALARY < 70000 (Alice, Bob, Charlie, Eve, Fay, Ivy)
    filter_salary = Filter('<', parent, val2=70000, col_idx1=4, col_idx2=None) 

    # LogicalFilter: AND
    logical_op = LogicalFilter(
        op='AND',
        left_child=filter_age,
        right_child=filter_salary,
        parent=parent
    )
    
    results = list(logical_op.next())
    
    # Intersection: Alice (30, 60k)
    expected = [
        (1, 'Alice', 30, 'NY', 60000),
    ]
    
    assert len(results) == 1
    assert sorted(results) == sorted(expected)


def test_logical_filter_city_or_age():
    """Test LogicalFilter using OR: (CITY = 'LA') OR (AGE = 19)."""
    parent = MockParent(TEST_DATA_FULL_SCHEMA)
    
    # Filter 1 (Left Child): CITY = 'LA' (Dave, Hank)
    filter_city = Filter('=', parent, val2='LA', col_idx1=3, col_idx2=None)
    
    # Filter 2 (Right Child): AGE = 19 (Eve)
    filter_age = Filter('=', parent, val2=19, col_idx1=2, col_idx2=None) 

    # LogicalFilter: OR
    logical_op = LogicalFilter(
        op='OR',
        left_child=filter_city,
        right_child=filter_age,
        parent=parent
    )
    
    results = list(logical_op.next())
    
    # Union: Dave, Hank, Eve
    expected = [
        (4, 'Dave', 40, 'LA', 70000),
        (8, 'Hank', 22, 'LA', 70000),
        (5, 'Eve', 19, 'BOS', 30000),
    ]
    
    assert len(results) == 3
    assert sorted(results) == sorted(expected)

def test_logical_filter_or_with_overlap():
    """Test LogicalFilter using OR with overlapping results."""
    parent = MockParent(TEST_DATA_FULL_SCHEMA)
    
    # Filter 1 (Left Child): AGE = 22 (Bob, Fay, Hank)
    filter_age = Filter('=', parent, val2=22, col_idx1=2, col_idx2=None)
    
    # Filter 2 (Right Child): SALARY = 45000 (Bob, Fay)
    filter_salary = Filter('=', parent, val2=45000, col_idx1=4, col_idx2=None) 

    # LogicalFilter: OR
    logical_op = LogicalFilter(op='OR', left_child=filter_age, right_child=filter_salary, parent=parent)
    
    results = list(logical_op.next())
    
    # Expected: Bob (22, 45k), Fay (22, 45k), Hank (22, 70k). (Duplicates must be merged)
    expected = [
        (2, 'Bob', 22, 'SF', 45000),
        (6, 'Fay', 22, 'SF', 45000),
        (8, 'Hank', 22, 'LA', 70000),
    ]
    
    assert len(results) == 3
    assert sorted(results) == sorted(expected)

def test_logical_filter_and_empty_set():
    """Test LogicalFilter using AND that returns zero results."""
    parent = MockParent(TEST_DATA_FULL_SCHEMA)
    
    # Filter 1 (Left Child): CITY = 'SF' (Bob, Fay)
    filter_city = Filter('=', parent, val2='SF', col_idx1=3, col_idx2=None)
    
    # Filter 2 (Right Child): AGE = 40 (Dave)
    filter_age = Filter('=', parent, val2=40, col_idx1=2, col_idx2=None) 

    # LogicalFilter: AND
    logical_op = LogicalFilter(op='AND', left_child=filter_city, right_child=filter_age, parent=parent)
    
    results = list(logical_op.next())
    
    # Intersection: None
    expected = []
    
    assert len(results) == 0
    assert results == expected

def test_logical_filter_or_with_one_empty_child():
    """Test LogicalFilter using OR where one child returns no rows."""
    parent = MockParent(TEST_DATA_FULL_SCHEMA)
    
    # Filter 1 (Left Child): SALARY > 90000 (None)
    filter_high_salary = Filter('>', parent, val2=90000, col_idx1=4, col_idx2=None)
    
    # Filter 2 (Right Child): CITY = 'BOS' (Eve)
    filter_bos_city = Filter('=', parent, val2='BOS', col_idx1=3, col_idx2=None) 

    # LogicalFilter: OR
    logical_op = LogicalFilter(op='OR', left_child=filter_high_salary, right_child=filter_bos_city, parent=parent)
    
    results = list(logical_op.next())
    
    # Union: Eve (from the right child)
    expected = [
        (5, 'Eve', 19, 'BOS', 30000),
    ]
    
    assert len(results) == 1
    assert results == expected

# --- 5 Unit Tests for Sorter Operator ---

# Helper function to generate a correct Sorter operator for testing
def create_sorter_op(sort_keys):
    parent = MockParent(TEST_DATA_FULL_SCHEMA)
    # The actual Sorter needs the correct compare_rows logic internally.
    # We rely on the imported Sorter class having a constructor that accepts 
    # sort_keys and parent.
    return Sorter(sort_keys, parent)

def test_sorter_single_key_desc():
    """Test Sorter on AGE DESC (index 2, is_descending=True)."""
    
    # Sort Keys: AGE (2, True)
    sort_keys = [(2, True)] 
    sorter_op = create_sorter_op(sort_keys)
    
    results = list(sorter_op.next())
    
    # Expected Age order: 40, 30, 30, 25, 25, 22, 22, 22, 19
    result_ages = [row[2] for row in results]
    assert result_ages == [40, 30, 30, 25, 25, 22, 22, 22, 19]

def test_sorter_single_key_asc():
    """Test Sorter on NAME ASC (index 1, is_descending=False)."""
    
    # Sort Keys: NAME (1, False)
    sort_keys = [(1, False)] 
    sorter_op = create_sorter_op(sort_keys)
    
    results = list(sorter_op.next())
    
    # Expected Name order: Alice, Bob, Charlie, Dave, Eve, Fay, Grace, Hank, Ivy
    result_names = [row[1] for row in results]
    expected_names_asc = sorted([row[1] for row in TEST_DATA_FULL_SCHEMA])
    
    assert result_names == expected_names_asc

def test_sorter_multi_key_asc():
    """Test Sorter on AGE ASC, ID ASC (indices 2, 0, both False)."""
    
    # Sort Keys: AGE (2, False), ID (0, False)
    sort_keys = [(2, False), (0, False)] 
    sorter_op = create_sorter_op(sort_keys)
    
    results = list(sorter_op.next())
    
    # Expected order:
    # 1. 19: Eve (ID 5)
    # 2. 22: Bob (ID 2), Fay (ID 6), Hank (ID 8) -> sorted by ID ASC
    # 3. 25: Charlie (ID 3), Ivy (ID 9) -> sorted by ID ASC
    # ...
    
    expected_ids_for_age_22 = [2, 6, 8] # Bob, Fay, Hank
    
    # Extract the IDs for the 22-year-olds in the result
    result_ids_for_age_22 = [row[0] for row in results if row[2] == 22]
    
    assert result_ids_for_age_22 == expected_ids_for_age_22

def test_sorter_multi_key_mixed_direction():
    """Test Sorter on CITY ASC, SALARY DESC (indices 3, 4)."""
    
    # Sort Keys: CITY (3, False), SALARY (4, True)
    sort_keys = [(3, False), (4, True)] 
    sorter_op = create_sorter_op(sort_keys)
    
    results = list(sorter_op.next())
    
    # Expected order:
    # 1. BOS: Eve (30k)
    # 2. LA: Dave (70k), Hank (70k) -> sorted by SALARY DESC (70k, 70k)
    # 3. NY: Grace (80k), Alice (60k), Charlie (55k), Ivy (55k) -> sorted by SALARY DESC
    # 4. SF: Bob (45k), Fay (45k) -> sorted by SALARY DESC
    
    # Check the NY group (City index 3, Value 'NY'): Grace (80k) should be first.
    ny_rows = [row[4] for row in results if row[3] == 'NY']
    
    assert ny_rows == [80000, 60000, 55000, 55000]

def test_sorter_limit_integration():
    """Test Sorter followed by Limit (ORDER BY SALARY DESC LIMIT 3)."""
    
    # Sort Keys: SALARY (4, True)
    sort_keys = [(4, True)]
    sorter_op = create_sorter_op(sort_keys)
    
    # Limit 3
    limit_op = Limit(count=3, parent=sorter_op)
    
    results = list(limit_op.next())
    
    # Expected highest salaries: 80k (Grace), 70k (Dave), 70k (Hank)
    # The order of Dave/Hank is non-deterministic (tie).
    expected_top_salaries = [80000, 70000, 70000]
    
    result_salaries = [row[4] for row in results]
    
    assert len(results) == 3
    assert sorted(result_salaries, reverse=True) == sorted(expected_top_salaries, reverse=True)


# --- 5 Unit Tests for Distinct and Limit Operators ---
def test_distinct_on_single_column_age_correct_ctor():
    """Test Distinct operator eliminating duplicate AGE values (index 2)."""
    parent = MockParent(TEST_DATA_FULL_SCHEMA)
    
    # DISTINCT AGE (Index 2)
    distinct_op = Distinct(column_indices=[2], parent=parent)
    
    results = list(distinct_op.next())
    
    # Ages in rows: 30, 22, 25, 40, 19, 22, 30, 22, 25
    # Expected unique keys: (30), (22), (25), (40), (19)
    
    # We must extract the AGE from the full row output by Distinct
    result_ages = [row[2] for row in results]

    assert len(results) == 5
    assert len(set(result_ages)) == 5 # Ensure only 5 unique ages were returned
    assert sorted(result_ages) == [19, 22, 25, 30, 40]

def test_distinct_on_multiple_columns_age_salary_correct_ctor():
    """Test Distinct operator on the combination of AGE (2) and SALARY (4)."""
    parent = MockParent(TEST_DATA_FULL_SCHEMA)
    
    # DISTINCT AGE, SALARY (Indices 2, 4)
    distinct_op = Distinct(column_indices=[2, 4], parent=parent)
    
    results = list(distinct_op.next())
    
    # Duplicates based on (AGE, SALARY):
    # (22, 45k) appears twice
    # (25, 55k) appears twice
    # (30, 60k) appears once, (30, 80k) is separate
    # Total rows: 9. Unique pairs: 7.
    
    result_keys = [(row[2], row[4]) for row in results]
    
    # Check that exactly 7 unique rows were yielded
    assert len(results) == 7
    assert len(set(result_keys)) == 7

def test_distinct_on_all_columns_correct_ctor():
    """Test Distinct on all columns (SELECT DISTINCT *). Should return all 9 rows."""
    parent = MockParent(TEST_DATA_FULL_SCHEMA)
    
    # All columns: [0, 1, 2, 3, 4]
    distinct_op = Distinct(column_indices=[0, 1, 2, 3, 4], parent=parent)
    
    results = list(distinct_op.next())
    
    # Since the ID (index 0) is unique for every row, all 9 rows should be returned.
    assert len(results) == 9
    assert results == TEST_DATA_FULL_SCHEMA

def test_limit_operator_basic():
    """Test Limit(4) restricts output to the first 4 rows."""
    parent = MockParent(TEST_DATA_FULL_SCHEMA)
    
    limit_op = Limit(count=4, parent=parent)
    
    results = list(limit_op.next())
    
    # Expected: The first 4 rows from the source data
    expected = TEST_DATA_FULL_SCHEMA[:4]
    
    assert len(results) == 4
    assert results == expected

def test_distinct_then_limit():
    """Test Distinct (CITY, index 3) followed by Limit 3."""
    
    parent = MockParent(TEST_DATA_FULL_SCHEMA)
    
    # 1. Apply Distinct on CITY (index 3)
    distinct_op = Distinct(column_indices=[3], parent=parent)
    
    # 2. Apply Limit 3
    limit_op = Limit(count=3, parent=distinct_op)
    
    results = list(limit_op.next())
    
    # Unique Cities: NY, SF, LA, BOS. The order will be NY, SF, LA based on input order.
    expected_unique_cities = [('NY',), ('SF',), ('LA',), ('BOS',)] 
    
    assert len(results) == 3
    
    # Extract the city column (index 3) from the full output rows
    result_cities = [(row[3],) for row in results]
    
    # Ensure the three returned cities are unique and valid
    assert len(set(result_cities)) == 3
    for city_tuple in result_cities:
        assert city_tuple in expected_unique_cities

def test_aggregate_global_sum():
    """Test SUM(SALARY) without GROUP BY (Global Aggregate)."""
    parent = MockParent(TEST_DATA_FULL_SCHEMA)
    
    # SUM(SALARY) (Index 4). Total Salary: 510000
    aggregate_op = Aggregate(
        parent=parent,
        group_key_indices=[],             # No GROUP BY
        aggregate_specs=[('SUM', 4)],     # SUM(SALARY)
        output_names=['SUM(SALARY)']
    )
    
    results = list(aggregate_op.next())
    
    expected = [(510000,)]
    
    assert len(results) == 1
    assert results == expected

def test_aggregate_group_by_city_count_all():
    """Test COUNT(*) grouped by CITY, using the '*' wildcard."""
    parent = MockParent(TEST_DATA_FULL_SCHEMA)
    
    # Group By CITY (Index 3), COUNT(*) (Wildcard '*')
    aggregate_op = Aggregate(
        parent=parent,
        group_key_indices=[3],              # GROUP BY CITY
        aggregate_specs=[('COUNT', '*')],   # COUNT(*)
        output_names=['CITY', 'COUNT(*)']
    )
    
    results = list(aggregate_op.next())
    
    # Expected: NY (4), SF (2), LA (2), BOS (1)
    expected = [
        ('NY', 4), ('SF', 2), ('LA', 2), ('BOS', 1)
    ]
    
    assert len(results) == 4
    assert sorted(results) == sorted(expected)

def test_aggregate_group_by_city_avg_salary():
    """Test AVG(SALARY) grouped by CITY."""
    parent = MockParent(TEST_DATA_FULL_SCHEMA)
    
    # Group By CITY (Index 3), AVG(SALARY) (Index 4)
    # NY Avg: 62500
    aggregate_op = Aggregate(
        parent=parent,
        group_key_indices=[3],             # GROUP BY CITY
        aggregate_specs=[('AVG', 4)],      # AVG(SALARY)
        output_names=['CITY', 'AVG(SALARY)']
    )
    
    results = list(aggregate_op.next())
    
    expected = [
        ('NY', 62500), ('SF', 45000), ('LA', 70000), ('BOS', 30000)
    ]
    
    assert len(results) == 4
    assert sorted(results) == sorted(expected)

def test_aggregate_group_by_age_max_salary():
    """Test MAX(SALARY) grouped by AGE."""
    parent = MockParent(TEST_DATA_FULL_SCHEMA)
    
    # Group By AGE (Index 2), MAX(SALARY) (Index 4)
    aggregate_op = Aggregate(
        parent=parent,
        group_key_indices=[2],             # GROUP BY AGE
        aggregate_specs=[('MAX', 4)],      # MAX(SALARY)
        output_names=['AGE', 'MAX(SALARY)']
    )
    
    results = list(aggregate_op.next())
    
    expected = [
        (30, 80000), # Grace
        (22, 70000), # Hank
        (25, 55000), # Charlie/Ivy
        (40, 70000), # Dave
        (19, 30000)  # Eve
    ]
    
    assert len(results) == 5
    assert sorted(results) == sorted(expected)

def test_aggregate_min_age_global():
    """Test MIN(AGE) without GROUP BY."""
    parent = MockParent(TEST_DATA_FULL_SCHEMA)
    
    # MIN(AGE) (Index 2). Min Age: 19
    aggregate_op = Aggregate(
        parent=parent,
        group_key_indices=[],             # No GROUP BY
        aggregate_specs=[('MIN', 2)],     # MIN(AGE)
        output_names=['MIN(AGE)']
    )
    
    results = list(aggregate_op.next())
    
    expected = [(19,)]
    
    assert len(results) == 1
    assert results == expected
