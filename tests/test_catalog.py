import pytest
from catalog import Catalog, Table, Page
from tests.test_transaction import transaction

@pytest.fixture
def empty_catalog():
    return Catalog([])

@pytest.fixture
def populated_catalog():
    # Page 3 is a gap. max_page_id should be 4.
    t1 = Table("t1", ["id"], [int], [1, 2])
    t2 = Table("t2", ["id"], [int], [4])
    cat = Catalog([t1, t2])
    # Catalog logic should identify 3 is free and max is 4
    return cat

def test_initial_free_list_detection(populated_catalog):
    assert 3 in populated_catalog.free_page_ids
    assert populated_catalog.max_page_id == 4

def test_get_free_page_id_increments_max(empty_catalog):
    # Fixed argument name to transaction_id
    pid1 = empty_catalog.get_free_page_id(transaction_id=1)
    pid2 = empty_catalog.get_free_page_id(transaction_id=1)
    assert pid1 == 1
    assert pid2 == 2

def test_get_free_page_id_reuses_freed_id(populated_catalog):
    pid = populated_catalog.get_free_page_id(transaction_id=1)
    assert pid == 3 # Reuses the gap
    assert 3 not in populated_catalog.free_page_ids

def test_return_page_ids(empty_catalog):
    """Test returning page IDs adds them back to the free list."""
    empty_catalog.return_page_ids([10, 11])
    assert 10 in empty_catalog.free_page_ids
    assert 11 in empty_catalog.free_page_ids
    
    # Verify we can get them back
    pid = empty_catalog.get_free_page_id(transaction_id=1)
    assert pid == 10  # Assuming FIFO or order preservation, but either way it should be one of them

def test_add_new_table(empty_catalog):
    """Test adding a table to the internal dictionary."""
    t = Table("users", ["id"], [int])
    empty_catalog.add_new_table(t)
    
    assert "users" in empty_catalog.tables
    assert empty_catalog.get_table_by_name("users") == t

def test_add_duplicate_table_raises_error(empty_catalog):
    """Test that adding a table with an existing name raises an exception."""
    t1 = Table("users", ["id"], [int])
    empty_catalog.add_new_table(t1)
    
    with pytest.raises(Exception):
        empty_catalog.add_new_table(t1)

def test_drop_table_frees_pages(populated_catalog):
    """Test that dropping a table returns its pages to the free pool."""
    # t2 has page [4]
    populated_catalog.drop_table_by_name("t2")
    
    assert "t2" not in populated_catalog.tables
    assert 4 in populated_catalog.free_page_ids

def test_catalog_serialization(populated_catalog):
    """Test that the catalog can be serialized to a Page and restored."""
    page = populated_catalog.to_page()
    assert page.page_id == 0
    
    # Restore
    restored_catalog = Catalog.from_page(page)
    assert "t1" in restored_catalog.tables
    assert "t2" in restored_catalog.tables
    # Check if free list logic ran correctly on restore
    assert 3 in restored_catalog.free_page_ids

def test_borrowed_page_ids_tracking(empty_catalog):
    """Test that the catalog tracks which transaction borrowed which page."""
    txn_id = 99
    pid = empty_catalog.get_free_page_id(txn_id)
    
    assert txn_id in empty_catalog.borrowed_page_ids
    assert pid in empty_catalog.borrowed_page_ids[txn_id]

def test_get_table_case_insensitive(empty_catalog):
    """Test that table retrieval is case insensitive."""
    t = Table("Users", ["id"], [int])
    empty_catalog.add_new_table(t)
    
    fetched = empty_catalog.get_table_by_name("uSeRs")
    assert fetched == t
