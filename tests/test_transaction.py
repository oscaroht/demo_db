import pytest
from unittest.mock import MagicMock
from transaction import Transaction
from catalog import Table, Page, Catalog

@pytest.fixture
def mock_buffer_manager():
    bm = MagicMock()
    # Mock get_page to return a valid Page object
    bm.get_page.return_value = Page(1, ["row1"], is_dirty=False)
    return bm

@pytest.fixture
def mock_catalog():
    cat = MagicMock()
    cat.tables = {}
    cat.get_free_page_id.side_effect = range(100, 200) # generating mock page IDs
    return cat

@pytest.fixture
def transaction(mock_buffer_manager, mock_catalog):
    return Transaction(1, mock_buffer_manager, mock_catalog)

def test_get_table_fetches_from_catalog_initially(transaction, mock_catalog):
    """Test that before modification, tables are fetched from the real catalog."""
    real_table = Table("users", [], [])
    mock_catalog.get_table_by_name.return_value = real_table
    
    t = transaction.get_table_by_name("users")
    assert t == real_table

def test_get_new_page_creates_shadow_table(transaction):
    """Test that allocating a new page creates a shadow table copy."""
    original = Table("users", [], [], [1])
    transaction.catalog.get_table_by_name.return_value = original
    
    # Action: Get a new page (e.g., during insert)
    transaction.get_new_page(original)
    
    # Assert: Shadow table exists
    assert "users" in transaction.shadow_tables
    shadow = transaction.shadow_tables["users"]
    
    # Shadow should have the old page [1] AND the new page [100]
    assert shadow.page_id == [1, 100]
    # Original should be untouched
    assert original.page_id == [1]

def test_copy_on_write_logic(transaction, mock_buffer_manager):
    """Test that writing to an existing page creates a copy (Shadow Paging)."""
    original = Table("users", [], [], [1])
    transaction.catalog.get_table_by_name.return_value = original
    
    # Action: Request existing page for write
    shadow_page = transaction.get_existing_page_for_write(original, 1)
    
    # Assert: A new page ID was allocated (100)
    assert shadow_page.page_id == 100
    # The shadow table should point to 100, not 1
    shadow_table = transaction.shadow_tables["users"]
    assert shadow_table.page_id == [100]
    # Buffer manager should have received the new page
    mock_buffer_manager.put.assert_called_with(shadow_page)

def test_add_new_table_only_in_shadow(transaction):
    """Test that creating a table places it in shadow, not real catalog."""
    new_table = Table("new_tab", [], [])
    transaction.add_new_table(new_table)
    
    assert "new_tab" in transaction.shadow_tables
    # Should not have called create on catalog yet
    transaction.catalog.add_new_table.assert_not_called()

def test_commit_applies_changes(transaction):
    """Test that commit pushes shadow tables to the catalog."""
    t_obj = Table("users", [], [])
    transaction.shadow_tables["users"] = t_obj
    
    transaction.commit()
    
    transaction.catalog.create_or_replace_table.assert_called_with(t_obj)

def test_rollback_returns_page_ids(transaction):
    """Test that rollback returns allocated IDs and touches no catalog tables."""
    transaction.obtained_page_ids = [100, 101]
    
    transaction.rollback()
    
    transaction.catalog.return_page_ids.assert_called_with([100, 101])
    transaction.catalog.create_or_replace_table.assert_not_called()

def test_drop_table_shadow_logic(transaction):
    """Test that dropping a table marks it as None in shadow."""
    t = Table("users", [], [])
    transaction.catalog.get_table_by_name.return_value = t
    
    transaction.drop_table_by_name("users")
    
    assert "users" in transaction.shadow_tables
    assert transaction.shadow_tables["users"] is None

def test_commit_dropped_table(transaction):
    """Test that committing a dropped table calls drop on catalog."""
    transaction.shadow_tables["users"] = None
    transaction.catalog.tables = {"users": "exists"} # Simulate existence
    
    transaction.commit()
    
    transaction.catalog.drop_table_by_name.assert_called_with("users")

def test_create_table_already_exists_error(transaction):
    """Test error when creating a table that exists in shadow or catalog."""
    transaction.shadow_tables["users"] = Table("users", [], [])
    
    with pytest.raises(Exception) as exc:
        transaction.add_new_table(Table("users", [], []))
    assert "already exists" in str(exc.value)

def test_get_page_generator_uses_buffer(transaction, mock_buffer_manager):
    """Test that scanning a table yields pages via buffer manager."""
    t = Table("users", [], [], [1, 2])
    transaction.shadow_tables["users"] = t
    
    # mock_buffer_manager.get_pages is a generator
    def mock_gen(pids):
        yield from pids
    mock_buffer_manager.get_pages.side_effect = mock_gen

    gen = transaction.get_page_generator_from_table_by_name("users")
    results = list(gen)
    
    assert results == [1, 2] # Should yield the page objects (mocked here as ints for simplicity)
