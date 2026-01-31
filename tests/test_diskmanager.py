import pytest
import os
import pickle
from diskmanager import DiskManager
from catalog import Page, PageHeader, PAGE_SIZE

@pytest.fixture
def db_file(tmp_path):
    """Creates a temporary DB file path."""
    return str(tmp_path / "test.db")

def test_create_new_db_file(db_file):
    """Test that initializing DM creates a file with Page 0."""
    assert not os.path.exists(db_file)
    dm = DiskManager(db_file)
    assert os.path.exists(db_file)
    assert os.path.getsize(db_file) == PAGE_SIZE # Should have Page 0

def test_read_page_zero_bootstrap(db_file):
    """Test that page 0 is automatically initialized with an empty Catalog."""
    dm = DiskManager(db_file)
    page = dm.read_page(0)
    
    assert page.page_id == 0
    # Data should be a Catalog object (or dict representing it)
    assert hasattr(page.data, 'tables') 

def test_write_and_read_page(db_file):
    """Test writing a generic page and reading it back."""
    dm = DiskManager(db_file)
    data = [(1, "Alice")]
    p1 = Page(1, data)
    
    dm.write_page(p1)
    
    # Read back
    p1_read = dm.read_page(1)
    assert p1_read.page_id == 1
    assert p1_read.data == data

def test_page_offset_calculation(db_file):
    """Test that writing page 2 doesn't overwrite page 1."""
    dm = DiskManager(db_file)
    
    p1 = Page(1, ["data1"])
    p2 = Page(2, ["data2"])
    
    dm.write_page(p1)
    dm.write_page(p2)
    
    assert dm.read_page(1).data == ["data1"]
    assert dm.read_page(2).data == ["data2"]

def test_persistence_reopen(db_file):
    """Test data persists after closing and reopening DiskManager."""
    dm = DiskManager(db_file)
    p1 = Page(1, ["persistent"])
    dm.write_page(p1)
    
    del dm # simulate close
    
    dm2 = DiskManager(db_file)
    assert dm2.read_page(1).data == ["persistent"]

def test_write_too_large_page(db_file):
    """Test that writing a page exceeding PAGE_SIZE raises error."""
    dm = DiskManager(db_file)
    
    # Create massive data
    huge_data = [b"x" * (PAGE_SIZE + 100)] 
    p_huge = Page(1, huge_data)
    
    with pytest.raises(MemoryError): # Page.to_bytes raises this
        dm.write_page(p_huge)

def test_update_existing_page(db_file):
    """Test overwriting an existing page with new data."""
    dm = DiskManager(db_file)
    p1 = Page(1, ["v1"])
    dm.write_page(p1)
    
    p1_v2 = Page(1, ["v2"])
    dm.write_page(p1_v2)
    
    assert dm.read_page(1).data == ["v2"]

def test_read_uninitialized_page_error(db_file):
    """Test behavior when reading a page ID that hasn't been written."""
    dm = DiskManager(db_file)
    # Page 5 has never been written. File size is small.
    # Python's file seek past end is valid, but read returns empty bytes.
    # Page.from_bytes will likely fail on empty bytes or random noise.
    
    with pytest.raises(Exception): 
        dm.read_page(5)

def test_file_growth(db_file):
    """Test that file grows in increments of PAGE_SIZE."""
    dm = DiskManager(db_file)
    initial_size = os.path.getsize(db_file) # Just page 0
    
    # Write page 10. The file usually must fill gaps with zeros or be sparse.
    # In 'r+b' mode with seek, it might behave sparsely or require fill.
    # Your current write_page seeks. If file is small, seek past end works?
    # Actually, standard open() 'r+b' won't extend file if you seek past end usually 
    # unless you write. 
    p10 = Page(10, ["far away"])
    dm.write_page(p10)
    
    new_size = os.path.getsize(db_file)
    # Expected: (10 + 1) * PAGE_SIZE
    assert new_size >= 11 * PAGE_SIZE

def test_is_dirty_flag_respected(db_file):
    """
    Note: DiskManager doesn't care about dirty flags, it just writes what it's given.
    This test confirms DiskManager doesn't alter the object state unexpectedly.
    """
    dm = DiskManager(db_file)
    p1 = Page(1, ["test"], is_dirty=True)
    dm.write_page(p1)
    
    p_read = dm.read_page(1)
    # When read from disk, is_dirty is False (default in from_bytes)
    assert p_read.is_dirty is False
