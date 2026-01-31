import pytest
from unittest.mock import MagicMock
from buffermanager import BufferManager
from catalog import Page

@pytest.fixture
def mock_disk_manager():
    return MagicMock()

@pytest.fixture
def buffer_manager(mock_disk_manager):
    return BufferManager(mock_disk_manager, capacity=2)

def test_fetch_page_miss(buffer_manager, mock_disk_manager):
    """Test fetching a page not in buffer triggers disk read."""
    # Setup mock to return a page
    mock_disk_manager.read_page.return_value = Page(1, [])
    
    page = buffer_manager.get_page(1)
    
    assert page.page_id == 1
    mock_disk_manager.read_page.assert_called_once_with(1)
    assert 1 in buffer_manager.buffer

def test_fetch_page_hit(buffer_manager, mock_disk_manager):
    """Test fetching a page already in buffer hits cache."""
    mock_disk_manager.read_page.return_value = Page(1, [])
    buffer_manager.get_page(1) # Load into buffer
    
    mock_disk_manager.read_page.reset_mock()
    
    page = buffer_manager.get_page(1) # Should be hit
    
    assert page.page_id == 1
    mock_disk_manager.read_page.assert_not_called()

def test_lru_eviction(buffer_manager, mock_disk_manager):
    """Test that the oldest accessed page is evicted when capacity is reached."""
    # Fill buffer (capacity 2)
    p1 = Page(1, [], is_dirty=False)
    p2 = Page(2, [], is_dirty=False)
    p3 = Page(3, [], is_dirty=False)
    
    mock_disk_manager.read_page.side_effect = [p1, p2, p3]
    
    buffer_manager.get_page(1)
    buffer_manager.get_page(2)
    
    # Access 3, should evict 1 (LRU)
    buffer_manager.get_page(3)
    
    assert 3 in buffer_manager.buffer
    assert 2 in buffer_manager.buffer
    assert 1 not in buffer_manager.buffer

def test_lru_update_on_access(buffer_manager, mock_disk_manager):
    """Test that accessing an existing page updates its LRU position."""
    p1 = Page(1, [], is_dirty=False)
    p2 = Page(2, [], is_dirty=False)
    p3 = Page(3, [], is_dirty=False)
    mock_disk_manager.read_page.side_effect = [p1, p2, p3]

    buffer_manager.get_page(1)
    buffer_manager.get_page(2)
    
    # Access 1 again, making 2 the LRU
    buffer_manager.get_page(1)
    
    # Access 3, should evict 2 (not 1)
    buffer_manager.get_page(3)
    
    assert 1 in buffer_manager.buffer
    assert 3 in buffer_manager.buffer
    assert 2 not in buffer_manager.buffer

def test_evict_dirty_page_writes_to_disk(buffer_manager, mock_disk_manager):
    """Test that evicting a dirty page triggers a disk write."""
    p1 = Page(1, [], is_dirty=True) # Dirty!
    p2 = Page(2, [], is_dirty=False)
    p3 = Page(3, [], is_dirty=False)
    mock_disk_manager.read_page.side_effect = [p1, p2, p3]

    buffer_manager.get_page(1)
    buffer_manager.get_page(2)
    
    # Evict 1 (Dirty)
    buffer_manager.get_page(3)
    
    mock_disk_manager.write_page.assert_called_once_with(p1)

def test_evict_clean_page_no_write(buffer_manager, mock_disk_manager):
    """Test that evicting a clean page does NOT trigger disk write."""
    p1 = Page(1, [], is_dirty=False) # Clean
    p2 = Page(2, [], is_dirty=False)
    p3 = Page(3, [], is_dirty=False)
    mock_disk_manager.read_page.side_effect = [p1, p2, p3]

    buffer_manager.get_page(1)
    buffer_manager.get_page(2)
    
    # Evict 1
    buffer_manager.get_page(3)
    
    mock_disk_manager.write_page.assert_not_called()

def test_flush_writes_all_dirty(buffer_manager, mock_disk_manager):
    """Test flush() writes all dirty pages but keeps them in buffer."""
    p1 = Page(1, [], is_dirty=True)
    p2 = Page(2, [], is_dirty=True)
    buffer_manager.put(p1)
    buffer_manager.put(p2)
    
    buffer_manager.flush()
    
    assert mock_disk_manager.write_page.call_count == 2
    # Ensure they are still in buffer
    assert 1 in buffer_manager.buffer
    assert 2 in buffer_manager.buffer

def test_get_pages_iterator(buffer_manager, mock_disk_manager):
    """Test get_pages yields from buffer first, then disk."""
    p1 = Page(1, [])
    p2 = Page(2, [])
    buffer_manager.put(p1)
    mock_disk_manager.read_page.return_value = p2
    
    # Request 1 (in RAM) and 2 (Disk)
    pages = list(buffer_manager.get_pages([1, 2]))
    
    assert len(pages) == 2
    assert pages[0].page_id == 1
    assert pages[1].page_id == 2
    mock_disk_manager.read_page.assert_called_once_with(2)

def test_put_existing_page_updates_cache(buffer_manager):
    """Test explicitly putting a page updates the buffer object."""
    p1_old = Page(1, ["old"])
    p1_new = Page(1, ["new"])
    
    buffer_manager.put(p1_old)
    buffer_manager.put(p1_new)
    
    assert buffer_manager.buffer[1].data == ["new"]

def test_capacity_zero_behavior(mock_disk_manager):
    """Edge case: effectively no cache."""
    bm = BufferManager(mock_disk_manager, capacity=0)
    p1 = Page(1, [], is_dirty=True)
    
    # Put should immediately evict
    bm.put(p1)
    
    assert 1 not in bm.buffer
    mock_disk_manager.write_page.assert_called_once_with(p1)
