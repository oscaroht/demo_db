
from buffermanager import BufferManager
from collections import OrderedDict

# Create a mock DiskManager
class MockDiskManager:
    def read_page(self, page_info):
        return f"Page {page_info.page_id} data"

# Create mock PageInfo objects
class MockPage:
    def __init__(self, page_id):
        self.page_id = page_id


def test_buffer_manager_initialization():
    # Initialize BufferManager with a mock DiskManager and a capacity of 5
    disk_manager = MockDiskManager()
    buffer_manager = BufferManager(diskmanager=disk_manager, capacity=5)

    # Check if the buffer is initialized correctly
    assert isinstance(buffer_manager.buffer, OrderedDict)
    assert buffer_manager.capacity == 5
    assert buffer_manager.diskmanager == disk_manager

    # Check if the buffer is empty initially
    assert len(buffer_manager.buffer) == 0

def test_buffer_manager_get_and_put():
    # Initialize BufferManager with a mock DiskManager and a capacity of 5
    disk_manager = MockDiskManager()
    buffer_manager = BufferManager(diskmanager=disk_manager, capacity=5)

    page_info1 = MockPage(page_id=1)
    page_info2 = MockPage(page_id=2)

    # Put a page into the buffer
    buffer_manager.put(page_info1.page_id, buffer_manager.diskmanager.read_page(page_info1))
    
    # Check if the page is in the buffer
    assert len(buffer_manager.buffer) == 1
    assert buffer_manager.buffer[page_info1.page_id] == "Page 1 data"

    # Get the same page from the buffer
    retrieved_page = buffer_manager.get(page_info1)
    
    # Check if the retrieved page is correct
    assert retrieved_page == "Page 1 data"
    
    # Put another page into the buffer
    buffer_manager.put(page_info2.page_id, buffer_manager.diskmanager.read_page(page_info2))
    
    # Check if both pages are in the buffer
    assert len(buffer_manager.buffer) == 2
    assert buffer_manager.buffer[page_info2.page_id] == "Page 2 data"

def test_buffer_manager_eviction():
    # Initialize BufferManager with a mock DiskManager and a capacity of 2
    disk_manager = MockDiskManager()
    buffer_manager = BufferManager(diskmanager=disk_manager, capacity=2)

    page_info1 = MockPage(page_id=1)
    page_info2 = MockPage(page_id=2)
    page_info3 = MockPage(page_id=3)

    # Put two pages into the buffer
    buffer_manager.put(page_info1.page_id, buffer_manager.diskmanager.read_page(page_info1))
    buffer_manager.put(page_info2.page_id, buffer_manager.diskmanager.read_page(page_info2))

    # Check if both pages are in the buffer
    assert len(buffer_manager.buffer) == 2

    # Add a third page, which should evict the first one (LRU)
    buffer_manager.put(page_info3.page_id, buffer_manager.diskmanager.read_page(page_info3))

    # Check if the first page was evicted
    assert len(buffer_manager.buffer) == 2
    assert page_info1.page_id not in buffer_manager.buffer
    assert page_info2.page_id in buffer_manager.buffer
    assert page_info3.page_id in buffer_manager.buffer

def test_buffer_manager_get_lru():
    # Initialize BufferManager with a mock DiskManager and a capacity of 2
    disk_manager = MockDiskManager()
    buffer_manager = BufferManager(diskmanager=disk_manager, capacity=2)

    page_info1 = MockPage(page_id=1)
    page_info2 = MockPage(page_id=2)

    # Put two pages into the buffer
    buffer_manager.put(page_info1.page_id, buffer_manager.diskmanager.read_page(page_info1))
    buffer_manager.put(page_info2.page_id, buffer_manager.diskmanager.read_page(page_info2))

    # Access the first page to make it the most recently used
    retrieved_page = buffer_manager.get(page_info1)

    # Add a third page, which should evict the second one (LRU)
    page_info3 = MockPage(page_id=3)
    buffer_manager.put(page_info3.page_id, buffer_manager.diskmanager.read_page(page_info3))

    # Check if the second page was evicted
    assert len(buffer_manager.buffer) == 2
    assert page_info1.page_id in buffer_manager.buffer
    assert page_info2.page_id not in buffer_manager.buffer
    assert page_info3.page_id in buffer_manager.buffer

def test_buffer_manager_get_more_pages_than_buffer_size():
    # Initialize BufferManager with a mock DiskManager and a capacity of 2
    disk_manager = MockDiskManager()
    buffer_manager = BufferManager(diskmanager=disk_manager, capacity=2)

    page_info1 = MockPage(page_id=1)
    page_info2 = MockPage(page_id=2)
    page_info3 = MockPage(page_id=3)
    page_info4 = MockPage(page_id=4)

    # Get pages from the buffer (should be empty initially)
    pages = list(buffer_manager.get_pages([page_info1, page_info2, page_info3, page_info4]))
    
    # Check if the pages are fetched from disk
    assert len(pages) == 4
    assert pages[0] == "Page 1 data"
    assert pages[1] == "Page 2 data"
    assert pages[2] == "Page 3 data"
    assert pages[3] == "Page 4 data"

    # Check if the pages are now in the buffer
    assert len(buffer_manager.buffer) == 2
