from diskmanager import DiskManager
from metaclasses import PageInfo, Page, TableInfo



def test_diskmanager_read_page():
    # Assuming you have a test database file 'test.db' with appropriate content
    disk_manager = DiskManager('test.db')
    
    # Create a mock PageInfo object
    page_info = PageInfo(
        page_id=1,
        page_offset=0,
        page_size=4096,
        table_info=None  # This should be set to a valid TableInfo object in real tests
    )

    tbl_info = TableInfo(
        table_name='tbl',
        column_names=['id', 'num', 'letter'],
        column_datatypes=[int, int, str],
        page_info=[page_info]
    )

    page_info.table_info = tbl_info
    
    # Read the page
    page = disk_manager.read_page(page_info)
    
    # Check if the page is not None and has the expected properties
    assert page is not None
    assert isinstance(page, Page)
    assert len(page.rows) > 0  # Assuming there are rows in the page
