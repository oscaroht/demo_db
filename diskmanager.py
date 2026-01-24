import os
from config import PAGE_SIZE
from catalog import Catalog, Page

class DiskManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        # Create file if it doesn't exist
        if not os.path.exists(db_path):
            with open(db_path, 'wb') as f:
                catalog = Catalog.get_empty_catalog()
                page = catalog.to_page()
                f.write(page.to_bytes())

    def read_page(self, page_id: int) -> Page:
        offset = page_id * PAGE_SIZE
        with open(self.db_path, 'rb') as f:
            f.seek(offset)
            return Page(page_id, f.read(PAGE_SIZE))

    def write_page(self, page: Page):
        data = page.to_bytes()
        if len(data) != PAGE_SIZE:
            raise ValueError("Data must be exactly PAGE_SIZE")
        offset = page.page_id * PAGE_SIZE
        with open(self.db_path, 'r+b') as f:
            f.seek(offset)
            f.write(data)
