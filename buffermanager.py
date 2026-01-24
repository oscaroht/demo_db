from collections import OrderedDict
from typing import List, Generator

from diskmanager import DiskManager
from catalog import Page

class BufferManager:

    # initialising capacity
    def __init__(self,  diskmanager: DiskManager, capacity: int = 10):
        self.buffer = OrderedDict()
        self.capacity = capacity
        self.diskmanager = diskmanager
    
    def get_pages(self, page_ids: List[int]):
        """If multiple pages are needed then first yield the pages that are in the buffer. Only afterwards read from disk.
        Otherwise pages might be evicted from the cache that are needed for the same query."""
        needed_from_disk = []
        for page_id in page_ids:
            if page_id in self.buffer:
                yield self.get(page_id)
            else:
                needed_from_disk.append(page_id)
        for disk_page_info in needed_from_disk:
            yield self.get(disk_page_info)

    def get(self, page_id: int) -> Page:
        """Retrieve a page from cache or disk"""
        if page_id not in self.buffer:
            page = self.diskmanager.read_page(page_id)
            self.put(page_id, page)
            return page
        self.buffer.move_to_end(page_id)
        return self.buffer[page_id]

    def put(self, key: int, value: Page) -> None:
        self.buffer[key] = value
        self.buffer.move_to_end(key)
        if len(self.buffer) > self.capacity:
            page_id, page = self.buffer.popitem(last = False)
            if page.is_dirty:
                self.diskmanager.write_page(page_id, page)

    def flush(self):
        for page_id, page in self.buffer.items():
            if page.is_dirty:
                self.diskmanager.write_page(page_id, page)
