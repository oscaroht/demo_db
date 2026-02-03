from collections import OrderedDict
from typing import Iterable, List, Generator

from diskmanager import DiskManager
from catalog import Page, ShadowPage

class BufferManager:

    def __init__(self,  diskmanager: DiskManager, capacity: int = 10):
        self.buffer = OrderedDict()
        self.capacity = capacity
        self.diskmanager = diskmanager
    
    def get_pages(self, page_ids: Iterable[int]):
        """If multiple pages are needed then first yield the pages that are in the buffer. Only afterwards read from disk.
        Otherwise pages might be evicted from the cache that are needed for the same query."""
        needed_from_disk = []
        for page_id in page_ids:
            if page_id in self.buffer:
                yield self.get_page(page_id)
            else:
                needed_from_disk.append(page_id)
        for disk_page_info in needed_from_disk:
            yield self.get_page(disk_page_info)

    def get_page(self, page_id: int) -> Page | ShadowPage:
        """Retrieve a page from cache or disk"""
        if page_id not in self.buffer:
            page = self.diskmanager.read_page(page_id)
            self.put(page)
            return page
        self.buffer.move_to_end(page_id)
        return self.buffer[page_id]

    def put(self, page: Page | ShadowPage) -> None:
        self.buffer[page.page_id] = page
        self.buffer.move_to_end(page.page_id)
        if len(self.buffer) > self.capacity:
            _, evicted_page = self.buffer.popitem(last = False)
            if evicted_page.is_dirty:
                self.diskmanager.write_page(evicted_page)

    def flush(self):
        for _, page in self.buffer.items():
            if page.is_dirty or isinstance(page, ShadowPage):  # actually only shadow pages can be dirty and I guess every shadow page is dirty otherwise there is not point in making a shadow page
                print(f"FLUSHING PAGE {page.page_id}")
                self.diskmanager.write_page(page)
