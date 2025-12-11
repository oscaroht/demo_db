from collections import OrderedDict
from typing import List, Generator

from diskmanager import DiskManager
from metaclasses import Page, PageInfo

class BufferManager:

    # initialising capacity
    def __init__(self,  diskmanager: DiskManager, capacity: int = 10):
        self.buffer = OrderedDict()
        self.capacity = capacity
        self.diskmanager = diskmanager
    
    def get_pages(self, page_info_list: List[PageInfo]):
        """If multiple pages are needed then first yield the pages that are in the buffer. Only afterwards read from disk.
        Otherwise pages might be evicted from the cache that are needed for the same query."""
        needed_from_disk = []
        for page_info in page_info_list:
            if page_info.page_id in self.buffer:
                yield self.get(page_info)
            else:
                needed_from_disk.append(page_info)
        for disk_page_info in needed_from_disk:
            yield self.get(disk_page_info)

    def get(self, page_info: PageInfo) -> int:
        if page_info.page_id not in self.buffer:
            page = self.diskmanager.read_page(page_info)
            self.put(page_info.page_id, page)
            return page
        self.buffer.move_to_end(page_info.page_id)
        return self.buffer[page_info.page_id]

    def put(self, key: int, value: int) -> None:
        self.buffer[key] = value
        self.buffer.move_to_end(key)
        if len(self.buffer) > self.capacity:
            self.buffer.popitem(last = False)
