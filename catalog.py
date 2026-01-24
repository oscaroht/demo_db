from collections import namedtuple
from dataclasses import dataclass, field
import pickle
from typing import Iterable, List, Type, Any

from config import PAGE_SIZE

class Row(tuple):
    pass


import ctypes

class PageHeader(ctypes.BigEndianStructure):
    _pack_ = 1  # Ensures no padding between fields
    _fields_ = [
        ("page_id", ctypes.c_int32),
        ("data_length", ctypes.c_int32),
        # ("row_count", ctypes.c_int16),
    ]

HEADER_SIZE = ctypes.sizeof(PageHeader)

class Page:
    def __init__(self, page_id, data: Any, header=None, is_dirty=True):  # data is list[Row] or Catalog
        self.page_id = page_id
        self.data = data
        self.header: None | PageHeader = header
        self.is_dirty = is_dirty

    @classmethod
    def from_bytes(cls, page_id: int, raw_data: bytes):
        header = PageHeader.from_buffer_copy(raw_data)
        
        if header.data_length == 0:
            return cls(page_id, [], header=header)
            
        pickled_data = raw_data[HEADER_SIZE : HEADER_SIZE + header.data_length]
        rows = pickle.loads(pickled_data)
        return cls(page_id, rows, header=header, is_dirty=False)

    def to_bytes(self) -> bytes:
        pickled_rows = pickle.dumps(self.data)
        data_length = len(pickled_rows)
        
        if data_length > (PAGE_SIZE - HEADER_SIZE):
            raise MemoryError("Page overflow! Too many rows for one page.")

        header = PageHeader(self.page_id, data_length)
        
        page_data = bytes(header) + pickled_rows
        padding = b'\x00' * (PAGE_SIZE - len(page_data))
        return page_data + padding

@dataclass
class Table:
    table_name: str
    column_names: List[str]
    column_datatypes: List[Type]
    page_id: List[int] = field(default_factory=list)



class Catalog:
    """Mock database schema information."""
    def __init__(self, tables: Iterable[Table]):
        self.tables = {table.table_name: table for table in tables}

    def get_all_column_names(self, table_name) -> list[str]:
        table = self.tables.get(table_name.lower())
        if table is None:
            raise Exception("Table not found")
        return table.column_names

    def get_all_page_ids(self, table_name) -> list[int]:
        return self.tables[table_name.lower()].page_id

    def to_page(self) -> Page:
        page = Page(0, self)
        return page

    @classmethod
    def from_page(cls, page: Page):
        """Page.data contains the catalog object"""
        return page.data

    @classmethod
    def get_empty_catalog(cls):
        return cls([])
