import abc
from collections import namedtuple
from dataclasses import dataclass, field
import pickle
from typing import Iterable, List, Type, Any

from config import PAGE_SIZE
import transaction

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
        self.bytes_length = HEADER_SIZE
    
    def has_space_for(self, row: Any) -> bool:
        """Check if adding this row would exceed PAGE_SIZE."""
        # recalculating this bytes size by serializing the page and returning the result is inefficient
        # better would be to only calculate the new data. However, I do not think bytelen(data + [row])
        # is equal to bytelength(data) + bytelength(row). Better would be to not use native types 
        # and let everything be a bytes. For now this is find.
        new_data_state = self.data + [row]
        projected_size = len(pickle.dumps(new_data_state))
        return (HEADER_SIZE + projected_size) <= PAGE_SIZE

    def add_row(self, row: Any) -> bool:
        """
        Attempts to add a row. Returns True if successful, 
        False if the page is full.
        """
        if self.has_space_for(row):
            self.data.append(row)
            self.is_dirty = True
            return True
        return False
    
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
        sorted_page_ids = sorted([id for table in tables for id in table.page_id])
        self.free_page_ids = []  # should derive from sorted_page_ids. Too late now to fix
        self.max_page_id = max([id for table in tables for id in table.page_id])
        self.borrowed_page_ids = {}  # tranaction_id: page_id

    def get_table_by_name(self, name: str):
        table = self.tables.get(name.lower())
        if table is None:
            raise Exception("Table not found")
        return table

    def drop_table_by_name(self, name: str):
        table = self.get_table_by_name(name)
        self.free_page_ids += table.page_id  # take the table's pages and add them to the free list for reassignment later
        del self.tables[table.table_name]  # remove from dict

    def get_all_page_ids(self, table_name) -> list[int]:
        return self.tables[table_name.lower()].page_id

    def get_free_page_id(self, tranaction_id: int) -> int:
        if self.free_page_ids:
            page_id = self.free_page_ids.pop(0)
        else:
            page_id = self.max_page_id + 1
            self.max_page_id = page_id
        if tranaction_id not in self.borrowed_page_ids:
            self.borrowed_page_ids[transaction] = []
        self.borrowed_page_ids[tranaction_id].append(page_id)
        return page_id

    def return_page_ids(self, page_ids: list[int]):
        self.free_page_ids += page_ids

    def add_new_table(self, table: Table):
        name = table.table_name.lower()
        if name in self.tables:
            raise Exception(f"Table with name '{table.table_name}' already exists")
        self.tables[name] = table

    def create_or_replace_table(self, table: Table):
        self.tables[table.table_name] = table


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
