from dataclasses import dataclass
from typing import List, Type

class Row(tuple):
    pass

class Page:
    def __init__(self, page_info, rows: List[Row]):
        self.page_info = page_info
        self.rows = rows


@dataclass
class PageInfo:
    page_id: int
    page_offset: int
    page_size: int
    table_info: object


@dataclass
class TableInfo:
    table_name: str
    column_names: List[str]
    column_datatypes: List[Type]
    page_info: List[PageInfo] = None


class TableInfoCollection(dict):
    pass