
from typing import List

from metaclasses import Page, PageInfo, Row


class DiskManager:
    def __init__(self, disk_path: str):
        self.disk_path = disk_path

    def write_page(self, page: Page):
        # Logic to write the page to disk
        pass

    def read_page(self, page_info: PageInfo) -> Page:
        with open(self.disk_path, 'r') as f:  # opening and closing in the init??
            f.seek(page_info.page_offset)
            content = f.read(page_info.page_size)
        # content = content.decode('utf-8')
        rows = content.split('\n')
        if rows[-1] == '':  # in linux final line is read as an empty string
            rows = rows[:-1]
        row_collection: List[Row] = []
        for row in rows:
            str_values = row.split(',')
            if len(str_values) != len(page_info.table_info.column_names):
                raise ValueError(f"Row has {len(str_values)} columns and does not match table schema with {len(page_info.table_info.column_names)} columns")
            row = Row([dt(v) for v, dt in zip(str_values, page_info.table_info.column_datatypes)])
                # Convert values to the appropriate datatype
                # row.append(dt(v))
            row_collection.append(row)
        return Page(page_info, row_collection)
