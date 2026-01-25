import buffermanager
from catalog import Catalog, Table, Page
import catalog
import diskmanager
from engine import DatabaseEngine
from diskmanager import DiskManager
from buffermanager import BufferManager
from cli import repl

table_data = {"employee":[
    (1, 'Alice', 30, 'NY', 60000),
    (2, 'Bob', 22, 'SF', 45000),
    (3, 'Charlie', 25, 'NY', 55000),
    (4, 'Dave', 40, 'LA', 70000),
    (5, 'Eve', 19, 'BOS', 30000),
    (6, 'Fay', 22, 'SF', 45000),
    (7, 'Grace', 30, 'NY', 80000)],
    "contract": [(1, 1, '2025-01-01', '2027-12-31'),
                  (2, 1, '2023-01-01', '2024-12-31'),
                  (3, 2, '2024-01-01', '2027-07-01'),
                  (4, 2, '2023-01-01', '2023-12-31'),
                  (5, 3, '2026-01-01', '2026-03-31')],
}

mock_schema = {
    'employee': ['id', 'name', 'age', 'city', 'salary'],
    'contract': ['id', 'employee_id', 'start_date', 'end_date'],
}
table1 = Table('employee', ['id', 'name', 'age', 'city', 'salary'], [int, str, int, str, float], [1] )
table2 = Table('contract', ['id', 'employee_id', 'start_date', 'end_date'], [int, str, str, str], [2])

BOOTSTRAP = False

diskmanager = DiskManager('.db')
buffermanager = BufferManager(diskmanager, 10)

catalog_page = buffermanager.get_page(0)
catalog = Catalog.from_page(catalog_page)
if BOOTSTRAP:
    catalog = Catalog([table1, table2])
    rows = table_data['employee']
    buffermanager.put(Page(1, rows, is_dirty=True))
    rows = table_data['contract']
    buffermanager.put(Page(2, rows, is_dirty=True))

print(buffermanager.buffer)
print(catalog)

engine = DatabaseEngine(catalog, buffermanager)
print(engine.catalog.tables)
repl(engine)
diskmanager.write_page(catalog.to_page())
buffermanager.flush()

