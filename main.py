
from metaclasses import PageInfo, TableInfo, TableInfoCollection
from diskmanager import DiskManager
from buffermanager import BufferManager
from queryplanner import QueryPlanner
from sql_interpreter import tonkenizer, parser

user_input = "SELECT id, num, letter FROM tbl WHERE id = 2;"
# user_input = input("Press enter to continue...")

page_info = PageInfo(
    page_id=1,
    page_offset=0,
    page_size=4096,
    table_info=None
)

tbl_info = TableInfo(
    table_name='tbl',
    column_names=['id', 'num', 'letter'],
    column_datatypes=[int, int, str],
    page_info=[page_info]
)

page_info.table_info = tbl_info

table_collection = TableInfoCollection({tbl_info.table_name: tbl_info})



tokens = tonkenizer(user_input)
structured_query = parser(tokens)

dm = DiskManager('demo.db')
bm = BufferManager(dm, capacity=10)

qp = QueryPlanner(bm, table_collection)
plan_handle = qp.create_plan(structured_query)
qp.execute_plan(plan_handle)

print('')
