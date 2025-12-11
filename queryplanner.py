from buffermanager import BufferManager
from query_steps import Iterator, Filter

class QueryPlanner:
    def __init__(self, buffer_manager: BufferManager, table_collection):
        self.buffer_manager = buffer_manager
        self.table_collection = table_collection

    def create_plan(self, structured_query):
        table_info = self.table_collection[structured_query['table']]
        page_info_list = [page_info for page_info in table_info.page_info]
        iterator = Iterator(self.buffer_manager.get_pages(page_info_list))
        
        if 'where' in structured_query:


            fil = Filter(
                comparison=structured_query['where']['operator'],
                val1=structured_query['where']['slot1']['value'] if structured_query['where']['slot1']['type'] == 'value' else None,
                col_idx1=table_info.column_names.index(structured_query['where']['slot1']['value']) if structured_query['where']['slot1']['type'] == 'column' else None,
                val2=structured_query['where']['slot2']['value'] if structured_query['where']['slot2']['type'] == 'value' else None,
                col_idx2=table_info.column_names.index(structured_query['where']['slot2']['value']) if structured_query['where']['slot2']['type'] == 'column' else None,
                parent=iterator
            )
        
        handle = fil

        return handle
    def execute_plan(self, plan_handle):
        for row in plan_handle.next():
            print(row)