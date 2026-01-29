from buffermanager import BufferManager
from catalog import Page, Table
from collections import defaultdict

class Transaction:
    def __init__(self, id: int, buffer_manager, catalog):
        """The transaction class is a placeholder for catalog changes that are not yet committed. 
        It tracks changes in the shadow_tables attribute. Here, Table objects are stored that are
        altered. When a page is altered, the old page is copied (with alterations) into a new 
        page. If the query is committed, the Table object in the catalog is replaced with the 

        Table object with the new page_id. If rolled back, the new page is not registered in the 
        catalog. Thus 
        
        """
        self.id = id
        self.buffer_manager: BufferManager = buffer_manager
        self.catalog = catalog
        # Keep track of which tables we modified so we can update them at commit
        self.shadow_tables = {}  # Table.table_name: Table
        self.obtained_page_ids = []  # in case of rollback, give these ids back
        # in case of commit give these (removed) pages back)
        self.freed_page_ids = defaultdict(list[int])  # Table.table_map : [page_id, ]
        self._has_terminated = False

    def get_table_by_name(self, name: str):
        table: None | Table = self.shadow_tables.get(name.lower())
        if table: return table
        return self.catalog.get_table_by_name(name)

    def _get_or_create_shadow_table(self, table: Table) -> Table:
        table_name = table.table_name
        if table_name not in self.shadow_tables:
            # copy the table and put it in the shadow table map
            shadow_table = table.deepcopy()
            if shadow_table.page_id == []:
                self.get_new_page(shadow_table)
            else:
                self._get_existing_page_for_write(shadow_table, shadow_table.page_id[-1])
            self.shadow_tables[table_name] = shadow_table
        return self.shadow_tables[table_name]

    def _swap_page_id(self, table: Table, old_page_id, new_page_id):
        idx = table.page_id.index(old_page_id)
        table.page_id[idx] = new_page_id


    def _get_free_page_id(self) -> int:
        page_id = self.catalog.get_free_page_id(self.id)
        self.obtained_page_ids.append(page_id)
        return page_id

    def add_new_table(self, table: Table):
        name = table.table_name.lower()
        has_been_droped = name in self.shadow_tables and self.shadow_tables[name] is None
        exists_in_shadow = name in self.shadow_tables and self.shadow_tables[name] is not None
        exists_in_catalog = name in self.catalog.tables
        if exists_in_shadow:
            raise Exception(f"Table with name '{table.table_name}' already exists")
        if exists_in_catalog and not has_been_droped:
            raise Exception(f"Table with name '{table.table_name}' already exists")
        self.shadow_tables[name] = table
    
    def drop_table_by_name(self, name: str):
        table = self.get_table_by_name(name)
        self.freed_page_ids[table.table_name] += table.page_id  # free all pages
        self.shadow_tables[table.table_name] = None  # remove the referenced

    def get_page_generator_from_table_by_name(self, name):
        table = self.get_table_by_name(name)
        yield from self.buffer_manager.get_pages(table.page_id)


    def _get_existing_page_for_write(self, shadow_table: Table, old_pid):
        """
        Creates a shadow page (copy) of the original page. Swaps the orignal page for the shadow page
        Returns the shadow page.
        """
        # shadow_table = self._get_or_create_shadow_table(table)
        # get the original page
        original_page = self.buffer_manager.get_page(old_pid)
        # get new page_id for the shadow
        shadow_pid = self._get_free_page_id()
        # copy the data so we start with the same state. Add is_dirty to write to disk if buffer is full
        shadow_page = Page(shadow_pid, list(original_page.data), is_dirty=True)
        # copy content but swap page_id to the shaowpage
        self._swap_page_id(shadow_table, old_pid, shadow_pid)
        # free the old page id
        self.freed_page_ids[shadow_table.table_name].append(old_pid)
        # put shadow in the buffer. Can spill to disk if need
        self.buffer_manager.put(shadow_page)
        return shadow_page

    def get_new_page(self, shadow_table: Table) -> Page:
        """
        Allocates a brand new 'orphan' page for a table.
        """
        # shadow_table = self._get_or_create_shadow_table(table)
        # get new page_id
        new_pid = self._get_free_page_id()
        # create the blank page
        new_page = Page(new_pid, data=[], is_dirty=True)
        # put in buffer so it can spill to disk if memory is tight
        self.buffer_manager.put(new_page)
        # register new page_id
        shadow_table.page_id.append(new_pid)
        return new_page

    def commit(self):
        """Persist the changes in the catalog.

        - remove droped tables
        - create or replace the Table object of modified or new tables
        - return all the old no longer valid pages
        """
        if self._has_terminated:
            return
        for name, table_obj in self.shadow_tables.items():
            if table_obj is None:
                # Table was dropped
                if name in self.catalog.tables:
                    self.catalog.drop_table_by_name(name)  # removes reference and frees page_ids
            else:
                self.catalog.create_or_replace_table(table_obj)
        self.catalog.return_page_ids([v for k, v in self.freed_page_ids.items()])
        self._has_terminated = True


    def rollback(self):
        """In rollback none of the new pages get registered in the catalog.
        However, the obtained page_ids are returned to the free list. The 
        new Table objects an pages will be overwriten since they are marked
        as free page ids."""
        self.catalog.return_page_ids(self.obtained_page_ids)
        self._has_terminated = True
