from buffermanager import BufferManager
from errors import ValidationError
from catalog import Page, Table, Catalog, ShadowTable, ShadowPage, from_shadow_table
from collections import defaultdict

from errors import ValidationError

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
        self.catalog: Catalog = catalog
        # Keep track of which tables we modified so we can update them at commit
        self.shadow_tables: dict[str, None | ShadowTable] = {}  # None means table has been droped
        self.obtained_page_ids = set()  # in case of rollback, give these ids back
        # in case of commit give these (removed) pages back)
        self.freed_page_ids = []
        self._has_terminated = False

    def get_table_by_name(self, name: str) -> Table | ShadowTable:
        table: None | ShadowTable = self.shadow_tables.get(name.lower())
        if table: return table
        return self.catalog.get_table_by_name(name)

    def get_or_create_shadow_table(self, table: Table | ShadowTable) -> ShadowTable:
        table_name = table.table_name
        if table_name not in self.shadow_tables and isinstance(table, Table):
            # copy the table and put it in the shadow table map
            # if the transaction is successfull this will be the new table object
            shadow_table: ShadowTable = table.to_shadow_table()
            self.shadow_tables[shadow_table.table_name] = shadow_table
            return shadow_table
        shadow_table_or_none: None | ShadowTable = self.shadow_tables[table_name]
        if shadow_table_or_none is None:
            raise ValidationError(f"Table with name {table_name} no longer exists.")
        return shadow_table_or_none
    
    def prepare_shadow_table_for_write(self, shadow_table: ShadowTable):
        if not shadow_table.table_name in self.shadow_tables:
            raise Exception("Shadow table not yet geristered")
        if shadow_table.page_id == []:
            # attach a new shadow page
            return self.get_new_page(shadow_table)
        else:
            # swap the current latest page for a shadow page
            return self._get_existing_page_for_write(shadow_table, shadow_table.page_id[-1])

    def _swap_page_id(self, table: ShadowTable, old_page_id: int, new_page_id: int):
        idx = table.page_id.index(old_page_id)
        table.page_id[idx] = new_page_id
        print(f"pids: {table.page_id}")


    def _get_free_page_id(self) -> int:
        page_id = self.catalog.get_free_page_id(self.id)
        self.obtained_page_ids.add(page_id)
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
        shadow_table = table.to_shadow_table()
        self.shadow_tables[name] = shadow_table
    
    def drop_table_by_name(self, name: str):
        table: Table | ShadowTable = self.get_table_by_name(name)
        self.freed_page_ids += table.page_id  # free all pages
        self.shadow_tables[table.table_name] = None  # remove the referenced

    def get_page_generator_from_table_by_name(self, name):
        table = self.get_table_by_name(name)
        yield from self.buffer_manager.get_pages(table.page_id)


    def _get_existing_page_for_write(self, shadow_table: ShadowTable, old_pid):
        """
        Creates a shadow page (copy) of the original page. Swaps the orignal page for the shadow page
        Returns the shadow page.
        """
        # get the original page
        original_page: Page | ShadowPage = self.buffer_manager.get_page(old_pid)
        # get new page_id for the shadow
        if isinstance(original_page, ShadowPage):
            return original_page

        shadow_pid: int = self._get_free_page_id()
        # copy the data so we start with the same state. Add is_dirty to write to disk if buffer is full
        # shadow_page = ShadowPage(shadow_pid, list(original_page.data), is_dirty=True)

        # get mutable page
        shadow_page: ShadowPage = original_page.to_shadow_page(shadow_pid)
        # swap page_id to the shadowpage
        self._swap_page_id(shadow_table, old_pid, shadow_pid)
        # free the old page id
        self.freed_page_ids.append(old_pid)
        # put shadow in the buffer. Can spill to disk if need
        self.buffer_manager.put(shadow_page)
        return shadow_page

    def get_new_page(self, shadow_table: ShadowTable) -> ShadowPage:
        """
        Allocates a brand new 'orphan' page for a table.
        """
        if shadow_table.table_name not in self.shadow_tables:
            raise Exception("Table is not a shadow table")
        # get new page_id
        new_pid = self._get_free_page_id()
        # create the blank page
        new_page = ShadowPage(new_pid, data=[], is_dirty=True)
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
        for name, shadow_table in self.shadow_tables.items():
            if shadow_table is None:
                # Table was dropped
                if name in self.catalog.tables:
                    self.catalog.drop_table_by_name(name)  # removes reference and frees page_ids
            else:
                table = from_shadow_table(shadow_table)
                self.catalog.create_or_replace_table(table)
        self.catalog.return_page_ids(list(self.freed_page_ids))
        self._has_terminated = True


    def rollback(self):
        """In rollback none of the new pages get registered in the catalog.
        However, the obtained page_ids are returned to the free list. The 
        new Table objects an pages will be overwriten since they are marked
        as free page ids."""
        self.catalog.return_page_ids(list(self.obtained_page_ids))
        self._has_terminated = True
