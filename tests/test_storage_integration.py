import pytest
import os
import catalog
from engine import DatabaseEngine
from buffermanager import BufferManager
from diskmanager import DiskManager
from catalog import Catalog, PAGE_SIZE
from request import QueryRequest
from transaction import Transaction

DB_FILE = "test_system.db"

@pytest.fixture
def db_engine():
    """Sets up a clean DB for each test."""
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    
    # Bootstrap
    dm = DiskManager(DB_FILE)
    bm = BufferManager(dm, capacity=10) # Limited capacity to force logic checks
    
    # Initialize empty catalog
    catalog = Catalog.from_page(bm.get_page(0))
    
    engine = DatabaseEngine(catalog, bm)
    yield engine
    
    # Cleanup
    bm.flush()
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)

def reopen_database():
    """Helper to simulate a restart (new memory, same disk file)."""
    dm = DiskManager(DB_FILE)
    bm = BufferManager(dm, capacity=10)
    # Reload catalog from Page 0
    catalog = Catalog.from_page(bm.get_page(0))
    return DatabaseEngine(catalog, bm)

# 1. Basic Persistence (Auto-Commit)
def test_create_insert_select_persistence(db_engine):
    """Test that standard auto-commit queries persist to disk."""
    db_engine.execute(QueryRequest("CREATE TABLE users (id INT, name TEXT);"))
    db_engine.execute(QueryRequest("INSERT INTO users VALUES (1, 'Alice');"))
    
    # Force persist to disk
    db_engine.buffer_manager.flush()
    db_engine.buffer_manager.diskmanager.write_page(db_engine.catalog.to_page())

    # Restart
    new_engine = reopen_database()
    
    res = new_engine.execute(QueryRequest("SELECT name FROM users WHERE id = 1;"))
    assert res.rows == [('Alice',)]

# 2. Manual Commit Logic
def test_manual_commit_persistence(db_engine):
    """Test explicit transaction management with auto_commit=False."""
    # 1. Start a transaction explicitly
    txn = db_engine.get_new_transaction()
    assert txn.id == 1
    
    # 2. Execute operations attached to this transaction ID
    # Note: auto_commit=False prevents the engine from committing inside execute()
    req1 = QueryRequest("CREATE TABLE data (val INT);", transaction_id=txn.id, auto_commit=False)
    r1 = db_engine.execute(req1)
    assert r1.transaction_id == 1

    req2 = QueryRequest("INSERT INTO data VALUES (100);", transaction_id=txn.id, auto_commit=False)
    db_engine.execute(req2)
    assert r1.transaction_id == 1

    res = db_engine.execute(QueryRequest("SELECT val FROM data;", transaction_id=txn.id, auto_commit=False))
    assert res.rows == [(100,)]
    
    # 3. Commit manually
    txn.commit()
    
    res = db_engine.execute(QueryRequest("SELECT val FROM data;"))
    assert res.rows == [(100,)]
    # 4. Flush and Verify
    db_engine.buffer_manager.flush()
    db_engine.buffer_manager.diskmanager.write_page(db_engine.catalog.to_page())
    
    new_engine = reopen_database()
    res = new_engine.execute(QueryRequest("SELECT val FROM data;"))
    assert res.rows == [(100,)]

# 3. Rollback Discards Changes
def test_manual_rollback_discard(db_engine):
    """Test that rollback prevents data from reaching the catalog."""
    db_engine.execute(QueryRequest("CREATE TABLE safe (id INT);"))
    db_engine.execute(QueryRequest("INSERT INTO safe VALUES (1);"))
    
    txn = db_engine.get_new_transaction()
    
    # Attempt to drop the table in a transaction
    req = QueryRequest("DROP TABLE safe;", transaction_id=txn.id, auto_commit=False)
    db_engine.execute(req)
    
    # Verify shadow state: Table should appear dropped (None) inside the txn context
    # Accessing internal state for verification
    assert txn.shadow_tables['safe'] is None
    
    # Rollback
    txn.rollback()
    
    # Verify Table still exists in the main catalog
    res = db_engine.execute(QueryRequest("SELECT * FROM safe;"))
    assert res.rows == [(1,)]

# 4. Rollback Atomicity (Internal State)
def test_rollback_atomicity_internals(db_engine):
    """Test rollback using direct internal manipulation (as per your style preference)."""
    db_engine.execute(QueryRequest("CREATE TABLE logs (msg TEXT);"))
    
    txn: Transaction = db_engine.get_new_transaction()
    table = db_engine.catalog.get_table_by_name("logs")
    
    # Simulate a destructive action in the transaction
    txn.drop_table_by_name(table.table_name)
    
    # Verify it's gone in the shadow
    assert txn.shadow_tables['logs'] is None
    
    txn.rollback()
    
    # Verify the table remains untouched in the catalog
    # The rollback should return obtained page IDs but not apply drop
    assert db_engine.catalog.get_table_by_name("logs") == table

# 5. Concurrent Transactions: Independent Inserts
def test_concurrent_transactions_success(db_engine):
    """Test two transactions executing simultaneously and both committing."""
    db_engine.execute(QueryRequest("CREATE TABLE t1 (x INT);"))
    db_engine.execute(QueryRequest("CREATE TABLE t2 (y INT);"))
    
    txn1 = db_engine.get_new_transaction()
    txn2 = db_engine.get_new_transaction()
    
    # T1 inserts into T1
    db_engine.execute(QueryRequest("INSERT INTO t1 VALUES (10);", transaction_id=txn1.id, auto_commit=False))
    
    # T2 inserts into T2
    db_engine.execute(QueryRequest("INSERT INTO t2 VALUES (20);", transaction_id=txn2.id, auto_commit=False))
    
    # Both commit
    txn1.commit()
    txn2.commit()
    
    # Verify both visible
    res1 = db_engine.execute(QueryRequest("SELECT * FROM t1;"))
    res2 = db_engine.execute(QueryRequest("SELECT * FROM t2;"))
    assert res1.rows == [(10,)]
    assert res2.rows == [(20,)]

# 6. Concurrent Transactions: Isolation & Rollback
def test_concurrent_transactions_one_rollback(db_engine):
    """Test T1 committing and T2 rolling back does not affect T1."""
    db_engine.execute(QueryRequest("CREATE TABLE shared (val INT);"))
    
    txn1 = db_engine.get_new_transaction()
    txn2 = db_engine.get_new_transaction()
    
    # T1 inserts 1
    db_engine.execute(QueryRequest("INSERT INTO shared VALUES (1);", transaction_id=txn1.id, auto_commit=False))
    
    # T2 inserts 2
    db_engine.execute(QueryRequest("INSERT INTO shared VALUES (2);", transaction_id=txn2.id, auto_commit=False))
    
    # T1 Commits, T2 Rolls back
    txn1.commit()
    txn2.rollback()
    
    # Verify only 1 exists
    res = db_engine.execute(QueryRequest("SELECT * FROM shared;"))
    assert res.rows == [(1,)]

# 7. Page Allocation on Disk
def test_disk_storage_growth(db_engine):
    """Test that inserting data actually increases file size."""
    initial_size = os.path.getsize(DB_FILE)
    
    db_engine.execute(QueryRequest("CREATE TABLE growth (data TEXT);"))
    
    # Insert enough to force a write
    for i in range(20):
        db_engine.execute(QueryRequest(f"INSERT INTO growth VALUES ('Row {i}');"))
        
    db_engine.buffer_manager.flush()
    
    final_size = os.path.getsize(DB_FILE)
    assert final_size > initial_size
    # Should be at least Page 0 (Catalog) + Page 1 (Data)
    assert final_size >= 2 * PAGE_SIZE

# 8. Buffer Manager State
def test_buffer_manager_page_residency(db_engine):
    """Test that accessed pages reside in the buffer manager."""
    db_engine.execute(QueryRequest("CREATE TABLE resid (id INT);"))
    db_engine.execute(QueryRequest("INSERT INTO resid VALUES (999);"))
    
    # Get the table info
    table = db_engine.catalog.get_table_by_name("resid")
    page_id = table.page_id[0]
    
    # The page should be in the buffer
    assert page_id in db_engine.buffer_manager.buffer
    # It should be dirty because we just wrote to it and haven't flushed
    assert db_engine.buffer_manager.buffer[page_id].is_dirty == True

# 9. Large Volume Swapping (Capacity Limit)
def test_large_volume_swapping(db_engine):
    """Test behavior when data exceeds buffer capacity (forcing swap)."""
    # Buffer capacity is 10 (defined in fixture). We insert 50 rows.
    # Depending on row size, this should fill multiple pages.
    db_engine.execute(QueryRequest("CREATE TABLE heavy (id INT);"))
    
    txn = db_engine.get_new_transaction()
    
    # Insert a lot
    for i in range(100):
        db_engine.execute(QueryRequest(f"INSERT INTO heavy VALUES ({i});", transaction_id=txn.id, auto_commit=False))
    
    txn.commit()
    
    # Flush logic
    db_engine.buffer_manager.flush()
    db_engine.buffer_manager.diskmanager.write_page(db_engine.catalog.to_page())
    
    # Reopen and Count
    new_engine = reopen_database()
    res = new_engine.execute(QueryRequest("SELECT COUNT(*) FROM heavy;"))
    assert res.rows == [(100,)]

# 10. Catalog Free List Integrity
def test_drop_table_restores_free_list(db_engine):
    """Test that dropping a table correctly returns pages to the free list."""
    # 1. Create table and allocate pages
    db_engine.execute(QueryRequest("CREATE TABLE temp_drop (id INT);"))
    db_engine.execute(QueryRequest("INSERT INTO temp_drop VALUES (1);"))
    
    table = db_engine.catalog.get_table_by_name("temp_drop")
    used_page_id = table.page_id[0]
    
    # 2. Drop table
    db_engine.execute(QueryRequest("DROP TABLE temp_drop;"))
    
    # 3. Verify Page ID is free
    # (Note: Depends on if catalog recycles immediately or appends to list)
    assert used_page_id in db_engine.catalog.free_page_ids
