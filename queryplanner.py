from syntax_tree import ASTNode, SelectStatement, LogicalExpression, Comparison, Literal, ColumnRef, AggregateCall
from operators import Filter, BaseIterator, Projection, Sorter, Limit, LogicalFilter
from catalog import Catalog

def _get_mock_page_generator():
    class MockPage:
        def __init__(self, rows):
            self.rows = rows
    # Mock data: (id, name, age, city, salary) -> Indices: 0, 1, 2, 3, 4
    mock_data = [
        MockPage([(1, 'Alice', 30, 'NY', 60000)]),
        MockPage([(2, 'Bob', 22, 'SF', 45000)]),
        MockPage([(3, 'Charlie', 25, 'NY', 55000)]),
        MockPage([(4, 'Dave', 40, 'LA', 70000)]),
        MockPage([(5, 'Eve', 19, 'BOS', 30000)]),
    ]
    for page in mock_data:
        yield page


class QueryPlanner:
    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    def plan_query(self, ast_root: ASTNode):
        """
        Main entry point for planning a SelectStatement AST.
        Returns the root of the executable Query Plan (Projection).
        """
        if not isinstance(ast_root, SelectStatement):
            raise TypeError("Planner only accepts SelectStatement AST root.")

        table_name = ast_root.table
        
        # Assuming ast_root is a SelectStatement with new attributes
        table_name = ast_root.table
        base_iterator = BaseIterator(table_name, _get_mock_page_generator())
        current_plan_root = base_iterator
        
        # 1. Plan WHERE clause (Filter)
        if ast_root.where_clause:
            filter_operator = self._plan_filter_clause(ast_root.where_clause, base_iterator, table_name)
            current_plan_root = filter_operator
            
        # 2. Plan ORDER BY clause (Sorter)
        if hasattr(ast_root, 'order_by_clause') and ast_root.order_by_clause:
            sort_keys = self._get_sort_keys(ast_root.order_by_clause, table_name)
            
            # The Sorter's parent is the current root (Filter or BaseIterator)
            sorter_operator = Sorter(sort_keys, current_plan_root)
            current_plan_root = sorter_operator

        # 3. Plan LIMIT clause (Limit)
        if hasattr(ast_root, 'limit_clause') and ast_root.limit_clause:
            limit_count = ast_root.limit_clause.count
            
            # The Limit's parent is the current root (Sorter or Filter/BaseIterator)
            limit_operator = Limit(limit_count, current_plan_root)
            current_plan_root = limit_operator

        # 4. Plan SELECT clause (Projection)
        column_indices = self._get_projection_indices(ast_root.columns, table_name)
        projection_operator = Projection(column_indices, current_plan_root)
        
        return projection_operator
        
    def _get_sort_keys(self, order_by_clause, table_name):
        """Converts OrderBy AST nodes into a list of (index, direction) tuples."""
        sort_keys = []
        for item in order_by_clause.sort_items:
            # We must resolve the column index
            index = self.catalog.get_column_index(table_name, item.column.name)
            sort_keys.append((index, item.direction))
        return sort_keys

    
    def _plan_filter_clause(self, ast_node: ASTNode, parent_iterator, table_name):
        """Recursively plans the logical and comparison expressions."""
        
        if isinstance(ast_node, Comparison):
            # Create a Filter operator
            return self._plan_comparison(ast_node, parent_iterator, table_name)

        elif isinstance(ast_node, LogicalExpression):
            # Recursively plan the children
            left_plan = self._plan_filter_clause(ast_node.left, parent_iterator, table_name)
            right_plan = self._plan_filter_clause(ast_node.right, parent_iterator, table_name)
            
            # Create the logical operator (AND or OR)
            return LogicalFilter(
                op=ast_node.op, 
                left_child=left_plan, 
                right_child=right_plan, 
                parent=parent_iterator # The logical node needs the base source
            )
            
        else:
            raise TypeError(f"Unknown AST node type in WHERE clause: {type(ast_node)}")


    def _plan_comparison(self, ast_node: Comparison, parent_iterator, table_name):
        """Converts a Comparison AST node into a Filter operator."""
        
        # Helper to convert AST operand to (value, column_index) tuple
        def resolve_operand(operand_node):
            if isinstance(operand_node, Literal):
                return (operand_node.value, None) # Literal value, no index
            elif isinstance(operand_node, ColumnRef):
                # Look up the column index using the catalog
                index = self.catalog.get_column_index(table_name, operand_node.name)
                return (None, index) # No value, index provided
            raise TypeError(f"Invalid operand type: {type(operand_node)}")

        val1, idx1 = resolve_operand(ast_node.left)
        val2, idx2 = resolve_operand(ast_node.right)

        # Optimization Note: For a real DBMS, we would check if this Filter 
        # can be replaced by an IndexScan or pushed down, but here we just create the Filter.
        return Filter(
            comparison=ast_node.op,
            val1=val1,
            val2=val2,
            col_idx1=idx1,
            col_idx2=idx2,
            parent=parent_iterator # The filter needs the base source
        )

    def _get_projection_indices(self, column_nodes, table_name):
        """Converts ColumnRef/AggregateCall nodes into a list of indices."""
        indices = []
        for node in column_nodes:
            if isinstance(node, ColumnRef):
                if node.name == '*':
                    return ['*'] # Special case for SELECT *
                
                indices.append(self.catalog.get_column_index(table_name, node.name))
            
            elif isinstance(node, AggregateCall):
                # For now, we only support aggregates on the entire result set,
                # and just return the argument index for a simple pass-through.
                if node.argument == '*':
                     indices.append('*')
                else:
                    indices.append(self.catalog.get_column_index(table_name, node.argument))
            
            # In a full system, a separate Aggregate Operator would be inserted 
            # *before* the Projection. For simplicity here, we only handle simple column selection.
        return indices

    def execute_plan(self, plan_handle):
        for row in plan_handle.next():
            print(row)

# class QueryPlanner:
#     def __init__(self, buffer_manager: BufferManager, table_collection):
#         self.buffer_manager = buffer_manager
#         self.table_collection = table_collection
#
#     def create_plan(self, structured_query):
#         table_info = self.table_collection[structured_query['table']]
#         page_info_list = [page_info for page_info in table_info.page_info]
#         iterator = Iterator(self.buffer_manager.get_pages(page_info_list))
#
#         if 'where' in structured_query:
#
#
#             fil = Filter(
#                 comparison=structured_query['where']['operator'],
#                 val1=structured_query['where']['slot1']['value'] if structured_query['where']['slot1']['type'] == 'value' else None,
#                 col_idx1=table_info.column_names.index(structured_query['where']['slot1']['value']) if structured_query['where']['slot1']['type'] == 'column' else None,
#                 val2=structured_query['where']['slot2']['value'] if structured_query['where']['slot2']['type'] == 'value' else None,
#                 col_idx2=table_info.column_names.index(structured_query['where']['slot2']['value']) if structured_query['where']['slot2']['type'] == 'column' else None,
#                 parent=iterator
#             )
#
#         handle = fil
#
#         return handle
#     def execute_plan(self, plan_handle):
#         for row in plan_handle.next():
#             print(row)
