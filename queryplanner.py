from syntax_tree import ASTNode, SelectStatement, LogicalExpression, Comparison, Literal, ColumnRef, AggregateCall
from operators import Filter, BaseIterator, Projection, Sorter, Limit, LogicalFilter, Aggregate, Distinct
from catalog import Catalog


def generate_output_column_names(ast_columns: list, table_name: str, catalog) -> list[str]:
    """
    Translates the list of AST nodes from the SELECT clause into a list of 
    display column names, handling SELECT * using the Catalog.
    """
    
    # --- Check for the SELECT * case ---
    if len(ast_columns) == 1 and isinstance(ast_columns[0], ColumnRef) and ast_columns[0].name == '*':
        # Use the Catalog to get the expanded list of names
        return catalog.get_all_column_names(table_name)
    
    names = []
    for item in ast_columns:
        if isinstance(item, ColumnRef):
            names.append(item.name.upper())
            
        elif isinstance(item, AggregateCall):
            func = item.function_name
            arg = item.argument
            names.append(f"{func}({arg})")
            
        elif isinstance(item, Literal):
            names.append(str(item.value).upper())
            
        else:
            names.append("EXPR")
            
    return names



class QueryPlanner:
    def __init__(self, catalog: Catalog, buffer_manager):
        self.catalog = catalog
        self.buffer_manager = buffer_manager # Stored as the Buffer Manager

    def plan_query(self, ast_root: ASTNode):
        """
        The main dispatch method for all AST nodes.
        Delegates the planning based on the specific type of the AST root.
        """
        if isinstance(ast_root, SelectStatement):
            return self._plan_select_statement(ast_root)
        
        # Add future planners here:
        # if isinstance(ast_root, CreateStatement):
        #     return self._plan_create_statement(ast_root)
        # if isinstance(ast_root, DeleteStatement):
        #     return self._plan_delete_statement(ast_root)

        raise TypeError(f"Unsupported AST root type for planning: {type(ast_root)}")

    def _plan_select_statement(self, ast_root: SelectStatement):
        """
        Plans a SelectStatement AST. This replaces the old plan_query function.
        """
        table_name = ast_root.table

        data_generator = self.buffer_manager.get_data_generator(table_name)
        base_iterator = BaseIterator(table_name, data_generator, self.catalog)
        current_plan_root = base_iterator
        
        # --- 1. Filter (No more hasattr) ---
        if ast_root.where_clause:
            filter_operator = self._plan_filter_clause(ast_root.where_clause, base_iterator, table_name)
            current_plan_root = filter_operator

        # --- 2. Aggregate ---
        aggregate_specs = self._get_aggregate_specs(ast_root.columns, table_name)
        group_key_ast_nodes = ast_root.group_by_clause.columns if ast_root.group_by_clause else []
        is_aggregation_present = (ast_root.group_by_clause and ast_root.group_by_clause.columns) or aggregate_specs
        
        if is_aggregation_present:
            group_key_indices = self._get_group_key_indices(ast_root.group_by_clause, table_name) if ast_root.group_by_clause else []
            
            # 1. Calculate the names the Aggregate operator WILL output.
            # Note: We must call a helper that orders names as [GROUP_KEYS | AGGREGATES]
            aggregate_output_names = self._generate_aggregate_output_names(
                ast_columns=ast_root.columns, 
                group_key_ast_nodes=group_key_ast_nodes,
                table_name=table_name
            )
            aggregate_operator = Aggregate(group_key_indices, aggregate_specs, current_plan_root, aggregate_output_names)
            current_plan_root = aggregate_operator

        # --- 3. Pre-Projection and Distinct (No more hasattr) ---
        if ast_root.is_distinct:
            
            # A. Calculate the indices and names for the PRE-PROJECTION.
            if is_aggregation_present:
                pre_projection_indices = self._get_final_projection_indices(
                    ast_root.columns, group_key_ast_nodes, table_name
                )
            else:
                pre_projection_indices = self._get_projection_indices(ast_root.columns, table_name)
                
            pre_projection_names = generate_output_column_names(
                ast_columns=ast_root.columns,
                table_name=table_name,
                catalog=self.catalog
            )
            
            # B. Insert the Pre-Projection operator.
            pre_projection_operator = Projection(
                column_indices=pre_projection_indices,
                column_names=pre_projection_names, 
                parent=current_plan_root
            )
            current_plan_root = pre_projection_operator
            
            # C. Insert the Distinct operator. 
            distinct_indices = list(range(len(pre_projection_names)))
            distinct_operator = Distinct(distinct_indices, current_plan_root) 
            current_plan_root = distinct_operator


        if ast_root.order_by_clause:

            if ast_root.is_distinct:
                # Sort against pre-projection output
                output_names = generate_output_column_names(
                    ast_columns=ast_root.columns,
                    table_name=table_name,
                    catalog=self.catalog
                )

                sort_keys = self._get_trivial_sort_keys(
                    ast_root.order_by_clause,
                    output_names
                )

            elif is_aggregation_present:
                aggregate_output_names = self._generate_aggregate_output_names(
                    ast_root.columns,
                    group_key_ast_nodes,
                    table_name
                )

                sort_keys = self._get_sort_keys_for_aggregate(
                    ast_root.order_by_clause,
                    aggregate_output_names
                )

            else:
                # Normal non-distinct, non-aggregate case
                sort_keys = self._get_sort_keys(
                    ast_root.order_by_clause,
                    table_name
                )

            sorter_operator = Sorter(sort_keys, current_plan_root)
            current_plan_root = sorter_operator
        if ast_root.limit_clause:
            limit_count = ast_root.limit_clause.count
            limit_operator = Limit(limit_count, current_plan_root)
            current_plan_root = limit_operator

        output_names = generate_output_column_names(
            ast_columns=ast_root.columns,
            table_name=table_name,
            catalog=self.catalog
        )

        if ast_root.is_distinct:
            # Indices are trivial (0, 1, 2, ...) because Pre-Projection handled the mapping.
            final_projection_indices = list(range(len(output_names)))
        elif is_aggregation_present:
            # Recalculate based on Aggregate output structure
            final_projection_indices = self._get_final_projection_indices(
                ast_root.columns, 
                group_key_ast_nodes,
                table_name
            )
        else:
            # Standard non-aggregate/non-distinct projection
            final_projection_indices = self._get_projection_indices(ast_root.columns, table_name)
            
        projection_operator = Projection(
            column_indices=final_projection_indices, 
            column_names=output_names, 
            parent=current_plan_root
        )
        
        return projection_operator


    def _get_trivial_sort_keys(self, order_by_clause, output_column_names):
        """
        Resolves ORDER BY columns against the current projected schema
        (used after DISTINCT / Pre-Projection).
        """
        sort_keys = []

        # Map output column names to their indices
        name_to_index = {name.upper(): i for i, name in enumerate(output_column_names)}

        for item in order_by_clause.sort_items:
            # Resolve column name
            if isinstance(item.column, AggregateCall):
                sort_column_name = self._generate_aggregate_column_name(item.column)
            elif isinstance(item.column, ColumnRef):
                sort_column_name = item.column.name.upper()
            else:
                raise TypeError(
                    f"Unsupported ORDER BY column type: {type(item.column)}"
                )

            if sort_column_name not in name_to_index:
                raise ValueError(
                    f"ORDER BY column '{sort_column_name}' not found in SELECT output"
                )

            index = name_to_index[sort_column_name]
            is_desc = item.direction == 'DESC'
            sort_keys.append((index, is_desc))

        return sort_keys


    def _get_group_key_indices(self, group_by_clause, table_name):
        """Resolves ColumnRef nodes in GROUP BY to indices."""
        indices = []
        for col_ref in group_by_clause.columns:
            indices.append(self.catalog.get_column_index(table_name, col_ref.name))
        return indices

    def _get_aggregate_specs(self, column_nodes, table_name):
        """Extracts (function_name, argument_index) tuples from SELECT list."""
        specs = []
        for node in column_nodes:
            if isinstance(node, AggregateCall):
                func_name = node.function_name  # MAX, COUNT
                arg = node.argument  # column name or literal or *
                
                if arg == '*':
                    arg_index = '*'
                else:
                    arg_index = self.catalog.get_column_index(table_name, arg)
                
                specs.append((func_name, arg_index))
        return specs
        
    def _get_final_projection_indices(self, column_nodes, group_key_ast_nodes, table_name):
        """
        Maps the requested SELECT columns to the final Aggregate operator's output indices.
        Output row structure: [Group Keys | Aggregate Results]
        """
        
        # NOTE: The parameter name changed from 'group_key_indices' (list of integers) 
        # to 'group_key_ast_nodes' (list of ColumnRef objects) for clarity in this function.
        # This requires adjusting the plan_query call above.

        # Case 1: Global Aggregation (No GROUP BY)
        if not group_key_ast_nodes:
            # The Nth aggregate is at output index N-1
            return list(range(len(column_nodes)))
        
        # --- Case 2: Grouped Aggregation ---
        final_indices = []
        
        # 1. Create a map for Group Key output indices
        num_group_keys = len(group_key_ast_nodes)
        group_key_output_map = {}
        for i, col_ref in enumerate(group_key_ast_nodes):
            # Map the actual column name to its new index in the Aggregate output (0, 1, 2, ...)
            group_key_output_map[col_ref.name] = i 

        # 2. Iterate SELECT list and determine final projection indices
        agg_counter = 0 # Tracks how many AggregateCalls have been processed
        
        for node in column_nodes:
            if isinstance(node, ColumnRef):
                # ColumnRef: Must be a column that appeared in the GROUP BY clause.
                col_name = node.name
                
                if col_name in group_key_output_map:
                    # The index is its position in the group key output
                    final_indices.append(group_key_output_map[col_name])
                else:
                    # This is the SQL error state: selecting a non-grouped, non-aggregated column
                    raise SyntaxError(
                        f"Column '{col_name}' must appear in the GROUP BY clause or be used in an aggregate function."
                    )
            
            elif isinstance(node, AggregateCall):
                # AggregateCall: Index starts after all group keys
                agg_output_index = num_group_keys + agg_counter
                
                final_indices.append(agg_output_index)
                agg_counter += 1
                
        return final_indices

    def _get_sort_keys(self, order_by_clause, table_name):
        """Converts OrderBy AST nodes into a list of (index, direction) tuples 
           relative to the full table schema."""
        sort_keys = []
        for item in order_by_clause.sort_items:
            
            # 1. Get the column index from the Catalog (e.g., age -> 2, name -> 1)
            index = self.catalog.get_column_index(table_name, item.column.name)
            
            # 2. Determine direction
            is_descending = item.direction == 'DESC'
            
            sort_keys.append((index, is_descending))
        
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
            parent=parent_iterator,
            val1=val1,
            val2=val2,
            col_idx1=idx1,
            col_idx2=idx2
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

    def _generate_aggregate_output_names(self, ast_columns, group_key_ast_nodes, table_name):
        """
        Generates the column names for the Aggregate operator's output schema, 
        ensuring the order is [GROUP_KEYS] followed by [AGGREGATES].
        """
        # 1. Get the names for the GROUP BY keys (order matters)
        group_key_names = generate_output_column_names(
            ast_columns=group_key_ast_nodes,
            table_name=table_name,
            catalog=self.catalog
        )
        
        # 2. Get the names for all items in the SELECT list (aggregates and group keys)
        select_list_names = generate_output_column_names(
            ast_columns=ast_columns,
            table_name=table_name,
            catalog=self.catalog
        )

        # 3. Separate the SELECT list into only the aggregate names
        group_names_set = set(group_key_names)
        aggregate_names_only = [name for name in select_list_names if name not in group_names_set]

        # Combine: Aggregate schema is always [GROUP KEYS] + [AGGREGATES]
        return group_key_names + aggregate_names_only

    def _generate_aggregate_column_name(self, agg_call: AggregateCall) -> str:
        """Generates the canonical column name for an AggregateCall (e.g., 'COUNT(*)', 'SUM(salary)')."""
        
        if isinstance(agg_call.argument, str):
            # Handles COUNT(*) or SUM(id) (if ID is passed as string)
            argument_name = agg_call.argument
        elif hasattr(agg_call.argument, 'name'):
            # Handles SUM(ColumnRef.name)
            argument_name = agg_call.argument.name
        else:
            # Fallback for complex arguments (shouldn't happen with current AST)
            argument_name = str(agg_call.argument)

        return f"{agg_call.function_name.upper()}({argument_name.upper()})"


    def _get_sort_keys_for_aggregate(self, order_by_clause, aggregate_output_names):
        """Converts ORDER BY items to indices based on the Aggregate output schema."""
        sort_keys = []
        name_to_index = {name: i for i, name in enumerate(aggregate_output_names)}

        for item in order_by_clause.sort_items:
            
            # Resolve name from ColumnRef (e.g., city) or AggregateCall (e.g., COUNT(*))
            sort_column_ast = item.column
            if isinstance(sort_column_ast, AggregateCall):
                sort_column_name = self._generate_aggregate_column_name(sort_column_ast)
            elif isinstance(sort_column_ast, ColumnRef):
                sort_column_name = sort_column_ast.name.upper()
            else:
                # Use TypeError for an unexpected AST type
                raise TypeError(f"Unsupported sort column type encountered during planning: {type(sort_column_ast)}")
            
            # FIX: Use ValueError instead of the undefined PlanningError
            if sort_column_name not in name_to_index:
                raise ValueError(
                    f"Planning Error: Sort key '{sort_column_name}' not found in the aggregated output schema. "
                    "Ensure the sort column is either a GROUP BY key or an aggregate function."
                )

            index = name_to_index[sort_column_name]
            direction = item.direction == 'DESC'
            sort_keys.append((index, direction))
            
        return sort_keys

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
