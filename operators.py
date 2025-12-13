
operators = {
    '=': lambda x, y: x == y,
    '!=': lambda x, y: x != y,
    '>': lambda x, y: x > y,
    '<': lambda x, y: x < y,
    '>=': lambda x, y: x >= y,
    '<=': lambda x, y: x <= y
}

class BaseIterator:
    def __init__(self, table_name, page_generator):
        self.table_name = table_name
        self.page_generator = page_generator
    
    def next(self):
        """Yields all rows from the table source."""
        for page in self.page_generator:
            for row in page.rows:
                yield row

    def display_plan(self, level=0):
        indent = '  ' * level
        return f"{indent}* TableScan (Source: {self.table_name})"


class Filter:
    def __init__(self, comparison: str, val1=None, val2=None, col_idx1=None, col_idx2=None, parent=None):
        self.comparison_function = operators[comparison]
        self.val1, self.val2 = val1, val2
        self.col_idx1, self.col_idx2 = col_idx1, col_idx2
        self.parent = parent

    def check(self, row):
        """Evaluates the condition for a single row based on indices/values."""
        x = self.val1 if self.col_idx1 is None else row[self.col_idx1]
        y = self.val2 if self.col_idx2 is None else row[self.col_idx2]
        return self.comparison_function(x, y)

    def next(self):
        for row in self.parent.next():
            if self.check(row):
                yield row
    
    def display_plan(self, level=0):
        indent = '  ' * level
        
        left_op = f"Col[{self.col_idx1}]" if self.col_idx1 is not None else f"Lit[{self.val1!r}]"
        right_op = f"Col[{self.col_idx2}]" if self.col_idx2 is not None else f"Lit[{self.val2!r}]"
        
        output = [f"{indent}* Filter (Condition: {left_op} {self.comparison_function.__name__} {right_op})"]
        if self.parent:
            output.append(self.parent.display_plan(level + 1))
            
        return '\n'.join(output)

class LogicalFilter:
    def __init__(self, op, left_child, right_child, parent):
        self.op = op
        self.left = left_child
        self.right = right_child
        self.parent = parent

    def check(self, row):
        left_result = self.left.check(row)
        right_result = self.right.check(row)
        
        if self.op == 'AND':
            return left_result and right_result
        elif self.op == 'OR':
            return left_result or right_result
        return False # Should not happen

    def next(self):
        """The logical operator iterates over the parent's result and applies the check."""
        for row in self.parent.next():
            if self.check(row):
                yield row

    def display_plan(self, level=0):
        indent = '  ' * level
        
        output = [f"{indent}* LogicalFilter ({self.op})"]
        
        output.append(f"{indent}  Left Branch:")
        output.append(self.left.display_plan(level + 2))
        
        output.append(f"{indent}  Right Branch:")
        output.append(self.right.display_plan(level + 2))
        
        if self.parent:
            output.append(self.parent.display_plan(level + 1)) 
            
        return '\n'.join(output)

class Projection:
    def __init__(self, column_indices, column_names, parent):
        self.column_indices = column_indices # List of indices to keep
        self.column_names = column_names
        self.parent = parent

    def next(self):
        for row in self.parent.next():
            # If the Projection is at the root, it handles printing/yielding the final result
            if '*' in self.column_indices:
                yield row
            else:
                yield tuple(row[i] for i in self.column_indices)
    
    def display_plan(self, level=0):
        indent = '  ' * level
        
        indices_str = self.column_indices 
        output = [f"{indent}* Projection (Columns: {indices_str})"]
        
        if self.parent:
            output.append(self.parent.display_plan(level + 1))
            
        return '\n'.join(output)

class Sorter:
    def __init__(self, sort_keys, parent):
        self.sort_keys = sort_keys  # List of (column_index, direction)
        self.parent = parent
        
    def next(self):
        all_rows = list(self.parent.next())
        
        sort_params = [(idx, direction == 'DESC') for idx, direction in self.sort_keys]
        
        primary_key_index = sort_params[0][0]
        reverse_flag = sort_params[0][1]

        sorted_rows = sorted(
            all_rows,
            key=lambda row: row[primary_key_index],
            reverse=reverse_flag
        )
        
        for row in sorted_rows:
            yield row

    def display_plan(self, level=0):
        indent = '  ' * level
        sort_items_str = ', '.join([f"Col[{idx}] {direction}" for idx, direction in self.sort_keys])
        output = [f"{indent}* Sorter (Keys: {sort_items_str})"]
        output.append(self.parent.display_plan(level + 1))
        return '\n'.join(output)


class Limit:
    def __init__(self, count, parent):
        self.count = count # Integer limit
        self.parent = parent
        
    def next(self):
        rows_yielded = 0
        for row in self.parent.next():
            if rows_yielded < self.count:
                yield row
                rows_yielded += 1
            else:
                return
                
    def display_plan(self, level=0):
        indent = '  ' * level
        output = [f"{indent}* Limit (Count: {self.count})"]
        output.append(self.parent.display_plan(level + 1))
        return '\n'.join(output)

#
# # Performance of this Aggregate will be horible for mem performance.
# # This is a DBMS, we cannot put the entire table in mem.
# AGG_FUNCTIONS = {
#     'COUNT': lambda values: len(values),
#     'MIN': lambda values: min(values) if values else None,
#     'MAX': lambda values: max(values) if values else None,
#     'AVG': lambda values: sum(values) / len(values) if values else None,
# }
#
# class Aggregate:
#     def __init__(self, group_key_indices, aggregate_specs, parent):
#         self.group_key_indices = group_key_indices  # [index1, index2, ...]
#         self.aggregate_specs = aggregate_specs      # [(func_name, arg_index), ...]
#         self.parent = parent
#
#     def next(self):
#         grouped_data = {}
#
#         for row in self.parent.next():
#             group_key = tuple(row[i] for i in self.group_key_indices)
#
#             if group_key not in grouped_data:
#                 grouped_data[group_key] = []
#
#             grouped_data[group_key].append(row)
#
#         for group_key, rows in grouped_data.items():
#             result_row = list(group_key)
#             for func_name, arg_index in self.aggregate_specs:
#                 if arg_index == '*': # Special case for COUNT(*)
#                     aggregate_values = rows
#                 else:
#                     aggregate_values = [row[arg_index] for row in rows]
#
#                 agg_result = AGG_FUNCTIONS[func_name](aggregate_values)
#                 result_row.append(agg_result)
#             yield tuple(result_row)
#
#     def display_plan(self, level=0):
#         indent = '  ' * level
#         group_keys_str = ', '.join([f"Col[{idx}]" for idx in self.group_key_indices])
#
#         agg_specs_str = ', '.join([
#             f"{func}(Col[{idx}])" if idx != '*' else f"{func}(*)" 
#             for func, idx in self.aggregate_specs
#         ])
#
#         output = [f"{indent}* Aggregate (GROUP BY: {group_keys_str})"]
#         output.append(f"{indent}  Aggregates: {agg_specs_str}")
#         output.append(self.parent.display_plan(level + 1))
#         return '\n'.join(output)
class AggregationState:
    """Base class defining the interface for all aggregate functions."""
    def __init__(self):
        self.result = self._get_initial_value()
        
    def _get_initial_value(self):
        """Returns the starting value for the aggregate."""
        raise NotImplementedError

    def update(self, value):
        """Updates the running state with a new value."""
        raise NotImplementedError
        
    def finalize(self):
        """Calculates the final result (needed for AVG)."""
        return self.result

class SumState(AggregationState):
    def _get_initial_value(self): return 0
    def update(self, value):
        if value is not None:
            self.result += value

class CountState(AggregationState):
    def _get_initial_value(self): return 0
    def update(self, value):
        # COUNT(*) (value is always a row) or COUNT(col) (value is non-None)
        if value is not None: 
            self.result += 1

class MaxState(AggregationState):
    def _get_initial_value(self): return None
    def update(self, value):
        if value is not None:
            if self.result is None or value > self.result:
                self.result = value

class MinState(AggregationState):
    def _get_initial_value(self): return None
    def update(self, value):
        if value is not None:
            if self.result is None or value < self.result:
                self.result = value
                
class AvgState(AggregationState):
    def __init__(self):
        self.sum_state = SumState()
        self.count_state = CountState()

    def _get_initial_value(self): return (0, 0) # Store (sum, count) - not used
    def update(self, value):
        self.sum_state.update(value)
        self.count_state.update(value)

    def finalize(self):
        """Final calculation: sum / count"""
        total_sum = self.sum_state.result
        total_n = self.count_state.result
        
        if total_n == 0:
            return None # Division by zero
        return total_sum / total_n



# Mapping from function name (from AST) to the new State class
AGGREGATE_MAP = {
    'SUM': SumState,
    'COUNT': CountState,
    'MAX': MaxState,
    'MIN': MinState, # Added MIN
    'AVG': AvgState,
}

class Aggregate:
    def __init__(self, group_key_indices, aggregate_specs, parent):
        self.group_key_indices = group_key_indices
        self.aggregate_specs = aggregate_specs # [(func_name, arg_index), ...]
        self.parent = parent

    def next(self):
        # Dictionary to hold the state: 
        # {group_key_tuple: [AggStateObject1, AggStateObject2, ...]}
        grouped_states = {}
        
        # 1. Iterate and Update Aggregate State Objects
        for row in self.parent.next():
            group_key = tuple(row[i] for i in self.group_key_indices)
            
            if group_key not in grouped_states:
                # Initialize a list of state objects for a new group
                state_objects = []
                for func, arg_index in self.aggregate_specs:
                    StateClass = AGGREGATE_MAP[func]
                    state_objects.append(StateClass())
                grouped_states[group_key] = state_objects

            # Retrieve the list of state objects for the current group
            current_states = grouped_states[group_key]

            # Update each state object
            for i, (func, arg_index) in enumerate(self.aggregate_specs):
                state_object = current_states[i]
                
                # Get the value to pass to the update method
                if arg_index == '*':
                    # For COUNT(*), we pass a non-None value (e.g., True) to signal a row exists
                    value = True 
                else:
                    value = row[arg_index]
                
                state_object.update(value)

        # 2. Finalize and Yield Results
        for group_key, state_objects in grouped_states.items():
            result_row = list(group_key)
            
            for state_object in state_objects:
                # Call finalize() which calculates the final result (e.g., SUM/COUNT for AVG)
                final_agg_result = state_object.finalize() 
                result_row.append(final_agg_result)

            yield tuple(result_row)

    def display_plan(self, level=0):
        indent = '  ' * level
        
        # 1. Format GROUP BY Keys
        # NOTE: This assumes you have a way (like a Catalog) to map index back to name, 
        # but for plan display, referencing the index is often sufficient if names are unknown.
        
        if self.group_key_indices:
            # Display grouping columns if they exist
            group_keys_str = ', '.join([f"Col[{idx}]" for idx in self.group_key_indices])
            group_by_line = f"(GROUP BY: {group_keys_str})"
        else:
            # This handles global aggregation (no GROUP BY)
            group_by_line = "(Global Aggregation)"

        # 2. Format Aggregates
        agg_specs_str = ', '.join([
            f"{func}(Col[{idx}])" if idx != '*' else f"{func}(*)" 
            for func, idx in self.aggregate_specs
        ])
        
        output = [f"{indent}* Aggregate {group_by_line}"]
        output.append(f"{indent}  Aggregates: {agg_specs_str}")
        
        # 3. Recursively call display_plan on the parent operator
        output.append(self.parent.display_plan(level + 1))
        
        return '\n'.join(output)

if __name__ == '__main__':
    pass
