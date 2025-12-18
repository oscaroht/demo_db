import abc
from typing import Generator, List, Any
from functools import cmp_to_key

Row = tuple[Any, ...]

class Operator(abc.ABC):
    """Abstract Base Class for all relational operators."""

    @abc.abstractmethod
    def next(self) -> Generator[Row, None, None]:
        raise NotImplementedError

    @abc.abstractmethod
    def display_plan(self, indent: int = 0) -> str:
        raise NotImplementedError
        
    @abc.abstractmethod
    def get_output_schema_names(self) -> List[str]:
        """Returns the column names that this operator outputs."""
        raise NotImplementedError

class Predicate(abc.ABC):
    @abc.abstractmethod
    def evaluate(self, row) -> bool:
        pass

class BaseIterator(Operator):
    def __init__(self, table_name, data_generator, catalog):
        self.table_name = table_name
        self.data_generator = data_generator
        self.catalog = catalog
    
    def next(self):
        """Yields all rows from the table source."""
        for row in self.data_generator():
            yield row

    def display_plan(self, level=0) -> str:
        indent = '  ' * level
        return f"{indent}* TableScan (Source: {self.table_name})"
    
    def get_output_schema_names(self) -> List[str]:
        # The schema is the list of all column names in the source table.
        # This assumes your Catalog has a method to retrieve column names.
        return self.catalog.get_all_column_names(self.table_name)

comparison_operators = {
    '=': lambda x, y: x == y,
    '!=': lambda x, y: x != y,
    '>': lambda x, y: x > y,
    '<': lambda x, y: x < y,
    '>=': lambda x, y: x >= y,
    '<=': lambda x, y: x <= y
}

logical_operators = {
    'AND': lambda x, y: x and y,
    'OR': lambda x, y: x or y
}

class LogicalPredicate(Predicate):
    def __init__(self, op: str, left: Predicate, right: Predicate):
        self.func = logical_operators[op]
        self.left = left
        self.right = right

    def evaluate(self, row):
        if self.func is logical_operators['AND']:
            return self.left.evaluate(row) and self.right.evaluate(row)
        return self.left.evaluate(row) or self.right.evaluate(row)
    def __str__(self):
        return f"({self.left} {self.func.__name__.upper()} {self.right})"


class ComparisonPredicate(Predicate):
    def __init__(self, comparison: str, val1: Any, val2: Any, col_idx1: None | int, col_idx2: None | int):
        self.comparison_function = comparison_operators[comparison]
        self.val1, self.val2 = val1, val2
        self.col_idx1, self.col_idx2 = col_idx1, col_idx2

    def evaluate(self, row):
        """Evaluates the condition for a single row based on indices/values."""
        x = self.val1 if self.col_idx1 is None else row[self.col_idx1]
        y = self.val2 if self.col_idx2 is None else row[self.col_idx2]
        return self.comparison_function(x, y)

    def __str__(self):
        left = f"Col[{self.col_idx1}]" if self.col_idx1 is not None else f"Lit[{self.val1!r}]"
        right = f"Col[{self.col_idx2}]" if self.col_idx2 is not None else f"Lit[{self.val2!r}]"
        return f"{left} {self.comparison_function.__name__} {right}"
        
class Filter(Operator):
    def __init__(self, predicate: Predicate, parent: Operator):
        self.predicate = predicate
        self.parent = parent

    def next(self):
        for row in self.parent.next():
            if self.predicate.evaluate(row):
                yield row

    def display_plan(self, level=0):
        indent = '  ' * level
        output = [f"{indent}* Filter (Condition: {self.predicate})"]
        output.append(self.parent.display_plan(level + 1))
        return '\n'.join(output)

    def get_output_schema_names(self) -> List[str]:
        return self.parent.get_output_schema_names()

#
#
# class Filter(Operator):
#     def __init__(self, comparison: str, parent: Operator, val1=None, val2=None, col_idx1=None, col_idx2=None):
#         self.comparison_function = comparison_operators[comparison]
#         self.val1, self.val2 = val1, val2
#         self.col_idx1, self.col_idx2 = col_idx1, col_idx2
#         self.parent = parent
#
#     def check(self, row):
#         """Evaluates the condition for a single row based on indices/values."""
#         x = self.val1 if self.col_idx1 is None else row[self.col_idx1]
#         y = self.val2 if self.col_idx2 is None else row[self.col_idx2]
#         return self.comparison_function(x, y)
#
#     def next(self):
#         for row in self.parent.next():
#             if self.check(row):
#                 yield row
#
#     def display_plan(self, level=0) -> str:
#         indent = '  ' * level
#
#         left_op = f"Col[{self.col_idx1}]" if self.col_idx1 is not None else f"Lit[{self.val1!r}]"
#         right_op = f"Col[{self.col_idx2}]" if self.col_idx2 is not None else f"Lit[{self.val2!r}]"
#
#         output = [f"{indent}* Filter (Condition: {left_op} {self.comparison_function.__name__} {right_op})"]
#         if self.parent:
#             output.append(self.parent.display_plan(level + 1))
#
#         return '\n'.join(output)
#
#     def get_output_schema_names(self) -> List[str]:
#         return self.parent.get_output_schema_names()
#
class LogicalFilter(Operator):
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
    def get_output_schema_names(self) -> List[str]:
        return self.parent.get_output_schema_names()

class Projection(Operator):
    def __init__(self, column_indices: List[int], column_names, parent: Operator):
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

    def get_output_schema_names(self) -> List[str]:
        # The Projection operator *is* the final output schema.
        return self.column_names

class Sorter(Operator):
    def __init__(self, sort_keys, parent):
        self.sort_keys = sort_keys  # List of (column_index, direction)
        self.parent = parent
        
    def next(self):
        all_rows = list(self.parent.next())
        
        if not all_rows:
            return

        def compare_rows(row_a, row_b):
            """
            Compares two rows based on all defined sort keys.
            Returns:
              -1 if row_a comes before row_b
               1 if row_b comes before row_a
               0 if rows are equal according to the sort keys
            """
            for index, is_descending in self.sort_keys:
                val_a = row_a[index]
                val_b = row_b[index]

                if val_a < val_b:
                    return -1 if not is_descending else 1
                
                if val_a > val_b:
                    return 1 if not is_descending else -1
                
                
            return 0 # All sort keys were equal

        sorted_rows = sorted(
            all_rows,
            key=cmp_to_key(compare_rows)
        )
        
        # 4. Yield the sorted results
        yield from sorted_rows
    def display_plan(self, level=0):
        indent = '  ' * level
        sort_items_str = ', '.join([f"Col[{idx}] {direction}" for idx, direction in self.sort_keys])
        output = [f"{indent}* Sorter (Keys: {sort_items_str})"]
        output.append(self.parent.display_plan(level + 1))
        return '\n'.join(output)
    def get_output_schema_names(self) -> List[str]:
        return self.parent.get_output_schema_names()

class Distinct(Operator): # Inherits from the ABC
    def __init__(self, column_indices: list[int], parent: Operator):
        self.column_indices = column_indices
        self.parent: Operator = parent
        self.unique_keys = set() 

    def next(self) -> Generator[Row, None, None]:
        for row in self.parent.next():
            key = tuple([row[i] for i in self.column_indices])
            
            if key in self.unique_keys:
                continue
            print(f"Unique key {key}") 
            self.unique_keys.add(key)
            yield row
            
    def display_plan(self, indent: int = 0) -> str:
        prefix = "  " * indent
        output = f"{prefix}Distinct Operator (on columns: {self.column_indices})\n"
        output += self.parent.display_plan(indent + 1)
        return output

    def get_output_schema_names(self) -> List[str]:
        return self.parent.get_output_schema_names()

class Limit(Operator):
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

    def get_output_schema_names(self) -> List[str]:
        return self.parent.get_output_schema_names()

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
        if value is not None: 
            self.result += 1

class MaxState(AggregationState):
    def _get_initial_value(self): return None
    def update(self, value):
        if value is not None and (self.result is None or value > self.result):
            self.result = value

class MinState(AggregationState):
    def _get_initial_value(self): return None
    def update(self, value):
        if value is not None and (self.result is None or value < self.result):
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

class CountDistinctState(AggregationState):
    def _get_initial_value(self):
        return set()
    def update(self, value):
        if value is not None:
            self.result.add(value)
    def finalize(self):
        return len(self.result)


# Mapping from function name (from AST) to the new State class
AGGREGATE_MAP = {
    'SUM': SumState,
    'COUNT': CountState,
    'COUNT DISTINCT': CountDistinctState,
    'MAX': MaxState,
    'MIN': MinState,
    'AVG': AvgState,
}

class Aggregate:
    def __init__(self, group_key_indices, aggregate_specs, parent):
        self.group_key_indices = group_key_indices
        self.aggregate_specs = aggregate_specs # [(func_name, arg_index, is_distinct), ...]
        self.parent = parent
        parent_schema = parent.get_output_schema_names()
        group_names = [parent_schema[i] for i in group_key_indices]
        agg_names = []
        for func, arg, is_distinct in aggregate_specs:
            dist = 'DISTINCT ' if is_distinct else ''
            if arg == '*':
                agg_names.append(f"{func}({dist}*)")
            else:
                agg_names.append(f"{func}({dist}{parent_schema[arg]})")

        self.output_names = group_names + agg_names

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
                for func, arg_index, is_distinct in self.aggregate_specs:
                    if is_distinct:
                        func += ' DISTINCT'
                    StateClass = AGGREGATE_MAP[func]
                    state_objects.append(StateClass())
                grouped_states[group_key] = state_objects

            # Retrieve the list of state objects for the current group
            current_states = grouped_states[group_key]

            # Update each state object
            for i, (func, arg_index, is_distinct) in enumerate(self.aggregate_specs):
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
            for func, idx, is_distinct in self.aggregate_specs
        ])
        
        output = [f"{indent}* Aggregate {group_by_line}"]
        output.append(f"{indent}  Aggregates: {agg_specs_str}")
        
        # 3. Recursively call display_plan on the parent operator
        output.append(self.parent.display_plan(level + 1))
        
        return '\n'.join(output)

    def get_output_schema_names(self) -> List[str]:
        return self.output_names

if __name__ == '__main__':
    pass
