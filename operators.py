import abc
from ast import Call
from typing import Callable, Generator, List, Any
from functools import cmp_to_key
from dataclasses import dataclass
from typing import Generator, List, Any, Optional  # <--- Added Optional here
import operator


from syntax_tree import ProjectionTarget
from schema import Schema, ColumnInfo
Row = tuple[Any, ...]


@dataclass
class AggregateSpec:
    function: str
    arg_index: int | None  # None for COUNT(*)
    is_distinct: bool
    output_name: str


class Operator(abc.ABC):
    """Abstract Base Class for all relational operators."""

    @abc.abstractmethod
    def next(self) -> Generator[Row, None, None]:
        raise NotImplementedError

    @abc.abstractmethod
    def display_plan(self, level: int = 0) -> str:
        raise NotImplementedError
        
    @abc.abstractmethod
    def get_output_schema(self) -> Schema:
        """Returns the column names that this operator outputs."""
        raise NotImplementedError

class Predicate(abc.ABC):
    @abc.abstractmethod
    def evaluate(self, row) -> bool:
        pass

comparison_operators = {
    '=': lambda x, y: x == y,
    '!=': lambda x, y: x != y,
    '>': lambda x, y: x > y,
    '<': lambda x, y: x < y,
    '>=': lambda x, y: x >= y,
    '<=': lambda x, y: x <= y,
    '+': lambda x, y: x + y
}

logical_operators = {
    'AND': lambda x, y: x and y,
    'OR': lambda x, y: x or y
}

OPERATOR_MAP = {
    '+': operator.add, '-': operator.sub, '*': operator.mul, '/': operator.truediv,
    '=': operator.eq, '!=': operator.ne, '>': operator.gt, '<': operator.lt,
    '>=': operator.ge, '<=': operator.le
}

class LogicalPredicate(Predicate):
    def __init__(self, op: str, left: Predicate, right: Predicate):
        self.op = op
        self.left = left
        self.right = right

    def evaluate(self, row):
        if self.op == 'AND':
            return self.left.evaluate(row) and self.right.evaluate(row)
        return self.left.evaluate(row) or self.right.evaluate(row)

    def __str__(self):
        return f"({self.left} {self.op} {self.right})"

class ComparisonPredicate(Predicate):
    def __init__(self, comparison: str, val1: Any, val2: Any, col_idx1: Optional[int], col_idx2: Optional[int]):
        self.comparison_function = comparison_operators[comparison]
        self.val1, self.val2 = val1, val2
        self.col_idx1, self.col_idx2 = col_idx1, col_idx2

    def evaluate(self, row):
        x = self.val1 if self.col_idx1 is None else row[self.col_idx1]
        y = self.val2 if self.col_idx2 is None else row[self.col_idx2]
        return self.comparison_function(x, y)

    def __str__(self):
        left = f"Col[{self.col_idx1}]" if self.col_idx1 is not None else f"'{self.val1}'"
        right = f"Col[{self.col_idx2}]" if self.col_idx2 is not None else f"'{self.val2}'"
        return f"{left} OP {right}"
        
class ScanOperator(Operator):
    def __init__(self, table_name: str, data_generator, schema: Schema):
        self.table_name = table_name
        self.data_generator = data_generator
        self.schema = schema

    def next(self):
        yield from self.data_generator()

    def get_output_schema(self) -> Schema:
        return self.schema

    def display_plan(self, level=0) -> str:
        indent = '  ' * level
        return f"{indent}* TableScan (Source: {self.table_name})"

class Filter(Operator):
    def __init__(self, predicate: Callable, parent: Operator):
        self.predicate = predicate
        self.parent = parent

    def next(self):
        for row in self.parent.next():
            # if self.predicate.evaluate(row):
            if self.predicate(row):
                yield row

    def get_output_schema(self) -> Schema:
        return self.parent.get_output_schema()

    def display_plan(self, level=0):
        indent = '  ' * level
        output = [f"{indent}* Filter (Cond: {self.predicate})"]
        output.append(self.parent.display_plan(level + 1))
        return '\n'.join(output)

class Projection(Operator):
    def __init__(self, targets: List[ProjectionTarget], output_schema: Schema, parent: Operator):
        self.output_schema = output_schema
        self.parent = parent
        self.extractors = []  # list of callables that either pass the value or the row[i] allong
        for t in targets:
            if t.index is not None:
                # For index expressions (col ref, agg call) at the value at the row's index
                self.extractors.append(lambda row, i=t.index: row[i])
            elif t.value is not None:
                # Literal values just take value regardless of the row
                self.extractors.append(lambda row, v=t.value: v)
            elif t.extractor is not None:
                self.extractors.append(t.extractor)

    def next(self):
        for row in self.parent.next():
            yield tuple(extractor(row) for extractor in self.extractors)
    
    def get_output_schema(self) -> Schema:
        return self.output_schema

    def display_plan(self, level=0):
        indent = '  ' * level
        names = self.output_schema.get_names()
        output = [f"{indent}* Projection (Columns: {names})"]
        output.append(self.parent.display_plan(level + 1))
        return '\n'.join(output)

class Sorter(Operator):
    def __init__(self, sort_keys: List[tuple], parent: Operator):
        self.sort_keys = sort_keys  # (index, is_descending)
        self.parent = parent
        
    def next(self):
        all_rows = list(self.parent.next())
        if not all_rows: return

        def compare_rows(row_a, row_b):
            for index, is_descending in self.sort_keys:
                if row_a[index] < row_b[index]: return -1 if not is_descending else 1
                if row_a[index] > row_b[index]: return 1 if not is_descending else -1
            return 0

        yield from sorted(all_rows, key=cmp_to_key(compare_rows))

    def get_output_schema(self) -> Schema:
        return self.parent.get_output_schema()

    def display_plan(self, level=0):
        indent = '  ' * level
        keys = ", ".join([f"Col[{i}]" for i, _ in self.sort_keys])
        output = [f"{indent}* Sorter (Keys: {keys})"]
        output.append(self.parent.display_plan(level + 1))
        return '\n'.join(output)

class Distinct(Operator):
    def __init__(self, column_indices: List[int], parent: Operator):
        self.column_indices = column_indices
        self.parent = parent
        self.unique_keys = set() 

    def next(self) -> Generator[Row, None, None]:
        # Reset state on each execution if necessary
        self.unique_keys = set()
        for row in self.parent.next():
            # We create a key based on the indices we want to be distinct
            key = tuple(row[i] for i in self.column_indices)
            
            if key not in self.unique_keys:
                self.unique_keys.add(key)
                yield row
            
    def get_output_schema(self) -> Schema:
        # Distinct doesn't change column names or order
        return self.parent.get_output_schema()

    def display_plan(self, level: int = 0) -> str:
        indent = "  " * level
        output = [f"{indent}* Distinct (on indices: {self.column_indices})"]
        output.append(self.parent.display_plan(level + 1))
        return "\n".join(output)


class Limit(Operator):
    def __init__(self, count: int, parent: Operator):
        self.count = count
        self.parent = parent
        
    def next(self):
        yielded = 0
        for row in self.parent.next():
            if yielded < self.count:
                yield row
                yielded += 1
            else:
                break
                
    def get_output_schema(self) -> Schema:
        return self.parent.get_output_schema()

    def display_plan(self, level=0):
        indent = '  ' * level
        output = [f"{indent}* Limit (Count: {self.count})"]
        output.append(self.parent.display_plan(level + 1))
        return '\n'.join(output)

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

class Aggregate(Operator):
    def __init__(self, group_indices: List[int], specs: List[AggregateSpec], output_schema: Schema, parent: Operator):
        self.group_indices = group_indices
        self.specs = specs
        self.output_schema = output_schema
        self.parent = parent

    def next(self):
        grouped_states = {}
        for row in self.parent.next():
            group_key = tuple(row[i] for i in self.group_indices)
            if group_key not in grouped_states:
                # Build state objects based on specs
                states = []
                for spec in self.specs:
                    key = spec.function + (' DISTINCT' if spec.is_distinct else '')
                    states.append(AGGREGATE_MAP[key]())
                grouped_states[group_key] = states

            for i, spec in enumerate(self.specs):
                val = True if spec.arg_index is None else row[spec.arg_index]
                grouped_states[group_key][i].update(val)

        for group_key, states in grouped_states.items():
            yield tuple(list(group_key) + [s.finalize() for s in states])

    def get_output_schema(self) -> Schema:
        return self.output_schema

    def display_plan(self, level=0):
        indent = '  ' * level
        output = [f"{indent}* Aggregate (Groups: {self.group_indices})"]
        output.append(self.parent.display_plan(level + 1))
        return '\n'.join(output)

class NestedLoopJoin(Operator):
    """NestedLoopJoin retreives all right side rows to memory and then iterate though the left rows to validate the predicate

    Pretty poor performance both memory and cpu. If it runs out of memory then there is currently no back up mechanism.
    It is still useful because it can handle any type of predicate. Predicates such as left.id > right.id + 5 cannot be solved
    with a hash join. 
    """

    def __init__(self, left: Operator, right: Operator, predicate: Predicate):
        self.left = left
        self.right = right
        self.predicate = predicate
        self._right_rows = None

    def next(self):
        if self._right_rows is None:
            self._right_rows = list(self.right.next())

        for left_row in self.left.next():
            for right_row in self._right_rows:
                combined_row = left_row + right_row
                if self.predicate.evaluate(combined_row):
                    yield combined_row

    def get_output_schema(self) -> Schema:
        # Schema concatenation handles all name clashing logic
        return self.left.get_output_schema() + self.right.get_output_schema()

    def display_plan(self, level=0):
        indent = '  ' * level
        return f"{indent}* Join\n{self.left.display_plan(level+1)}\n{self.right.display_plan(level+1)}"

if __name__ == '__main__':
    pass
