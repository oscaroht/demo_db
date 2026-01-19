import abc
from typing import Callable, Generator, List, Any, Optional
from functools import cmp_to_key
from dataclasses import dataclass

from schema import Schema
Row = tuple[Any, ...]

@dataclass
class AggregateSpec:
    function: str
    extractor: Callable[[Row], Any] # Changed from arg_index to extractor
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
    def __init__(self, extractors: List[Callable[[Row], Any]], output_schema: Schema, parent: Operator):
        self.output_schema = output_schema
        self.parent = parent
        self.extractors = extractors

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
    def __init__(self, sort_keys: List[tuple[Callable, bool]], parent: Operator):
        self.sort_keys = sort_keys  # (extractor_func, is_descending)
        self.parent = parent
        
    def next(self):
        all_rows = list(self.parent.next())
        if not all_rows: return

        def compare_rows(row_a, row_b):
            for extractor, is_descending in self.sort_keys:
                val_a = extractor(row_a)
                val_b = extractor(row_b)
                
                # Handle None/Nulls if necessary (Python comparison might fail on None)
                if val_a == val_b:
                    continue
                
                # Basic Comparison
                if val_a < val_b: return -1 if not is_descending else 1
                if val_a > val_b: return 1 if not is_descending else -1
            return 0

        yield from sorted(all_rows, key=cmp_to_key(compare_rows))

    def get_output_schema(self) -> Schema:
        return self.parent.get_output_schema()

    def display_plan(self, level=0):
        indent = '  ' * level
        output = [f"{indent}* Sorter (Complex Keys)"]
        output.append(self.parent.display_plan(level + 1))
        return '\n'.join(output)

class Distinct(Operator):
    def __init__(self, extractors: List[Callable[[Row], Any]], parent: Operator):
        self.extractors = extractors
        self.parent = parent
        self.unique_keys = set() 

    def next(self) -> Generator[Row, None, None]:
        self.unique_keys = set()
        for row in self.parent.next():
            key = tuple(ext(row) for ext in self.extractors)
            if key not in self.unique_keys:
                self.unique_keys.add(key)
                yield row
            
    def get_output_schema(self) -> Schema:
        return self.parent.get_output_schema()

    def display_plan(self, level: int = 0) -> str:
        indent = "  " * level
        output = [f"{indent}* Distinct"]
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

# --- Aggregation States (Unchanged logic, just context) ---
class AggregationState:
    def __init__(self): self.result = self._get_initial_value()
    def _get_initial_value(self): raise NotImplementedError
    def update(self, value): raise NotImplementedError
    def finalize(self): return self.result

class SumState(AggregationState):
    def _get_initial_value(self): return 0
    def update(self, value): 
        if value is not None: self.result += value

class CountState(AggregationState):
    def _get_initial_value(self): return 0
    def update(self, value): 
        if value is not None: self.result += 1

class MaxState(AggregationState):
    def _get_initial_value(self): return None
    def update(self, value):
        if value is not None and (self.result is None or value > self.result): self.result = value

class MinState(AggregationState):
    def _get_initial_value(self): return None
    def update(self, value):
        if value is not None and (self.result is None or value < self.result): self.result = value
                
class AvgState(AggregationState):
    def __init__(self):
        self.sum_state = SumState()
        self.count_state = CountState()
    def _get_initial_value(self): return (0, 0)
    def update(self, value):
        self.sum_state.update(value)
        self.count_state.update(value)
    def finalize(self):
        total_sum = self.sum_state.result
        total_n = self.count_state.result
        if total_n == 0: return None
        return total_sum / total_n

class CountDistinctState(AggregationState):
    def _get_initial_value(self): return set()
    def update(self, value): 
        if value is not None: self.result.add(value)
    def finalize(self): return len(self.result)

AGGREGATE_MAP = {
    'SUM': SumState, 'COUNT': CountState, 'COUNT DISTINCT': CountDistinctState,
    'MAX': MaxState, 'MIN': MinState, 'AVG': AvgState,
}

class Aggregate(Operator):
    def __init__(self, group_extractors: List[Callable], specs: List[AggregateSpec], output_schema: Schema, parent: Operator):
        self.group_extractors = group_extractors # List of callables
        self.specs = specs
        self.output_schema = output_schema
        self.parent = parent

    def next(self):
        grouped_states = {}
        for row in self.parent.next():
            # Calculate group key using extractors
            group_key = tuple(extractor(row) for extractor in self.group_extractors)
            
            if group_key not in grouped_states:
                states = []
                for spec in self.specs:
                    key = spec.function + (' DISTINCT' if spec.is_distinct else '')
                    states.append(AGGREGATE_MAP[key]())
                grouped_states[group_key] = states

            for i, spec in enumerate(self.specs):
                # Calculate value to aggregate using extractor
                val = spec.extractor(row)
                grouped_states[group_key][i].update(val)

        for group_key, states in grouped_states.items():
            yield tuple(list(group_key) + [s.finalize() for s in states])

    def get_output_schema(self) -> Schema:
        return self.output_schema

    def display_plan(self, level=0):
        indent = '  ' * level
        output = [f"{indent}* Aggregate (Complex Groups)"]
        output.append(self.parent.display_plan(level + 1))
        return '\n'.join(output)

class NestedLoopJoin(Operator):
    def __init__(self, left: Operator, right: Operator, predicate: Callable):
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
                if self.predicate(combined_row):
                    yield combined_row

    def get_output_schema(self) -> Schema:
        return self.left.get_output_schema() + self.right.get_output_schema()

    def display_plan(self, level=0):
        indent = '  ' * level
        return f"{indent}* Join\n{self.left.display_plan(level+1)}\n{self.right.display_plan(level+1)}"
