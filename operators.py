import abc
from typing import Callable, Generator, List, Any, Literal, Optional
from functools import cmp_to_key
from dataclasses import dataclass
from catalog import ShadowPage, ShadowTable, Table, Catalog, Page
from syntax_tree import Literal

from schema import ColumnIdentifier, Schema
from tests.test_transaction import transaction
from transaction import Transaction
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

class StatusOperator(Operator):
    def __init__(self, status: str) -> None:
        self.status = status
    def next(self):
        yield tuple([self.status]), None, None
    def display_plan(self, level: int = 0) -> str:
        pass
    def get_output_schema(self) -> Schema:
        return Schema([ColumnIdentifier('status')])


class ScanOperator(Operator):
    def __init__(self, table_display_name: str, page_generator, schema: Schema):
        self.table_name = table_display_name
        self.schema = schema
        self.gen = page_generator
        
    def next(self):
        for page in self.gen:
            for idx, row in enumerate(page.data):
                yield row, page.page_id, idx 

    # def next(self):
    #     for page_id in self.transaction.get_table_by_name(self.table_name).page_id:
    #         page = self.transaction.buffer_manager.get_page(page_id)
    #         for idx, row in enumerate(page.data):
    #             # Critical: Yield location info (page_id, idx) alongside data
    #             yield row, page_id, idx

    def get_output_schema(self) -> Schema:
        return self.schema

    def display_plan(self, level=0) -> str:
        indent = '  ' * level
        return f"{indent}* TableScan (Source: {self.table_name})"

class Insert(Operator):
    def __init__(self, table: ShadowTable, data_generator: Generator, column_indices: list[int], transaction):
        self.shadow_table = table
        self.transaction: Transaction = transaction
        self.transaction.prepare_shadow_table_for_write(self.shadow_table)
        self.data_generator = data_generator
        self.column_indices = column_indices

    def next(self):
        for raw_val_tuple in self.data_generator():
            new_row = self._prepare_row(raw_val_tuple)
            page: Page | ShadowPage = self.transaction.buffer_manager.get_page(self.shadow_table.page_id[-1])
            print(f"GET PAGE {page.page_id}")
            if isinstance(page, Page):
                raise Exception("Page object not writable")
            page_is_full = not page.add_row(new_row)
            if page_is_full:
                page = self.transaction.get_new_page(self.shadow_table)
                print(f"PAGE IS FULL NEW PAGE {page.page_id}")
                page_is_full = not page.add_row(new_row)
                if page_is_full:
                    raise Exception("Page size is to small for even 1 row!")
        yield(tuple(['SUCCESS']), None, None)


    def _prepare_row(self, raw_val_tuple) -> Row:
        new_row = []
        for src_idx in self.column_indices:
            if src_idx is not None:
                typ = self.shadow_table.column_datatypes[len(new_row)]
                val = raw_val_tuple[src_idx]
                if isinstance(val, Literal):
                    val = val.value
                new_row.append(typ(val))
            else:
                new_row.append(None) # Default/Null
        return tuple(new_row)

        
    def display_plan(self, level=0) -> str:
        indent = '  ' * level
        return f"{indent}* Insert into: {self.shadow_table.table_name})"
    def get_output_schema(self) -> Schema:
        return Schema([ColumnIdentifier('status')])


class Delete(Operator):
    def __init__(self, shadow_table, transaction: Transaction, parent: Operator):
        self.shadow_table = shadow_table
        self.parent = parent
        self.transaction = transaction
        self.drop_dict = {}

    def next(self):
        row_count = 0
        current_pid = None
        drop_list = []
        shadow_page: None | ShadowPage = None
        for _, pid, idx in self.parent.next():
            if pid != current_pid:
                if shadow_page and drop_list:
                    shadow_page.delete_rows(drop_list)
                shadow_page = self.transaction.copy_on_write(self.shadow_table, pid)
                drop_list = []
                current_pid = pid
            drop_list.append(idx)
            row_count += 1
        if shadow_page:
            shadow_page.delete_rows(drop_list)

        yield tuple([f'Deleted {row_count} rows']), None, None

    def get_output_schema(self) -> Schema:
        return Schema([ColumnIdentifier('status')])

    def display_plan(self, level: int = 0) -> str:
        indent = '  ' * level
        output = [f"{indent}* Delete"]
        output.append(self.parent.display_plan(level + 1))
        return '\n'.join(output)
            

class Filter(Operator):
    def __init__(self, predicate: Callable, parent: Operator):
        self.predicate = predicate
        self.parent = parent

    def next(self):
        for row, pid, idx in self.parent.next():
            if self.predicate(row):
                yield row, pid, idx

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
        for row, pid, idx in self.parent.next():
            yield tuple(extractor(row) for extractor in self.extractors), pid, idx
    
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
        for row, pid, idx in self.parent.next():
            key = tuple(ext(row) for ext in self.extractors)
            if key not in self.unique_keys:
                self.unique_keys.add(key)
                yield row, pid, idx
            
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
        for row, pid, idx in self.parent.next():
            if yielded < self.count:
                yield row, pid, idx
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
        self.group_extractors = group_extractors
        self.specs = specs
        self.output_schema = output_schema
        self.parent = parent

    def next(self):
        grouped_states = {}
        for row, _, _ in self.parent.next():
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
            yield tuple(list(group_key) + [s.finalize() for s in states]), None, None

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

        for left_row, _, _ in self.left.next():
            for right_row, _, _ in self._right_rows:
                combined_row = left_row + right_row
                if self.predicate(combined_row):
                    yield combined_row, None, None

    def get_output_schema(self) -> Schema:
        return self.left.get_output_schema() + self.right.get_output_schema()

    def display_plan(self, level=0):
        indent = '  ' * level
        return f"{indent}* Join\n{self.left.display_plan(level+1)}\n{self.right.display_plan(level+1)}"
