from __future__ import annotations
import abc
from typing import List

class ASTNode:
    """Base class for all AST nodes."""
    def __repr__(self):
        return f"{self.__class__.__name__}({', '.join(f'{k}={v!r}' for k, v in self.__dict__.items())})"
    
    def display(self, level=0) -> str:
        indent = '  ' * level
        node_name = self.__class__.__name__
        
        # Start with the node's type
        output = [f"{indent}* {node_name}"]

        # Iterate over the node's attributes (fields)
        for name, value in self.__dict__.items():
            
            # Skip hidden attributes if any
            if name.startswith('_'):
                continue

            field_indent = '  ' * (level + 1)
            
            if isinstance(value, ASTNode):
                # If the attribute is another AST node, recurse
                output.append(f"{field_indent}{name}:")
                output.append(value.display(level + 2))
            
            elif isinstance(value, list) and all(isinstance(v, ASTNode) for v in value):
                # If the attribute is a list of AST nodes (e.g., columns)
                output.append(f"{field_indent}{name}: (List)")
                for item in value:
                    output.append(item.display(level + 2))
            else:
                output.append(f"{field_indent}{name}: {value!r}")
        return '\n'.join(output)

class Expression(ASTNode):
    """Base for anything that can appear in a SELECT list."""
    alias: str | None = None

    @abc.abstractmethod
    def get_lookup_name(self) -> str:
        """Returns the string used to find this expression in a Schema."""
        raise NotImplementedError


class Star(Expression):
    """Represents the '*'."""
    def __repr__(self): return "*"

    def get_lookup_name(self) -> str:
        return "*"

class Literal(Expression):
    """Represents a constant value (number, string)."""
    def __init__(self, value, alias = None):
        self.value = value  # anything as long as it can be cast to a string
        self.alias = alias

    def get_lookup_name(self) -> str:
        return str(self.value)

class ColumnRef(Expression):
    """Represents a column name reference."""
    def __init__(self, qualifier: None | str, name: str, alias: None | str = None):
        self.qualifier = qualifier.lower() if isinstance(qualifier, str) else qualifier
        self.name = name.lower()
        self.alias = alias

    def get_lookup_name(self) -> str:
        return self.name

class AggregateCall(Expression):
    """Represents an aggregate function call (e.g., COUNT(*), MAX(col))."""
    def __init__(self, function_name, argument, is_distinct=False, alias=None):
        self.function_name = function_name
        self.argument: Expression = argument
        self.is_distinct = is_distinct
        self.alias = alias

    def get_lookup_name(self) -> str: 
        dist = 'DISTINCT ' if self.is_distinct else ''
        # Recursively call get_lookup_name on the argument!
        return f"{self.function_name}({dist}{self.argument.get_lookup_name()})"

class TableRef(ASTNode):
    def __init__(self, name: str, alias: None | str =None):
        self.name = name.lower()
        self.alias = alias

class OrderByClause(ASTNode):
    """Represents the ORDER BY clause: a list of columns and their direction."""
    def __init__(self, sort_items):
        self.sort_items = sort_items # List of SortItem nodes

class SortItem(ASTNode):
    """Represents one column in the ORDER BY list."""
    def __init__(self, column, direction='ASC'):
        self.column = column    # ColumnRef node
        self.direction = direction # 'ASC' or 'DESC'

class DistinctClause(ASTNode):
    """Represents the DISTINCT clause."""
    def __init__(self, column):
        self.column = column
 
class LimitClause(ASTNode):
    """Represents the LIMIT clause."""
    def __init__(self, count):
        self.count = count # Literal node (integer)

class BinaryOp(Expression):
    """Base class for binary operators (comparisons, arithmetic, logic)."""
    def __init__(self, op: str, left: BinaryOp | Expression, right: BinaryOp | Expression):
        self.op = op      # Operator string (e.g., '=', '>', 'AND')
        self.left = left  # Left-hand side AST node
        self.right = right # Right-hand side AST node

    def get_lookup_name(self):
        return f"{self.left.get_lookup_name()} {self.op} {self.right.get_lookup_name()}"

class Comparison(BinaryOp):
    pass
class Arithmetic(BinaryOp):
    pass
class LogicalExpression(BinaryOp):
    pass
class GroupByClause(ASTNode):
    """Represents the GROUP BY clause: a list of ColumnRef nodes."""
    def __init__(self, columns: List[ColumnRef]):
        self.columns = columns # List of ColumnRef nodes
 
class Join(ASTNode):
    def __init__(self, left: TableRef | Join, right: TableRef | Join, condition: BinaryOp):
        self.left = left
        self.right = right
        self.condition = condition


class SelectStatement(ASTNode):
    """Represents the entire SELECT query."""
    def __init__(self, columns, from_clause: TableRef | Join, where_clause: None | Expression = None, is_distinct=False, order_by_clause: None | OrderByClause = None, limit_clause: None | LimitClause =None, group_by_clause=None):
        self.columns = columns        # List of ColumnRef or AggregateCall nodes
        self.from_clause = from_clause
        self.where_clause = where_clause  # Logical condition AST node
        self.group_by_clause = group_by_clause
        self.is_distinct = is_distinct
        self.order_by_clause = order_by_clause
        self.limit_clause = limit_clause

class CreateStatement(ASTNode):
    def __init__(self, table_name, column_names, column_types) -> None:
        self.table_name = table_name
        self.column_names = column_names
        self.column_types = column_types

class InsertStatement(ASTNode):
    def __init__(self, table_name, columns, values=None, select=None) -> None:
        self.table_name = table_name
        self.columns: list[ColumnRef] = columns
        self.values: None | list[Literal] = values
        self.select: None | SelectStatement = select

class DropStatement(ASTNode):
    def __init__(self, table_name: str) -> None:
        self.table_name = table_name
