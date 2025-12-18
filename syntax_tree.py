
class ASTNode:
    """Base class for all AST nodes."""
    def __repr__(self):
        # A helpful representation for debugging
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

class SelectStatement(ASTNode):
    """Represents the entire SELECT query."""
    def __init__(self, columns, table, where_clause=None, is_distinct=False, order_by_clause=None, limit_clause=None, group_by_clause=None):
        self.columns = columns        # List of ColumnRef or AggregateCall nodes
        self.table = table            # String: table name
        self.where_clause = where_clause  # Logical condition AST node
        self.group_by_clause = group_by_clause
        self.is_distinct = is_distinct
        self.order_by_clause = order_by_clause
        self.limit_clause = limit_clause

class ColumnRef(ASTNode):
    """Represents a column name reference."""
    def __init__(self, name, alias=None):
        self.name = name.lower()
        self.alias = alias

class Literal(ASTNode):
    """Represents a constant value (number, string)."""
    def __init__(self, value):
        self.value = value # Converted type (e.g., int, float)

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

class BinaryOp(ASTNode):
    """Base class for binary operators (comparisons, arithmetic, logic)."""
    def __init__(self, op, left, right):
        self.op = op      # Operator string (e.g., '=', '>', 'AND')
        self.left = left  # Left-hand side AST node
        self.right = right # Right-hand side AST node

# Comparison nodes (used in WHERE clauses)
class Comparison(BinaryOp):
    pass

# Logical nodes (used to combine comparisons in WHERE)
class LogicalExpression(BinaryOp):
    pass
class GroupByClause(ASTNode):
    """Represents the GROUP BY clause: a list of ColumnRef nodes."""
    def __init__(self, columns):
        self.columns = columns # List of ColumnRef nodes
class AggregateCall(ASTNode):
    """Represents an aggregate function call (e.g., COUNT(*), MAX(col))."""
    def __init__(self, function_name, argument=None, is_distinct=False, alias=None):
        self.function_name = function_name # String (e.g., 'COUNT', 'MAX')
        self.argument = argument           # ColumnRef or '*'
        self.is_distinct = is_distinct
        self.alias = alias
