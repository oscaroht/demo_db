from typing import List, Callable
import operator
from syntax_tree import (
    ASTNode, BinaryOp, SelectStatement,
    TableRef, AggregateCall, Join, Expression, Star, ColumnRef, Literal) 
from operators import ( Filter, ScanOperator, Projection, Sorter, Limit, Aggregate,
    Distinct, NestedLoopJoin, AggregateSpec, Operator
)
# We define ProjectionTarget locally or import if available. 
# Since it was removed from syntax_tree in the previous step, let's keep it simple:
class ProjectionTarget:
    def __init__(self, col_info, extractor=None):
        self.info = col_info
        self.extractor = extractor

from catalog import Catalog
from schema import ColumnInfo, Schema

OPERATOR_MAP = {
    '+': operator.add, '-': operator.sub, '*': operator.mul, '/': operator.truediv,
    '=': operator.eq, '!=': operator.ne, '>': operator.gt, '<': operator.lt,
    '>=': operator.ge, '<=': operator.le,
    'AND': lambda x, y: bool(x) and bool(y),
    'OR':  lambda x, y: bool(x) or bool(y),
}

class QueryPlanner:
    def __init__(self, catalog: Catalog, buffer_manager):
        self.catalog = catalog
        self.buffer_manager = buffer_manager

    def plan_query(self, ast_root: ASTNode):
        if not isinstance(ast_root, SelectStatement):
            raise TypeError(f"Unsupported AST root: {type(ast_root)}")
        return self._plan_select(ast_root)

    def _plan_select(self, stmt: SelectStatement):
        # 1. FROM & JOIN
        plan = self._plan_from(stmt.from_clause)

        # 2. WHERE
        if stmt.where_clause:
            input_schema = plan.get_output_schema()
            predicate_fn = self._compile_expression(stmt.where_clause, input_schema)
            plan = Filter(predicate_fn, plan)

        # 3. GROUP BY & AGGREGATE
        if stmt.group_by_clause or self._has_aggregates(stmt.columns):
            plan = self._plan_aggregate(stmt, plan)

        # 4. DISTINCT (Pre-projection)
        if stmt.is_distinct and not stmt.group_by_clause:
            plan = self._plan_projection(stmt.columns, plan)
            projected_schema = plan.get_output_schema()
            extractors = [lambda row, i=idx: row[i] for idx in range(len(projected_schema.columns))]
            plan = Distinct(extractors, plan)

        # 5. ORDER BY
        if stmt.order_by_clause:
            plan = self._plan_order_by(stmt.order_by_clause, plan)

        # 6. LIMIT
        if stmt.limit_clause:
            plan = Limit(stmt.limit_clause.count, plan)

        # 7. FINAL PROJECTION
        if not (stmt.is_distinct and not stmt.group_by_clause):
            plan = self._plan_projection(stmt.columns, plan)

        return plan

    def _compile_expression(self, expr: Expression, schema: Schema) -> Callable:
        """
        Recursively compiles an AST expression into a callable function.
        """
        if isinstance(expr, Literal):
            return lambda row, v=expr.value: v

        if isinstance(expr, ColumnRef):
            idx = schema.resolve(expr.qualifier, expr.name)
            return lambda row, i=idx: row[i]

        if isinstance(expr, AggregateCall):
            # Used when referencing an aggregate result in ORDER BY or HAVING
            # The schema passed here must be the output of the Aggregate operator
            idx = schema.resolve(None, expr.get_lookup_name())
            return lambda row, i=idx: row[i]

        if isinstance(expr, BinaryOp):
            left_fn = self._compile_expression(expr.left, schema)
            right_fn = self._compile_expression(expr.right, schema)
            op_fn = OPERATOR_MAP[expr.op]
            return lambda row: op_fn(left_fn(row), right_fn(row))

        raise ValueError(f"Planner Error: Do not know how to compile AST node {type(expr)}")

    def _plan_from(self, node) -> ScanOperator | NestedLoopJoin:
        if isinstance(node, TableRef):
            table_name = node.name
            alias = node.alias or node.name
            cols = self.catalog.get_all_column_names(table_name)
            
            schema = Schema([ColumnInfo(f"{alias}.{c}") for c in cols])
            return ScanOperator(
                table_name=table_name,
                data_generator=self.buffer_manager.get_data_generator(table_name),
                schema=schema
            )

        if isinstance(node, Join):
            left = self._plan_from(node.left)
            right = self._plan_from(node.right)
            
            combined_schema = left.get_output_schema() + right.get_output_schema()
            predicate = self._compile_expression(node.condition, combined_schema)
            
            return NestedLoopJoin(left, right, predicate)

        raise TypeError(f"Unknown FROM node: {type(node)}")

    def _plan_aggregate(self, stmt: SelectStatement, plan) -> Aggregate:
        input_schema = plan.get_output_schema()
        
        group_extractors: List[Callable] = []
        group_cols: List[ColumnInfo] = []
        
        if stmt.group_by_clause:
            for col in stmt.group_by_clause.columns:
                # Compile the group expression
                extractor = self._compile_expression(col, input_schema)
                group_extractors.append(extractor)
                
                # Determine name for the schema
                if isinstance(col, ColumnRef):
                    idx = input_schema.resolve(col.qualifier, col.name)
                    original_info = input_schema.columns[idx]
                    group_cols.append(ColumnInfo(original_info.full_name))
                else:
                    group_cols.append(ColumnInfo(col.get_lookup_name()))

        specs: List[AggregateSpec] = []
        agg_cols: List[ColumnInfo] = []
        
        for col in stmt.columns:
            if isinstance(col, AggregateCall):
                # Resolve the argument extractor
                if isinstance(col.argument, Star):
                    # COUNT(*) -> just return 1 (not None) to count the row
                    arg_extractor = lambda row: 1
                else:
                    # COUNT(x), SUM(x+y) -> Compile the argument
                    arg_extractor = self._compile_expression(col.argument, input_schema)

                lookup_name = col.get_lookup_name()
                
                specs.append(AggregateSpec(
                    function=col.function_name,
                    extractor=arg_extractor, # Pass the callable
                    is_distinct=col.is_distinct,
                    output_name=lookup_name
                ))
                agg_cols.append(ColumnInfo(lookup_name, col.alias, is_aggregate=True))

        output_schema = Schema(group_cols + agg_cols)
        return Aggregate(group_extractors, specs, output_schema, plan)

    def _plan_projection(self, columns: List[Expression], plan: Operator) -> Projection:
        input_schema: Schema = plan.get_output_schema()
        
        extractors = []
        schema_columns_columns = []
        for expr in columns:
            if isinstance(expr, Star):
                for i, col_info in enumerate(input_schema.columns):
                    extractors.append(lambda row, idx=i: row[idx])
                    schema_columns_columns.append(col_info)
            else:
                extractors.append(self._compile_expression(expr, input_schema))
                schema_columns_columns.append(ColumnInfo(expr.get_lookup_name(), expr.alias))

        return Projection(extractors, Schema(schema_columns_columns), plan)

    def _plan_order_by(self, order_by_clause, plan) -> Sorter:
        schema: Schema = plan.get_output_schema()
        sort_keys = []
        
        for item in order_by_clause.sort_items:
            # Compile the order expression (e.g., price * quantity)
            # This allows sorting by values not explicitly in the SELECT list
            extractor = self._compile_expression(item.column, schema)
            sort_keys.append((extractor, item.direction == "DESC"))
            
        return Sorter(sort_keys, plan)

    def _has_aggregates(self, columns) -> bool:
        return any(isinstance(c, AggregateCall) for c in columns)
