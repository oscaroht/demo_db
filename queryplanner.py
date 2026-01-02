from typing import List, Optional
from syntax_tree import (
    ASTNode, BinaryOp, ProjectionTarget, SelectStatement, LogicalExpression, Comparison,
    Literal, ColumnRef, TableRef, AggregateCall, Join, Expression, Star
)
from operators import (
    Filter, Predicate, ScanOperator, Projection, Sorter, Limit, Aggregate,
    Distinct, ComparisonPredicate, LogicalPredicate, NestedLoopJoin,
    AggregateSpec, Operator
)
from catalog import Catalog
from schema import ColumnInfo, Schema

class ParserError(Exception): pass

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
            predicate = self._build_predicate(stmt.where_clause, input_schema)
            plan = Filter(predicate, plan)

        # 3. GROUP BY & AGGREGATE
        # Aggregates change the schema: Output = [Group Keys] + [Agg Results]
        if stmt.group_by_clause or self._has_aggregates(stmt.columns):
            plan = self._plan_aggregate(stmt, plan)

        # 4. DISTINCT (Pre-projection)
        if stmt.is_distinct and not stmt.group_by_clause:
            plan = self._plan_projection(stmt.columns, plan)
            schema = plan.get_output_schema()
            plan = Distinct(list(range(len(schema.columns))), plan)        

        # 5. ORDER BY
        if stmt.order_by_clause:
            plan = self._plan_order_by(stmt.order_by_clause, plan)

        # 6. LIMIT
        if stmt.limit_clause:
            plan = Limit(stmt.limit_clause.count, plan)

        # 7. FINAL PROJECTION (If not already handled by Distinct)
        if not (stmt.is_distinct and not stmt.group_by_clause):
            plan = self._plan_projection(stmt.columns, plan)

        return plan

    def _plan_from(self, node) -> ScanOperator | NestedLoopJoin:
        if isinstance(node, TableRef):
            table_name = node.name
            alias = node.alias or node.name
            cols = self.catalog.get_all_column_names(table_name)
            
            # Create a Schema object immediately
            schema = Schema([ColumnInfo(f"{alias}.{c}") for c in cols])
            return ScanOperator(
                table_name=table_name,
                data_generator=self.buffer_manager.get_data_generator(table_name),
                schema=schema
            )

        if isinstance(node, Join):
            left = self._plan_from(node.left)
            right = self._plan_from(node.right)
            
            # Concatenate schemas using the new Schema object logic
            combined_schema = left.get_output_schema() + right.get_output_schema()
            predicate = self._build_predicate(node.condition, combined_schema)
            
            return NestedLoopJoin(left, right, predicate)

        raise TypeError(f"Unknown FROM node: {type(node)}")

    def _plan_aggregate(self, stmt: SelectStatement, plan) -> Aggregate:
        input_schema = plan.get_output_schema()
        
        # Group indices
        group_indices: List[int] = []
        group_cols: List[ColumnInfo] = []
        if stmt.group_by_clause:
            for col in stmt.group_by_clause.columns:
                target: ProjectionTarget = col.bind(input_schema)[0]
                if target.index is None:
                    raise Exception("Group key should be a column index not literal value")
                group_indices.append(target.index)
                group_cols.append(target.info)

        # Aggregates logic
        specs: List[AggregateSpec] = []
        agg_cols: List[ColumnInfo] = []
        for col in stmt.columns:
            if isinstance(col, AggregateCall):
                arg_targets: List[ProjectionTarget] = col.argument.bind(input_schema)
                arg_idx = arg_targets[0].index if not isinstance(col.argument, Star) else None
                
                lookup_name = col.get_lookup_name()
                specs.append(AggregateSpec(col.function_name, arg_idx, col.is_distinct, lookup_name))
                agg_cols.append(ColumnInfo(lookup_name, col.alias))

        output_schema = Schema(group_cols + agg_cols)
        return Aggregate(group_indices, specs, output_schema, plan)

    def _plan_projection(self, columns: List[Expression], plan: Operator) -> Projection:
        input_schema: Schema = plan.get_output_schema()
        
        all_targets: List[ProjectionTarget] = []
        for expr in columns:
            all_targets += expr.bind(input_schema)

        new_schema = Schema([t.info for t in all_targets])
        
        return Projection(all_targets, new_schema, plan)

    def _plan_order_by(self, order_by_clause, plan) -> Sorter:
        schema: Schema = plan.get_output_schema()
        sort_keys = []
        for item in order_by_clause.sort_items:
            target = item.column.bind(schema)[0]
            sort_keys.append((target.index, item.direction == "DESC"))
        return Sorter(sort_keys, plan)

    def _build_predicate(self, expr: BinaryOp, schema: Schema) -> Predicate:
        if isinstance(expr, Comparison):
                target_left = expr.left.bind(schema)[0]
                target_right = expr.right.bind(schema)[0]
                
                return ComparisonPredicate(
                    expr.op, 
                    val1=target_left.value,   # Will be None for columns
                    val2=target_right.value,  # Will be None for columns
                    col_idx1=target_left.index, # Will be None for literals
                    col_idx2=target_right.index  # Will be None for literals
                )

        if isinstance(expr, LogicalExpression):
            left = self._build_predicate(expr.left, schema)
            right = self._build_predicate(expr.right, schema)
            return LogicalPredicate(expr.op, left, right)
    
        raise TypeError(f"Unsupported expression: {type(expr)}")

    def _has_aggregates(self, columns) -> bool:
        return any(isinstance(c, AggregateCall) for c in columns)
