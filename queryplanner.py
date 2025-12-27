from typing import List, Optional
from syntax_tree import (
    ASTNode, SelectStatement, LogicalExpression, Comparison,
    Literal, ColumnRef, TableRef, AggregateCall, Join
)
from operators import (
    Filter, ScanOperator, Projection, Sorter, Limit, Aggregate,
    Distinct, ComparisonPredicate, LogicalPredicate, NestedLoopJoin,
    AggregateSpec
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

    def _plan_from(self, node):
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

    def _plan_aggregate(self, stmt: SelectStatement, plan):
        input_schema = plan.get_output_schema()
        
        # Resolve group indices
        group_indices = []
        group_cols = []
        if stmt.group_by_clause:
            for col in stmt.group_by_clause.columns:
                idx = input_schema.resolve(col.table, col.name)
                group_indices.append(idx)
                group_cols.append(input_schema.columns[idx])

        # Extract aggregate specs and build output schema
        specs = []
        agg_cols = []
        for col in stmt.columns:
            if isinstance(col, AggregateCall):
                arg_idx = None # For COUNT(*)
                if col.argument != "*":
                    arg_idx = input_schema.resolve(col.argument.table, col.argument.name)
                
                dist = 'DISTINCT ' if col.is_distinct else ''
                arg_name = col.argument.name if col.argument != "*" else "*"
                output_name = f"{col.function_name}({dist}{arg_name})"
                
                specs.append(AggregateSpec(col.function_name, arg_idx, col.is_distinct, output_name))
                agg_cols.append(ColumnInfo(full_name=output_name, is_aggregate=True))

        # The new schema for the output of the Aggregate Operator
        output_schema = Schema(group_cols + agg_cols)
        return Aggregate(group_indices, specs, output_schema, plan)

    def _plan_projection(self, columns, plan):
        input_schema = plan.get_output_schema()
        projected_indices = []
        projected_cols = []

        for col in columns:
            if isinstance(col, ColumnRef) and col.name == "*":
                # ASTERISK EXPANSION
                for i, info in enumerate(input_schema.columns):
                    projected_indices.append(i)
                    projected_cols.append(info)
            
            elif isinstance(col, (ColumnRef, AggregateCall)):
                # Logic for both raw columns and pre-calculated aggregates is now the same
                name = self._get_name_from_node(col)
                idx = input_schema.resolve(getattr(col, 'table', None), name)
                
                projected_indices.append(idx)
                # Apply alias to the new schema if provided
                orig_col = input_schema.columns[idx]
                projected_cols.append(ColumnInfo(orig_col.full_name, col.alias))

        return Projection(projected_indices, Schema(projected_cols), plan)

    def _plan_order_by(self, order_by_clause, plan):
        schema = plan.get_output_schema()
        sort_keys = []
        for item in order_by_clause.sort_items:
            name = self._get_name_from_node(item.column)
            idx = schema.resolve(getattr(item.column, 'table', None), name)
            sort_keys.append((idx, item.direction == "DESC"))
        return Sorter(sort_keys, plan)

    def _build_predicate(self, expr, schema: Schema):
        if isinstance(expr, Comparison):
            v1, i1 = self._resolve_operand(expr.left, schema)
            v2, i2 = self._resolve_operand(expr.right, schema)
            return ComparisonPredicate(expr.op, v1, v2, i1, i2)

        if isinstance(expr, LogicalExpression):
            left = self._build_predicate(expr.left, schema)
            right = self._build_predicate(expr.right, schema)
            return LogicalPredicate(expr.op, left, right)
        raise TypeError(f"Unsupported expression: {type(expr)}")

    def _resolve_operand(self, operand, schema: Schema):
        if isinstance(operand, Literal):
            return operand.value, None
        if isinstance(operand, ColumnRef):
            return None, schema.resolve(operand.table, operand.name)
        raise TypeError(f"Unsupported operand: {type(operand)}")

    def _get_name_from_node(self, node) -> str:
        """Helper to get the lookup name for a column or aggregate call."""
        if isinstance(node, ColumnRef):
            return node.name
        if isinstance(node, AggregateCall):
            dist = 'DISTINCT ' if node.is_distinct else ''
            arg_name = node.argument.name if node.argument != "*" else "*"
            return f"{node.function_name}({dist}{arg_name})"
        return str(node)

    def _has_aggregates(self, columns) -> bool:
        return any(isinstance(c, AggregateCall) for c in columns)
