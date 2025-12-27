from typing import List

from syntax_tree import (
    ASTNode,
    SelectStatement,
    LogicalExpression,
    Comparison,
    Literal,
    ColumnRef,
    TableRef,
    AggregateCall,
    Join
)
from operators import (
    Filter,
    ScanOperator,
    Projection,
    Sorter,
    Limit,
    Aggregate,
    Distinct,
    ComparisonPredicate,
    LogicalPredicate,
    JoinOperator,
    AggregateSpec
)
from catalog import Catalog



class ParserError(Exception):
    pass

class AmbiguousColumnError(ParserError):
    """Column name is present in multiple projections."""
    pass


class QueryPlanner:
    def __init__(self, catalog: Catalog, buffer_manager):
        self.catalog = catalog
        self.buffer_manager = buffer_manager

    def plan_query(self, ast_root: ASTNode):
        if not isinstance(ast_root, SelectStatement):
            raise TypeError(f"Unsupported AST root: {type(ast_root)}")

        return self._plan_select(ast_root)

    def _plan_select(self, stmt: SelectStatement):
        plan = self._plan_from(stmt.from_clause)

        if stmt.where_clause:
            plan = self._plan_where(stmt.where_clause, plan)

        input_schema = plan.get_output_schema_names()
        aggregates = self._extract_aggregate_specs(stmt.columns, input_schema)
        if aggregates or stmt.group_by_clause:
            group_keys = self._resolve_group_keys(stmt.group_by_clause, input_schema)
            plan = Aggregate(group_keys, aggregates, plan)

        if stmt.is_distinct and not stmt.group_by_clause:  #  distinct has no affect in combination with a group by clause (except COUNT(DISTINCT .))
            plan = self._plan_projection(stmt.columns, plan)
            schema = plan.get_output_schema_names()
            plan = Distinct(list(range(len(schema))), plan)        

        if stmt.order_by_clause:
            plan = self._plan_order_by(stmt.order_by_clause, plan)

        if stmt.limit_clause:
            plan = Limit(stmt.limit_clause.count, plan)

        plan = self._plan_projection(stmt.columns, plan)

        return plan

    def _plan_where(self, ast_node, plan) -> Filter:
        input_schema = plan.get_output_schema_names()
        predicate = self._build_predicate(ast_node, input_schema)
        return Filter(predicate, plan)


    def _build_predicate(self, expr, schema):
        if isinstance(expr, Comparison):
            return self._build_comparison_predicate(expr, schema)

        if isinstance(expr, LogicalExpression):
            left = self._build_predicate(expr.left, schema)
            right = self._build_predicate(expr.right, schema)
            return LogicalPredicate(expr.op, left, right)

        raise TypeError(expr)

    def _build_comparison_predicate(self, cmp, schema):
        def resolve(operand):
            print(f"Resolve {operand}")
            if isinstance(operand, Literal):
                return operand.value, None

            if isinstance(operand, ColumnRef):
                idx = self._resolve_column_index(operand, schema)
                return None, idx

            raise TypeError(operand)

        v1, i1 = resolve(cmp.left)
        v2, i2 = resolve(cmp.right)

        return ComparisonPredicate(cmp.op, v1, v2, i1, i2)

    def _resolve_column_index(self, col_ref: ColumnRef, schema: List[str]) -> int:
        """
        Internal resolution using FQN (table.column).
        """
        target_table = col_ref.table
        target_name = col_ref.name

        # If the parser found a table prefix (e.g., users.id)
        if target_table:
            full_name = f"{target_table}.{target_name}"
            if full_name in schema:
                return schema.index(full_name)
        
        # If no table prefix was provided, we search for the column name.
        # We still treat the internal schema as FQN (Table.Column).
        matches = [i for i, col in enumerate(schema) if col.split('.')[-1] == target_name]

        if not matches:
            raise ValueError(f"Column '{target_name}' not found in schema: {schema}")
        if len(matches) > 1:
            raise ValueError(f"Ambiguous column '{target_name}'. Found in multiple tables: {[schema[i] for i in matches]}")

        return matches[0]

    def _build_base_iterator(self, table: TableRef) -> ScanOperator:
        table_name = table.name
        alias = table.alias or table.name
        data_generator = self.buffer_manager.get_data_generator(table.name)

        columns = self.catalog.get_all_column_names(table_name)

        schema = [
            f"{alias}.{col}"
            for col in columns
        ]

        return ScanOperator(
            table_name=table_name,
            data_generator=data_generator,
            output_schema=schema,
        )

    # def _build_base_iterator(self, table: TableRef):
    #     data_generator = self.buffer_manager.get_data_generator(table.name)
    #     return BaseIterator(
    #         table.name,
    #         data_generator,
    #         self.catalog
    #     )

    def _plan_from(self, node):
        if isinstance(node, TableRef):
            return self._build_base_iterator(node)

        if isinstance(node, Join):
            left_plan = self._plan_from(node.left)
            right_plan = self._plan_from(node.right)

            schema = (
                left_plan.get_output_schema_names()
                + right_plan.get_output_schema_names()
            )

            predicate = self._build_predicate(node.condition, schema)

            return JoinOperator(left_plan, right_plan, predicate)

        raise TypeError(node)

    def _plan_join(self, join_ast: Join) -> JoinOperator:
        left_plan = self._plan_from(join_ast.left)
        right_plan = self._plan_from(join_ast.right)

        # Combined schema
        left_schema = left_plan.get_output_schema_names()
        right_schema = right_plan.get_output_schema_names()
        combined_schema = left_schema + right_schema

        # Plan ON condition using combined schema
        predicate = self._plan_join_condition(
            join_ast.condition,
            combined_schema,
        )

        return JoinOperator(left_plan, right_plan, predicate)


    def _plan_join_condition(self, expr, schema) -> ComparisonPredicate | LogicalPredicate:
        if isinstance(expr, Comparison):
            return self._build_predicate(expr, schema)

        if isinstance(expr, LogicalExpression):
            left = self._plan_join_condition(expr.left, schema)
            right = self._plan_join_condition(expr.right, schema)
            return LogicalPredicate(expr.op, left, right)

        raise TypeError(expr)



    def _extract_aggregate_specs(self, columns, input_schema):
        specs = []
        for col in columns:
            if isinstance(col, AggregateCall):
                arg_index = None
                if col.argument != "*":
                    arg_index = self._resolve_column_index(col.argument, input_schema)
                
                dist = 'DISTINCT ' if col.is_distinct else ''
                arg_name = col.argument.name if col.argument != "*" else "*"
                output_name = f"{col.function_name}({dist}{arg_name})"

                specs.append(AggregateSpec(
                    function=col.function_name,
                    arg_index=arg_index,
                    is_distinct=col.is_distinct,
                    output_name=output_name
                ))
        return specs    

    def _default_aggregate_name(self, agg: AggregateCall) -> str:
        if agg.argument == "*":
            return f"{agg.function_name}(*)"
        return f"{agg.function_name}({agg.argument.name})"

    def _resolve_group_keys(self, group_by_clause, input_schema) -> List[int]:
        group_indices = []
        if not group_by_clause:
            return []
        for col in group_by_clause.columns:
            idx = self._resolve_column_index(col, input_schema)
            group_indices.append(idx)
        return group_indices    

    def _plan_order_by(self, order_by_clause, plan):
        schema = plan.get_output_schema_names()
        name_to_index = {name: i for i, name in enumerate(schema)}

        sort_keys = []
        for item in order_by_clause.sort_items:
            if isinstance(item.column, AggregateCall):
                name = f"{item.column.function_name}({item.column.argument.upper()})"
            else:
                name = item.column.name

            if name not in name_to_index:
                raise ValueError(f"ORDER BY column '{name}' not in output")

            sort_keys.append((name_to_index[name], item.direction == "DESC"))

        return Sorter(sort_keys, plan)

    def _plan_projection(self, columns, plan):
        input_schema = plan.get_output_schema_names()
        print(f"Projection input schema {input_schema}")
        print(f"Projection columns {columns}")

        # Handle SELECT *
        if len(columns) == 1 and isinstance(columns[0], ColumnRef) and columns[0].name == "*":
            return Projection(list(range(len(input_schema))), input_schema, plan)

        indices = []
        names = []

        for col in columns:
            if isinstance(col, ColumnRef):
                # Use the new resolver to find the correct index in a joined schema
                idx = self._resolve_column_index(col, input_schema)
                indices.append(idx)
                # Use alias if provided, otherwise use the original column name
                col_name = col.alias if col.alias else col.name
                names.append(col_name)

            elif isinstance(col, AggregateCall):
                # Existing aggregate logic, but ensure the argument is resolved correctly
                dist = 'DISTINCT ' if col.is_distinct else ''
                # Logic for mapping aggregate output names back to schema
                name = f"{col.function_name}({dist}{col.argument if col.argument == '*' else col.argument.name})"
                
                if name not in input_schema:
                    raise ValueError(f"Aggregate {name} not found in input schema {input_schema}")
                
                idx = input_schema.index(name)
                indices.append(idx)                
                display_name: str = col.argument.name if isinstance(col.argument, ColumnRef) else col.argument
                names.append(col.alias if col.alias else display_name)

        return Projection(indices, input_schema, plan)
