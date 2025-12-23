from dataclasses import dataclass

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
    BaseIterator,
    Projection,
    Sorter,
    Limit,
    Aggregate,
    Distinct,
    ComparisonPredicate,
    LogicalPredicate,
    JoinOperator
)
from catalog import Catalog


@dataclass
class AggregateSpec:
    function: str
    arg_index: int | None  # None for COUNT(*)
    is_distinct: bool
    output_name: str


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

        if stmt.group_by_clause:
            input_schema = plan.get_output_schema_names()
            group_keys = self._resolve_group_keys(stmt.group_by_clause, input_schema)
            aggregates = self._extract_aggregate_specs(stmt.columns, input_schema)
            plan = Aggregate(plan, group_keys, aggregates)

        if stmt.is_distinct and not stmt.group_by_clause:  #  distinct has no affect in combination with a group by clause (except COUNT(DISTINCT .))
            plan = self._plan_projection(stmt.columns, plan)
            schema = plan.get_output_schema_names()
            plan = Distinct(list(range(len(schema))), plan)        

        if stmt.order_by_clause:
            plan = self._plan_order_by(stmt.order_by_clause, plan)

        if stmt.limit_clause:
            plan = Limit(stmt.limit_clause.count, plan)

        # 7. FINAL PROJECTION
        if not stmt.is_distinct or stmt.group_by_clause:
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
            if isinstance(operand, Literal):
                return operand.value, None

            if isinstance(operand, ColumnRef):
                if operand.table:
                    name = f"{operand.table}.{operand.name}"
                else:
                    name = operand.name

                try:
                    idx = schema.index(name)
                except ValueError:
                    raise SemanticError(f"Unknown column: {name}")

                return None, idx

            raise TypeError(operand)

        v1, i1 = resolve(cmp.left)
        v2, i2 = resolve(cmp.right)

        return ComparisonPredicate(cmp.op, v1, v2, i1, i2)


    def _build_base_iterator(self, table: TableRef):
        data_generator = self.buffer_manager.get_data_generator(table.name)
        return BaseIterator(
            table.name,
            data_generator,
            self.catalog
        )

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
            if not isinstance(col, AggregateCall):
                continue

            if col.argument == "*":
                arg_index = None
            else:
                col_name = self._resolve_column_name(col.argument, input_schema)
                arg_index = input_schema.index(col_name)

            output_name = (
                col.alias
                if col.alias
                else self._default_aggregate_name(col)
            )

            specs.append(
                AggregateSpec(
                    function=col.function_name,
                    arg_index=arg_index,
                    is_distinct=col.is_distinct,
                    output_name=output_name,
                )
            )

        return specs

    def _default_aggregate_name(self, agg: AggregateCall) -> str:
        if agg.argument == "*":
            return f"{agg.function_name}(*)"
        return f"{agg.function_name}({agg.argument.name})"


    def _resolve_group_keys(self, group_by_clause, table):
        return [self.catalog.get_column_index(table, col.name) for col in group_by_clause.columns]

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

        # SELECT *
        if len(columns) == 1 and isinstance(columns[0], ColumnRef) and columns[0].name == "*":
            return Projection(list(range(len(input_schema))), input_schema, plan)

        indices = []
        names = []

        for col in columns:
            if isinstance(col, ColumnRef):
                idx = input_schema.index(col.name)
                indices.append(idx)
                col_name = col.alias if col.alias else col.name
                names.append(col_name)

            elif isinstance(col, AggregateCall):
                dist = 'DISTINCT ' if col.is_distinct else ''
                if col.argument == "*":
                    name = f"{col.function_name}({dist}*)"
                else:
                    name = f"{col.function_name}({dist}{col.argument})"

                idx = input_schema.index(name)
                indices.append(idx)
                col_name = col.alias if col.alias else name
                names.append(col_name)

            elif isinstance(col, Literal):
                raise NotImplementedError("Literal SELECT items not supported")

        return Projection(indices, names, plan)
