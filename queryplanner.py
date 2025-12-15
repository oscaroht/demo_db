
from syntax_tree import (
    ASTNode,
    SelectStatement,
    LogicalExpression,
    Comparison,
    Literal,
    ColumnRef,
    AggregateCall,
)
from operators import (
    Filter,
    BaseIterator,
    Projection,
    Sorter,
    Limit,
    LogicalFilter,
    Aggregate,
    Distinct,
)
from catalog import Catalog


class QueryPlanner:
    def __init__(self, catalog: Catalog, buffer_manager):
        self.catalog = catalog
        self.buffer_manager = buffer_manager

    def plan_query(self, ast_root: ASTNode):
        if not isinstance(ast_root, SelectStatement):
            raise TypeError(f"Unsupported AST root: {type(ast_root)}")

        return self._plan_select(ast_root)

    def _plan_select(self, stmt: SelectStatement):
        table = stmt.table

        # 1. Table scan
        data_gen = self.buffer_manager.get_data_generator(table)
        plan = BaseIterator(table, data_gen, self.catalog)

        # 2. WHERE
        if stmt.where_clause:
            plan = self._plan_where(stmt.where_clause, plan, table)

        # 3. AGGREGATION / GROUP BY
        aggregate_specs = self._extract_aggregate_specs(stmt.columns, table)
        if aggregate_specs or stmt.group_by_clause:
            group_key_indices = (
                self._resolve_group_keys(stmt.group_by_clause, table)
                if stmt.group_by_clause
                else []
            )

            plan = Aggregate(
                group_key_indices=group_key_indices,
                aggregate_specs=aggregate_specs,
                parent=plan,
            )

        # 4. DISTINCT 
        if stmt.is_distinct and not stmt.group_by_clause:  #  distinct has no affect in combination with a group by clause (except COUNT(DISTINCT .))
            plan = self._plan_projection(stmt.columns, plan)
            schema = plan.get_output_schema_names()
            plan = Distinct(list(range(len(schema))), plan)        

        # 5. ORDER BY
        if stmt.order_by_clause:
            plan = self._plan_order_by(stmt.order_by_clause, plan)

        # 6. LIMIT
        if stmt.limit_clause:
            plan = Limit(stmt.limit_clause.count, plan)

        # 7. FINAL PROJECTION
        if not stmt.is_distinct or stmt.group_by_clause:
            plan = self._plan_projection(stmt.columns, plan)

        return plan

    def _plan_where(self, ast_node, parent, table_name):
        if isinstance(ast_node, Comparison):
            return self._plan_comparison(ast_node, parent, table_name)

        if isinstance(ast_node, LogicalExpression):
            left = self._plan_where(ast_node.left, parent, table_name)
            right = self._plan_where(ast_node.right, parent, table_name)
            return LogicalFilter(ast_node.op, left, right, parent)

        raise TypeError(f"Unsupported WHERE node: {type(ast_node)}")

    def _plan_comparison(self, cmp: Comparison, parent, table):
        def resolve(operand):
            if isinstance(operand, Literal):
                return operand.value, None
            if isinstance(operand, ColumnRef):
                idx = self.catalog.get_column_index(table, operand.name)
                return None, idx
            raise TypeError(type(operand))

        v1, i1 = resolve(cmp.left)
        v2, i2 = resolve(cmp.right)

        return Filter(
            comparison=cmp.op,
            parent=parent,
            val1=v1,
            val2=v2,
            col_idx1=i1,
            col_idx2=i2,
        )

    def _extract_aggregate_specs(self, columns, table):
        specs = []
        for col in columns:
            if isinstance(col, AggregateCall):
                if col.argument == "*":
                    specs.append((col.function_name, "*"))
                else:
                    idx = self.catalog.get_column_index(table, col.argument)
                    specs.append((col.function_name, idx))
        return specs

    def _resolve_group_keys(self, group_by_clause, table):
        return [self.catalog.get_column_index(table, col.name) for col in group_by_clause.columns]

    def _plan_order_by(self, order_by_clause, plan):
        schema = plan.get_output_schema_names()
        name_to_index = {name.upper(): i for i, name in enumerate(schema)}

        sort_keys = []
        for item in order_by_clause.sort_items:
            if isinstance(item.column, AggregateCall):
                name = f"{item.column.function_name.upper()}({item.column.argument.upper()})"
            else:
                name = item.column.name.upper()

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
                names.append(col.name.upper())

            elif isinstance(col, AggregateCall):
                if col.argument == "*":
                    name = f"{col.function_name.upper()}(*)"
                else:
                    name = f"{col.function_name.upper()}({col.argument})"

                idx = input_schema.index(name)
                indices.append(idx)
                names.append(name)

            elif isinstance(col, Literal):
                raise NotImplementedError("Literal SELECT items not supported")

        return Projection(indices, names, plan)
