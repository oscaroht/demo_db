from re import error
import traceback

import buffermanager
from catalog import Catalog
from request import QueryRequest
from result import QueryResult
from queryplanner import QueryPlanner
from sql_interpreter import tokenize, TokenStream, Parser

class DatabaseEngine:
    def __init__(self, catalog, buffermanager):
        self.catalog = catalog
        self.buffer_manager = buffermanager

    def execute(self, request: QueryRequest) -> QueryResult:
        sql = request.sql
        error_message = ''
        try:
            if sql[-1] != ';':
                sql += ';'
            tokens = tokenize(sql)
            error_message += str(tokens) + '\n'
            stream = TokenStream(tokens)
            parser = Parser(stream) 
            ast_root = parser.parse()
            error_message += ast_root.display() + '\n'

            planner = QueryPlanner(self.catalog, self.buffer_manager)
            query_plan_root = planner.plan_query(ast_root)
            error_message += query_plan_root.display_plan()

            rows = list(query_plan_root.next())

            return QueryResult(
                columns=query_plan_root.get_output_schema_names(),
                rows=rows,
                sql=sql,
                rowcount=len(rows)
            )

        except Exception as e:
            return QueryResult(
                columns=[],
                rows=[],
                sql=sql,
                error=error_message + '\n' + traceback.format_exc(),
            )
