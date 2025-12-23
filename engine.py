from re import error
import traceback
from operators import Operator

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
        tokens = []
        ast_root = None
        query_plan_root = None
        try:
            if sql[-1] != ';':
                sql += ';'
            tokens = tokenize(sql)
            stream = TokenStream(tokens)
            parser = Parser(stream) 
            ast_root = parser.parse()

            planner = QueryPlanner(self.catalog, self.buffer_manager)
            query_plan_root: Operator = planner.plan_query(ast_root)
            # error_message += query_plan_root.display_plan()

            rows = list(query_plan_root.next())

            return QueryResult(
                columns=query_plan_root.get_output_schema_names(),
                rows=rows,
                sql=sql,
                tokens=tokens,
                ast=ast_root,
                query_plan=query_plan_root,
                rowcount=len(rows)
            )

        except Exception:
            return QueryResult(
                columns=[],
                rows=[],
                sql=sql,
                tokens=tokens,
                ast=ast_root,
                query_plan=query_plan_root,
                error=traceback.format_exc(),
            )
