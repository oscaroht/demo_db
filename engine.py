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
        try:
            tokens = tokenize(request.sql)
            stream = TokenStream(tokens)
            parser = Parser(stream) 
            ast_root = parser.parse()

            planner = QueryPlanner(self.catalog, self.buffer_manager)
            query_plan_root = planner.plan_query(ast_root)

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
                error=str(e)
            )
