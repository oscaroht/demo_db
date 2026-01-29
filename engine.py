from operators import Operator
import traceback
from typing import List

from request import QueryRequest
from result import QueryResult
from queryplanner import QueryPlanner
from schema import Schema
from sql_interpreter import tokenize, TokenStream, Parser
from transaction import Transaction

class DatabaseEngine:
    def __init__(self, catalog, buffermanager):
        self.catalog = catalog
        self.buffer_manager = buffermanager
        self.transactions = {}  # id: transaction

    def get_transaction_by_id(self, id: int):
        t = self.transactions.get(id)
        if t is None:
            raise Exception(f"No transaction with id {id}")
        return t

    def get_new_transaction(self):
        new_id = max(self.transactions.keys()) + 1 if self.transactions.keys() else 1
        transaction = Transaction(new_id, self.buffer_manager, self.catalog)
        self.transactions[new_id] = transaction
        return transaction

    def execute(self, request: QueryRequest) -> QueryResult:
        sql = request.sql
        if request.transaction_id == -1:
            # new transaction
            transaction = self.get_new_transaction()
        else:
            transaction = self.get_transaction_by_id(request.transaction_id)
            
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
            planner = QueryPlanner(transaction)
            query_plan_root: Operator = planner.plan_query(ast_root)

            rows = list(query_plan_root.next())

            schema: Schema = query_plan_root.get_output_schema()
            column_names: List[str] = schema.get_names()

            if request.auto_commit:
                transaction.commit()

            return QueryResult(
                columns=column_names,
                rows=rows,
                sql=sql,
                tokens=tokens,
                ast=ast_root,
                query_plan=query_plan_root,
                rowcount=len(rows),
                transaction_id=transaction.id
            )

        except Exception:
            transaction.rollback()
            return QueryResult(
                columns=[],
                rows=[],
                sql=sql,
                tokens=tokens,
                ast=ast_root,
                query_plan=query_plan_root,
                error=traceback.format_exc(),
                transaction_id=transaction.id
            )
        finally:
            if request.auto_commit and transaction.id in self.transactions:
                del self.transactions[transaction.id]
