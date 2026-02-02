from operators import Operator
import traceback
from typing import List

from enums import TransactionStatus
from errors import DBError, ParserError, SQLSyntaxError, TableNotFoundError
from request import QueryRequest
from result import QueryResult
from queryplanner import QueryPlanner
from schema import Schema
from sql_interpreter import tokenize, TokenStream, Parser
from syntax_tree import TransactionModifier, BeginStatement, CommitStatement, RollbackStatement
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

    def get_annonimous_transaction(self):
        transaction = Transaction(-1, self.buffer_manager, self.catalog)
        self.transactions[-1] = transaction
        return transaction

    def get_new_transaction(self):
        new_id = max(self.transactions.keys()) + 1 if self.transactions.keys() else 1
        transaction = Transaction(new_id, self.buffer_manager, self.catalog)
        self.transactions[new_id] = transaction
        return transaction

    def execute(self, request: QueryRequest) -> QueryResult:
        sql = request.sql
        tokens = []
        ast_root = None
        query_plan_root = None
        transaction = None
        transaction_status = TransactionStatus.OPEN
        try:
            if sql[-1] != ';':
                sql += ';'
            tokens = tokenize(sql)
            stream = TokenStream(tokens)
            parser = Parser(stream) 
            ast_root = parser.parse()
            # if isinstance(ast_root, BeginStatement) and request.auto_commit:
            #     raise Exception("Cannot use auto commit with transaction modifiers.")
            if isinstance(ast_root, BeginStatement) and request.transaction_id != -1:
                raise ParserError("First commit or rollback current transaction.")
            if isinstance(ast_root, (CommitStatement, RollbackStatement)) and request.transaction_id == -1:
                raise ParserError("Cannot end transaction before starting one.")

            if isinstance(ast_root, BeginStatement):
                transaction = self.get_new_transaction()
                return QueryResult(columns=['status'], rows=[('Success',)], sql=sql, tokens=tokens, ast=ast_root, query_plan=query_plan_root, rowcount=1, transaction_status=transaction_status, transaction_id=transaction.id)

            if request.transaction_id != -1:
                transaction = self.get_transaction_by_id(request.transaction_id)
            elif request.auto_commit:
                transaction = self.get_annonimous_transaction()
            else:
                transaction = self.get_new_transaction()

            if isinstance(ast_root, CommitStatement):
                transaction.commit()
                del self.transactions[transaction.id]
                return QueryResult(columns=['status'], rows=[('Success',)], sql=sql, tokens=tokens, ast=ast_root, query_plan=query_plan_root, rowcount=1, transaction_status=TransactionStatus.CLOSED, transaction_id=transaction.id)
            if isinstance(ast_root, RollbackStatement):
                transaction.rollback()
                del self.transactions[transaction.id]
                return QueryResult(columns=['status'], rows=[('Success',)], sql=sql, tokens=tokens, ast=ast_root, query_plan=query_plan_root, rowcount=1, transaction_status=TransactionStatus.CLOSED, transaction_id=transaction.id)  # put tranaction id back to -1

            planner = QueryPlanner(transaction)
            query_plan_root: Operator = planner.plan_query(ast_root)

            rows = [row for row, _ ,_ in query_plan_root.next()]

            schema: Schema = query_plan_root.get_output_schema()
            column_names: List[str] = schema.get_names()

            if request.auto_commit and transaction.id == -1:
                # if this is an annonimous transaction commit it
                transaction.commit()
                del self.transactions[transaction.id]
                transaction_status = TransactionStatus.CLOSED

            return QueryResult(
                columns=column_names,
                rows=rows,
                sql=sql,
                tokens=tokens,
                ast=ast_root,
                query_plan=query_plan_root,
                rowcount=len(rows),
                transaction_status=transaction_status,
                transaction_id=transaction.id
            )
        # except ParserError:
        #     return QueryResult(
        #         columns=['status'],
        #         rows=[('ParserError',)],
        #         sql=sql,
        #         tokens=tokens,
        #         ast=ast_root,
        #         query_plan=query_plan_root,
        #         error=traceback.format_exc(),
        #         transaction_id=transaction.id if transaction else request.transaction_id,
        #         transaction_status=transaction_status
        #     )

        except Exception as e:
            if transaction and getattr(e, 'rollback', True):
                transaction.rollback()
                transaction_status = TransactionStatus.CLOSED
            return QueryResult(
                columns=['status'],
                rows=[('Error',)],
                sql=sql,
                tokens=tokens,
                ast=ast_root,
                query_plan=query_plan_root,
                error=traceback.format_exc(),
                transaction_id=transaction.id if transaction else request.transaction_id,
                transaction_status=transaction_status
            )
