from enums import TransactionStatus
from request import QueryRequest
from result import QueryResult
from engine import DatabaseEngine

import readline

import os
import sys

def clear_screen():
    if sys.platform == "win32":
        os.system("cls")
    else:
        os.system("clear")

logo = """


 ██████╗ ███████╗ ██████╗ █████╗ ██████╗     ██████╗ ██████╗ 
██╔═══██╗██╔════╝██╔════╝██╔══██╗██╔══██╗    ██╔══██╗██╔══██╗
██║   ██║███████╗██║     ███████║██████╔╝    ██║  ██║██████╔╝
██║   ██║╚════██║██║     ██╔══██║██╔══██╗    ██║  ██║██╔══██╗
╚██████╔╝███████║╚██████╗██║  ██║██║  ██║    ██████╔╝██████╔╝
 ╚═════╝ ╚══════╝ ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝    ╚═════╝ ╚═════╝ 


"""



def repl(engine: DatabaseEngine):

    welcome_msg = """Welcome to Oscar db. This is a db for educatioinal purposes. Begin by creating a table and insert some data. \n 

explain - turn on abstract syntax tree and query plan visualization
exit    - exit program
quit    - same as exit
clear   - clear the terminal text
                      """
    print(logo)
    print(welcome_msg)
    explain = False
    transaction_id = -1  # start a new transaction
    while True:
        try:
            prefix = f"({transaction_id}) " if transaction_id!= -1 else ""
            sql = input( prefix + "db> ").strip()

            if not sql:
                continue
            if sql.lower() in {"exit", "quit"}:
                break
            if sql.lower() == "clear":
                clear_screen()
                continue
            if sql.lower() == "explain":
                explain = True

            request = QueryRequest(sql=sql, transaction_id=transaction_id)
            result = engine.execute(request)
            render_result(result, explain)

            transaction_id = result.transaction_id  # if transaction has started, keep going
            if result.transaction_status == TransactionStatus.CLOSED:
                transaction_id = -1

        except KeyboardInterrupt:
            print("\nbye")
            break

def render_result(result: QueryResult, explain: bool):
    """
    Formats and prints query results in an ASCII table.
    """

    query_string = result.sql  # destructure to reuse old code
    columns = result.columns
    results = result.rows

    if explain:
        render_explain(result)

    print("\n" + "="*80)
    print(f"QUERY: {query_string}")
    print("="*80)
    
    if result.error:
        print("ERROR: " + str(result.error))
        return

    if not results:
        print("RESULT: (Empty set)")
        print("="*80)
        return

    string_results = [tuple(str(x) for x in row) for row in results]
    string_columns = [str(c) for c in columns]

    num_cols = len(string_columns)
    max_widths = [len(header) for header in string_columns]

    for row in string_results:
        for i in range(num_cols):
            max_widths[i] = max(max_widths[i], len(row[i]))

    col_widths = [w + 2 for w in max_widths]

    header_line = ""
    for i in range(num_cols):
        header_line += f"| {string_columns[i].center(col_widths[i] - 2)} "
    header_line += "|"
    
    separator = "+" + "+".join("-" * w for w in col_widths) + "+"

    print(separator)
    print(header_line)
    print(separator)

    for row in string_results:
        row_line = ""
        for i in range(num_cols):
            # Left-align the data
            row_line += f"| {row[i].ljust(col_widths[i] - 2)} "
        row_line += "|"
        print(row_line)

    print(separator)
    print(f"({len(results)} rows in set)")
    print("="*80)

def render_explain(result):
    if result.tokens:
        print("\n" + "="*80)
        print(f"TOKENS:")
        print(result.tokens)

    if result.ast:
        print("\n" + "="*80)
        print(f"ABSTRACT SYNTAX TREE:")
        print(result.ast.display())

    if result.query_plan:
        print("\n" + "="*80)
        print("QUERY PLAN:")
        print(result.query_plan.display_plan())
