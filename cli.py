from request import QueryRequest
from result import QueryResult
from engine import DatabaseEngine
from sql_interpreter import qtrans, qcomparators, qarithmaticoperators, qseparators, qtype

import readline
import re
from prompt_toolkit.lexers import Lexer
from prompt_toolkit.styles import Style
from prompt_toolkit import PromptSession

import os
import sys

def clear_screen():
    if sys.platform == "win32":
        os.system("cls")
    else:
        os.system("clear")

SQL_STYLE = Style.from_dict({
    "keyword": "bold ansiblue",
    "operator": "ansiyellow",
    "comparator": "ansired",
    "number": "ansimagenta",
    "string": "ansigreen",
    "punctuation": "ansicyan",
    "default": "",
})

KEYWORDS = {e.value.upper() for e in qtype} | {e.value.upper() for e in qtrans}
OPERATORS = {e.value for e in qarithmaticoperators}
COMPARATORS = {e.value for e in qcomparators}

class SQLLexer(Lexer):
    def lex_document(self, document):
        lines = document.text.splitlines()

        def get_line(lineno):
            if lineno >= len(lines):
                return []

            line = lines[lineno]
            tokens = []

            i = 0
            while i < len(line):
                c = line[i]

                # Match multi-character tokens first
                if line[i:i+2] in COMPARATORS:
                    tokens.append(("class:comparator", line[i:i+2]))
                    i += 2
                elif c in COMPARATORS or c in OPERATORS:
                    tokens.append(("class:operator", c))
                    i += 1
                elif c.isspace():
                    tokens.append(("class:default", c))
                    i += 1
                else:
                    # Match words (keywords, identifiers, numbers, strings)
                    m = re.match(r"[a-zA-Z_][a-zA-Z0-9_]*", line[i:])
                    if m:
                        word = m.group(0)
                        if word.upper() in KEYWORDS:
                            tokens.append(("class:keyword", word))
                        else:
                            tokens.append(("class:identifier", word))
                        i += len(word)
                    else:
                        # Any other single char
                        tokens.append(("class:default", c))
                        i += 1

            return tokens

        return get_line

session = PromptSession(
    lexer=SQLLexer(),
    style=SQL_STYLE,
    multiline=False,
    prompt_continuation="... ",
)


logo = """


 ██████╗ ███████╗ ██████╗ █████╗ ██████╗     ██████╗ ██████╗ 
██╔═══██╗██╔════╝██╔════╝██╔══██╗██╔══██╗    ██╔══██╗██╔══██╗
██║   ██║███████╗██║     ███████║██████╔╝    ██║  ██║██████╔╝
██║   ██║╚════██║██║     ██╔══██║██╔══██╗    ██║  ██║██╔══██╗
╚██████╔╝███████║╚██████╗██║  ██║██║  ██║    ██████╔╝██████╔╝
 ╚═════╝ ╚══════╝ ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝    ╚═════╝ ╚═════╝ 


"""



def repl(engine: DatabaseEngine, prompt_session: None | PromptSession = None ):

    welcome_msg = """Welcome to Oscar db. This is a db for demonstration purposes. Currently there 2 tables: employee and contract \n 
Supported: SELECT ... FROM users [ WHERE ... ] [ GROUP BY ... ] [ ORDER BY ... ] [ LIMIT ... ] ;

Suggestion for first query: SELECT * FROM users;

exit  - exit program
quit  - same as exit
clear - clear the terminal text
                      """
    print(logo)
    print(welcome_msg)
    while True:
        try:
            get_input = input
            if prompt_session:
                get_input = prompt_session.prompt
            sql = get_input("db> ").strip()

            if not sql:
                continue
            if sql.lower() in {"exit", "quit"}:
                break
            if sql.lower() == "clear":
                clear_screen()
                continue

            request = QueryRequest(sql=sql)
            result = engine.execute(request)
            render_result(result)

        except KeyboardInterrupt:
            print("\nbye")
            break

def render_result(result: QueryResult):
    """
    Formats and prints query results in an ASCII table.
    """

    query_string = result.sql  # destructure to reuse old code
    columns = result.columns
    results = result.rows

    if result.ast:
        print("\n" + "="*80)
        print(f"ABSTRACT SYNTAX TREE:")
        print(result.ast.display())

    if result.query_plan:
        print("\n" + "="*80)
        print("QUERY PLAN:")
        print(result.query_plan.display_plan())

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
