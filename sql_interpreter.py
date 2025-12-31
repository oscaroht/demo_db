from enum import StrEnum, auto
from typing import Tuple, List
from syntax_tree import Expression, SelectStatement, LogicalExpression, Comparison, Literal, ColumnRef, AggregateCall, SortItem, OrderByClause, GroupByClause, LimitClause, Join, TableRef, Star
import re

class qtype(StrEnum):
    SELECT = 'SELECT'
    CREATE = 'CREATE'
    DELETE = 'DELETE'
    INSERT = 'INSERT'

class qtrans(StrEnum):
    WHERE = 'WHERE'
    ORDER = 'ORDER'
    GROUP = 'GROUP'
    DISTINCT = 'DISTINCT'
    BY = 'BY'
    LIMIT = 'LIMIT'
    AND = 'AND'
    OR = 'OR'
    COUNT = 'COUNT'
    MIN = 'MIN'
    MAX = 'MAX'
    AVG = 'AVG'
    SUM = 'SUM'
    DESC = 'DESC'
    ASC = 'ASC'
    FROM = 'FROM'
    AS = 'AS'
    JOIN = 'JOIN'
    ON = 'ON'

class qarithmaticoperators(StrEnum):
    ADD = '+'
    SUB = '-'
    DIV = '/'
    PRD = '*'
    OB = '('
    CB = ')'

class qcomparators(StrEnum):
    EQ = '='
    NEQ = '!='
    LT = '<'
    GT = '>'
    LE = '<='
    GE = '>='

class qwhitespaces(StrEnum):
    NEWLINE = '\n'
    TAB = '\t'
    WHITESPACE = ' '

class qseparators(StrEnum):
    SEMICOLON = ';'
    COMMA = ','

token_separators = [e.value for e in qarithmaticoperators] + [e.value for e in qseparators] + [e.value for e in qcomparators] + [e.value for e in qwhitespaces]
keywords_set = set([e.value for e in qtype] + [e.value for e in qtrans])

def tokenize(query: str) -> list[str]:
    """The goal is to split the function by whitespace, comma, dot and semicolon."""

    token_separators.sort(key=lambda s: len(s), reverse=True)  # this list is iterated to match with tokens. Longest tokens should go first

    tokens = []
    char_index = 0
    prev_char_index = char_index
    while char_index < len(query):
        if query[char_index] == r"'":
            # literal detection
            # look ahead for the closing quote
            start_quote_index = char_index
            char_index += 1
            while char_index < len(query) and not (query[char_index] == "'" and query[char_index-1] != "\\"):  # escape ' is handled this way. -1 is safe because the cursor has advanced by one in prev line
                char_index += 1
            if char_index == len(query):
                raise SyntaxError("Unclosed string literal in query.")

            literal_token = query[start_quote_index : char_index + 1]
            tokens.append(literal_token)
            
            char_index += 1 # Move past the closing quote
            prev_char_index = char_index
            continue

        # check for separator 
        # nested loop hurts a bit but performance gains in tokenizer are tiny compared to overal performance
        for k in token_separators:
            if len(query) >= char_index+len(k) and query[char_index:char_index+len(k)] == k:
                # append previous token
                token = query[prev_char_index:char_index]
                if token != '':
                    tokens.append(token if token.upper() not in keywords_set else token.upper())
                if k not in qwhitespaces:
                    tokens.append(k)
                char_index += len(k)
                prev_char_index = char_index
                break
        else:
            char_index += 1

    token = query[prev_char_index:char_index]
    if token != '':
        tokens.append(token if token.upper() not in keywords_set else token.upper())
    return tokens
       

class TokenStream:
    def __init__(self, tokens):
        self.tokens = tokens
        self.pos = 0

    def current(self):
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return None

    def advance(self):
        self.pos += 1

    def match(self, expected_token):
        """Matches the current token against an expected token and advances."""
        token = self.current()
        if token == expected_token:
            self.advance()
            return token
        raise SyntaxError(f"Expected '{expected_token}', got '{token}' at token position {self.pos}")
    
    def peek(self, offset=1):
        """Looks ahead without advancing."""
        if self.pos + offset < len(self.tokens):
            return self.tokens[self.pos + offset]
        return None


class Parser:
    
    def __init__(self, ts: TokenStream):
        self.stream = ts

    def parse(self):
        """Entry point: Parses a single SQL statement."""
        
        current_token = self.stream.current()

        if current_token == qtype.SELECT:
            return self._parse_select_statement()
        # Add elif for CREATE, INSERT, DELETE here later
        
        raise SyntaxError(f"Unsupported query type: {current_token}")


    def _parse_select_statement(self):
        """
        Parses: SELECT [ DISTINCT ] ... FROM ... [ WHERE ... ] [ GROUP BY ... ] [ ORDER BY ... ] [ LIMIT ... ] ;
        """
        
        self.stream.match(qtype.SELECT)
        is_distinct = False
        if self.stream.current() == qtrans.DISTINCT:
            self.stream.match(qtrans.DISTINCT)
            is_distinct = True

        columns = self._parse_column_list()

        from_clause = self._parse_from_clause()

        where_clause = None
        if self.stream.current() == qtrans.WHERE:
            self.stream.match(qtrans.WHERE)
            where_clause = self._parse_logical_expression()
            
        group_by_clause = None
        if self.stream.current() == qtrans.GROUP:
            self.stream.match(qtrans.GROUP)
            self.stream.match(qtrans.BY)
            group_by_clause = self._parse_group_by()
            
        order_by_clause = None
        if self.stream.current() == qtrans.ORDER:
            self.stream.match(qtrans.ORDER)
            self.stream.match(qtrans.BY)
            order_by_clause = self._parse_order_by()
            
        limit_clause = None
        if self.stream.current() == qtrans.LIMIT:
            self.stream.match(qtrans.LIMIT)
            limit_clause = self._parse_limit()
            
        self.stream.match(qseparators.SEMICOLON)
        
        return SelectStatement(columns=columns, 
                               from_clause=from_clause,
                               is_distinct=is_distinct,
                               where_clause=where_clause,
                               group_by_clause=group_by_clause,
                               order_by_clause=order_by_clause, 
                               limit_clause=limit_clause)



    def _parse_table_ref(self) -> TableRef:
        name = self._parse_table_identifier()
        alias = None

        if self.stream.current() == qtrans.AS:
            self.stream.match(qtrans.AS)
            alias = self._parse_table_identifier()
        # for now no implicait alias
        # elif self.stream.current_is_identifier():
            # implicit alias: "users u"
            # alias = self._parse_identifier()

        return TableRef(name, alias)


    def _parse_from_clause(self) -> TableRef | Join:
        self.stream.match(qtrans.FROM)

        left: TableRef = self._parse_table_ref()

        while self.stream.current() == qtrans.JOIN:
            self.stream.match(qtrans.JOIN)
            right = self._parse_table_ref()

            self.stream.match(qtrans.ON)
            condition = self._parse_logical_expression()

            left = Join(left, right, condition)

        return left


    def _parse_group_by(self):
        """Parses: col1, col2, ..."""
        group_columns = []
        while True:
            # Grouping columns must be simple ColumnRef nodes
            col_ref = ColumnRef(*self._parse_column_identifier())
            group_columns.append(col_ref)
            
            if self.stream.current() == qseparators.COMMA:
                self.stream.match(qseparators.COMMA)
            else:
                break
                
        return GroupByClause(group_columns)       
    def _parse_order_by(self):
        """Parses: col1 [ASC|DESC], col2 [ASC|DESC], ..."""
        sort_items = []
        while True:
            # Column name reference
            col_ref = self._parse_expression()
            
            # Check for optional direction (ASC/DESC)
            direction = 'ASC'
            current = self.stream.current()
            if current in ['ASC', 'DESC']:
                direction = self.stream.match(current)
                
            sort_items.append(SortItem(col_ref, direction))
            
            # Look for subsequent columns separated by commas
            if self.stream.current() == qseparators.COMMA:
                self.stream.match(qseparators.COMMA)
            else:
                break
                
        return OrderByClause(sort_items)
        
    def _parse_limit(self):
        """Parses: integer_literal"""
        # The LIMIT value must be a literal (number)
        limit_operand = self._parse_operand()
        
        if not isinstance(limit_operand, Literal):
             raise SyntaxError("LIMIT must be followed by a numeric literal.")
                
        return LimitClause(limit_operand.value)    

    def _parse_column_list(self):
        """
        Parses: * | column_ref | aggregate_call ( , ... ) | literal
        """
        columns = []
        
        # Parse one or more columns/aggregates separated by commas
        while True:
            columns.append(self._parse_expression())
            
            if self.stream.current() == qseparators.COMMA:
                self.stream.match(qseparators.COMMA)
            else:
                break
                    
        return columns

    
    def _parse_expression(self) -> Expression:
        """
        Parses a single column or aggregate function call.
        (e.g., id, name, COUNT(*), MAX(price), name as first_name)
        """
        
        current = self.stream.current()
        if current is None:
            raise SyntaxError(f"Expected expression (column ref, function call, literal) found end of tokens.")

        if current == '*':
            self.stream.advance()
            return Star()
        
        # Check for aggregate functions
        if current in ['COUNT', 'MIN', 'MAX', 'AVG', 'SUM']:
            function_name = self.stream.match(current)
            self.stream.match('(')
            is_distinct = False 
            # Check for COUNT(DISTINCT ..)
            if current == qtrans.COUNT and self.stream.current() == qtrans.DISTINCT:
                is_distinct = True
                self.stream.advance()
            # Check for COUNT(*)
            if self.stream.current() == '*':
                argument = self.stream.match('*')
                aggcol = Star()
            else:
                argument = self._parse_column_identifier() # Column inside the aggregate
                aggcol = ColumnRef(*argument)
            
            self.stream.match(')')
            aggcall = AggregateCall(function_name, aggcol, is_distinct=is_distinct)
            aggcall.alias = self._parse_alias()
            return aggcall
        
        if current.startswith("'") and current.endswith("'") or current.isdigit():
            col = self._parse_literal()
        else:
            table, col_name = self._parse_column_identifier()
            col = ColumnRef(table, col_name)
        col.alias = self._parse_alias()
        return col

    def _parse_alias(self) -> None | str:
        if self.stream.current() == 'AS':
            self.stream.match('AS')
            alias = self.stream.current()
            self.stream.advance()
            return alias

    def _parse_table_identifier(self) -> str:
        """Parse a table name."""
        token = self.stream.current()
        if token is None:
            raise SyntaxError("Expected an identifier, got end of stream.")
        self.stream.advance()
        return token

    def _parse_column_identifier(self) -> Tuple[None | str, str]:
        """Parse a table name or column name."""
        token = self.stream.current()
        if token is None:
            raise SyntaxError("Expected an identifier, got end of stream.")
        table = None
        column = token
        if '.' in token:
            parts: List[str] = token.split('.')
            if len(parts) > 2:
                raise SyntaxError(f"Identifier {token} has multiple '.'. Expected table.column")
            table, column = parts[0], parts[1]
        
        self.stream.advance()
        return table, column

    def _parse_literal(self) -> Literal:
        token: None | str = self.stream.current()
        if token is None:
            raise SyntaxError("Expected literal value not end of query")
        value: str | int = token
        if token.isdigit():
            value = int(token)
        else:
            value = token[1:-1]  # strip ' and '
        self.stream.advance()
        return Literal(value)

    def _parse_logical_expression(self) -> Comparison | LogicalExpression:
        """
        Handles OR conditions (Lowest Precedence).
        <LogicalExpression> -> <AndExpression> ( OR <AndExpression> )*
        """
        left_node = self._parse_and_expression()

        while self.stream.current() == qtrans.OR:
            op = self.stream.match(qtrans.OR)
            right_node = self._parse_and_expression()
            left_node = LogicalExpression(op, left_node, right_node)
            
        return left_node

    def _parse_and_expression(self) -> Comparison | LogicalExpression:
        """
        Handles AND conditions (Higher Precedence).
        <AndExpression> -> <Comparison> ( AND <Comparison> )*
        """
        left_node = self._parse_comparison()

        while self.stream.current() == qtrans.AND:
            op = self.stream.match(qtrans.AND)
            right_node = self._parse_comparison()
            left_node = LogicalExpression(op, left_node, right_node)
        return left_node

    def _parse_comparison(self) -> Comparison | LogicalExpression:
        """
        Handles simple comparisons or parenthesized expressions.
        <Comparison> -> <Operand> <Operator> <Operand> | ( <LogicalExpression> )
        """
        
        current = self.stream.current()
        
        if current == '(':
            self.stream.match('(')
            node = self._parse_logical_expression() # Recurse back to the highest precedence
            self.stream.match(')')
            return node
        
        left_operand = self._parse_operand()
        
        # Check for comparison operator
        comparison_op = self.stream.current()
        if comparison_op in [e.value for e in qcomparators]:
            self.stream.advance()
            right_operand = self._parse_operand()
            return Comparison(comparison_op, left_operand, right_operand)

        # If it wasn't a comparison and not parentheses, it's an error
        raise SyntaxError(f"Expected comparison operator or '(' at: {current}")


    def _parse_operand(self) -> ColumnRef | Literal:
        """
        Parses a single operand: ColumnRef or Literal value.
        """
        token = self.stream.current()

        if token is None:
            raise SyntaxError("Expected an operand, got end of stream.")

        # 1. String Literal Check (The fix from last time, now robust!)
        if token.startswith("'") and token.endswith("'"):
            self.stream.advance()
            # Pass the stripped value as a Literal
            return Literal(token.strip("'").replace(r"\'", "'") )

        if token.isdigit():
            self.stream.advance()
            return Literal(int(token))

        if re.match(r'\d*[.]\d+', token):
            # Simple conversion to Literal node
            self.stream.advance()
            # Try to cast to integer or float
            return Literal(float(token))
        
        # Otherwise, assume it's a Column Reference (identifier)
        return ColumnRef(*self._parse_column_identifier())



if __name__ == "__main__":
    pass

