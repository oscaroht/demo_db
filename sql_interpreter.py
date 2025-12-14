from enum import StrEnum
from syntax_tree import SelectStatement, LogicalExpression, Comparison, Literal, ColumnRef, AggregateCall, SortItem, OrderByClause, GroupByClause, LimitClause
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


class qseparators(StrEnum):
    SEMICOLON = ';'
    COMMA = ','
    # DOT = '.'
    WHITESPACE = ' '


token_separators = [e.value for e in qarithmaticoperators] + [e.value for e in qseparators] + [e.value for e in qcomparators]
keywords_set = set([e.value for e in qtype] + [e.value for e in qtrans])

def tokenize(query: str) -> list[str]:
    """The goal is to split the function by whitespace, comma, dot and semicolon."""

    token_separators.sort(key=lambda s: len(s), reverse=True)

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

        for k in token_separators:
            if len(query) >= char_index+len(k) and query[char_index:char_index+len(k)] == k:
                token = query[prev_char_index:char_index]
                if token != '':
                    tokens.append(token)
                tokens.append(k)
                char_index += len(k)
                prev_char_index = char_index
                break
        else:
            char_index += 1
    if query[prev_char_index:char_index] != '':
        tokens.append(query[prev_char_index:char_index])
    return tokens
       

class TokenStream:
    def __init__(self, tokens):
        # Filter out WHITESPACE tokens and make keywords uppercase
        self.tokens = [
            t.upper() if t.upper() in keywords_set else t
            for t in tokens if t != qseparators.WHITESPACE
        ]
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
        Parses: SELECT ... FROM ... [ WHERE ... ] [ GROUP BY ... ] [ ORDER BY ... ] [ LIMIT ... ] ;
        """
        
        self.stream.match(qtype.SELECT)
        is_distinct = False
        if self.stream.current() == qtrans.DISTINCT:
            self.stream.match(qtrans.DISTINCT)
            is_distinct = True
        columns = self._parse_column_list()
        
        # ... (Parsing FROM and WHERE remains the same) ...
        if self.stream.current() == 'FROM':
            self.stream.match('FROM')
            table_name = self._parse_identifier()
        else:
            raise SyntaxError("Expected 'FROM'.")
        
        where_clause = None
        if self.stream.current() == qtrans.WHERE:
            self.stream.match(qtrans.WHERE)
            where_clause = self._parse_logical_expression()
            
        # 1. Parse optional GROUP BY clause (NEW LOGIC)
        group_by_clause = None
        if self.stream.current() == qtrans.GROUP:
            self.stream.match(qtrans.GROUP)
            self.stream.match(qtrans.BY)
            group_by_clause = self._parse_group_by()
            
        # 2. Parse optional ORDER BY clause (Existing Logic)
        order_by_clause = None
        if self.stream.current() == qtrans.ORDER:
            self.stream.match(qtrans.ORDER)
            self.stream.match(qtrans.BY)
            order_by_clause = self._parse_order_by()
            
        # 3. Parse optional LIMIT clause (Existing Logic)
        limit_clause = None
        if self.stream.current() == qtrans.LIMIT:
            self.stream.match(qtrans.LIMIT)
            limit_clause = self._parse_limit()
            
        self.stream.match(qseparators.SEMICOLON)
        
        return SelectStatement(columns, table_name, 
                               is_distinct=is_distinct,
                               where_clause=where_clause,
                               group_by_clause=group_by_clause, # Added to SelectStatement
                               order_by_clause=order_by_clause, 
                               limit_clause=limit_clause)
    
    def _parse_distinct(self):
        """Parse """

    def _parse_group_by(self):
        """Parses: col1, col2, ..."""
        group_columns = []
        while True:
            # Grouping columns must be simple ColumnRef nodes
            col_ref = ColumnRef(name=self._parse_identifier())
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
            col_ref = self._parse_column_or_aggregate()
            
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
        Parses: * | column_ref | aggregate_call ( , ... )*
        """
        columns = []
        
        # Check for SELECT *
        if self.stream.current() == '*':
            columns.append(ColumnRef(name='*'))
            self.stream.advance()
        else:
            # Parse one or more columns/aggregates separated by commas
            while True:
                columns.append(self._parse_column_or_aggregate())
                
                if self.stream.current() == qseparators.COMMA:
                    self.stream.match(qseparators.COMMA)
                else:
                    break
                    
        return columns

    
    def _parse_column_or_aggregate(self):
        """
        Parses a single column or aggregate function call.
        (e.g., id, name, COUNT(*), MAX(price))
        """
        
        current = self.stream.current()
        
        # Check for aggregate functions
        if current in ['COUNT', 'MIN', 'MAX', 'AVG', 'SUM']:
            function_name = self.stream.match(current)
            self.stream.match('(')
            
            # Check for COUNT(*)
            if self.stream.current() == '*':
                argument = self.stream.match('*')
            else:
                argument = self._parse_identifier() # Column inside the aggregate
            
            self.stream.match(')')
            return AggregateCall(function_name, argument=argument)
            
        # Otherwise, it's a simple column reference (identifier)
        return ColumnRef(name=self._parse_identifier())

    
    def _parse_identifier(self):
        """Helper to parse a table name or column name."""
        # Simple rule: an identifier is any token that is not a reserved keyword or separator/operator
        token = self.stream.current()
        # In a full parser, you'd check if 'token' is a keyword, but for this simple version, 
        # we'll assume the token is a valid identifier.
        if token is None:
            raise SyntaxError("Expected an identifier, got end of stream.")
        
        self.stream.advance()
        return token


    # --- Recursive Descent for WHERE Clause Logic ---
    # Following the precedence rule (AND > OR)

    def _parse_logical_expression(self):
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

    def _parse_and_expression(self):
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

    def _parse_comparison(self):
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


    def _parse_operand(self):
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
        return ColumnRef(name=self._parse_identifier())



if __name__ == "__main__":
    pass

