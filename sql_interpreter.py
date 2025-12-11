from enum import StrEnum

class qtype(StrEnum):
    SELECT = 'SELECT'
    CREATE = 'CREATE'
    DELETE = 'DELETE'
    INSERT = 'INSERT'

class qtrans(StrEnum):
    WHERE = 'WHERE'
    ORDER = 'ORDER'
    GROUP = 'DELETE'
    LIMIT = 'LIMIT'

class qoperators(StrEnum):
    EQ = '='
    NEQ = '!='
    LT = '<'
    GT = '>'
    LE = '<='
    GE = '>='

def peek_tokenizer(user_input):
    """Non tested"""
    keywords = qtype.__members__.values() + qtrans.__members__.values() + qoperators.__members__.values()
    token = ''
    tokens = []
    while len(user_input) > 0:
        user_input = user_input.strip()
        for k in keywords:
            if user_input[0:len(k)] == k:
                if token != '':
                    tokens.append(token)
                    token = ''
                tokens.append(k)
                user_input = user_input[len(k):]
                break
        else:
            token += user_input[0]
            user_input = user_input[1:]
        
        


def tonkenizer(user_input):
    user_input = user_input.strip()
    if user_input == '':
        return []
    queries = [q for q in user_input.split(';') if q != '']  # if ; is at the beginning or end of the string the split command returns an empty string
    if len(queries)>1:
        raise NotImplementedError("insert only one query")
    query = queries[0]

    """Split by whitepace, but keep string literals intact. Use a stack to track the quotes."""
    
    quote_stack = []
    splits = []
    part = ''
    for char in query:
        if char in (' ', '>', '<', '!') and len(quote_stack) == 0:
            if part != '':
                splits.append(part)
            part = ''
            if char in ('>', '<', '!'):
                part += char
        elif char == '=' and len(quote_stack) == 0:
            part += char
            splits.append(part)
            part = ''
        elif char != ' ' or len(quote_stack) > 0:
            part += char

        if char == "'" and len(quote_stack)==0:
            """Start of a string literal"""
            quote_stack.append(char)
        elif char == "'" and len(quote_stack)>0:
            """End of a string literal"""
            quote_stack.pop()
    splits.append(part)  # append the last part after the loop ends

    return splits 

def parser(tokens):
    result = {}
    type_token = tokens.pop(0).upper()
    if type_token not in qtype.__members__:
        raise Exception("No valid type")
    query_type = qtype[type_token]
    if query_type == qtype.INSERT:
        into_maybe = tokens.pop(0)
        if into_maybe != 'INTO':
            raise Exception("Expect into after insert")
    elif query_type == qtype.CREATE:
        raise NotImplementedError("CREATE not implemented")
    elif query_type == qtype.DELETE:
        raise NotImplementedError("DELETE not implemented")
    elif query_type == qtype.SELECT:
        result['type'] = query_type
        column_selection = []
        column = tokens.pop(0)
        while column.upper() != 'FROM':
            column_selection += [c for c in column.split(',') if c != '']  # if ; is at the beginning or end of the string the split command returns an empty string
            column = tokens.pop(0)
        result['columns'] = column_selection
        
        table_selection = tokens.pop(0)
        result['table'] = table_selection

        if len(tokens) == 0:
            return result
        transformation_token = tokens.pop(0)
        if transformation_token not in qtrans.__members__:
            raise Exception("No valid trans")
        query_transformation = qtrans[transformation_token]
        if query_transformation == qtrans.WHERE:
            where = {}
            token = tokens.pop(0)
            if token.isdigit() or token[0]==token[-1]=="'":  # literal value, e.g., '1' or 1
                if token.isdigit():
                    slot1 = {"type": "value", "value": int(token)}
                else:
                    slot1 = {"type": "value", "value": token.strip("'")}
            else:
                # token is colun name
                slot1 = {"type": "column", "value": token}
            
            where['slot1'] = slot1

            token = tokens.pop(0)
            if token not in [q.value for q in qoperators]:
                raise Exception("Expect operator like = or !=")
            where['operator'] = token
            
            token = tokens.pop(0)
            if token.isdigit() or token[0]==token[-1]=="'":  # literal value, e.g., '1' or 1
                if token.isdigit():
                    slot2 = {"type": "value", "value": int(token)}
                else:
                    slot2 = {"type": "value", "value": token.strip("'")}
            else:
                # token is colun name
                slot2 = {"type": "column", "value": token}
            
            where['slot2'] = slot2

            result['where'] = where


            # token = tokens.pop(0)
            # stmt = ''
            # while len(tokens)!=0 and tokens[0] not in qtrans.__members__:
            #     stmt += token + ' '
            #     token = tokens.pop(0)
            # stmt += token
            # result['where'] = stmt
        elif query_transformation == qtrans.ORDER:
            raise NotImplementedError("ORDER not implemented")
        elif query_transformation == qtrans.GROUP:
            raise NotImplementedError("GROUP not implemented")
        elif query_transformation == qtrans.LIMIT:
            raise NotImplementedError("LIMIT not implemented")
    return result
