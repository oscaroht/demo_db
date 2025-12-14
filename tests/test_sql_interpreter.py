import pytest
from sql_interpreter import tonkenize, parser

def test_tokenizer():
    user_input = "SELECT id, num, letter FROM tbl WHERE id = 1;"
    tokens = tonkenizer(user_input)
    assert tokens == ['SELECT', 'id,', 'num,', 'letter', 'FROM', 'tbl', 'WHERE', 'id', '=', '1']

def test_tokenizer_no_space_equal():
    user_input = "SELECT id, num, letter FROM tbl WHERE id=1;"
    tokens = tonkenizer(user_input)
    assert tokens == ['SELECT', 'id,', 'num,', 'letter', 'FROM', 'tbl', 'WHERE', 'id', '=', '1']

def test_tokenizer_literals():
    user_input = "SELECT id, num, 'je moeder' as letter FROM tbl WHERE id = 1;"
    tokens = tonkenizer(user_input)
    assert tokens == ['SELECT', 'id,', 'num,', '\'je moeder\'', 'as', 'letter', 'FROM', 'tbl', 'WHERE', 'id', '=', '1']

def test_tokenizer_multiple_literals():
    user_input = "SELECT id, num, 'je moeder' as letter FROM tbl WHERE letter = 'je moeder twee';"
    tokens = tonkenizer(user_input)
    assert tokens == ['SELECT', 'id,', 'num,', '\'je moeder\'', 'as', 'letter', 'FROM', 'tbl', 'WHERE', 'letter', '=', '\'je moeder twee\'']


def test_tokenizer_multiple_queries():
    user_input = "SELECT id FROM tbl; SELECT num FROM tbl;"
    with pytest.raises(NotImplementedError):
        tonkenizer(user_input)

def test_tokenizer_empty():
    user_input = ""
    tokens = tonkenizer(user_input)
    assert tokens == []

# def test_parser_select():
#     tokens = ['SELECT', 'id,', 'num,', 'letter', 'FROM', 'tbl', 'WHERE', 'id', '=', '1']
#     result = parser(tokens)
#     assert result == {
#         'type': 'SELECT',
#         'columns': ['id', 'num', 'letter'],
#         'table': 'tbl',
#         'where': {
#             'column': 'id',
#             'operator': '=',
#             'value': '1'
#         }
#     }

