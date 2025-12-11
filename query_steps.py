
from typing import Generator

def gt(x, y):
    return x > y

def lt(x, y):
    return x < y

def ge(x, y):
    return x >= y

def le(x, y):
    return x <= y

def ne(x, y):
    return x != y

def eq(x, y):
    return x == y

operators = {'=': eq,
             '!=': ne,
             '>': gt,
             '<': lt,
             '>=': ge,
             '<=': le}

class Filter:
    def __init__(self, comparison: str, val1=None, val2=None, col_idx1=None, col_idx2=None, parent=None):
        if comparison not in operators:
            raise ValueError(f"Invalid comparison operator: {comparison}")
        self.comparison_function = operators[comparison]
        self.val1 = val1
        self.val2 = val2
        self.col_idx1 = col_idx1
        self.col_idx2 = col_idx2

        self.parent = parent

    def next(self):
        for row in self.parent.next():
            x = row[self.col_idx1] if self.val1 is None else self.val1
            y = row[self.col_idx2] if self.val2 is None else self.val2
            if self.comparison_function(x, y):
                yield row


class Iterator:
    def __init__(self, page_generator=None):
        self.page_generator = page_generator

    def set_source_generator(self, page_generator):
        self.page_generator = page_generator

    def next(self):
        if self.page_generator is None:
            raise ValueError("No source generator set for Iterator")
        for page in self.page_generator:
            for row in page.rows:
                yield row