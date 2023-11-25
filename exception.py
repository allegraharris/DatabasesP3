# general syntax error
class Syntax_Error(Exception):
    def __init__(self, message="Syntax Error"):
        self.message = message
        super().__init__(self.message)

# Error if non-keyword use preserved keyword
class Keyword_Used(Exception):
    def __init__(self, message="Keyword_Used"):
        self.message = message
        super().__init__(self.message)

class Invalid_Type(Exception):
    def __init__(self, message="Invalid Data Type"):
        self.message = message
        super().__init__(self.message)

# Error if item is repeated. Such as repeated primary key, repeated column, etc.
class Duplicate_Item(Exception):
    def __init__(self, message="Duplicate Item"):
        self.message = message
        super().__init__(self.message)

class Not_Exist(Exception):
    def __init__(self, message="Table not exist"):
        self.message = message
        super().__init__(self.message)

class Unsupported_Functionality(Exception):
    def __init__(self, message="Unsupported functionality"):
        self.message = message
        super().__init__(self.message)