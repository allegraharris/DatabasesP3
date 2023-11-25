"""
class Table {
    columns = list() -> list that store the column names
    column_data = dict() -> hashtable that store column datas {key: col_name, value: col_data}
    keys = set() -> set that store the column names
"""

from BTrees._OOBTree import OOBTree
from exception import Invalid_Type, Syntax_Error, Duplicate_Item, Keyword_Used, Table_Exist, Unsupported_Functionality

class Table:
    def __init__(self):
        self.columns = list()
        self.column_data = dict()
        self.keys = set()
        self.indexing = dict()
        self.tuples = list()
        self.size = 0
    
    def add_column(self,col_name,col_type):
        print(col_name,col_type)
        if col_name in self.column_data:
            raise Duplicate_Item(col_name + "in the table already")
        if col_type != 'INT' and col_type != 'STRING':
            raise Invalid_Type(col_name + "date type is invalid")
        self.column_data[col_name] = [col_type,0,len(self.columns)]
        self.columns.append(col_name)
    
    def add_key(self,col_name):
        if col_name not in self.column_data:
            print("error")
        if col_name in self.keys:
            print("error")
        self.keys.add(col_name)
    
    def lock_keys(self):
        self.keys = frozenset(self.keys)

    def add_tuples(self,info):
        new_tuple = dict()
        primary_key = set()
        for i in range (len(self.columns)):
            new_tuple[self.columns[i]] = info[i]
        for key in self.keys:
            primary_key.add(new_tuple[key])
        self.indexing[frozenset(key)] = new_tuple

    def print_internal(self):
        for col in self.columns:
            print(f"column name: {col}, column info: {self.column_data[col]}")
        print(f"primary keys are {self.keys}")
        print(f"There are {self.size} tuples")
        for tuple in self.tuples:
            print(tuple)


# set1 = frozenset({1})
# set2 = frozenset({2})
# tree = OOBTree()
# tree[set1] = 1
# tree[set2] = 2
# print(list(tree))