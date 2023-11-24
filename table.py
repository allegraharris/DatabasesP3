"""
class Table {
    columns = list() -> list that store the column names
    column_data = dict() -> hashtable that store column datas {key: col_name, value: col_data}
    keys = set() -> set that store the column names
"""

from BTrees._OOBTree import OOBTree

class Table:
    def __init__(self):
        self.columns = list()
        self.column_data = dict()
        self.keys = set()
        self.indexing = {}
        self.tuples = list()
    
    def add_column(self,col_name,col_data):
        if col_name in self.column_data:
            print("error")
            return
        self.column_data[col_name] = col_data
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
    

# set1 = frozenset({1})
# set2 = frozenset({2})
# tree = OOBTree()
# tree[set1] = 1
# tree[set2] = 2
# print(list(tree))