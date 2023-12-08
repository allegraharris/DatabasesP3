INT_MIN = -2147483647
INT_MAX = 2147483648
JOIN_KEY = 0
INCLUDE_OPERATOR = ['=','<=','>=']

import time
import re
from tabulate import tabulate as tb
from exception import Invalid_Type, Syntax_Error, Duplicate_Item, Keyword_Used, Not_Exist, Unsupported_Functionality
start_time = time.time()
end_time = time.time()
databases = {} # global variable database that stores all of the tables

def start():
    global start_time
    # print("start time")
    start_time = time.time()

def end():
    global end_time
    # print("end time")
    end_time = time.time()

def getTime():
    print(f"Time: {end_time-start_time:.3f}s")

def evaluateCondition(value1, operator, value2):
    # print(value1,value2)
    # value2 = int(value2)
    if operator == '=':
        return value1 == value2
    elif operator == '!=':
        return value1 != value2
    elif operator == '>':
        return value1 > value2
    elif operator == '>=':
        return value1 >= value2
    elif operator == '<':
        return value1 < value2
    elif operator == '<=':
        return value1 <= value2
    return False

class Table:
    def __init__(self):
        self.name = str()
        self.columns = list()
        self.column_data = dict()
        self.pri_keys = set()
        self.for_keys = dict()
        self.ref_table = ""
        self.indexing = dict()
        self.size = 0
        self.pri_lock = False # True for primary key defined, false not defined
        self.for_lock = False # True for primary key defined, false not defined

    def copy(self,table):
        self.name = table.name
        self.columns = table.columns
        self.column_data = table.column_data
        self.pri_keys = table.pri_keys
        self.for_keys = table.for_keys
        self.ref_table = table.ref_table
        self.indexing = dict()
        self.size = 0
        self.pri_lock = table.pri_lock # True for primary key defined, false not defined
        self.for_lock = table.pri_lock # True for primary key defined, false not defined

    ### add column, primary key, or foreign key ###
    ### attribute is the raw input, e.g: net_id INT, PRIMARY KEY (net_id), PRIMARY KEY ()
    def add_attribute(self,attribute):
        tokens = [token for token in re.split(r'(\w+|\([^)]*\))',attribute) if token.strip()]
        # print(tokens)
        if tokens[0] == 'PRIMARY': # primary key definition
            if len(tokens) != 3 or tokens[1].upper() != 'KEY':
                raise Syntax_Error("Syntax Error: Primary Key")
            if self.pri_lock == True:
                raise Syntax_Error("Syntax Error: Primary Key redefined")
            self.add_primary_key(tokens[2])
            return
        
        if tokens[0] == 'FOREIGN': # foreign key definition
            if len(tokens) != 6 or tokens[1].upper() != 'KEY' or tokens[3].upper() != 'REFERENCES': # check syntax error
                raise Syntax_Error("Syntax Error: Foreign Key")
            if tokens[4] not in databases: # check existence of referencing table
                raise Not_Exist(f"Referenced Table {tokens[4]} not in database")
            if self.for_lock == True:
                raise Syntax_Error("Syntax Error: Foreign Key redefined")
            self.add_foreign_key(tokens[2],tokens[5],tokens[4])
            return
        
        if len(tokens) != 2: # column definition
            raise Syntax_Error("Syntax Error: Invalid column definition")
        self.add_column(tokens[0],tokens[1].upper())

    def add_primary_key(self,keys):
        keys = keys[1:len(keys)-1].strip()
        tokens = [token.strip() for token in re.split(r',', keys) if token.strip()]
        if len(tokens) == 0:
            raise Syntax_Error("Syntax Error: Primary Key Attributes are empty")
        for key in tokens:
            if key not in self.column_data: # key not in table
                raise Not_Exist(f"Invalid Primary Key {key}")
            if self.column_data[key][1] != 0: # key redefined
                raise Duplicate_Item(f"Duplicate Primary {key}")
            self.column_data[key][1] = 1
            self.pri_keys.add(key)
        self.lock_pri_keys() # lock primary key when it is finished

    def add_foreign_key(self,keys,foreign_keys,table_name):
        foreign_table = databases[table_name]
        keys = keys[1:len(keys)-1].strip()
        foreign_keys = foreign_keys[1:len(foreign_keys)-1].strip()
        tokens = [token.strip() for token in re.split(r',', keys) if token.strip()]
        foreign_tokens = [token.strip() for token in re.split(r',', foreign_keys) if token.strip()]
        if len(tokens) == 0 or len(foreign_tokens) == 0: #
            raise Syntax_Error("Syntax Error: Foreign Key Attributes are empty")
        if len(tokens) != len(foreign_tokens):
            raise Syntax_Error("Syntax Error: Foreign Key and Referencing Key does not match in numbers")
        for i in range (0,len(tokens)):
            key = tokens[i]
            foreign_key = foreign_tokens[i]
            if key not in self.column_data:
                raise Not_Exist(f"Invalid Foreign Key {key}")
            if foreign_key not in foreign_table.column_data:
                raise Not_Exist(f"Invalid Referencing Key {foreign_key}")
            if foreign_key not in foreign_table.pri_keys:
                raise Syntax_Error(f"Syntax Error: Referencing Key {foreign_key} is not a Primary key in {table_name}")
            if self.column_data[key][0] != foreign_table.column_data[foreign_key][0]:
                raise Invalid_Type(f"Incompatible Data Type between Foreign Key {key} and Referencing Key {foreign_key}")
            self.for_keys[key] = foreign_key
            self.column_data[key][1] = 2
        self.ref_table = table_name
        self.lock_for_keys()
        return
    
    def add_column(self,col_name,col_type):
        if col_name in self.column_data: # Duplicate column
            raise Duplicate_Item(col_name + " in the table already")
        if col_type != 'INT' and col_type != 'STRING': # invalid data type
            raise Invalid_Type(col_name + " data type is invalid")
        self.column_data[col_name] = [col_type,0,len(self.columns)]
        self.columns.append(col_name)
    
    def lock_pri_keys(self):
        self.pri_keys = frozenset(self.pri_keys)
        self.pri_lock = True
    
    def lock_for_keys(self):
        self.for_lock = True

    def add_tuples(self,tuples):
        tuples = [value.strip() for value in re.split(r',(?![^()]*\))',tuples)]
        for tuple in tuples:
            value = tuple[1:len(tuple)-1].strip()
            values = [token.strip() for token in re.split(r',', value) if token.strip()]
            new_tuple = dict()
            if len(values) != len(self.columns):
                raise Syntax_Error("Syntax Error: Inserted Tuple columns does not match")
            for i in range (0,len(values)):
                if self.column_data[self.columns[i]][0] == 'INT':
                    try:
                        int(values[i])
                    except ValueError:
                        raise Invalid_Type("Incompatible Type INT")
                    new_tuple[self.columns[i]] = int(values[i])
                if self.column_data[self.columns[i]][0] == 'STRING':
                    if values[i][0] != "'" or values[i][len(values[i])-1] != "'":
                        raise Invalid_Type("Incompatible Type STRING")
                    new_tuple[self.columns[i]] = values[i][1:len(values[i])-1]
            
            primary_keys = set()
            foreign_keys = set()
            for key in self.pri_keys:
                primary_keys.add(new_tuple[key])
            if frozenset(primary_keys) in self.indexing:
                raise Duplicate_Item("Duplicate Primary Key")
            if self.for_lock == True:
                for key in self.for_keys:
                    foreign_keys.add(new_tuple[key])
                if frozenset(foreign_keys) not in databases[self.ref_table].indexing:
                    raise Not_Exist(f"Constriant by Foreign Key from Table {self.ref_table}")
            self.indexing[frozenset(primary_keys)] = new_tuple
            self.size += 1
        end()
        return

    def print_internal(self):
        end()
        if self.size == 0:
            print("<Empty Set>")
            return
        headers = []
        tuples = []
        for column in self.columns:
            headers.append(column)
        for key in self.indexing.keys():
            tuple = []
            for column in headers:
                tuple.append(self.indexing[key][column])
            tuples.append(tuple)
        print(tb(tuples, headers, tablefmt='outline'))
        print(f"{self.size} rows in set")
        return
    
    def print_internal_select(self,column):
        end()
        if self.size == 0:
            print("<Empty Set>")
            return
        columns = [token.strip() for token in column.split(',') if token]
        tuples = []
        for key in self.indexing.keys():
            tuple = []
            for column in columns:
                tuple.append(self.indexing[key][column])
            tuples.append(tuple)
        print(tb(tuples, columns, tablefmt='outline'))
        print(f"{self.size} rows in set")
        return

    def describe(self):
        headers = ['Field','Type','KEY']
        tuples = []
        for column in self.columns:
            tuple = []
            tuple.append(column)
            tuple.append(self.column_data[column][0])
            if self.column_data[column][1] == 0:
                tuple.append('')
            elif self.column_data[column][1] == 1:
                tuple.append('PRI')
            else:
                tuple.append('FOR')
            tuples.append(tuple)
        end()
        print(tb(tuples,headers,tablefmt='outline'))

    def single_where(self,column,value,optr):
        table = Table()
        table.copy(self)

        if optr == '=' and column in self.pri_keys and len(self.pri_keys) == 1:
            key = frozenset({value})
            if key in self.indexing:
                table.indexing[key] = self.indexing[key]
                table.size += 1

        else:
            for key in self.indexing.keys():
                if evaluateCondition(self.indexing[key][column],optr,value):
                    table.indexing[key] = self.indexing[key]
                    table.size += 1

        return table
    
    def single_where_column(self,col1,col2,optr):
        table = Table()
        table.copy(self)
        if col1 == col2 and optr in INCLUDE_OPERATOR:
            return self
        if col1 == col2:
            return table
        for key in self.indexing.keys():
            if evaluateCondition(self.indexing[key][col1],optr,self.indexing[key][col2]):
                table.indexing[key] = self.indexing[key]
                table.size += 1
        return table

    def double_where(self,col1,col2,optr1,optr2,val1,val2,log):
        key = set()
        table = Table()
        table.copy(self)
        if log == 'AND':
            if col1 in self.pri_keys and col2 in self.pri_keys and len(self.pri_keys) == 2 and col1 == col2:
                key = frozenset({val1,val2})
                if key in self.indexing:
                    table.indexing[key] = self.indexing[key]
            elif col1 in self.pri_keys:
                table = self.single_where(col1,val1,optr1)
                table = table.single_where(col2,val2,optr2)
            else:
                table = self.single_where(col2,val2,optr2)
                table = table.single_where(col1,val1,optr1)
        elif log == 'OR':
            table_1 = self.single_where(col1,val1,optr1)
            table_2 = self.single_where(col2,val2,optr2)
            table.indexing.update(table_1.indexing)
            table.indexing.update(table_2.indexing)
            table.size = len(table.indexing)
        return table
    
    def double_where_column(self,col1,col2,optr1,optr2,col3,col4,log):
        table = Table()
        table.copy(self)
        if log == 'AND':
            if (col1 == col3 and optr1 not in INCLUDE_OPERATOR) or (col2 == col4 and optr2 not in INCLUDE_OPERATOR):
                return table
            if col1 == col3 and col2 == col4:
                return self
            if col1 == col3:
                return self.single_where_column(col2,col4,optr2)
            if col2 == col4:
                return self.single_where_column(col1,col3,optr1) 
            return (self.single_where_column(col1,col3,optr1)).single_where_column(col2,col4,optr2)
        elif log == 'OR':
            if (col1 == col3 and optr1 in INCLUDE_OPERATOR) or (col2 == col4 and optr2 in INCLUDE_OPERATOR):
                return self
            if col1 == col3 and col2 == col4:
                return table
            if col1 == col3:
                return self.single_where_column(col2,col4,optr2)
            if col2 == col4:
                return self.single_where_column(col1,col3,optr1)
            table_1 = self.single_where_column(col1,col3,optr1)
            table_2 = self.single_where_column(col2,col4,optr2)
            table.indexing.update(table_1.indexing)
            table.indexing.update(table_2.indexing)
            table.size = len(table.indexing)
            return table
            
    def max(self,column):
        if column == '*':
            raise Syntax_Error('Syntax Error: Cannot take max of a multiple columns')
        if(self.column_data[column][0] == 'STRING'):
            raise Syntax_Error('Syntax Error: Cannot take max of a string column')
        max_column = f"max({column})"
        max_table = Table()
        max_table.columns.append(max_column)
        max_value = INT_MIN
        max_table.size = 1
        if(self.size > 0):
            for key in self.indexing.keys():
                if self.indexing[key][column] > max_value:
                    max_value = self.indexing[key][column]
        else:
            max_value = 'NULL'
        max_table.indexing[0] = {max_column:max_value}
        return max_table
    
    def min(self,column):
        if column == '*':
            raise Syntax_Error('Cannot take min of a multiple columns')
        if(self.column_data[column][0] == 'STRING'):
            raise Syntax_Error('Cannot take min of a string column')
        min_column = f"min({column})"
        min_table = Table()
        min_table.columns.append(min_column)
        min_value = INT_MAX
        min_table.size = 1
        if(self.size > 0):
            for key in self.indexing.keys():
                if self.indexing[key][column] < min_value:
                    min_value = self.indexing[key][column]
        else:
            min_value = 'NULL'
        min_table.indexing[0] = {min_column:min_value}
        return min_table
    
    def avg(self,column):
        if column == '*':
            raise Syntax_Error('Cannot take avg of a multiple columns')
        if(self.column_data[column][0] == 'STRING'):
            raise Syntax_Error('Cannot take avg of a string column')
        avg_column = f"avg({column})"
        avg_table = Table()
        avg_table.columns.append(avg_column)
        sum_value = 0
        avg_value = 0
        avg_table.size = 1
        if(self.size > 0):
            for key in self.indexing.keys():
                sum_value += self.indexing[key][column]
            avg_value = float(sum_value/self.size)
        else:
            avg_value = 'NULL'
        avg_table.indexing[0] = {avg_column:avg_value}
        return avg_table
    
    def sum(self,column):
        if column == '*':
            raise Syntax_Error('Cannot take sum of a multiple columns')
        if(self.column_data[column][0] == 'STRING'):
            raise Syntax_Error('Cannot take avg of a string column')
        sum_column = f"sum({column})"
        sum_table = Table()
        sum_table.columns.append(sum_column)
        sum_value = 0
        sum_table.size = 1
        if(self.size > 0):
            for key in self.indexing.keys():
                sum_value += self.indexing[key][column]
        else:
            sum_value = 'NULL'
        sum_table.indexing[0] = {sum_column:sum_value}
        return sum_table
    
    def join_tables(self,table_1,table_2,tab_1,col_1,tab_2,col_2):
        # swap join condition table names are aligned
        if tab_1 != tab_2 and tab_1 == table_2.name:
            return self.join_tables(table_1,table_2,tab_2,col_2,tab_1,col_1)
        
        # Join column attribute, don't need to modify primary and foreign keys since the table is temporary
        table = Table()
        table_name_1 = table_1.name
        table_name_2 = table_2.name
        for key in table_1.column_data.keys():
            table.column_data[f"{table_name_1}.{key}"] = table_1.column_data[key]
            table.columns.append(f"{table_name_1}.{key}")
        offset = len(table.column_data)
        for key in table_2.column_data.keys():
            table.column_data[f"{table_name_2}.{key}"] = table_2.column_data[key]
            table.column_data[f"{table_name_2}.{key}"][2] += offset
            table.columns.append(f"{table_name_2}.{key}")

        # Join condition with same columns in one table
        if tab_1 == tab_2 and col_1 == col_2:
            return table.cartesian_product(table_1,table_2)
        
        if tab_1 == tab_2:
            if tab_1 == table_name_1:
                table_1 = table_1.single_where_column(col_1,col_2,'=')
            else:
                table_2 = table_2.single_where_column(col_1,col_2,'=')
            return table.cartesian_product(table_1,table_2)
   
        return table.join_tuples(table_1,table_2,tab_1,col_1,tab_2,col_2)

    def cartesian_product(self,table_1,table_2):
        if table_1.size > table_2.size:
            return self.cartesian_product(table_2,table_1)
        name_1 = table_1.name
        name_2 = table_2.name
        for key_1 in table_1.indexing.keys():
            tuple_1 = {f"{name_1}." + key: value for key, value in table_1.indexing[key_1].items()}
            for key_2 in table_2.indexing.keys():
                new_tuple = dict()
                tuple_2 = {f"{name_2}." + key: value for key, value in table_2.indexing[key_2].items()}
                new_tuple.update(tuple_1)
                new_tuple.update(tuple_2)
                self.indexing[self.size] = new_tuple
                self.size += 1
        return self

    def join_tuples(self,table_1,table_2,tab_1,col_1,tab_2,col_2):
        if (col_1 in table_1.pri_keys and len(table_1.pri_keys) == 1) and (col_2 in table_2.pri_keys and len(table_2.pri_keys) == 1):
            return self.pri_join_tuples(table_1,table_2)
        if (max(table_1.size,table_2.size) < 50*min(table_1.size,table_2.size)):
            return self.mergeScan(table_1,table_2,col_1,col_2)
        return self.nestedLoop(table_1,table_2,col_1,col_2)
    
    def pri_join_tuples(self,table_1,table_2):
        if table_1.size > table_2.size:
            return self.pri_join_tuples(table_2,table_1)
        table_name_1 = table_1.name
        table_name_2 = table_2.name
        for key in table_1.indexing.keys():
            if key in table_2.indexing.keys():
                new_tuple = dict()
                tuple_1 = table_1.indexing[key]
                tuple_2 = table_2.indexing[key]
                for col in tuple_1.keys():
                    new_tuple[f"{table_name_1}.{col}"] = tuple_1[col]
                for col in tuple_2.keys():
                    new_tuple[f"{table_name_2}.{col}"] = tuple_2[col]
                self.indexing[self.size] = new_tuple
                self.size += 1
        return self
    
    def nestedLoop(self,table_1,table_2,col_1,col_2):
        if table_1.size > table_2.size:
            return self.nestedLoop(table_2,table_1)
        table_name_1 = table_1.name
        table_name_2 = table_2.name
        for tuple_1 in table_1.indexing.values():
            for tuple_2 in table_2.indexing.values():
                if tuple_1[col_1] == tuple_2[col_2]:
                    new_tuple = dict()
                    for key in tuple_1.keys():
                        new_tuple[f"{table_name_1}.{key}"] = tuple_1[key]
                    for key in tuple_2.keys():
                        new_tuple[f"{table_name_2}.{key}"] = tuple_2[key]
                    self.indexing[self.size] = new_tuple
                    self.size += 1
        return self
    
    def mergeScan(self,table_1,table_2,col_1,col_2):
        table_name_1 = table_1.name
        table_name_2 = table_2.name
        size_1 = table_1.size
        size_2 = table_2.size
        sorted_tuples_1 = sorted(table_1.indexing.items(), key=lambda x: x[1][col_1])
        sorted_tuples_2 = sorted(table_2.indexing.items(), key=lambda x: x[1][col_2])
        i = 0
        j = 0
        while i < size_1 and j < size_2:
            left = sorted_tuples_1[i][1]
            right = sorted_tuples_2[j][1]
            if left[col_1] < right[col_2]:
                i += 1
            elif left[col_1] > right[col_2]:
                j += 1
            elif left[col_1] == right[col_2]:
                k = j
                while (k < size_2 and left[col_1] == sorted_tuples_2[k][1][col_2]):
                    right = sorted_tuples_2[k][1]
                    new_tuple = dict()
                    for key in left.keys():
                        new_tuple[f"{table_name_1}.{key}"] = left[key]
                    for key in right.keys():
                        new_tuple[f"{table_name_2}.{key}"] = right[key]
                    self.indexing[self.size] = new_tuple
                    self.size += 1
                    k += 1
                i += 1
        return self