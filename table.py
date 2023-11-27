"""
class Table {
    columns = list() -> list that store the column names
    column_data = dict() -> hashtable that store column datas {key: col_name, value: col_data}
    keys = set() -> set that store the column names
"""

databases = {} # global variable database that stores all of the tables

INT_MIN = -2147483647

import re
from BTrees._OOBTree import OOBTree
from tabulate import tabulate as tb
from exception import Invalid_Type, Syntax_Error, Duplicate_Item, Keyword_Used, Not_Exist, Unsupported_Functionality

def evaluateCondition(value1, operator, value2):
        try:
            value2 = int(value2)
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
        except ValueError:
            value2 = value2.replace("'", "")
            if operator == '=':
                return value1 == value2
            elif operator == '!=':
                return value1 != value2
            else:
                raise Syntax_Error('Syntax Error: can only use = or != on string type')
                
        return False
class Table:
    def __init__(self):
        self.columns = list()
        self.column_data = dict()
        self.pri_keys = set()
        self.for_keys = dict()
        self.ref_table = ""
        self.indexing = dict()
        # self.tuples = list() # only need one
        self.size = 0
        self.pri_lock = False # True for primary key defined, false not defined
        self.for_lock = False # True for primary key defined, false not defined

    ### add column, primary key, or foreign key ###
    ### attribute is the raw input, e.g: net_id INT, PRIMARY KEY (net_id), PRIMARY KEY ()
    def add_attribute(self,attribute):
        tokens = [token for token in re.split(r'(\([^)]*\))|\s+',attribute) if token]
        # print(tokens)
        if tokens[0] == 'PRIMARY': # primary key definition
            if len(tokens) != 3 or tokens[1].upper() != 'KEY':
                raise Syntax_Error("Syntax Error: Primary Key")
            if self.pri_lock == True:
                raise Syntax_Error("Syntax Error: Primary Key redefined")
            self.add_primary_key(tokens[2])
            return
        
        if tokens[0] == 'FOREIGN': # foreign key definition
            if len(tokens) != 6 or tokens[1] != 'KEY' or tokens[3].upper() != 'REFERENCES': # check syntax error
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
        # print(col_name,col_type)
        if col_name in self.column_data: # Duplicate column
            raise Duplicate_Item(col_name + "in the table already")
        if col_type != 'INT' and col_type != 'STRING': # invalid data type
            raise Invalid_Type(col_name + "date type is invalid")
        self.column_data[col_name] = [col_type,0,len(self.columns)]
        self.columns.append(col_name)
    
    def lock_pri_keys(self):
        self.pri_keys = frozenset(self.pri_keys)
        self.pri_lock = True
    
    def lock_for_keys(self):
        self.for_lock = True

    def add_tuples(self,tuples):
        tuples = [value.strip() for value in re.split(r',(?![^()]*\))',tuples)]
        # print(tuples)
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
        return

    def print_internal(self):
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
        if self.size == 0:
            print("<Empty Set>")
            return
        if column == '*':
            self.print_internal()
            return
        columns = [token.strip() for token in column.split(',') if token]
        print(columns)
        headers = columns
        tuples = []
        for key in self.indexing.keys():
            tuple = []
            for column in headers:
                tuple.append(self.indexing[key][column])
            tuples.append(tuple)
        print(tb(tuples, headers, tablefmt='outline'))
        print(f"{self.size} rows in set")
        

        


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
        print(tb(tuples,headers,tablefmt='outline'))


    def copyColumns(self, tempTable, newColumns, conditions, single):
        #arguments:
        #tempTable = the new empty table to copy data into
        #newColumns = the columns we are trying to select
        #conditions = a list of the format ['a', '=', '2', 'and','b', '<', '6']
            # Will either have size 3 or 7 dependending on whether there are two conditions or not
        #single = an integer (0,1,2) which specifies how many conditions there are 

        addRow = False
        first = False
        second = False

        #If there is no where clause, copy values from relevant columns into new relation
        if(len(conditions) == 0):
            for key, inner_dict in self.indexing.items():
                for column, value in inner_dict.items():
                    if key not in tempTable.indexing:
                        tempTable.indexing[key] = {}
                    if column in newColumns:
                        if column not in tempTable.indexing[key]:
                            tempTable.indexing[key][column] = value
                        else:
                            tempTable.indexing[key][column].add(value)
                        tempTable.size+=1
            tempTable.columns = list(newColumns)
        #if only one condition, add all values from relevant columns into new relation that meet the condition
        elif(single == 1):
            for key, inner_dict in self.indexing.items():
                for column, value in inner_dict.items():
                    if column in newColumns and column == conditions[0]:
                        if(evaluateCondition(value, conditions[1], conditions[2])):

                            if key not in tempTable.indexing:
                                tempTable.indexing[key] = {}

                            tempTable.indexing[key][column] = value
                            tempTable.size+=1
                            addRow = True

                    if(addRow):
                        tempTable.indexing[key][column] = value

                addRow = False

            tempTable.columns = list(newColumns)
        #if two conditions, same idea as above but a bit more fiddly
        elif(single == 2):
            for key, inner_dict in self.indexing.items():
                for column, value in inner_dict.items():
                    if column in newColumns and (column == conditions[0]):
                        if(evaluateCondition(value, conditions[1], conditions[2])):
                            first = True
                    elif column in newColumns and (column == conditions[4]):
                        if(evaluateCondition(value, conditions[5], conditions[6])):
                            second = True

                        if(conditions[3] == 'AND' and first and second):
                            break
                        elif(conditions[3] == 'OR' and (first or second)):
                            break

                if(conditions[3] == 'AND' and first and second):
                    for column, value in inner_dict.items():
                        if column in newColumns:
                            if key not in tempTable.indexing:
                                tempTable.indexing[key] = {}
                            tempTable.indexing[key][column] = value
                            tempTable.size+=1
                elif(conditions[3] == 'OR' and (first or second)):
                    for column, value in inner_dict.items():
                        if column in newColumns:
                            if key not in tempTable.indexing:
                                tempTable.indexing[key] = {}
                            tempTable.indexing[key][column] = value
                            tempTable.size+=1
                first = False
                second = False

            tempTable.columns = list(newColumns)    

        return tempTable
    
    def max(self, column):
        max = INT_MIN
        if(column in self.pri_keys):
            if(len(self.pri_keys) == 1):
                for key in self.indexing.keys():
                    value = int(next(iter(key)))
                    if(value > max):
                        max = value
            else: 
                for key, cols in self.indexing.items():
                    value = cols[column]
                    if(value > max):
                        max = value
        else:
            for key, cols in self.indexing.items():
                value = cols[column]
                if(value > max):
                    max = value
                

        tempTable = Table()
        tempTable.columns = ["max(" + str(column) + ")"]
        tempTable.indexing[0] = {}
        tempTable.indexing[0]["max(" + str(column) + ")"] = max
        tempTable.size = 1

        return tempTable