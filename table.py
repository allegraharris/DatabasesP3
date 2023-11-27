"""
class Table {
    columns = list() -> list that store the column names
    column_data = dict() -> hashtable that store column datas {key: col_name, value: col_data}
    keys = set() -> set that store the column names
"""

databases = {} # global variable database that stores all of the tables

INT_MIN = -2147483647
JOIN_KEY = 0

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
        self.size = 0
        self.pri_lock = False # True for primary key defined, false not defined
        self.for_lock = False # True for primary key defined, false not defined

    ### add column, primary key, or foreign key ###
    ### attribute is the raw input, e.g: net_id INT, PRIMARY KEY (net_id), PRIMARY KEY ()
    def add_attribute(self,attribute):
        tokens = [token for token in re.split(r'\s+|([a-zA-Z_]+)|(\([^)]+\))',attribute) if token]
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
        print(tb(tuples,headers,tablefmt='outline'))


    def copyColumns(self, tempTable, newColumns, conditions, single):
        #arguments:
        #tempTable = the new empty table to copy data into
        #newColumns = the columns we are trying to select
        #conditions = a list of the format ['a', '=', '2', 'and','b', '<', '6']
            # Will either have size 3 or 7 dependending on whether there are two conditions or not
        #single = an integer (0,1,2) which specifies how many conditions there are 

        #If there is no where clause, copy values from relevant columns into new relation
        if(single == 0):
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
                value = inner_dict[conditions[0]]
                if(evaluateCondition(value, conditions[1], conditions[2])):
                    for column, val in inner_dict.items():
                        if(column in newColumns):
                            if(key not in tempTable.indexing):
                                tempTable.indexing[key] = {}
                            tempTable.indexing[key][column] = val
                            tempTable.size+=1
            tempTable.columns = list(newColumns)
        #if two conditions, same idea as above but a bit more fiddly
        elif(single == 2):
            for key, inner_dict in self.indexing.items():
                value1 = inner_dict[conditions[0]]
                value2 = inner_dict[conditions[4]]
                condition1 = evaluateCondition(value1, conditions[1], conditions[2])

                if(conditions[3] == 'AND' and condition1):
                    condition2 = evaluateCondition(value2, conditions[5], conditions[6])
                    if(condition2):
                        for column, value in inner_dict.items():
                            if column in newColumns:
                                if key not in tempTable.indexing:
                                    tempTable.indexing[key] = {}
                                tempTable.indexing[key][column] = value
                                tempTable.size+=1
                
                elif(conditions[3] == 'OR' and condition1):
                    for column, value in inner_dict.items():
                        if column in newColumns:
                            if key not in tempTable.indexing:
                                tempTable.indexing[key] = {}
                            tempTable.indexing[key][column] = value
                            tempTable.size+=1

                elif(conditions[3] == 'OR' and condition1 == False): #this could be combined with the first if but would create confusing logic so is being kept separate
                    condition2 = evaluateCondition(value2, conditions[5], conditions[6])
                    if(condition2):
                        for column, value in inner_dict.items():
                            if column in newColumns:
                                if key not in tempTable.indexing:
                                    tempTable.indexing[key] = {}
                                tempTable.indexing[key][column] = value
                                tempTable.size+=1

            tempTable.columns = list(newColumns)    

        return tempTable
    
    def max(self, column, conditions, single):
        max = INT_MIN
        if(self.column_data[column][0] == 'STRING'):
            raise Syntax_Error('Cannot take max of a string column')
        
        if(single == 0):
            for key, inner_dict in self.indexing.items():
                if(column in self.pri_keys):
                    if(len(self.pri_keys) == 1):
                        value = int(next(iter(key)))
                    else:
                        value = inner_dict[column]
                else:
                    value = inner_dict[column]
                if(value > max):
                    max = value
        elif(single == 1):
            for key, inner_dict in self.indexing.items():
                if(column in self.pri_keys):
                    if(len(self.pri_keys) == 1):
                        value = int(next(iter(key)))
                    else:
                        value = inner_dict[column]
                else:
                    value = inner_dict[column]

                condition_value = int(inner_dict[conditions[0]])
                if(value > max and evaluateCondition(condition_value, conditions[1], conditions[2])):
                            max = value

        elif(single == 2):
            for key, inner_dict in self.indexing.items():
                if(column in self.pri_keys):
                    if(len(self.pri_keys) == 1):
                        value = int(next(iter(key)))
                    else:
                        value = inner_dict[column]
                else:
                    value = inner_dict[column]
                
                condition_value_one = int(inner_dict[conditions[0]])
                condition_value_two = int(inner_dict[conditions[4]])
                condition1 = evaluateCondition(condition_value_one, conditions[1], conditions[2])

                if(value > max):
                    if(conditions[3] == 'AND' and condition1):
                        condition2 = evaluateCondition(condition_value_two, conditions[5], conditions[6])
                        if(condition2):
                            max = value
                    elif(conditions[3] == 'OR' and condition1):
                        max = value
                    elif(conditions[3] == 'OR' and condition1 == False):
                        condition2 = evaluateCondition(condition_value_two, conditions[5], conditions[6])
                        if(condition2):
                            max = value
                
        if(max == INT_MIN):
            tempTable = Table()
            return tempTable
        
        tempTable = Table()
        tempTable.columns = ["max(" + str(column) + ")"]
        tempTable.indexing[0] = {}
        tempTable.indexing[0]["max(" + str(column) + ")"] = max
        tempTable.size = 1

        return tempTable
    
    def nestedLoop(self, table, columns, joinConditions, self_name, table_name, single, conditions, constant, constant2):
        # larger relation is self, smaller relation is table

        tempTable = Table() 

        self_join_column = joinConditions[0][1]
        table_join_column = joinConditions[1][1]

        #DONE No conditions
        if(single == 0):
            for key, inner_dict in table.indexing.items():
                for key2, inner_dict2 in self.indexing.items():
                    if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                        tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)

        #DONE Single condition with a constant value
        elif(single == 1 and constant):
            condition_column = conditions[0][1]

            #DONE If condition table is the outer relation
            if(conditions[0][0] == table_name):
                for key, inner_dict in table.indexing.items():
                    condition_value = inner_dict[condition_column]
                    if(evaluateCondition(condition_value, conditions[1], conditions[2])):
                        for key2, inner_dict2 in self.indexing.items():
                            if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
            #DONE If condition table is the inner relation
            else:
                for key, inner_dict in table.indexing.items():
                    for key2, inner_dict2 in self.indexing.items():
                        condition_value = inner_dict2[condition_column]
                        if(inner_dict[table_join_column] == inner_dict2[self_join_column] and evaluateCondition(condition_value, conditions[1], conditions[2])):
                            tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)

        #DONE Single condition with a variable value
        elif(single == 1 and constant == False):
            condition_column1 = conditions[0][1]
            condition_column2 = conditions[2][1]

            #DONE If left table is the outer relation
            if(conditions[0][0] == table_name):
                for key, inner_dict in table.indexing.items():
                    condition_value1 = inner_dict[condition_column1]
                    for key2, inner_dict2 in self.indexing.items():
                        condition_value2 = inner_dict2[condition_column2]
                        if(inner_dict[table_join_column] == inner_dict2[self_join_column] and evaluateCondition(condition_value1, conditions[1], condition_value2)):
                            tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
            else:
                for key, inner_dict in table.indexing.items():
                    condition_value2 = inner_dict[condition_column2]
                    for key2, inner_dict2 in self.indexing.items():
                        condition_value1 = inner_dict2[condition_column1]
                        if(inner_dict[table_join_column] == inner_dict2[self_join_column] and evaluateCondition(condition_value1, conditions[1], condition_value2)):
                            tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)

        #DONE Double condition with one constant and one variable value
        elif(single == 2 and constant and constant2 == False):
            condition_column1 = conditions[0][1]
            condition_column1_a = conditions[4][1]
            condition_column2_a = conditions[6][1]

            #DONE
            if(conditions[0][0] == table_name and conditions[4][0] == table_name):
                for key, inner_dict in table.indexing.items():
                    condition_value1 = inner_dict[condition_column1]
                    condition1 = evaluateCondition(condition_value1, conditions[1], conditions[2])

                    if(conditions[3] == 'AND' and condition1):
                        condition_value2 = inner_dict[condition_column1_a]
                        condition_value2_a = inner_dict[condition_column2_a]
                        condition2 = evaluateCondition(condition_value2, conditions[5], condition_value2_a)

                        if(condition2):
                            for key2, inner_dict2 in self.indexing.items():
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)

                    elif(conditions[3] == 'OR' and condition1):
                        for key2, inner_dict2 in self.indexing.items():
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)

                    elif(conditions[3] == 'OR' and condition1 == False):
                        condition_value2 = inner_dict[condition_column1_a]
                        condition_value2_a = inner_dict[condition_column2_a]
                        condition2 = evaluateCondition(condition_value2, conditions[5], condition_value2_a)

                        if(condition2):
                            for key2, inner_dict2 in self.indexing.items():
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)

            #DONE
            elif(conditions[0][0] == self_name and conditions[4][0] == self_name):
                for key, inner_dict in table.indexing.items():
                    for key2, inner_dict2 in self.indexing.items():
                        condition_value1 = inner_dict2[condition_column1]
                        condition1 = evaluateCondition(condition_value1, conditions[1], conditions[2])

                        if(conditions[3] == 'AND' and condition1):
                            condition_value2 = inner_dict2[condition_column1_a]
                            condition_value2_a = inner_dict2[condition_column2_a]
                            condition2 = evaluateCondition(condition_value2, conditions[5], condition_value2_a)

                            if(condition2):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                    
                        elif(conditions[3] == 'OR' and condition1):
                            if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                        
                        elif(conditions[3] == 'OR' and condition1 == False):
                            condition_value2 = inner_dict2[condition_column1_a]
                            condition_value2_a = inner_dict2[condition_column2_a]
                            condition2 = evaluateCondition(condition_value2, conditions[5], condition_value2_a)

                            if(condition2):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
            
            #DONE
            elif(conditions[0][0] == table_name and conditions[4][0] == self_name):
                for key, inner_dict in table.indexing.items():
                    condition_value1 = inner_dict[condition_column1]
                    condition1 = evaluateCondition(condition_value1, conditions[1], conditions[2])

                    if(conditions[3] == 'AND' and condition1):
                        for key2, inner_dict2 in self.indexing.items():
                            condition_value2 = inner_dict2[condition_column1_a]
                            condition_value2_a = inner_dict2[condition_column2_a]
                            condition2 = evaluateCondition(condition_value2, conditions[5], condition_column2_a)
                            if(condition2):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                    elif(conditions[3] == 'OR' and condition1):
                        for key2, inner_dict2 in self.indexing.items():
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                    elif(conditions[3] == 'OR' and condition1 == False):
                        for key2, inner_dict2 in self.indexing.items():
                            condition_value2 = inner_dict2[condition_column1_a]
                            condition_value2_a = inner_dict2[condition_column2_a]
                            condition2 = evaluateCondition(condition_value2, conditions[5], condition_value2_a)
                            if(condition2):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
            
            #DONE
            elif(conditions[0][0] == self_name and conditions[4][0] == table_name):
                for key, inner_dict in table.indexing.items():
                    condition_value2 = inner_dict[condition_column1_a]
                    condition_value2_a = inner_dict[condition_column2_a]
                    condition2 = evaluateCondition(condition_value2, conditions[5], condition_value2_a)

                    if(conditions[3] == 'AND' and condition2):
                        for key2, inner_dict2 in self.indexing.items():
                            condition_value1 = inner_dict2[condition_column1]
                            condition1 = evaluateCondition(condition_value1, conditions[1], conditions[2])
                            if(condition1):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                    elif(conditions[3] == 'OR' and condition2):
                        for key2, inner_dict2 in self.indexing.items():
                            if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                    elif(conditions[3] == 'OR' and condition2 == False):
                        for key2, inner_dict2 in self.indexing.items():
                            condition_value1 = inner_dict2[condition_column1]
                            condition1 = evaluateCondition(condition_value1, conditions[1], conditions[2])
                            if(condition1):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)

        #DONE Double condition with one variable and one constant value
        elif(single == 2 and constant == False and constant2):
            condition_column1 = conditions[0][1]
            condition_column2 = conditions[2][1]
            condition_column1_a = conditions[4][1]

            #DONE
            if(conditions[0][0] == table_name and conditions[4][0] == table_name):
                for key, inner_dict in table.indexing.items():
                    condition_value1 = inner_dict[condition_column1]
                    condition_value2_a = inner_dict[condition_column2]
                    condition1 = evaluateCondition(condition_value1, conditions[1], condition_value2_a)

                    if(conditions[3] == 'AND' and condition1):
                        condition_value2 = inner_dict[condition_column1_a]
                        condition2 = evaluateCondition(condition_value2, conditions[5], conditions[6])

                        if(condition2):
                            for key2, inner_dict2 in self.indexing.items():
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)

                    elif(conditions[3] == 'OR' and condition1):
                        for key2, inner_dict2 in self.indexing.items():
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)

                    elif(conditions[3] == 'OR' and condition1 == False):
                        condition_value2 = inner_dict[condition_column1_a]
                        condition2 = evaluateCondition(condition_value2, conditions[5], conditions[6])

                        if(condition2):
                            for key2, inner_dict2 in self.indexing.items():
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)

            #DONE
            elif(conditions[0][0] == self_name and conditions[4][0] == self_name):
                for key, inner_dict in table.indexing.items():
                    for key2, inner_dict2 in self.indexing.items():
                        condition_value1 = inner_dict2[condition_column1]
                        condition_value2_a = inner_dict2[condition_column2]
                        condition1 = evaluateCondition(condition_value1, conditions[1], condition_value2_a)

                        if(conditions[3] == 'AND' and condition1):
                            condition_value2 = inner_dict2[condition_column1_a]
                            condition2 = evaluateCondition(condition_value2, conditions[5], conditions[6])

                            if(condition2):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                    
                        elif(conditions[3] == 'OR' and condition1):
                            if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                        
                        elif(conditions[3] == 'OR' and condition1 == False):
                            condition_value2 = inner_dict2[condition_column1_a]
                            condition2 = evaluateCondition(condition_value2, conditions[5], conditions[6])

                            if(condition2):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
            
            #DONE
            elif(conditions[0][0] == table_name and conditions[4][0] == self_name):
                for key, inner_dict in table.indexing.items():
                    condition_value1 = inner_dict[condition_column1]
                    condition_value2_a = inner_dict[condition_column2]
                    condition1 = evaluateCondition(condition_value1, conditions[1], condition_value2_a)

                    if(conditions[3] == 'AND' and condition1):
                        for key2, inner_dict2 in self.indexing.items():
                            condition_value2 = inner_dict2[condition_column1_a]
                            condition2 = evaluateCondition(condition_value2, conditions[5], conditions[6])
                            if(condition2):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                    elif(conditions[3] == 'OR' and condition1):
                        for key2, inner_dict2 in self.indexing.items():
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                    elif(conditions[3] == 'OR' and condition1 == False):
                        for key2, inner_dict2 in self.indexing.items():
                            condition_value2 = inner_dict2[condition_column1_a]
                            condition2 = evaluateCondition(condition_value2, conditions[5], conditions[6])
                            if(condition2):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
            
            #DONE
            elif(conditions[0][0] == self_name and conditions[4][0] == table_name):
                for key, inner_dict in table.indexing.items():
                    condition_value2_a = inner_dict[condition_column1_a]
                    condition2 = evaluateCondition(condition_value2_a, conditions[5], conditions[6])

                    if(conditions[3] == 'AND' and condition2):
                        for key2, inner_dict2 in self.indexing.items():
                            condition_value1 = inner_dict2[condition_column1]
                            condition_value2 = inner_dict2[condition_column2]
                            condition1 = evaluateCondition(condition_value1, conditions[1], condition_value2)
                            if(condition1):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                    elif(conditions[3] == 'OR' and condition2):
                        for key2, inner_dict2 in self.indexing.items():
                            if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                    elif(conditions[3] == 'OR' and condition2 == False):
                        for key2, inner_dict2 in self.indexing.items():
                            condition_value1 = inner_dict2[condition_column1]
                            condition_value2 = inner_dict2[condition_column2]
                            condition1 = evaluateCondition(condition_value1, conditions[1], condition_value2)
                            if(condition1):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
        #DONE Double conditions with variable values
        elif(single == 2 and constant == False and constant2 == False):
            condition_column1 = conditions[0][1]
            condition_column1_a = conditions[4][1]

            condition_column2 = conditions[2][1]
            condition_column2_a = conditions[6][1]

            #condition_column1 = condition_column2 AND/OR condition_column1_a = condition_column2_a
            
            #DONE
            if(conditions[0][0] == table_name and conditions[4][0] == table_name):

                for key, inner_dict in table.indexing.items():
                    condition_value1 = inner_dict[condition_column1]
                    condition_value2 = inner_dict[condition_column2]
                    condition1 = evaluateCondition(condition_value1, conditions[1], condition_value2)

                    if(conditions[3] == 'AND' and condition1):
                        condition_value3 = inner_dict[condition_column1_a]
                        condition_value4 = inner_dict[condition_column2_a]
                        condition2 = evaluateCondition(condition_value3, conditions[5], condition_value4)

                        if(condition2):
                            for key2, inner_dict2 in self.indexing.items():
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)

                    elif(conditions[3] == 'OR' and condition1):
                        for key2, inner_dict2 in self.indexing.items():
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)

                    elif(conditions[3] == 'OR' and condition1 == False):
                        condition_value3 = inner_dict[condition_column1_a]
                        condition_value4 = inner_dict[condition_column2_a]
                        condition2 = evaluateCondition(condition_value3, conditions[5], condition_value4)

                        if(condition2):
                            for key2, inner_dict2 in self.indexing.items():
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)

            #DONE                    
            elif(conditions[0][0] == self_name and conditions[4][0] == self_name):
                for key, inner_dict in table.indexing.items():
                    for key2, inner_dict2 in self.indexing.items():
                        condition_value1 = inner_dict2[condition_column1]
                        condition_value2 = inner_dict2[condition_column2]
                        condition1 = evaluateCondition(condition_value1, conditions[1], condition_value2)

                        if(conditions[3] == 'AND' and condition1):
                            condition_value3 = inner_dict2[condition_column1_a]
                            condition_value4 = inner_dict2[condition_column2_a]
                            condition2 = evaluateCondition(condition_value3, conditions[5], condition_value4)

                            if(condition2):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                    
                        elif(conditions[3] == 'OR' and condition1):
                            if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                        
                        elif(conditions[3] == 'OR' and condition1 == False):
                            condition_value3 = inner_dict2[condition_column1_a]
                            condition_value4 = inner_dict2[condition_column2_a]
                            condition2 = evaluateCondition(condition_value3, conditions[5], condition_value4)

                            if(condition2):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)

            #DONE
            elif(conditions[0][0] == table_name and conditions[4][0] == self_name):
                for key, inner_dict in table.indexing.items():
                    condition_value1 = inner_dict[condition_column1]
                    condition_value2 = inner_dict[condition_column2]
                    condition1 = evaluateCondition(condition_value1, conditions[1], condition_value2)

                    if(conditions[3] == 'AND' and condition1):
                        for key2, inner_dict2 in self.indexing.items():
                            condition_value3 = inner_dict2[condition_column1_a]
                            condition_value4 = inner_dict2[condition_column2_a]
                            condition2 = evaluateCondition(condition_value3, conditions[5], condition_value4)
                            if(condition2):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                    elif(conditions[3] == 'OR' and condition1):
                        for key2, inner_dict2 in self.indexing.items():
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                    elif(conditions[3] == 'OR' and condition1 == False):
                        for key2, inner_dict2 in self.indexing.items():
                            condition_value3 = inner_dict2[condition_column1_a]
                            condition_value4 = inner_dict2[condition_column2_a]
                            condition2 = evaluateCondition(condition_value3, conditions[5], condition_value4)
                            if(condition2):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)

            #DONE
            elif(conditions[0][0] == self_name and conditions[4][0] == table_name):
                for key, inner_dict in table.indexing.items():
                    condition_value3 = inner_dict[condition_column1_a]
                    condition_value4 = inner_dict[condition_column2_a]
                    condition2 = evaluateCondition(condition_value3, conditions[5], condition_value4)

                    if(conditions[3] == 'AND' and condition2):
                        for key2, inner_dict2 in self.indexing.items():
                            condition_value1 = inner_dict2[condition_column1]
                            condition_value2 = inner_dict2[condition_column2]
                            condition1 = evaluateCondition(condition_value1, conditions[1], condition_value2)
                            if(condition1):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                    elif(conditions[3] == 'OR' and condition2):
                        for key2, inner_dict2 in self.indexing.items():
                            if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                    elif(conditions[3] == 'OR' and condition2 == False):
                        for key2, inner_dict2 in self.indexing.items():
                            condition_value1 = inner_dict2[condition_column1]
                            condition_value2 = inner_dict2[condition_column2]
                            condition1 = evaluateCondition(condition_value1, conditions[1], condition_value2)
                            if(condition1):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
    
        #DONE Double conditions with constant values
        elif(single == 2 and constant and constant2):
            condition_column1 = conditions[0][1]
            condition_column1_a = conditions[4][1]
            
            #DONE
            if(conditions[0][0] == table_name and conditions[4][0] == table_name):

                for key, inner_dict in table.indexing.items():
                    condition_value1 = inner_dict[condition_column1]
                    condition1 = evaluateCondition(condition_value1, conditions[1], conditions[2])

                    if(conditions[3] == 'AND' and condition1):
                        condition_value2 = inner_dict[condition_column1_a]
                        condition2 = evaluateCondition(condition_value2, conditions[5], conditions[6])

                        if(condition2):
                            for key2, inner_dict2 in self.indexing.items():
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)

                    elif(conditions[3] == 'OR' and condition1):
                        for key2, inner_dict2 in self.indexing.items():
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)

                    elif(conditions[3] == 'OR' and condition1 == False):
                        condition_value2 = inner_dict[condition_column1_a]
                        condition2 = evaluateCondition(condition_value2, conditions[5], conditions[6])

                        if(condition2):
                            for key2, inner_dict2 in self.indexing.items():
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)

            #DONE                       
            elif(conditions[0][0] == self_name and conditions[4][0] == self_name):
                for key, inner_dict in table.indexing.items():
                    for key2, inner_dict2 in self.indexing.items():
                        condition_value1 = inner_dict2[condition_column1]
                        condition1 = evaluateCondition(condition_value1, conditions[1], conditions[2])

                        if(conditions[3] == 'AND' and condition1):
                            condition_value2 = inner_dict2[condition_column1_a]
                            condition2 = evaluateCondition(condition_value2, conditions[5], conditions[6])

                            if(condition2):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                    
                        elif(conditions[3] == 'OR' and condition1):
                            if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                        
                        elif(conditions[3] == 'OR' and condition1 == False):
                            condition_value2 = inner_dict2[condition_column1_a]
                            condition2 = evaluateCondition(condition_value2, conditions[5], conditions[6])

                            if(condition2):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)

            #DONE 
            elif(conditions[0][0] == table_name and conditions[4][0] == self_name):
                for key, inner_dict in table.indexing.items():
                    condition_value1 = inner_dict[condition_column1]
                    condition1 = evaluateCondition(condition_value1, conditions[1], conditions[2])

                    if(conditions[3] == 'AND' and condition1):
                        for key2, inner_dict2 in self.indexing.items():
                            condition_value2 = inner_dict2[condition_column1_a]
                            condition2 = evaluateCondition(condition_value2, conditions[5], conditions[6])
                            if(condition2):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                    elif(conditions[3] == 'OR' and condition1):
                        for key2, inner_dict2 in self.indexing.items():
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                    elif(conditions[3] == 'OR' and condition1 == False):
                        for key2, inner_dict2 in self.indexing.items():
                            condition_value2 = inner_dict2[condition_column1_a]
                            condition2 = evaluateCondition(condition_value2, conditions[5], conditions[6])
                            if(condition2):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)

            #DONE
            elif(conditions[0][0] == self_name and conditions[4][0] == table_name):
                for key, inner_dict in table.indexing.items():
                    condition_value2 = inner_dict[condition_column1_a]
                    condition2 = evaluateCondition(condition_value2, conditions[5], conditions[6])

                    if(conditions[3] == 'AND' and condition2):
                        for key2, inner_dict2 in self.indexing.items():
                            condition_value1 = inner_dict2[condition_column1]
                            condition1 = evaluateCondition(condition_value1, conditions[1], conditions[2])
                            if(condition1):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                    elif(conditions[3] == 'OR' and condition2):
                        for key2, inner_dict2 in self.indexing.items():
                            if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)
                    elif(conditions[3] == 'OR' and condition2 == False):
                        for key2, inner_dict2 in self.indexing.items():
                            condition_value1 = inner_dict2[condition_column1]
                            condition1 = evaluateCondition(condition_value1, conditions[1], conditions[2])
                            if(condition1):
                                if(inner_dict[table_join_column] == inner_dict2[self_join_column]):
                                    tempTable.addRow(inner_dict, inner_dict2, columns, self_name, table_name)

        return tempTable
    
    def addRowMergeScan(self, self_row, table_row, columns, self_column_names, table_column_names, self_name, table_name):
        join_columns = []
        global JOIN_KEY

        for i in range(len(self_row)):
            if(self_column_names[i] in columns[0]):
                if(JOIN_KEY not in self.indexing):
                    self.indexing[JOIN_KEY] = {}
                column_name = (str(self_name) + '.' + str(self_column_names[i]))
                self.indexing[JOIN_KEY][column_name] = self_row[i]
                self.size+=1
                join_columns.append(column_name)
            
        for i in range(len(table_row)):
            if(table_column_names[i] in columns[1]):
                if(JOIN_KEY not in self.indexing):
                    self.indexing[JOIN_KEY] = {}
                column_name = (str(table_name) + '.' + str(table_column_names[i]))
                self.indexing[JOIN_KEY][column_name] = table_row[i]
                join_columns.append(column_name)
        
        JOIN_KEY += 1

        self.columns = join_columns
    
    def mergeScanHelper(self, tempTable, sorted_self, sorted_table, i, j, columns, self_column_names, table_column_names, self_name, table_name):
        k = l = 0
        while k < len(sorted_self) and l < len(sorted_table):
            if sorted_self[k][i] == sorted_table[l][j]:
                tempTable.addRowMergeScan(sorted_self[k], sorted_table[l], columns, self_column_names, table_column_names, self_name, table_name)
                k += 1
                l += 1
            elif sorted_self[k][i] < sorted_table[l][j]:
                k += 1
            else:
                l += 1

    def mergeScanHelperSingle(self, tempTable, sorted_self, sorted_table, i, j, columns, conditions, self_column_names, table_column_names, self_name, table_name, constant):
        condition_column_var = ''
        condition_var_index = 0
        condition_column = conditions[0][1]
        if(constant == False):
            condition_column_var = conditions[2][1]
        
        if(conditions[0][0] == self_name):
            condition_index = self_column_names.index(condition_column)
            if(constant == False and conditions[2][0] == self_name):
                condition_var_index = self_column_names.index(condition_column_var)
            elif(constant == False and conditions[2][0] == table_name):
                condition_var_index = table_column_names.index(condition_column_var)
            k = l = 0

            while k < len(sorted_self) and l < len(sorted_table):
                if sorted_self[k][i] == sorted_table[l][j]:
                    condition_value = sorted_self[k][condition_index]
                    condition_met = False

                    if(constant):
                        condition_met = evaluateCondition(condition_value, conditions[1], conditions[2])
                    else:
                        if(conditions[2][0] == self_name):
                            condition_value_var = sorted_self[k][condition_var_index]
                        else:
                            condition_value_var = sorted_table[l][condition_var_index]
                        condition_met = evaluateCondition(condition_value, conditions[1], condition_value_var)

                    if(condition_met):
                        tempTable.addRowMergeScan(sorted_self[k], sorted_table[l], columns, self_column_names, table_column_names, self_name, table_name)
                    k += 1
                    l += 1
                elif sorted_self[k][i] < sorted_table[l][j]:
                    k += 1
                else:
                    l += 1
        else:
            condition_index = table_column_names.index(condition_column)
            if(constant == False and conditions[2][0] == self_name):
                condition_var_index = self_column_names.index(condition_column_var)
            elif(constant == False and conditions[2][0] == table_name):
                condition_var_index = table_column_names.index(condition_column_var)
            k = l = 0

            while k < len(sorted_self) and l < len(sorted_table):
                if sorted_self[k][i] == sorted_table[l][j]:
                    condition_value = sorted_table[l][condition_index]
                    condition_met = False
                    if(constant):
                        condition_met = evaluateCondition(condition_value, conditions[1], conditions[2])
                    else:
                        if(conditions[2][0] == self_name):
                            condition_value_var = sorted_self[k][condition_var_index]
                        else:
                            condition_value_var = sorted_table[l][condition_var_index]
                        condition_met = evaluateCondition(condition_value, conditions[1], condition_value_var)

                    if(condition_met):
                        tempTable.addRowMergeScan(sorted_self[k], sorted_table[l], columns, self_column_names, table_column_names, self_name, table_name)
                    k += 1
                    l += 1
                elif sorted_self[k][i] < sorted_table[l][j]:
                    k += 1
                else:
                    l += 1

    def mergeScanHelperOr(self, tempTable, sorted_self, sorted_table, i, j, columns, conditions, self_column_names, table_column_names, self_name, table_name, constant, constant2):
        condition_column_var = ''
        condition_column = conditions[0][1]
        condition_column2 = conditions[4][1]

        if(constant == False):
            condition_column_var = conditions[2][1]
        if(constant2 == False):
            condition_column_var_2 = conditions[6][1]

        if(conditions[0][0] == self_name and conditions[4][0] == self_name):
            condition_index = self_column_names.index(condition_column)
            condition_index2 = self_column_names.index(condition_column2)

            if(constant == False and conditions[2][0] == self_name):
                condition_var_index = self_column_names.index(condition_column_var)
            elif(constant == False and conditions[2][0] == table_name):
                condition_var_index = table_column_names.index(condition_column_var)

            if(constant2 == False and conditions[6][0] == self_name):
                condition_var_index_2 = self_column_names.index(condition_column_var_2)
            elif(constant2 == False and conditions[6][0] == table_name):
                condition_var_index_2 = table_column_names.index(condition_column_var_2)

            k = l = 0

            while k < len(sorted_self) and l < len(sorted_table):
                if sorted_self[k][i] == sorted_table[l][j]:
                    condition_value = sorted_self[k][condition_index]
                    condition_met = False

                    if(constant):
                        condition_met = evaluateCondition(condition_value, conditions[1], conditions[2])
                        if(constant2):
                            if(condition_met == False):
                                condition_value_2 = sorted_self[k][condition_index2]
                                condition_met = evaluateCondition(condition_value_2, conditions[5], conditions[6])
                        else:
                            if(conditions[6][0] == self_name):
                                if(condition_met == False):
                                    condition_value_2 = sorted_self[k][condition_index2]
                                    condition_value_var_2 = sorted_self[k][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                            else:
                                if(condition_met == False):
                                    condition_value_2 = sorted_self[condition_index2]
                                    condition_value_var_2 = sorted_table[l][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                    else:
                        if(conditions[2][0] == self_name):
                            condition_value = sorted_self[k][condition_var_index]
                            condition_value_var = sorted_self[k][condition_var_index_2]
                            condition_met = evaluateCondition(condition_value, conditions[1], condition_value_var)
                        else:
                            condition_value = sorted_self[k][condition_var_index]
                            condition_value_var = sorted_table[l][condition_var_index_2]
                            condition_met = evaluateCondition(condition_value, conditions[1], condition_value_var)

                        if(constant2):
                            if(condition_met == False):
                                condition_value_2 = sorted_self[k][condition_index2]
                                condition_met = evaluateCondition(condition_value_2, conditions[5], conditions[6])
                        else:
                            if(conditions[6][0] == self_name):
                                if(condition_met == False):
                                    condition_value_2 = sorted_self[k][condition_index2]
                                    condition_value_var_2 = sorted_self[k][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                            else:
                                if(condition_met == False):
                                    condition_value_2 = sorted_self[k][condition_index2]
                                    condition_value_var_2 - sorted_table[l][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)

                    if(condition_met):
                        tempTable.addRowMergeScan(sorted_self[k], sorted_table[l], columns, self_column_names, table_column_names, self_name, table_name)
                    k += 1
                    l += 1
                elif sorted_self[k][i] < sorted_table[l][j]:
                    k += 1
                else:
                    l += 1
        elif(conditions[0][0] == table_name and conditions[4][0] == table_name):
            condition_index = table_column_names.index(condition_column)
            condition_index2 = table_column_names.index(condition_column2)

            if(constant == False and conditions[2][0] == self_name):
                condition_var_index = self_column_names.index(condition_column_var)
            elif(constant == False and conditions[2][0] == table_name):
                condition_var_index = table_column_names.index(condition_column_var)

            if(constant2 == False and conditions[6][0] == self_name):
                condition_var_index_2 = self_column_names.index(condition_column_var_2)
            elif(constant2 == False and conditions[6][0] == table_name):
                condition_var_index_2 = table_column_names.index(condition_column_var_2)

            k = l = 0

            while k < len(sorted_self) and l < len(sorted_table):
                if sorted_self[k][i] == sorted_table[l][j]:
                    condition_value = sorted_table[l][condition_index]
                    condition_met = False

                    if(constant):
                        condition_met = evaluateCondition(condition_value, conditions[1], conditions[2])
                        if(constant2):
                            if(condition_met == False):
                                condition_value_2 = sorted_table[l][condition_index2]
                                condition_met = evaluateCondition(condition_value_2, conditions[5], conditions[6])
                        else:
                            if(conditions[6][0] == self_name):
                                if(condition_met == False):
                                    condition_value_2 = sorted_table[l][condition_index2]
                                    condition_value_var_2 = sorted_self[k][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                            else:
                                if(condition_met == False):
                                    condition_value_2 = sorted_table[l][condition_index2]
                                    condition_value_var_2 = sorted_table[l][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                    else:
                        if(conditions[2][0] == self_name):
                            condition_value = sorted_table[l][condition_var_index]
                            condition_value_var = sorted_self[k][condition_var_index_2]
                            condition_met = evaluateCondition(condition_value, conditions[1], condition_value_var)
                        else:
                            condition_value = sorted_table[l][condition_var_index]
                            condition_value_var = sorted_table[l][condition_var_index_2]
                            condition_met = evaluateCondition(condition_value, conditions[1], condition_value_var)

                        if(constant2):
                            if(condition_met == False):
                                condition_value_2 = sorted_table[l][condition_index2]
                                condition_met = evaluateCondition(condition_value_2, conditions[5], conditions[6])
                        else:
                            if(conditions[6][0] == self_name):
                                if(condition_met == False):
                                    condition_value_2 = sorted_table[l][condition_index2]
                                    condition_value_var_2 = sorted_self[k][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                            else:
                                if(condition_met == False):
                                    condition_value_2 = sorted_table[l][condition_index2]
                                    condition_value_var_2 - sorted_table[l][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)

                    if(condition_met):
                        tempTable.addRowMergeScan(sorted_self[k], sorted_table[l], columns, self_column_names, table_column_names, self_name, table_name)
                    k += 1
                    l += 1
                elif sorted_self[k][i] < sorted_table[l][j]:
                    k += 1
                else:
                    l += 1
        elif(conditions[0][0] == self_name and conditions[4][0] == table_name):
            condition_index = self_column_names.index(condition_column)
            condition_index2 = table_column_names.index(condition_column2)

            if(constant == False and conditions[2][0] == self_name):
                condition_var_index = self_column_names.index(condition_column_var)
            elif(constant == False and conditions[2][0] == table_name):
                condition_var_index = table_column_names.index(condition_column_var)

            if(constant2 == False and conditions[6][0] == self_name):
                condition_var_index_2 = self_column_names.index(condition_column_var_2)
            elif(constant2 == False and conditions[6][0] == table_name):
                condition_var_index_2 = table_column_names.index(condition_column_var_2)

            k = l = 0

            while k < len(sorted_self) and l < len(sorted_table):
                if sorted_self[k][i] == sorted_table[l][j]:
                    condition_value = sorted_self[k][condition_index]
                    condition_met = False

                    if(constant):
                        condition_met = evaluateCondition(condition_value, conditions[1], conditions[2])
                        if(constant2):
                            if(condition_met == False):
                                condition_value_2 = sorted_table[l][condition_index2]
                                condition_met = evaluateCondition(condition_value_2, conditions[5], conditions[6])
                        else:
                            if(conditions[6][0] == self_name):
                                if(condition_met == False):
                                    condition_value_2 = sorted_table[l][condition_index2]
                                    condition_value_var_2 = sorted_self[k][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                            else:
                                if(condition_met == False):
                                    condition_value_2 = sorted_table[l][condition_index2]
                                    condition_value_var_2 = sorted_table[l][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                    else:
                        if(conditions[2][0] == self_name):
                            condition_value = sorted_self[k][condition_var_index]
                            condition_value_var = sorted_self[k][condition_var_index_2]
                            condition_met = evaluateCondition(condition_value, conditions[1], condition_value_var)
                        else:
                            condition_value = sorted_self[k][condition_var_index]
                            condition_value_var = sorted_table[l][condition_var_index_2]
                            condition_met = evaluateCondition(condition_value, conditions[1], condition_value_var)

                        if(constant2):
                            if(condition_met == False):
                                condition_value_2 = sorted_table[l][condition_index2]
                                condition_met = evaluateCondition(condition_value_2, conditions[5], conditions[6])
                        else:
                            if(conditions[6][0] == self_name):
                                if(condition_met == False):
                                    condition_value_2 = sorted_table[l][condition_index2]
                                    condition_value_var_2 = sorted_self[k][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                            else:
                                if(condition_met == False):
                                    condition_value_2 = sorted_table[l][condition_index2]
                                    condition_value_var_2 - sorted_table[l][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)

                    if(condition_met):
                        tempTable.addRowMergeScan(sorted_self[k], sorted_table[l], columns, self_column_names, table_column_names, self_name, table_name)
                    k += 1
                    l += 1
                elif sorted_self[k][i] < sorted_table[l][j]:
                    k += 1
                else:
                    l += 1
        elif(conditions[0][0] == table_name and conditions[4][0] == self_name):
            condition_index = table_column_names.index(condition_column)
            condition_index2 = self_column_names.index(condition_column2)

            if(constant == False and conditions[2][0] == self_name):
                condition_var_index = self_column_names.index(condition_column_var)
            elif(constant == False and conditions[2][0] == table_name):
                condition_var_index = table_column_names.index(condition_column_var)

            if(constant2 == False and conditions[6][0] == self_name):
                condition_var_index_2 = self_column_names.index(condition_column_var_2)
            elif(constant2 == False and conditions[6][0] == table_name):
                condition_var_index_2 = table_column_names.index(condition_column_var_2)

            k = l = 0

            while k < len(sorted_self) and l < len(sorted_table):
                if sorted_self[k][i] == sorted_table[l][j]:
                    condition_value = sorted_table[l][condition_index]
                    condition_met = False

                    if(constant):
                        condition_met = evaluateCondition(condition_value, conditions[1], conditions[2])
                        if(constant2):
                            if(condition_met == False):
                                condition_value_2 = sorted_self[k][condition_index2]
                                condition_met = evaluateCondition(condition_value_2, conditions[5], conditions[6])
                        else:
                            if(conditions[6][0] == self_name):
                                if(condition_met == False):
                                    condition_value_2 = sorted_self[k][condition_index2]
                                    condition_value_var_2 = sorted_self[k][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                            else:
                                if(condition_met == False):
                                    condition_value_2 = sorted_self[k][condition_index2]
                                    condition_value_var_2 = sorted_table[l][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                    else:
                        if(conditions[2][0] == self_name):
                            condition_value = sorted_table[l][condition_var_index]
                            condition_value_var = sorted_self[k][condition_var_index_2]
                            condition_met = evaluateCondition(condition_value, conditions[1], condition_value_var)
                        else:
                            condition_value = sorted_table[l][condition_var_index]
                            condition_value_var = sorted_table[l][condition_var_index_2]
                            condition_met = evaluateCondition(condition_value, conditions[1], condition_value_var)

                        if(constant2):
                            if(condition_met == False):
                                condition_value_2 = sorted_self[k][condition_index2]
                                condition_met = evaluateCondition(condition_value_2, conditions[5], conditions[6])
                        else:
                            if(conditions[6][0] == self_name):
                                if(condition_met == False):
                                    condition_value_2 = sorted_self[k][condition_index2]
                                    condition_value_var_2 = sorted_self[k][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                            else:
                                if(condition_met == False):
                                    condition_value_2 = sorted_self[k][condition_index2]
                                    condition_value_var_2 - sorted_table[l][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)

                    if(condition_met):
                        tempTable.addRowMergeScan(sorted_self[k], sorted_table[l], columns, self_column_names, table_column_names, self_name, table_name)
                    k += 1
                    l += 1
                elif sorted_self[k][i] < sorted_table[l][j]:
                    k += 1
                else:
                    l += 1

    def mergeScanHelperAnd(self, tempTable, sorted_self, sorted_table, i, j, columns, conditions, self_column_names, table_column_names, self_name, table_name, constant, constant2):
        condition_column_var = ''
        condition_column = conditions[0][1]
        condition_column2 = conditions[4][1]

        if(constant == False):
            condition_column_var = conditions[2][1]
        if(constant2 == False):
            condition_column_var_2 = conditions[6][1]

        if(conditions[0][0] == self_name and conditions[4][0] == self_name):
            condition_index = self_column_names.index(condition_column)
            condition_index2 = self_column_names.index(condition_column2)

            if(constant == False and conditions[2][0] == self_name):
                condition_var_index = self_column_names.index(condition_column_var)
            elif(constant == False and conditions[2][0] == table_name):
                condition_var_index = table_column_names.index(condition_column_var)

            if(constant2 == False and conditions[6][0] == self_name):
                condition_var_index_2 = self_column_names.index(condition_column_var_2)
            elif(constant2 == False and conditions[6][0] == table_name):
                condition_var_index_2 = table_column_names.index(condition_column_var_2)

            k = l = 0

            while k < len(sorted_self) and l < len(sorted_table):
                if sorted_self[k][i] == sorted_table[l][j]:
                    condition_value = sorted_self[k][condition_index]
                    condition_met = False

                    if(constant):
                        condition_met = evaluateCondition(condition_value, conditions[1], conditions[2])
                        if(constant2):
                            if(condition_met):
                                condition_value_2 = sorted_self[k][condition_index2]
                                condition_met = evaluateCondition(condition_value_2, conditions[5], conditions[6])
                            else: 
                                condition_met = False
                        else:
                            if(conditions[6][0] == self_name):
                                if(condition_met):
                                    condition_value_2 = sorted_self[k][condition_index2]
                                    condition_value_var_2 = sorted_self[k][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                                else:
                                    condition_met = False
                            else:
                                if(condition_met):
                                    condition_value_2 = sorted_self[condition_index2]
                                    condition_value_var_2 = sorted_table[l][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                    else:
                        if(conditions[2][0] == self_name):
                            condition_value = sorted_self[k][condition_var_index]
                            condition_value_var = sorted_self[k][condition_var_index_2]
                            condition_met = evaluateCondition(condition_value, conditions[1], condition_value_var)
                        else:
                            condition_value = sorted_self[k][condition_var_index]
                            condition_value_var = sorted_table[l][condition_var_index_2]
                            condition_met = evaluateCondition(condition_value, conditions[1], condition_value_var)

                        if(constant2):
                            if(condition_met):
                                condition_value_2 = sorted_self[k][condition_index2]
                                condition_met = evaluateCondition(condition_value_2, conditions[5], conditions[6])
                            else:
                                condition_met = False
                        else:
                            if(conditions[6][0] == self_name):
                                if(condition_met):
                                    condition_value_2 = sorted_self[k][condition_index2]
                                    condition_value_var_2 = sorted_self[k][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                                else:
                                    condition_met = False
                            else:
                                if(condition_met):
                                    condition_value_2 = sorted_self[k][condition_index2]
                                    condition_value_var_2 - sorted_table[l][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)

                    if(condition_met):
                        tempTable.addRowMergeScan(sorted_self[k], sorted_table[l], columns, self_column_names, table_column_names, self_name, table_name)
                    k += 1
                    l += 1
                elif sorted_self[k][i] < sorted_table[l][j]:
                    k += 1
                else:
                    l += 1
        elif(conditions[0][0] == table_name and conditions[4][0] == table_name):
            condition_index = table_column_names.index(condition_column)
            condition_index2 = table_column_names.index(condition_column2)

            if(constant == False and conditions[2][0] == self_name):
                condition_var_index = self_column_names.index(condition_column_var)
            elif(constant == False and conditions[2][0] == table_name):
                condition_var_index = table_column_names.index(condition_column_var)

            if(constant2 == False and conditions[6][0] == self_name):
                condition_var_index_2 = self_column_names.index(condition_column_var_2)
            elif(constant2 == False and conditions[6][0] == table_name):
                condition_var_index_2 = table_column_names.index(condition_column_var_2)

            k = l = 0

            while k < len(sorted_self) and l < len(sorted_table):
                if sorted_self[k][i] == sorted_table[l][j]:
                    condition_value = sorted_table[l][condition_index]
                    condition_met = False

                    if(constant):
                        condition_met = evaluateCondition(condition_value, conditions[1], conditions[2])
                        if(constant2):
                            if(condition_met):
                                condition_value_2 = sorted_table[l][condition_index2]
                                condition_met = evaluateCondition(condition_value_2, conditions[5], conditions[6])
                            else: 
                                condition_met = False
                        else:
                            if(conditions[6][0] == self_name):
                                if(condition_met):
                                    condition_value_2 = sorted_table[l][condition_index2]
                                    condition_value_var_2 = sorted_self[k][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                                else:
                                    condition_met = False
                            else:
                                if(condition_met):
                                    condition_value_2 = sorted_table[l][condition_index2]
                                    condition_value_var_2 = sorted_table[l][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                    else:
                        if(conditions[2][0] == self_name):
                            condition_value = sorted_table[l][condition_var_index]
                            condition_value_var = sorted_self[k][condition_var_index_2]
                            condition_met = evaluateCondition(condition_value, conditions[1], condition_value_var)
                        else:
                            condition_value = sorted_table[l][condition_var_index]
                            condition_value_var = sorted_table[l][condition_var_index_2]
                            condition_met = evaluateCondition(condition_value, conditions[1], condition_value_var)

                        if(constant2):
                            if(condition_met):
                                condition_value_2 = sorted_table[l][condition_index2]
                                condition_met = evaluateCondition(condition_value_2, conditions[5], conditions[6])
                            else:
                                condition_met = False
                        else:
                            if(conditions[6][0] == self_name):
                                if(condition_met):
                                    condition_value_2 = sorted_table[l][condition_index2]
                                    condition_value_var_2 = sorted_self[k][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                                else:
                                    condition_met = False
                            else:
                                if(condition_met):
                                    condition_value_2 = sorted_table[l][condition_index2]
                                    condition_value_var_2 - sorted_table[l][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)

                    if(condition_met):
                        tempTable.addRowMergeScan(sorted_self[k], sorted_table[l], columns, self_column_names, table_column_names, self_name, table_name)
                    k += 1
                    l += 1
                elif sorted_self[k][i] < sorted_table[l][j]:
                    k += 1
                else:
                    l += 1
        elif(conditions[0][0] == self_name and conditions[4][0] == table_name):
            condition_index = self_column_names.index(condition_column)
            condition_index2 = table_column_names.index(condition_column2)

            if(constant == False and conditions[2][0] == self_name):
                condition_var_index = self_column_names.index(condition_column_var)
            elif(constant == False and conditions[2][0] == table_name):
                condition_var_index = table_column_names.index(condition_column_var)

            if(constant2 == False and conditions[6][0] == self_name):
                condition_var_index_2 = self_column_names.index(condition_column_var_2)
            elif(constant2 == False and conditions[6][0] == table_name):
                condition_var_index_2 = table_column_names.index(condition_column_var_2)

            k = l = 0

            while k < len(sorted_self) and l < len(sorted_table):
                if sorted_self[k][i] == sorted_table[l][j]:
                    condition_value = sorted_self[k][condition_index]
                    condition_met = False

                    if(constant):
                        condition_met = evaluateCondition(condition_value, conditions[1], conditions[2])
                        if(constant2):
                            if(condition_met):
                                condition_value_2 = sorted_table[l][condition_index2]
                                condition_met = evaluateCondition(condition_value_2, conditions[5], conditions[6])
                            else: 
                                condition_met = False
                        else:
                            if(conditions[6][0] == self_name):
                                if(condition_met):
                                    condition_value_2 = sorted_table[l][condition_index2]
                                    condition_value_var_2 = sorted_self[k][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                                else:
                                    condition_met = False
                            else:
                                if(condition_met):
                                    condition_value_2 = sorted_table[l][condition_index2]
                                    condition_value_var_2 = sorted_table[l][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                    else:
                        if(conditions[2][0] == self_name):
                            condition_value = sorted_self[k][condition_var_index]
                            condition_value_var = sorted_self[k][condition_var_index_2]
                            condition_met = evaluateCondition(condition_value, conditions[1], condition_value_var)
                        else:
                            condition_value = sorted_self[k][condition_var_index]
                            condition_value_var = sorted_table[l][condition_var_index_2]
                            condition_met = evaluateCondition(condition_value, conditions[1], condition_value_var)

                        if(constant2):
                            if(condition_met):
                                condition_value_2 = sorted_table[l][condition_index2]
                                condition_met = evaluateCondition(condition_value_2, conditions[5], conditions[6])
                            else:
                                condition_met = False
                        else:
                            if(conditions[6][0] == self_name):
                                if(condition_met):
                                    condition_value_2 = sorted_table[l][condition_index2]
                                    condition_value_var_2 = sorted_self[k][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                                else:
                                    condition_met = False
                            else:
                                if(condition_met):
                                    condition_value_2 = sorted_table[l][condition_index2]
                                    condition_value_var_2 - sorted_table[l][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)

                    if(condition_met):
                        tempTable.addRowMergeScan(sorted_self[k], sorted_table[l], columns, self_column_names, table_column_names, self_name, table_name)
                    k += 1
                    l += 1
                elif sorted_self[k][i] < sorted_table[l][j]:
                    k += 1
                else:
                    l += 1
        elif(conditions[0][0] == table_name and conditions[4][0] == self_name):
            condition_index = table_column_names.index(condition_column)
            condition_index2 = self_column_names.index(condition_column2)

            if(constant == False and conditions[2][0] == self_name):
                condition_var_index = self_column_names.index(condition_column_var)
            elif(constant == False and conditions[2][0] == table_name):
                condition_var_index = table_column_names.index(condition_column_var)

            if(constant2 == False and conditions[6][0] == self_name):
                condition_var_index_2 = self_column_names.index(condition_column_var_2)
            elif(constant2 == False and conditions[6][0] == table_name):
                condition_var_index_2 = table_column_names.index(condition_column_var_2)

            k = l = 0

            while k < len(sorted_self) and l < len(sorted_table):
                if sorted_self[k][i] == sorted_table[l][j]:
                    condition_value = sorted_table[l][condition_index]
                    condition_met = False

                    if(constant):
                        condition_met = evaluateCondition(condition_value, conditions[1], conditions[2])
                        if(constant2):
                            if(condition_met):
                                condition_value_2 = sorted_self[k][condition_index2]
                                condition_met = evaluateCondition(condition_value_2, conditions[5], conditions[6])
                            else: 
                                condition_met = False
                        else:
                            if(conditions[6][0] == self_name):
                                if(condition_met):
                                    condition_value_2 = sorted_self[k][condition_index2]
                                    condition_value_var_2 = sorted_self[k][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                                else:
                                    condition_met = False
                            else:
                                if(condition_met):
                                    condition_value_2 = sorted_self[k][condition_index2]
                                    condition_value_var_2 = sorted_table[l][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                    else:
                        if(conditions[2][0] == self_name):
                            condition_value = sorted_table[l][condition_var_index]
                            condition_value_var = sorted_self[k][condition_var_index_2]
                            condition_met = evaluateCondition(condition_value, conditions[1], condition_value_var)
                        else:
                            condition_value = sorted_table[l][condition_var_index]
                            condition_value_var = sorted_table[l][condition_var_index_2]
                            condition_met = evaluateCondition(condition_value, conditions[1], condition_value_var)

                        if(constant2):
                            if(condition_met):
                                condition_value_2 = sorted_self[k][condition_index2]
                                condition_met = evaluateCondition(condition_value_2, conditions[5], conditions[6])
                            else:
                                condition_met = False
                        else:
                            if(conditions[6][0] == self_name):
                                if(condition_met):
                                    condition_value_2 = sorted_self[k][condition_index2]
                                    condition_value_var_2 = sorted_self[k][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)
                                else:
                                    condition_met = False
                            else:
                                if(condition_met):
                                    condition_value_2 = sorted_self[k][condition_index2]
                                    condition_value_var_2 - sorted_table[l][condition_var_index_2]
                                    condition_met = evaluateCondition(condition_value_2, conditions[5], condition_value_var_2)

                    if(condition_met):
                        tempTable.addRowMergeScan(sorted_self[k], sorted_table[l], columns, self_column_names, table_column_names, self_name, table_name)
                    k += 1
                    l += 1
                elif sorted_self[k][i] < sorted_table[l][j]:
                    k += 1
                else:
                    l += 1

    def mergeScan(self, table, columns, joinConditions, self_name, table_name, single, conditions, constant, constant2):
        #columns[0] = self, columns[1] = table

        self_column_names = []
        table_column_names = []

        tempTable = Table()

        self_column = joinConditions[0][1]
        table_column = joinConditions[1][1]

        random_self_key = next(iter(self.indexing))
        i = 0
        seen_self = False

        for column in self.indexing[random_self_key]:
            self_column_names.append(column)
            if(column == self_column):
                seen_self = True
            if(seen_self == False):
                i+=1

        random_table_key = next(iter(table.indexing))
        j = 0
        seen_table = False

        for column in table.indexing[random_table_key]:
            table_column_names.append(column)
            if(column == table_column):
                seen_table = True
            if(seen_table == False):
                j+=1

        self_list = [[value for value in inner_dict.values()] for inner_dict in self.indexing.values()]
        table_list = [[value for value in inner_dict.values()] for inner_dict in table.indexing.values()]

        sorted_self = sorted(self_list, key=lambda x: x[i])
        sorted_table = sorted(table_list, key=lambda x: x[j])
        
        if(single == 0):
            self.mergeScanHelper(tempTable, sorted_self, sorted_table, i, j, columns, self_column_names, table_column_names, self_name, table_name)
        elif(single == 1):
            self.mergeScanHelperSingle(tempTable, sorted_self, sorted_table, i, j, columns, conditions, self_column_names, table_column_names, self_name, table_name, constant)
        elif(single == 2):
            if(conditions[3] == 'AND'):
                self.mergeScanHelperAnd(tempTable, sorted_self, sorted_table, i, j, columns, conditions, self_column_names, table_column_names, self_name, table_name, constant)
            else:
                self.mergeScanHelperOr(tempTable, sorted_self, sorted_table, i, j, columns, conditions, self_column_names, table_column_names, self_name, table_name, constant)
   
                
        return tempTable 
    


    def addRow(self, inner_dict, inner_dict2, columns, self_name, table_name):
        #columns[0] = self.columns, columns[1] = table.columns
        #inner_dict is table, inner_dict2 is self

        join_columns = []
        global JOIN_KEY

        for column, value in inner_dict.items():
            if(column in columns[1]):
                if(JOIN_KEY not in self.indexing):
                    self.indexing[JOIN_KEY] = {}
                column_name = (str(table_name) + '.' + str(column))
                self.indexing[JOIN_KEY][column_name] = value
                self.size+=1
                join_columns.append(column_name)
        
        for column, value in inner_dict2.items():
            if(column in columns[0]):
                if(JOIN_KEY not in self.indexing):
                    self.indexing[JOIN_KEY] = {}
                column_name = (str(self_name) + '.' + str(column))
                self.indexing[JOIN_KEY][column_name] = value
                self.size+=1
                join_columns.append(column_name)

        JOIN_KEY += 1

        self.columns = join_columns

