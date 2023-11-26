# DatabasesP3 Design Ideas
### **Assumption 1**: Only one valid and complete input is taken at one time
### **Assumption 2**: All of the key words are case insensitive, but everything else is sensitive
### **Assumption 3**: Assume we only need primary indexing structure for single key attribute
### **table definition**: 
#   table = [ columns, column, primary_key, indexing, tuple_1, tuple_2, ......, tuple_n ]
#   columns = { col_1:[<type>,<primary_key[yes:1,no:0]>,<index>], col_2, ......., col_n }
#   column = [ col_1, col_2, ......, col_n]
#   primary = { primary_key_1, primary_key_2, ......, primary_key_n }
#   indexing = { key_1:tuple_1, key_2:tuple_2, ......., key_n:tuple_n }
#   tuple_1 = [ col_1.value, col_2.value, ......, col_n.value ], tuple_2, ......, tuple_n
### **
# create,show,describe,insert does not need query optimizer

### NEW DESIGN ###

# databases = { table_name : table_object, table_name2 : table_object 2...}

# table (class) has attributes columns, column, primary_key, size and tuples (hashmap of hashmaps)

# columns =  { col_1:[ <type>, <primary_key[yes:1,no:0]>,<index>], col_2, ......., col_n }
# column = [ col_1, col_2, ......, col_n]
# primary = { primary_key_1, primary_key_2, ......, primary_key_n } (set)
# size = numTuples
# tuples = { key1 : tuple_1, key2 : tuple2... key_n tuple_n}
# tuple = { attribute_name : value }

databases = {}

import sqlparse
import time
import re
from tabulate import tabulate as tb
from BTrees._OOBTree import OOBTree
from table import Table, databases
from exception import Invalid_Type, Syntax_Error, Duplicate_Item, Keyword_Used, Not_Exist, Unsupported_Functionality


keywords = ['CREATE','SHOW','DESCRIBE','INSERT','INTO','TABLE','TABLES','REFERENCES','INT','STRING','PRIMARY','FOREIGN','KEY','WHERE','SELECT','EXECUTE']
sqlCommand = ['CREATE','SHOW','DESCRIBE','INSERT','SELECT','EXECUTE']
LOGICAL_OPERATORS = ['=', '!=', '>', '>=', '<', '<=']
STRING_OPERATORS = ['=', '!=']
datatype = ['INT','STRING']
key = ['PRIMARY','FOREIGN','KEY']
sql_query = "CREATE TABLE people ( key_no INT NOT NULL, first_name INT NOT NULL, last_name INT NOT NULL, primary_key (key_no) );"
query_tokens = []
quitting = False 
PROMPT = "-> "
PROMPT2 = "> "

SIMPLE_SELECT = False
SIMPLE_WILDCARD = False
AGGREGATE = False
SINGLE_WHERE = False
DOUBLE_WHERE = False
JOIN = False

### Input Parsing ###
#-----------------------------------------------------------#

def readInput():
    global sql_query
    sql_query = ""
    userInput = input(PROMPT)
    if len(userInput) == 0 or userInput.isspace():
        readInput()
        return
    if ';' in userInput: 
        sql_query += userInput.split(';')[0]
        sql_query += ';'
        return
    if userInput == "quit": # special keyword quit;
        sql_query += userInput + ';'
        return
    sql_query += userInput + ' '
    readInput2()
    return

def readInput2():
    global sql_query
    userInput = input(PROMPT2)
    if ';' in userInput:
        sql_query += userInput.split(';')[0]
        sql_query += ';'
        return
    sql_query += userInput + ' '
    readInput2()

def filter():
    global sql_query, query_tokens
    sql_query = sqlparse.format(sql_query,reindent=False, keyword_case='upper') # reformat user input
    query_tokens = []
    parsed = sqlparse.parse(sql_query) 
    for statement in parsed:
        for token in statement.tokens:
            if token.value.strip():
                query_tokens.append(token.value)

### End of Input Parsing

# some special function
# check if user input is keyword
def is_keyword(word):
    for str in keywords:
        if word == str:
            return True
    return False


## Begin evaluation functions
    
def show_table():
    headers = ['Tables']
    tables = []
    for table in databases.keys():
        tables.append([table])
    if len(tables) == 0:
        print("<Empty Set>")
        return
    print(tb(tables,headers,tablefmt='outline'))
    return

def create_table():
    table = Table()
    table = parse_columns(table)
    databases[query_tokens[2]] = table
    print("Query OK, 0 rows affected")
    return

def parse_columns(table):
    attributes = validateTableInput(query_tokens[3]) # parse columns into column and validate at the same time
    for attribute in attributes:
        table.add_attribute(attribute) # add attributes to the table
    if table.pri_lock == False: # check if primary key is defined
        raise Syntax_Error("Syntax Error: no Primary Key defined")
    return table

def describe_table(table_name):
    databases[table_name].describe()

def insert(table_name,tuples):
    databases[table_name].add_tuples(tuples)

def execute(filename):
    global sql_query, query_tokens
    try:
        with open(filename,'r') as file:
            file_content = file.read().replace('\n',' ')
    except FileNotFoundError:
        print(f"File {filename} not found")
    sql_query = sqlparse.format(file_content,reindent=False, keyword_case='upper')
    sql_queries = sqlparse.parse(sql_query)
    for stmt in sql_queries:
        query_tokens = []
        for token in stmt.tokens:
            if token.value.strip():
                query_tokens.append(token.value)
        start_time = time.time()
        eval_query()
        end_time = time.time()
        print(f"Time: {end_time-start_time:.3f}s")
        print()
    return

def eval_query():
    optr = query_tokens[0]
    if optr == 'CREATE':
        validateCreateTable(query_tokens)
        create_table()
        return
    elif optr == 'SHOW':
        if len(query_tokens) != 3 or query_tokens[1] != 'TABLES':
            raise Syntax_Error("Syntax Error: SHOW TABLES")
        show_table()
        return
    elif optr == 'DESCRIBE':
        table_name = validateDescribe(query_tokens)
        describe_table(table_name)
        return
    elif optr == 'INSERT':
        table_name = validateInsert(query_tokens)
        prev_size = databases[table_name].size
        insert(table_name,query_tokens[3][6:len(query_tokens[3])].strip())
        cur_size = databases[table_name].size
        print(f"Query OK, {cur_size-prev_size} rows affected")
        return
    elif optr == 'EXECUTE':
        filename = validateExecute(query_tokens)
        execute(filename)
        return
    elif optr == 'SELECT':
        if(validateSelect()):
            select()
        return
    raise Syntax_Error("Unknown SQL Command")

def select():
    tempTable = Table()
    if(SIMPLE_SELECT):
        table_name = query_tokens[3]
        if(',' in query_tokens[1]):
            columns = [value.strip() for value in query_tokens[1].split(',')]
        else:
            columns = [query_tokens[1]]

        if(SINGLE_WHERE):
            numChars = len(query_tokens[4])
            cleanClause = query_tokens[4][6:numChars-1] #removing where and semi-colon
            pattern = fr"({'|'.join(re.escape(op) for op in LOGICAL_OPERATORS)})"
            conditions = re.split(pattern, cleanClause)
            conditions = [value.strip() for value in conditions if value.strip()]

            tempTable = databases[table_name].copyColumns(tempTable, columns, conditions, 1)

        elif(DOUBLE_WHERE):
            numChars = len(query_tokens[4])
            cleanClause = query_tokens[4][6:numChars-1] #removing where and semi-colon
            pattern = fr"(\w+)\s({'|'.join(map(re.escape, LOGICAL_OPERATORS))})\s(\S+)\s(AND|OR)\s(\w+)\s({'|'.join(map(re.escape, LOGICAL_OPERATORS))})\s(\S+)"
            matches = re.match(pattern, cleanClause)
            conditions = [matches.group(i).strip() for i in range(1, 8)]

            tempTable = databases[table_name].copyColumns(tempTable, columns, conditions, 2)

        else:
            tempTable = databases[table_name].copyColumns(tempTable, columns, [], 0)
            
        tempTable.print_internal()
    elif(SIMPLE_WILDCARD and JOIN == False):
        table_name = query_tokens[3]
        columns = databases[table_name].columns

        if(SINGLE_WHERE):
            numChars = len(query_tokens[4])
            cleanClause = query_tokens[4][6:numChars-1] #removing where and semi-colon
            pattern = fr"({'|'.join(re.escape(op) for op in LOGICAL_OPERATORS)})"
            conditions = re.split(pattern, cleanClause)
            conditions = [value.strip() for value in conditions if value.strip()]

            tempTable = databases[table_name].copyColumns(tempTable, columns, conditions, 1)
            tempTable.print_internal()

        elif(DOUBLE_WHERE):
            numChars = len(query_tokens[4])
            cleanClause = query_tokens[4][6:numChars-1] #removing where and semi-colon
            pattern = fr"(\w+)\s({'|'.join(map(re.escape, LOGICAL_OPERATORS))})\s(\S+)\s(AND|OR)\s(\w+)\s({'|'.join(map(re.escape, LOGICAL_OPERATORS))})\s(\S+)"
            matches = re.match(pattern, cleanClause)
            conditions = [matches.group(i).strip() for i in range(1, 8)]

            tempTable = databases[table_name].copyColumns(tempTable, columns, conditions, 2)
            tempTable.print_internal()

        else:
            databases[table_name].print_internal()
    elif(AGGREGATE):
        table_name = query_tokens[3]
        pattern = r'^max\(([^)]+)\)$'
        match = re.match(pattern, query_tokens[1].strip())

        if('.' in match.group(1)):
            column_name = match.group(1).strip().split('.')[1]
        else:
            column_name = match.group(1)

        if(SINGLE_WHERE):
            numChars = len(query_tokens[4])
            cleanClause = query_tokens[4][6:numChars-1] #removing where and semi-colon
            pattern = fr"({'|'.join(re.escape(op) for op in LOGICAL_OPERATORS)})"
            conditions = re.split(pattern, cleanClause)
            conditions = [value.strip() for value in conditions if value.strip()]

            tempTable = databases[table_name].max(column_name, conditions, 1)

        elif(DOUBLE_WHERE):
            numChars = len(query_tokens[4])
            cleanClause = query_tokens[4][6:numChars-1] #removing where and semi-colon
            pattern = fr"(\w+)\s({'|'.join(map(re.escape, LOGICAL_OPERATORS))})\s(\S+)\s(AND|OR)\s(\w+)\s({'|'.join(map(re.escape, LOGICAL_OPERATORS))})\s(\S+)"
            matches = re.match(pattern, cleanClause)
            conditions = [matches.group(i).strip() for i in range(1, 8)]

            tempTable = databases[table_name].max(column_name, conditions, 2)

        else: 
            table_name = query_tokens[3]
            pattern = r'^max\(([^)]+)\)$'
            match = re.match(pattern, query_tokens[1].strip())

            if('.' in match.group(1)):
                column_name = match.group(1).strip().split('.')[1]
            else:
                column_name = match.group(1)

            tempTable = databases[table_name].max(column_name, [], 0)
        tempTable.print_internal()
    elif(JOIN):
        leftTable = query_tokens[3]
        rightTable = query_tokens[5]
        joinConditions = query_tokens[7].split('=')
        columns = [[],[]]

        left = joinConditions[0].strip().split('.')
        right = joinConditions[1].strip().split('.')

        if(SIMPLE_WILDCARD == False):
            pairs = query_tokens[1].split(',')
            table_column_dict = {}
    
            for pair in pairs:
                table, column = pair.strip().split('.')
        
                if table in table_column_dict:
                    table_column_dict[table].append(column)
                else:
                    table_column_dict[table] = [column]

        #call nestedLoop on instance of larger table with smaller table as arg 
        if(databases[left[0]].size > (1.5 * databases[right[0]].size)):

            if(SIMPLE_WILDCARD):
                columns = [databases[left[0]].columns, databases[right[0]].columns]
            else:
                for table, cols in table_column_dict.items():
                    if(table == left[0]):
                        columns[0] = cols
                    else:
                        columns[1] = cols
            
            joinConditions = [left, right]

            if(SINGLE_WHERE):
                numChars = len(query_tokens[8])
                cleanClause = query_tokens[8][6:numChars-1] #removing where and semi-colon

                pattern = fr"({'|'.join(re.escape(op) for op in LOGICAL_OPERATORS)})"
                conditions = re.split(pattern, cleanClause)         
                conditions = [value.strip() for value in conditions if value.strip()]

                conditions[0] = conditions[0].split('.')

                #Variable
                if('.' in conditions[2]):
                    conditions[2] = conditions[2].split('.')
                    tempTable = databases[left[0]].nestedLoop(databases[right[0]], columns, joinConditions, left[0], right[0], 1, conditions, False, False)
                #Constant 
                else:
                    tempTable = databases[left[0]].nestedLoop(databases[right[0]], columns, joinConditions, left[0], right[0], 1, conditions, True, False)

            elif(DOUBLE_WHERE):
                tempTable = databases[left[0]].nestedLoop(databases[right[0]], columns, joinConditions, left[0], right[0], 2, conditions)
            else:
                tempTable = databases[left[0]].nestedLoop(databases[right[0]], columns, joinConditions, left[0], right[0], 0, conditions, False, False)

            tempTable.print_internal()

        #call nestedLoop on instance of larger table with smaller table as arg 
        elif(databases[right[0]].size > (1.5 * databases[left[0]].size)):

            if(SIMPLE_WILDCARD):
                columns = [databases[right[0]].columns, databases[left[0]].columns]
            else:
                for table, cols in table_column_dict.items():
                    if(table == right[0]):
                        columns[0] = cols
                    else:
                        columns[1] = cols
            
            joinConditions = [right, left]

            if(SINGLE_WHERE):
                numChars = len(query_tokens[8])
                cleanClause = query_tokens[8][6:numChars-1] #removing where and semi-colon

                pattern = fr"({'|'.join(re.escape(op) for op in LOGICAL_OPERATORS)})"
                conditions = re.split(pattern, cleanClause)         
                conditions = [value.strip() for value in conditions if value.strip()]

                conditions[0] = conditions[0].split('.')

                #Variable
                if('.' in conditions[2]):
                    conditions[2] = conditions[2].split('.')
                    tempTable = databases[left[0]].nestedLoop(databases[right[0]], columns, joinConditions, left[0], right[0], 1, conditions, False, False)
                #Constant 
                else:
                    tempTable = databases[left[0]].nestedLoop(databases[right[0]], columns, joinConditions, left[0], right[0], 1, conditions, True, False)

            elif(DOUBLE_WHERE):
                tempTable = databases[right[0]].nestedLoop(databases[left[0]], columns, joinConditions, right[0], left[0], 2)
            else: 
                tempTable = databases[right[0]].nestedLoop(databases[left[0]], columns, joinConditions, right[0], left[0], 0)

            tempTable.print_internal()

        else:

            if(SIMPLE_WILDCARD == False):
                for table, cols in table_column_dict.items():
                    if(table == left[0]):
                        columns[0] = cols
                    else:
                        columns[1] = cols

            joinConditions = [left, right]

            tempTable = databases[left[0]].mergeScan(databases[right[0]], columns, joinConditions)
            tempTable.print_internal()

    nullify()


### VALIDATE SQL COMMAND ###
#------------------------------------------------------------------------------#

# 1. Validate CREATE TABLE

def validateCreateTable(tokens):
    if len(tokens) != 5 or tokens[1] != 'TABLE':
        raise Syntax_Error("Syntax Error: CREATE TABLE")
    validateTableName(tokens[2])

def validateTableName(table_name):
    if table_name in databases:
        raise Duplicate_Item("Table '" + table_name + "' exists")
    if (is_keyword(table_name)):
        raise Syntax_Error("Syntax Error: invalid table name: " + table_name)
    if(all(char.isalnum() or char == '_' for char in table_name) != True):
        raise Syntax_Error('Syntax Error: invalid table name ' + table_name)

def validateTableInput(cols_data):
    length = len(cols_data)
    if cols_data[0] != '(' or cols_data[length-1] != ')':
        raise Syntax_Error("Syntax Error: attributes is invalid")
    if length == 2 or cols_data[1:length-1].isspace():
        raise Syntax_Error("Syntax Error: attributes cannot be empty")
    cols_data = cols_data[1:length-1].strip()
    attributes = re.split(r',\s*(?![^()]*\))', cols_data)
    return attributes

# 2. Validate Insert
def validateInsert(tokens):
    if len(tokens) != 5 or tokens[1] != 'INTO':
        raise Syntax_Error("Syntax Error: INSERT")
    insert_info = re.split(r' \s*(?![^()]*\))',tokens[2])
    if len(insert_info) != 2:
        raise Syntax_Error("Syntax Error: INSERT[2]")
    table_name = insert_info[0]
    if table_name not in databases:
        raise Not_Exist(f"Table {table_name} does not exist")
    columns = insert_info[1][1:len(insert_info)-1].strip()
    columns = [token.strip() for token in re.split(r',', columns) if token.strip()]
    # print(columns)
    if len(columns) != 0:
        for i in range (0,len(columns)):
            if columns[i] != databases[table_name].columns[i]:
                raise Syntax_Error("Syntax Error: INSERT Columns does not match")
    if tokens[3] == 'VALUES':
        raise Syntax_Error("Syntax Error: Empty VALUES, no tuples inserted")
    return table_name

# 3. Validate Describe:
def validateDescribe(tokens):
    if len(tokens) != 3:
        raise Syntax_Error("Syntax Error: Describe")
    if tokens[1] not in databases:
        raise Not_Exist(f"Table {query_tokens[1]} does not exist")
    return tokens[1]

def validateExecute(tokens):
    if len(tokens) != 3:
        raise Syntax_Error("Syntax Error: Execute")
    return tokens[1]


### SELECTION VALIDATION FUNCTIONS ###
#------------------------------------------------------------------------------#

def validateSelect(): 
    global AGGREGATE
    length = len(query_tokens)
    select_columns = []
    table_name = ''
    wildcardFlag = False

    #Must have at least basic format of SELECT x FROM table
    if(length < 4 or query_tokens[2] != 'FROM'):
        raise Syntax_Error("Syntax Error: invalid select")

    #Either a join or selecting from many tables
    if('.' in query_tokens[1]):
        if(length >= 5):
            if(query_tokens[4] == 'JOIN'):
                global JOIN
                JOIN = True
                return validateJoin()
            elif(',' in query_tokens[3]):
                raise Unsupported_Functionality('Unsupported Functionality: cannot select from multiple tables if not in a join')
            elif('(' in query_tokens[1]):
                if(')' in query_tokens[1]):
                    AGGREGATE = True
                    validateAggregateFunction()
                else: 
                    raise Syntax_Error("Syntax Error: no closing parentheses")
            else:
                raise Unsupported_Functionality('Unsupported Functionality: cannot select table_name.column_name if not in a join')
                    
    #wildcard select        
    elif(query_tokens[1] == '*'):
        table_name = query_tokens[3]
        #selecting wildcard from multiple tables

        if(length >= 5):
            if(query_tokens[4] == 'JOIN'):
                global SIMPLE_WILDCARD
                SIMPLE_WILDCARD = True
                JOIN = True
                return validateWildcardJoin()
        if(',' in table_name):
            raise Unsupported_Functionality('Unsupported Functionality: cannot select from multiple tables if not in a join')
        if(table_name not in databases):
            raise Not_Exist("Table does not exist")
        #validate all columns exist in the table
        else:
            select_columns = databases[table_name].column_data #all columns in database
            wildcardFlag = True
            SIMPLE_WILDCARD = True
    elif('(' in query_tokens[1]):
        if(')' in query_tokens[1]):
            AGGREGATE = True
            validateAggregateFunction()
        else:
            raise Syntax_Error("Syntax Error: no closing parentheses")
    else:
        global SIMPLE_SELECT
        SIMPLE_SELECT = True 
        select_columns = [item.strip() for item in query_tokens[1].split(',')]
    
    #if the select doesn't follow format table_name.column_name but is selecting from multiple tables
    if(',' in query_tokens[3]):
        raise Syntax_Error("Syntax Error: ambiguous column(s) selected")
    else:
        if(query_tokens[3] not in databases):
            raise Not_Exist("Table does not exist")
        #validate all columns exist in the table (we already know they exist if we did a wildcard select)
        elif(wildcardFlag != True): 
            table_name = query_tokens[3]
            for column in select_columns:
                if(column not in databases[table_name].column_data):
                    raise Syntax_Error('Syntax Error: Column ' + column + ' does not exist')

    if(length >= 5):
        if(query_tokens[4].startswith('WHERE')):
            global WHERE
            WHERE = True
            return validateWhere([], table_name, query_tokens[4], False)
        elif(query_tokens[4].strip() != ';'):
            raise Syntax_Error('Syntax Error: ' + query_tokens[4])
    
    return True
    
def validateJoin():
    if(len(query_tokens) < 8):
        raise Syntax_Error('Syntax Error: Invalid join syntax')

    #isolating each table_name.column_name
    pairs = query_tokens[1].split(',')
    table_column_dict = {}
    joining_tables = []
    
    for pair in pairs:
        #separate table and column names
        table, column = pair.strip().split('.')
        
        #check if table exists in dict
        if table in table_column_dict:
            table_column_dict[table].append(column)
        else:
            #if table doesn't exist create new entry 
            table_column_dict[table] = [column]

    #validate all tables and their columns
    for table in table_column_dict:
        if(table not in databases):
            raise Not_Exist("Table does not exist")
        for column in table_column_dict[table]:
            if(column not in databases[table].column_data):
                raise Syntax_Error('Syntax Error: Column ' + column + ' does not exist')
            
    #we're only selecting from one table
    if(',' not in query_tokens[3]):
        joining_tables = [query_tokens[3]]
    #selecting from multiple tables
    else: 
        joining_tables = [value.strip() for value in query_tokens[3].split(',')]

    if(',' in query_tokens[5]):
        raise Syntax_Error('Syntax Error: ' + query_tokens[5])

    #adding the table we're joining on
    joining_tables.append(query_tokens[5])

    #checking that tables we're joining on exist 
    for table in joining_tables:
        if(table not in databases):
            raise Not_Exist("Table does not exist")

    #checking that we're selecting from tables that are specified in the join
    for table in table_column_dict:
        if(table not in joining_tables):
            raise Syntax_Error('Syntax Error: Cannot select from a table that is not specified in the join')
    
    if(query_tokens[6] != 'ON'):
        raise Syntax_Error('Syntax Error: Invalid join syntax')
    
    #must join on a certain column
    if('=' not in query_tokens[7]):
        raise Syntax_Error('Syntax Error: Invalid join syntax')
    
    #verifying that tables and their columns that we're joining on are valid
    joinPairs = query_tokens[7].strip().split('=')

    tabs = []
    cols = []

    for pair in joinPairs:
        table, column = pair.strip().split('.')
        print("Table: " + table + " Column: " + column)
        
        if(table not in joining_tables):
            raise Syntax_Error('Syntax Error: Invalid join syntax')
        
        if(column not in databases[table].column_data):
            raise Syntax_Error('Syntax Error: Column ' + column + ' does not exist')
        
        tabs.append(table)
        cols.append(column)

    if(databases[tabs[0]].column_data[cols[0]][0] != databases[tabs[1]].column_data[cols[1]][0]):
        raise Syntax_Error('Syntax Error: Cannot join on columns of different types')
    
    #checking if it also has a where clause 
    if(len(query_tokens) > 8):
        if(query_tokens[8].startswith('WHERE')):
            return validateWhere(joining_tables, ' ', query_tokens[8], True)
        elif(query_tokens[8] != ';'):
            raise Syntax_Error('Syntax Error: ' + query_tokens[8])
        
    return True

def validateWildcardJoin():
    tables = []

    if(len(query_tokens) < 8):
        raise Syntax_Error('Syntax Error: Invalid join syntax')
    
    if(query_tokens[2] != 'FROM'):
        raise Syntax_Error('Syntax Error: ' + query_tokens[2])
    
     #we're only selecting from one table
    if(',' not in query_tokens[3]):
        tables = [query_tokens[3]]
    #selecting from multiple tables
    else: 
        tables = [value.strip() for value in query_tokens[3].split(',')]

    if(',' in query_tokens[5]):
        raise Syntax_Error('Syntax Error: ' + query_tokens[5])

    #adding the table we're joining on
    tables.append(query_tokens[5])

    #checking that tables we're joining on exist 
    for table in tables:
        if(table not in databases):
            raise Not_Exist("Table does not exist")
        
    if(query_tokens[6] != 'ON'):
        raise Syntax_Error('Syntax Error: Invalid join syntax')
    
    #must join on a certain column
    if('=' not in query_tokens[7]):
        raise Syntax_Error('Syntax Error: Invalid join syntax')
    
    #verifying that tables and their columns that we're joining on are valid
    joinPairs = query_tokens[7].strip().split('=')

    for pair in joinPairs:
        table, column = pair.strip().split('.')

        if(table not in databases):
            raise Not_Exist('Table does not exist')
        
        if(column not in databases[table].column_data):
            raise Syntax_Error('Syntax Error: Column ' + column + ' does not exist')
        
    #checking if it also has a where clause 
    if(len(query_tokens) > 8):
        if(query_tokens[8].startswith('WHERE')):
            return validateWhere(tables, ' ', query_tokens[8], True)
        elif(query_tokens[8] != ';'):
            raise Syntax_Error('Syntax Error: ' + query_tokens[8])
        
    return True

def validateWhere(joining_tables, table_name, where_clause, join):

    numChars = len(where_clause)
    numOperators = 0
    numConditions = 0
    cols = []

    cleanClause = where_clause[6:numChars-1] #removing where and semi-colon

    #counting number of conidtions
    numConditions = cleanClause.count('AND') + cleanClause.count('OR')

    #counting number of operators 
    for char in cleanClause:
        if(char in LOGICAL_OPERATORS):
            numOperators+=1

    if(numConditions > 1 or numOperators > 2):
        raise Unsupported_Functionality('Unsupported functionality: can only support single two-clause logical conjunction or disjunction')
    if(numOperators == 1):
        global SINGLE_WHERE
        SINGLE_WHERE = True
    elif(numOperators == 2):
        global DOUBLE_WHERE
        DOUBLE_WHERE = True
    
    if(join):
        #isolating column names using regex
        pattern = r'(\w+)\.(\w+)\s[=!><]=?\s[^ANDOR\s]+\b'
        tables_and_columns = re.findall(pattern, cleanClause)

        for pair in tables_and_columns:
            if(pair[0] not in databases):
                raise Not_Exist('Table ' + pair[0] + ' does not exist')
            if(pair[0] not in joining_tables): 
                raise Syntax_Error('Syntax Error: ' + pair[0])
            if(pair[1] not in databases[pair[0]].column_data):
                raise Syntax_Error('Syntax Error: Column ' + pair[1] + ' does not exist')
    else: 
        #isolating column names using regex
        pattern = r'\b(\w+)\s[=!><]=?\s[^ANDOR\s]+\b'
        cols = re.findall(pattern, cleanClause)

        for col in cols:
            if(col not in databases[table_name].column_data):
                raise Syntax_Error('Syntax Error: Column ' + col + ' does not exist')
        
    return True
    
def validateAggregateFunction():
    table_name = ''
    from_table_name = ''
    column_name = ''
    pattern = r'^max\(([^)]+)\)$'

    #multiple columns can't be selected with an aggregate function
    if(',' in query_tokens[1]):
        raise Unsupported_Functionality('Unsupported Functionality: cannot select multiple attributes when using an aggregate function') 

    match = re.match(pattern, query_tokens[1].strip())

    if(match):
        #if specifying table name
        if('.' in match.group(1)):
            table_name, column_name = match.group(1).strip().split('.')
        else:
            column_name = match.group(1)
    else:
       raise Syntax_Error('Syntax Error or Unsupported Aggregate Function: ' + query_tokens[1])

    if(query_tokens[2] != 'FROM'):
        raise Syntax_Error('Syntax Error: ' + query_tokens[2])
    
    if(',' in query_tokens[3]):
        raise Unsupported_Functionality('Unsupported Functionality: cannot select from multiple tables with an aggregate function')
    else: 
        from_table_name = query_tokens[3]

    if(table_name == ''):
        if(from_table_name not in databases):
            raise Not_Exist('HERE 1: Table does not exist')
        if(column_name not in databases[from_table_name].column_data):
            raise Syntax_Error('Syntax Error: Column ' + column_name + ' does not exist')
    else: 
        if(table_name != from_table_name):
            raise Syntax_Error('Syntax Error: Cannot select from a table that is not specified')
        else:
            if(table_name not in databases):
                raise Not_Exist('HERE 2: Table does not exist')
            if(column_name not in databases[table_name].column_data):
                raise Syntax_Error('Syntax Error: Column ' + column_name + ' does not exist')
    
    if(databases[from_table_name].column_data[column_name][0] != 'INT'):
        raise Unsupported_Functionality('Unsupported Functionality: max(column) only supported for integer types')

    if(len(query_tokens) >= 5):
        if(query_tokens[4].startswith('WHERE')):
            validateWhere([], from_table_name, query_tokens[4], False)
        elif(query_tokens[4] != ';'):
            raise Syntax_Error('Syntax Error: ' + query_tokens[4])
    
    return True


### END OF SELECTION VALIDATION FUNCTIONS ###
#------------------------------------------------------------------------------#

### HELPER FUNCTIONS ###
#------------------------------------------------------------------------------#

def nullify():
    global SIMPLE_SELECT
    global SIMPLE_WILDCARD
    global AGGREGATE
    global SINGLE_WHERE
    global DOUBLE_WHERE
    global JOIN

    SIMPLE_SELECT = False
    SIMPLE_WILDCARD = False 
    AGGREGATE = False
    SINGLE_WHERE = False 
    DOUBLE_WHERE = False
    JOIN = False

### END OF HELPER FUNCTIONS ###
#------------------------------------------------------------------------------#

### MAIN ###

while quitting == False:
    try:
        readInput()
        filter()
        # print(sql_query)
        # print(query_tokens)
        if query_tokens[0] == "quit":
            break
        start_time = time.time()
        eval_query()
        end_time = time.time()
        print(f"Time: {end_time-start_time:.3f}s")
        # print(databases)
    # throw errors
    except Syntax_Error as e:
        print(f"{e}")
    except Keyword_Used as e:
        print(f"{e}")
    except Invalid_Type as e:
        print(f"{e}")
    except Duplicate_Item as e:
        print(f"{e}")
    except Not_Exist as e:
        print(f"{e}")
    except Unsupported_Functionality as e:
        print(f"{e}")
for table in databases.keys():
    databases[table].describe()
    databases[table].print_internal()

### END OF MAIN ###
    