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
datatype = ['INT','STRING']
key = ['PRIMARY','FOREIGN','KEY']
sql_query = "CREATE TABLE people ( key_no INT NOT NULL, first_name INT NOT NULL, last_name INT NOT NULL, primary_key (key_no) );"
query_tokens = []
quitting = False 
PROMPT = "-> "
PROMPT2 = "> "

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

### End od Input Parsing

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
    print(f"{len(tables)} rows in set")
    return

def create_table():
    table = Table()
    table = parse_columns(table)
    table.name = query_tokens[2]
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
        raise FileNotFoundError(f"File {filename} not found")
    start_time = time.time()
    statements = sqlparse.split(file_content)
    # print(statements)
    end_time = time.time()
    print(f"Parsing Time: {end_time-start_time:.3f}s")
    for statement in statements:
        start_time = time.time()
        sql_query = sqlparse.format(statement,reindent=False,keyword_case='upper')
        sql_query = sqlparse.parse(sql_query)
        for stmt in sql_query:
            query_tokens = []
            for token in stmt.tokens:
                if token.value.strip():
                    query_tokens.append(token.value)
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
    elif optr == 4:
        if(validateSelect()):
            print('PASSED!')
        else:
            print('FAILED!')
    raise Syntax_Error("Unknown SQL Command")

def select():
    print('select')

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
    insert_info = [token for token in re.split(r'\s(?![^()]*\))',tokens[2]) if token]
    if len(insert_info) > 2:
        raise Syntax_Error("Syntax Error: INSERT[2]")
    table_name = insert_info[0]
    if table_name not in databases:
        raise Not_Exist(f"Table {table_name} does not exist")
    if len(insert_info) == 2:
        columns = insert_info[1][1:len(insert_info)-1].strip()
        columns = [token.strip() for token in re.split(r',', columns) if token.strip()]
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
    length = len(query_tokens)
    select_columns = []
    table_name = ''
    wildcardFlag = False

    #Must have at least basic format of SELECT x FROM table
    if(length < 4):
        raise Syntax_Error("Syntax Error: invalid select")

    #Either a join or selecting from many tables
    if('.' in query_tokens[1]):
        if(length >= 5):
            if(query_tokens[4] == 'JOIN'):
                return validateJoin()
            elif(',' in query_tokens[3]):
                return validateMultiSelect()
            elif('(' in query_tokens[1]):
                if(')' in query_tokens[1]):
                    validateAggregateFunction()
                else: 
                    raise Syntax_Error("Syntax Error: no closing parentheses")
            else:
                select_columns = validateSelectWithTableNames()
                    
    #wildcard select        
    elif(query_tokens[1] == '*'):
        table_name = query_tokens[3]
        #selecting wildcard from multiple tables
        if(length >= 5):
            if(query_tokens[4] == 'JOIN'):
                return validateWildcardJoin()
        if(',' in table_name):
            return validateMultiSelectWildcard()
        if(table_name not in databases):
            raise Not_Exist("Table does not exist")
        #validate all columns exist in the table
        else:
            select_columns = databases[table_name][0] #all columns in database
            wildcardFlag = True
    elif('(' in query_tokens[1]):
        if(')' in query_tokens[1]):
            validateAggregateFunction()
        else:
            raise Syntax_Error("Syntax Error: no closing parentheses")
    else: 
        select_columns = [item.strip() for item in query_tokens[1].split(',')]
    
    if(query_tokens[2] != 'FROM'):
        raise Syntax_Error("Syntax Error: must include FROM")
    
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
                if(column not in databases[table_name][0]):
                    raise Syntax_Error('Syntax Error: Column ' + column + ' does not exist')

    if(length >= 5):
        if(query_tokens[4].startswith('WHERE')):
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
            if(column not in databases[table][0]):
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

    for pair in joinPairs:
        table, column = pair.strip().split('.')

        if(table not in joining_tables):
            raise Syntax_Error('Syntax Error: Invalid join syntax')
        
        if(column not in databases[table][0]):
            raise Syntax_Error('Syntax Error: Column ' + column + ' does not exist')
    
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
        
        if(column not in databases[table][0]):
            raise Syntax_Error('Syntax Error: Column ' + column + ' does not exist')
        
    #checking if it also has a where clause 
    if(len(query_tokens) > 8):
        if(query_tokens[8].startswith('WHERE')):
            return validateWhere(tables, ' ', query_tokens[8], True)
        elif(query_tokens[8] != ';'):
            raise Syntax_Error('Syntax Error: ' + query_tokens[8])
        
    return True

def validateSelectWithTableNames():
    table_column_dict = {}
    pairs = query_tokens[1].split(',')
    select_columns = []

    for pair in pairs:
        try: 
            table_name, column_name = pair.strip().split('.')
        except ValueError: 
            raise Unsupported_Functionality('Unsupported Functionality: must specify table names for all attributes if doing it for one')
        if table_name in table_column_dict:
            table_column_dict[table_name].append(column_name)
        else:
            table_column_dict[table_name] = [column_name]

    if(len(table_column_dict) > 1):
        raise Syntax_Error("Syntax Error: cannot select from a table that hasn't been specified")
    else:
        table_nm = str(next(iter(table_column_dict.keys())))

        if(table_nm not in databases):
            raise Not_Exist('Table does not exist')
        
        if(table_nm != query_tokens[3]):
            raise Syntax_Error("Syntax Error: cannot select from a table that hasn't been specified")
        
        for column in table_column_dict[table_nm]:
            if(column not in databases[table_nm][0]):
                raise Syntax_Error("Syntax Error: Column " + column + " does not exist")
            select_columns.append(column)

    return select_columns

def validateMultiSelect():
    table_column_dict = {}
    pairs = query_tokens[1].split(',')

    for pair in pairs:
        try: 
            table_name, column_name = pair.strip().split('.')
        except ValueError: 
            raise Unsupported_Functionality('Unsupported Functionality: must specify table names for all attributes if doing it for one')
        if table_name in table_column_dict:
            table_column_dict[table_name].append(column_name)
        else:
            table_column_dict[table_name] = [column_name]
    
    for table in table_column_dict: 
        if(table not in databases):
            raise Not_Exist('Table does not exist')
        for column in table_column_dict[table]:
            if(column not in databases[table][0]):
                raise Syntax_Error("Syntax Error: Column " + column + " does not exist")
    
    if(query_tokens[2] != 'FROM'):
        raise Syntax_Error('Syntax Error: ' + query_tokens[2])
            
    from_tables = [value.strip() for value in query_tokens[3].split(',')]

    for table in from_tables:
        if(table not in databases):
            raise Not_Exist('Table does not exist')

    for table in table_column_dict:
        if(table not in from_tables):
            raise Syntax_Error('Syntax Error: cannot select from an unspecified table')
    
    if(len(query_tokens) >= 5):
        if(query_tokens[4].startswith('WHERE')):
            validateWhere(from_tables, ' ', query_tokens[4], True) #this isn't a join but looks the same to the where clause parser
        elif(query_tokens[4] != ';'):
            raise Syntax_Error('Syntax Error: ' + query_tokens[4])

    return True 
        
def validateMultiSelectWildcard():
    if(query_tokens[2] != 'FROM'):
        raise Syntax_Error('Syntax Error: ' + query_tokens[2])
    
    from_tables = [value.strip() for value in query_tokens[3].split(',')]

    for table in from_tables: 
        if(table not in databases):
            raise Not_Exist('Table does not exist')
        
    if(len(query_tokens) >= 5):
        if(query_tokens[4].startswith('WHERE')):
            validateWhere(from_tables, ' ', query_tokens[4], True) #this isn't a join but looks the same to the where clause parser
        elif(query_tokens[4] != ';'):
            raise Syntax_Error('Syntax Error: ' + query_tokens[4])
        
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
    
    if(join):
        #isolating column names using regex
        pattern = r'(\w+)\.(\w+)\s[=!><]=?\s[^ANDOR\s]+\b'
        tables_and_columns = re.findall(pattern, cleanClause)

        for pair in tables_and_columns:
            if(pair[0] not in databases):
                raise Not_Exist('Table ' + pair[0] + ' does not exist')
            if(pair[0] not in joining_tables): 
                raise Syntax_Error('Syntax Error: ' + pair[0])
            if(pair[1] not in databases[pair[0]][0]):
                raise Syntax_Error('Syntax Error: Column ' + pair[1] + ' does not exist')
    else: 
        #isolating column names using regex
        pattern = r'\b(\w+)\s[=!><]=?\s[^ANDOR\s]+\b'
        cols = re.findall(pattern, cleanClause)

        for col in cols:
            if(col not in databases[table_name][0]):
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
        if(column_name not in databases[from_table_name][0]):
            raise Syntax_Error('Syntax Error: Column ' + column_name + ' does not exist')
    else: 
        if(table_name != from_table_name):
            raise Syntax_Error('Syntax Error: Cannot select from a table that is not specified')
        else:
            if(table_name not in databases):
                raise Not_Exist('HERE 2: Table does not exist')
            if(column_name not in databases[table_name][0]):
                raise Syntax_Error('Syntax Error: Column ' + column_name + ' does not exist')

    if(len(query_tokens) >= 5):
        if(query_tokens[4].startswith('WHERE')):
            validateWhere([], from_table_name, query_tokens[4], False)
        elif(query_tokens[4] != ';'):
            raise Syntax_Error('Syntax Error: ' + query_tokens[4])
    
    return True


### END OF SELECTION VALIDATION FUNCTIONS ###
#------------------------------------------------------------------------------#

### OPTIMISATION FUNCTIONS ###
#------------------------------------------------------------------------------#

def createQueryTree():
    OptimiserTree = Tree()
    i = 0
    while i < len(query_tokens):
        if(query_tokens[i] == 'SELECT'):
            #If it is a wildcard select 
            if(query_tokens[i+1] == '*'):
                columns_list = ''
                j = i+1
                while(query_tokens[j] != 'FROM'):
                    j+=1
                    table_name = query_tokens[j+1]
                    wildcard_columns = databases[table_name][0].keys #need to change internal structure but this will represent column names
                    k = 0
                    while(k < len(wildcard_columns)):
                        columns_list += wildcard_columns[k]
                        if(k != len(wildcard_columns) - 1):
                            columns_list += ', '
                        k+=1
                OptimiserTree.create_node('PROJECT', str(columns_list)) # root node (pre-optimisations)
            else: 
                OptimiserTree.create_node('PROJECT', str(query_tokens[i+1])) # root node (pre-optimisations)
            i+=1
        elif(query_tokens[i] == 'FROM'):
            OptimiserTree.create_node('TABLE NAME', query_tokens[i+1], parent='PROJECT')
        elif(query_tokens[i].startswith('WHERE')):
            if('AND' in query_tokens[i]):
                split_where = query_tokens[i].split('AND')
            elif('OR' in query_tokens[i]):
                split_where = query_tokens[i].split('OR')
            OptimiserTree.create_node('SELECT(' )
        return OptimiserTree

def optimiseTree():
    print("optimise tree")

### END OF OPTIMISATION FUNCTIONS ###
#------------------------------------------------------------------------------#

while quitting == False:
    try:
        readInput()
        start_time = time.time()
        filter()
        if query_tokens[0] == "quit":
            break
        eval_query()
        end_time = time.time()
        print(f"Time: {end_time-start_time:.3f}s")
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
    except FileNotFoundError as e:
        print(f"{e}")
# for table in databases.keys():
#     databases[table].describe()
#     databases[table].print_internal()
    