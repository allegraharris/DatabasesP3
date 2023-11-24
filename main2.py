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
from BTrees._OOBTree import OOBTree
from table import Table
from exception import Invalid_Type, Syntax_Error, Duplicate_Item, Keyword_Used, Table_Exist, Unsupported_Functionality


keywords = ['CREATE','SHOW','DESCRIBE','INSERT','INTO','TABLE','TABLES','INT','STRING','PRIMARY','FOREIGN','KEY', 'WHERE']
sqlCommand = ['CREATE','SHOW','DESCRIBE','INSERT', 'SELECT']
LOGICAL_OPERATORS = ['=', '!=', '>', '>=', '<', '<=']
datatype = ['INT','STRING']
key = ['PRIMARY','FOREIGN','KEY']
sql_query = "CREATE TABLE people ( key_no INT NOT NULL, first_name INT NOT NULL, last_name INT NOT NULL, primary_key (key_no) );"
query_tokens = []
quitting = False 
PROMPT = "-> "
PROMPT2 = "> "

database = {} #

# Begin Parsing Functions
def readInput():
    global sql_query
    sql_query = ""
    userInput = input(PROMPT)
    if ';' in userInput:
        sql_query += userInput.split(';')[0]
        sql_query += ';'
        return
    if userInput == "quit":
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
    global sql_query
    global query_tokens
    sql_query = sqlparse.format(sql_query,reindent=False, keyword_case='upper')
    query_tokens = []
    parsed = sqlparse.parse(sql_query)
    for statement in parsed:
        for token in statement.tokens:
            if token.value.strip():
                query_tokens.append(token.value)

# End of parsing functions

# some special function
# check if user input is keyword
def is_keyword(word):
    for str in keywords:
        if word == str:
            return True
    return False


## Begin evaluation functions
def find_operation():
    try:
        return sqlCommand.index(query_tokens[0])
    except ValueError:
        return -1
    
def show_table():
    table = [{'Tables':['STRING',0]},[],set(),dict()]
    for table_name in databases.keys():
        list = [table_name]
        table.append(list)
    return table

def create_table():
    global databases
    table_name = query_tokens[2]
    table = [dict(),list(),set(),dict()]
    table = parse_columns(table)
    databases[table_name] = table
    return ["Query OK, 0 rows affected"]

def parse_columns(table):
    parsed = query_tokens[3]
    if parsed[0] != '(' or parsed[len(parsed)-1] != ')':
        raise Syntax_Error("Syntax Error: attributes is invalid")
    if len(parsed) == 2 or parsed[1:len(parsed)-1].isspace():
        raise Syntax_Error("Syntax Error: attributes cannot be empty")
    attributes = re.split(r',\s*(?![^()]*\))', parsed[1:len(parsed)-1].strip())
    # print(attributes)
    index = 0
    for attribute in attributes:
        tokens = re.split(r' \s*(?![^()]*\))',attribute.strip())
        # print(tokens)
        name = tokens[0]
        type = tokens[1].upper()
        if name == 'PRIMARY':
            if len(tokens) != 3 or tokens[1] != 'KEY':
                raise Syntax_Error("Syntax Error: Primary key")
            keys = tokens[2][1:len(tokens[2])-1].strip().split(',')
            for key in keys:
                if key.strip() not in table[0]:
                    raise Syntax_Error("Syntax Error: Column '" + key + "' does not exist")
                table[0][key.strip()][1] = 1 
                table[2].add(key.strip())
        if name == 'FOREIGN':
            if len(tokens) != 3 or tokens[1] != 'KEY':
                raise Syntax_Error("Syntax Error: Primary key")
            keys = tokens[2][1:len(tokens[2])-1].strip().split(',')
            for key in keys:
                if key.strip() not in table[0]:
                    raise Syntax_Error("Syntax Error: Column '" + key + "' does not exist")
                table[0][key.strip()][1] = 2
                table[2].add(key.strip())
        if name != 'PRIMARY':
            if name in table[0]:
                raise Duplicate_Item("Duplicate column name " + tokens[0])
            if type not in datatype:
                raise Invalid_Type("Invalid Data type '" + tokens[1] + "'")
            table[0][name] = [type,0,index]
            index = index+1
            table[1].append(name)   
    return table


def describe_table(table_name):
    my_table = [describe_columns,list(),set(),dict()]
    table = databases[table_name]
    columns = table[0]
    for column in columns.keys():
        my_table.append([column,columns[column][0],columns[column][1]])
    return my_table

def insert(table_name):
    num = 0
    add_tuples = list()
    tuples = query_tokens[3][6:len(query_tokens[3])].strip()
    # print(tuples)
    items = re.split(r',\s*(?![^()]*\))',tuples)
    columns = databases[table_name][0]
    column = databases[table_name][1]
    primary_keys = databases[table_name][2]
    # print(tuples)
    # print(values)
    for item in items:
        values = re.split(r',\s*(?=(?:[^\'"]*[\'"][^\'"]*[\'"])*[^\'"]*$)',item[1:len(item)-1].strip())
        Tuple = list()
        index = 0
        for value in values:
            if columns[column[index]][0] == 'INT':
                Tuple.append(int(value.strip()))
            else:
                Tuple.append(value.strip()[1:len(value.strip())-1])
            index += 1
        add_tuples.append(Tuple)
        num += 1
    for Tuple in add_tuples:
        databases[table_name].append(Tuple)
        tuple_keys = set()
        for key in primary_keys:
            tuple_keys.add(Tuple[columns[key][2]])
        # print(tuple_keys)
        databases[table_name][3][tuple(tuple_keys)] = Tuple
    return [f"Query OK, {num} rows affected"]
    

def eval_query():
    optr = find_operation()
    # print(optr)
    if optr == 0:
        if query_tokens[1] != 'TABLE' or len(query_tokens) != 5:
            raise Syntax_Error("Syntax Error: TABLE")
        table_name = query_tokens[2]
        if is_keyword(table_name):
            raise Keyword_Used("Keyword used:" + query_tokens[2])
        if table_name in databases:
            raise TABLE_EXIST("Table '" + table_name + "' exists")
        return create_table()
    if optr == 1:
        if len(query_tokens) != 3 or query_tokens[1] != 'TABLES':
            raise Syntax_Error("Syntax Error: SHOW TABLES")
        return show_table()
    if optr == 2:
        if len(query_tokens) != 3:
            raise Syntax_Error("Syntax Error: Describe")
        if query_tokens[1] not in databases:
            raise TABLE_EXIST("Table '" + query_tokens[1] + "' does not exist")
        return describe_table(query_tokens[1])
    if optr == 3:
        if query_tokens[1] != 'INTO' or len(query_tokens) != 5:
            raise Syntax_Error("Syntax Error: INSERT")
        insert_info = re.split(r' \s*(?![^()]*\))',query_tokens[2])
        table_name = insert_info[0]
        if table_name not in databases:
            raise TABLE_EXIST("Table '" + insert_info[0] + "' does not exist")
        column = insert_info[1]
        if len(column) == 2 or column[1:len(column)-1].strip().isspace():
            return insert(table_name)
        attributes = column[1:len(column)-1].strip().split(',')
        for attribute in attributes:
            if attribute.strip() not in databases[table_name][0]:
                raise TABLE_EXIST(f"Column {attribute.strip()} does not exist")
        return insert(table_name)
    if optr == 4:
        if(validateSelect()):
            print('PASSED!')
        else:
            print('FAILED!')
        
    raise Syntax_Error("Unknown SQL Command")

# R is our relations
def print_table(R):
    if len(R) == 1: # not relation output (create, insert)
        print(R[0])
        return
    if len(R) == 4:
        print("Empty set")
        return
    message = []
    title = ""
    for key in R[0].keys():
        title += f"{key:20}"
    message.append(title)
    for i in range (4,len(R)):
        tuple = ""
        for attribute in R[i]:
            tuple += f"{attribute:20}"
        message.append(tuple)
    for line in message:
        print(line)
    return 

def select():
    print('select')

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
            raise TABLE_EXIST("Table does not exist")
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
            raise TABLE_EXIST("Table does not exist")
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
            raise TABLE_EXIST("Table does not exist")
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
            raise TABLE_EXIST("Table does not exist")

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
            raise TABLE_EXIST("Table does not exist")
        
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
            raise TABLE_EXIST('Table does not exist')
        
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
            raise TABLE_EXIST('Table does not exist')
        
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
            raise TABLE_EXIST('Table does not exist')
        for column in table_column_dict[table]:
            if(column not in databases[table][0]):
                raise Syntax_Error("Syntax Error: Column " + column + " does not exist")
    
    if(query_tokens[2] != 'FROM'):
        raise Syntax_Error('Syntax Error: ' + query_tokens[2])
            
    from_tables = [value.strip() for value in query_tokens[3].split(',')]

    for table in from_tables:
        if(table not in databases):
            raise TABLE_EXIST('Table does not exist')

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
            raise TABLE_EXIST('Table does not exist')
        
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
                raise TABLE_EXIST('Table ' + pair[0] + ' does not exist')
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
            raise TABLE_EXIST('HERE 1: Table does not exist')
        if(column_name not in databases[from_table_name][0]):
            raise Syntax_Error('Syntax Error: Column ' + column_name + ' does not exist')
    else: 
        if(table_name != from_table_name):
            raise Syntax_Error('Syntax Error: Cannot select from a table that is not specified')
        else:
            if(table_name not in databases):
                raise TABLE_EXIST('HERE 2: Table does not exist')
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
        filter()
        print(sql_query)
        print(query_tokens)
        if query_tokens[0] == "quit":
            break
        # start_time = time.time()
        # print_table(eval_query())
        # end_time = time.time()
        # print(f"Time: {end_time-start_time:.3f}s")
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
    except Table_Exist as e:
        print(f"{e}")
    except Unsupported_Functionality as e:
        print(f"{e}")
for table in database.keys():
    print(database[table])
    