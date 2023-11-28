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
Aggregate = ['MAX','MIN']
LOGICAL_OPERATORS = ['=', '!=', '>', '>=', '<', '<=']
STRING_OPERATORS = ['=', '!=']
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

def simple_select(cols,table):
    # print(cols)
    if cols == '*':
        table.print_internal()
        return
    table.print_internal_select(cols)
    return

def aggregate_select(func,table):
    output_table = Table()
    if func[0].upper() == 'MAX':
        output_table = table.max2(func[1])
    elif func[0].upper() == 'MIN':
        output_table = table.min2(func[1])
    output_table.print_internal()

def single_where(table,where_tokens):
    col = where_tokens[0]
    optr = where_tokens[1]
    val = where_tokens[2]
    if optr not in LOGICAL_OPERATORS:
        raise Syntax_Error("Invalid Logical Operators")
    if optr not in STRING_OPERATORS and table.column_data[col][0] == 'STRING':
        raise Invalid_Type("Incompatiple Type for Logical Operators")
    if table.column_data[col][0] == 'INT':
        try:
            val = int(val)
        except ValueError:
            raise Invalid_Type("Incompatible Type")
    return table.single_where(col,optr,val)

def double_where(table,where_tokens):
    col1 = where_tokens[0]
    optr1 = where_tokens[1]
    val1 = where_tokens[2]
    log = where_tokens[3]
    col2 = where_tokens[4]
    optr2 = where_tokens[5]
    val2 = where_tokens[6]
    if optr1 not in LOGICAL_OPERATORS or optr2 not in LOGICAL_OPERATORS:
        raise Syntax_Error("Invalid Logical Operators")
    if optr1 not in STRING_OPERATORS and table.column_data[col1][0] == 'STRING' or optr2 not in STRING_OPERATORS and table.column_data[col2][0] == 'STRING':
        raise Invalid_Type("Incompatiple Type for Logical Operators")
    if table.column_data[col1][0] == 'INT':
        try:
            val1 = int(val1)
        except ValueError:
            raise Invalid_Type("Incompatible Type")
    if table.column_data[col2][0] == 'INT':
        try:
            val2 = int(val2)
        except ValueError:
            raise Invalid_Type("Incompatible Type")
    return table.double_where(col1,col2,optr1,optr2,val1,val2,log)

def join(table_1,table_2,tabs,cols):
    table = Table()
    table = table.join_tables(table_1,table_2,tabs[0],cols[0],tabs[1],cols[1])
    return table

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
        validateSelect(query_tokens)
            # select()
        return
    raise Syntax_Error("Unknown SQL Command")


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
    # Basic Syntax Check
    if len(tokens) != 5 or tokens[1] != 'INTO':
        raise Syntax_Error("Syntax Error: INSERT")
    # Parse Table if it has ()
    insert_info = [token for token in re.split(r'(\w+|\([^)]*\))',tokens[2]) if token.strip()]
    if len(insert_info) > 2:
        raise Syntax_Error("Syntax Error: INSERT[2]")
    table_name = insert_info[0]
    if table_name not in databases:
        raise Not_Exist(f"Table {table_name} does not existed")
    if len(insert_info) == 2:
        # print(insert_info)
        column = insert_info[1][1:len(insert_info[1])-1].strip()
        columns = [token.strip() for token in re.split(r',', column) if token.strip()]
        if len(columns) != 0:
            for i in range (0,len(columns)):
                if columns[i] != databases[table_name].columns[i]:
                    raise Syntax_Error("Syntax Error,INSERT: columns does not match")
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

def validateColumns(column,table_name):
    if column == '*':
        return column
    columns = [token.strip() for token in column.split(',') if token]
    # print(columns)
    for col in columns:
        if col not in databases[table_name].column_data:
            raise Not_Exist(f"Column {column} does not exist")
    return column

def validateJoinColumns(column,tables):
    if column == '*':
        return column
    columns = [token.strip() for token in column.split(',') if token]
    for col in columns:
        if '.' not in col:
            raise Syntax_Error("Syntax Error: Join columns must indicate table")
        table,col_name = col.strip().split('.')
        if table not in databases:
            raise Not_Exist(f"Table {table} does not exist")
        if table not in tables:
            raise Syntax_Error(f"Syntax Error: Invalid table {table}")
        if col_name not in databases[table].column_data:
            raise Syntax_Error(f"Syntax Error: {col_name} not in {table}")
    return column

def validateSelect(tokens): 
    length = len(tokens)

    #Must have at least basic format of SELECT x FROM table
    if(length < 5 or query_tokens[2] != 'FROM'):
        raise Syntax_Error("Syntax Error: invalid select")
    
    # check if table exist
    if ',' in tokens[3]:
        raise Unsupported_Functionality("Select from Multi Tables is not supported")

    if tokens[3] not in databases:
        raise Not_Exist(f"Table {tokens[4]} does not exist")

    ## No Join
    if length == 5:
        table = databases[tokens[3]]
        if tokens[4] != ';' and tokens[4].startswith('WHERE'):
            where_tokens = validateWhere([],tokens[3],tokens[4],False)
            print(where_tokens)
            if len(where_tokens) == 3:
                table = single_where(table,where_tokens)
            if len(where_tokens) == 7:
                # print("entering this")
                table = double_where(table,where_tokens)
        if '(' in tokens[1]:
            if ')' in tokens[1]:
                func = validateAggregateFunction(tokens[1],tokens[3])
                aggregate_select(func,table)
                return
            else:
                raise Syntax_Error("Syntax Error: Aggregate function has no closing parentheses")
        else:
            simple_select(validateColumns(tokens[1],tokens[3]),table)
            return
    
    if length > 5:
        join_condition = validateJoin(tokens)
        table_a = databases[tokens[3]]
        table_b = databases[tokens[5]]
        cols = []
        if '(' in tokens[1]:
            if ')' in tokens[1]:
                cols = validateAggregateFunction(tokens[1],[tokens[3],tokens[5]])
                # print(cols)
        else:
            cols = validateJoinColumns(tokens[1],[tokens[3],tokens[5]])
        table = join(table_a,table_b,join_condition[0],join_condition[1])
        if tokens[8] == ';':
            if '(' in tokens[1]:
                # print(func)
                aggregate_select(cols,table)
            else:
                simple_select(cols,table)
            return
        if tokens[8] != ';' and tokens[8].startswith('WHERE'):
            where_tokens = validateWhere([tokens[3],tokens[5]],"",tokens[8],True)
            print(where_tokens)
            if len(where_tokens) == 5:
                table = single_where(table,[f"{where_tokens[0]}.{where_tokens[2]}",where_tokens[3],where_tokens[4]])
                # simple_select(tokens[1],table)
            elif len(where_tokens) == 11:
                table = double_where(table,[f"{where_tokens[0]}.{where_tokens[2]}",where_tokens[3],where_tokens[4],where_tokens[5],f"{where_tokens[6]}.{where_tokens[8]}",where_tokens[9],where_tokens[10]])
                # simple_select(tokens[1],table)
        if '(' in tokens[1]:
            aggregate_select(cols,table)
        else:
            simple_select(cols,table)
        
    return
    
def validateJoin(tokens):

    # basic join syntax
    if(len(tokens) < 9):
        raise Syntax_Error('Syntax Error: Invalid Join syntax')
    if tokens[4] != 'JOIN':
        raise Unsupported_Functionality("Only Join is supported")
    if ',' in tokens[5]:
        raise Unsupported_Functionality("Select from Multi Tables is not supported")
    if tokens[5] not in databases:
        raise Not_Exist(f"Table {tokens[5]} does not exist")
    if(tokens[6] != 'ON'):
        raise Syntax_Error('Syntax Error: Invalid join syntax')
    if('=' not in tokens[7]):
        raise Syntax_Error('Syntax Error: Invalid join syntax, only support equal join')

    joining_tables = [tokens[3],tokens[5]]
            
    # validate the join condition
    joinPairs = [token.strip() for token in tokens[7].strip().split('=') if token.strip()]

    tabs = []
    cols = []

    for pair in joinPairs:
        if '.' not in pair:
            raise Syntax_Error("Syntax Error: column names must have table names for join")

        table, column = pair.strip().split('.')
        
        if(table not in joining_tables):
            raise Syntax_Error(f'Syntax Error: {table} not exist ')
        
        if(column not in databases[table].column_data):
            raise Syntax_Error('Syntax Error: Column ' + column + ' does not exist')
        
        tabs.append(table)
        cols.append(column)

    if(databases[tabs[0]].column_data[cols[0]][0] != databases[tabs[1]].column_data[cols[1]][0]):
        raise Syntax_Error('Syntax Error: Cannot join on columns of different types')

    return [tabs,cols]


def validateWhere(joining_tables, table_name, where_clause, join):

    numChars = len(where_clause)
    # numOperators = 0
    numConditions = 0
    cols = []

    cleanClause = where_clause[6:numChars-1] #removing where and semi-colon

    #counting number of conidtions
    numConditions = cleanClause.count('AND') + cleanClause.count('OR')

    if(numConditions > 1):
        raise Unsupported_Functionality('Unsupported functionality: can only support single two-clause logical conjunction or disjunction')
    # if(numOperators == 1):
    #     global SINGLE_WHERE
    #     SINGLE_WHERE = True
    # elif(numOperators == 2):
    #     global DOUBLE_WHERE
    #     DOUBLE_WHERE = True
    
    if(join):
        #isolating column names using regex
        pattern = r'(\w+)\.(\w+)\s[=!><]=?\s[^ANDOR\s]+\b'
        tables_and_columns = re.findall(pattern, cleanClause)
        tokens = [token.strip("'") for token in re.findall(r"\b\w+\b|[=<>!ANDOR]+|'[^']*'|.", cleanClause) if token.strip()]
        # print(tables_and_columns)
        # print(tokens)

        for pair in tables_and_columns:
            if(pair[0] not in databases):
                raise Not_Exist('Table ' + pair[0] + ' does not exist')
            if(pair[0] not in joining_tables): 
                raise Syntax_Error('Syntax Error: ' + pair[0])
            if(pair[1] not in databases[pair[0]].column_data):
                raise Syntax_Error('Syntax Error: Column ' + pair[1] + ' does not exist')
        return tokens
    
    else: 
        #isolating column names using regex
        pattern = r'\b(\w+)\s[=!><]=?\s[^ANDOR\s]+\b'
        cols = re.findall(pattern, cleanClause)
        for col in cols:
            if(col not in databases[table_name].column_data):
                raise Syntax_Error('Syntax Error: Column ' + col + ' does not exist')
        tokens = [token.strip("'") for token in re.findall(r"\b\w+\b|[=<>!ANDOR]+|'[^']*'|.", cleanClause) if token.strip()]
        return tokens
        
    # return True
    
def validateAggregateFunction(func,table_name):
    # table_name = ''
    # from_table_name = ''
    # column_name = ''
    pattern = r'(\w+|\([^)]*\))'

    #multiple columns can't be selected with an aggregate function
    if(',' in func):
        raise Unsupported_Functionality('Unsupported Functionality: cannot select multiple attributes when using an aggregate function') 

    tokens = [token for token in re.split(pattern,func) if token.strip()]
    if tokens[0].upper() not in Aggregate:
        raise Unsupported_Functionality('Unsupported Functionality: cannot select multiple attributes when using an aggregate function') 
    tokens[1] = tokens[1][1:len(tokens[1])-1]
    
    if '.' not in tokens[1]:
        validateColumns(tokens[1],table_name)
        return tokens
    
    # table,column = tokens[1].strip().split('.')
    validateJoinColumns(tokens[1],table_name)
    return tokens


### END OF SELECTION VALIDATION FUNCTIONS ###
#------------------------------------------------------------------------------#

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
    except FileNotFoundError as e:
        print(f"{e}")
# for table in databases.keys():
#     databases[table].describe()
#     databases[table].print_internal()

### END OF MAIN ###
    