import sqlparse
import re
from tabulate import tabulate as tb
from table import Table, databases, start, end, getTime
from exception import Invalid_Type, Syntax_Error, Duplicate_Item, Keyword_Used, Not_Exist, Unsupported_Functionality

keywords = ['CREATE','SHOW','DESCRIBE','INSERT','INTO','TABLE','TABLES','REFERENCES','INT','STRING','PRIMARY','FOREIGN','KEY','WHERE','SELECT','EXECUTE','ON']
sqlCommand = ['CREATE','SHOW','DESCRIBE','INSERT','SELECT','EXECUTE']
Aggregate = ['MAX','MIN','AVG','SUM']
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
    global sql_query,query_tokens
    sql_query = sqlparse.format(sql_query,reindent=False, keyword_case='upper') # reformat user input
    query_tokens = []
    parsed = sqlparse.parse(sql_query) 
    for statement in parsed:
        for token in statement.tokens:
            if token.value.strip():
                query_tokens.append(token.value)

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
    end()
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
    end()
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
        start()
        query_tokens = []
        for token in stmt.tokens:
            if token.value.strip():
                query_tokens.append(token.value)
        eval_query()
        print()
    return

def simple_select(cols,table):
    if cols == '*':
        table.print_internal()
        return
    table.print_internal_select(cols)
    return

def aggregate_select(func,table):
    output_table = Table()
    if func[0].upper() == 'MAX':
        output_table = table.max(func[1])
    elif func[0].upper() == 'MIN':
        output_table = table.min(func[1])
    elif func[0].upper() == 'AVG':
        output_table = table.avg(func[1])
    elif func[0].upper() == 'SUM':
        output_table = table.sum(func[1])
    output_table.print_internal()

def single_where(table,where_tokens):
    col = where_tokens[0]
    optr = where_tokens[1]
    val = where_tokens[2]
    if optr not in LOGICAL_OPERATORS:
        raise Syntax_Error("Syntax Error: Invalid Logical Operators")
    if optr not in STRING_OPERATORS and table.column_data[col][0] == 'STRING':
        raise Invalid_Type("Incompatiple Type for Logical Operators")
    if val in table.column_data:
        if table.column_data[col][0] != table.column_data[val][0]:
            raise Invalid_Type("Incompatiple Type between columns")
        else:
            return table.single_where_column(col,val,optr)
    if table.column_data[col][0] == 'INT':
        try:
            val = int(val)
        except ValueError:
            raise Invalid_Type("Incompatible Type")
    return table.single_where(col,val,optr)

def double_where(table,where_tokens):
    col1 = where_tokens[0]
    optr1 = where_tokens[1]
    val1 = where_tokens[2]
    log = where_tokens[3]
    col2 = where_tokens[4]
    optr2 = where_tokens[5]
    val2 = where_tokens[6]
    if optr1 not in LOGICAL_OPERATORS or optr2 not in LOGICAL_OPERATORS:
        raise Syntax_Error("Syntax Error: Invalid Logical Operators")
    if optr1 not in STRING_OPERATORS and table.column_data[col1][0] == 'STRING' or optr2 not in STRING_OPERATORS and table.column_data[col2][0] == 'STRING':
        raise Invalid_Type("Incompatiple Type for Logical Operators")
    if val1 in table.column_data and val2 in table.column_data:
        return table.double_where_column(col1,col2,optr1,optr2,val1,val2,log)
    if val1 in table.column_data:
        if table.column_data[col2][0] == 'INT':
            try:
                val2 = int(val2)
            except ValueError:
                raise Invalid_Type("Incompatible Type")
        if log == 'AND':
            return (table.single_where(col2,val2,optr2)).single_where_column(col1,val1,optr1)
        elif log == 'OR':
            table_1 = table.single_where_column(col1,val1,optr1)
            table_2 = table.single_where(col2,val2,optr2)
            table_1.indexing.update(table_2.indexing)
            return table_1
    if val2 in table.column_data:
        where_tokens[0], where_tokens[4] = where_tokens[4], where_tokens[0]
        where_tokens[1], where_tokens[5] = where_tokens[5], where_tokens[1]
        where_tokens[2], where_tokens[6] = where_tokens[6], where_tokens[2]
        return double_where(table,where_tokens)
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
    if optr not in sqlCommand:
        raise Syntax_Error("Syntax Error: Unknown SQL Command")
    if optr == 'CREATE':
        validateCreateTable(query_tokens)
        create_table()
        getTime()
    elif optr == 'SHOW':
        if len(query_tokens) != 3 or query_tokens[1] != 'TABLES':
            raise Syntax_Error("Syntax Error: SHOW TABLES")
        show_table()
        getTime()
    elif optr == 'DESCRIBE':
        table_name = validateDescribe(query_tokens)
        describe_table(table_name)
        getTime()
    elif optr == 'INSERT':
        table_name = validateInsert(query_tokens)
        prev_size = databases[table_name].size
        insert(table_name,query_tokens[3][6:len(query_tokens[3])].strip())
        cur_size = databases[table_name].size
        print(f"Query OK, {cur_size-prev_size} rows affected")
        end()
        getTime()
    elif optr == 'SELECT':
        validateSelect(query_tokens)
        getTime()
    elif optr == 'EXECUTE':
        filename = validateExecute(query_tokens)
        execute(filename)


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
        raise Keyword_Used("Keyword Used: invalid table name: " + table_name)
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
    insert_info = [token for token in re.split(r'(\w+|\([^)]*\))',tokens[2]) if token.strip()]
    if len(insert_info) > 2:
        raise Syntax_Error("Syntax Error: INSERT[2]")
    table_name = insert_info[0]
    if table_name not in databases:
        raise Not_Exist(f"Table {table_name} does not existed")
    if len(insert_info) == 2:
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
        raise Unsupported_Functionality("Unsupported Functionality: SELECT from multiple tables is not supported")

    if tokens[3] not in databases:
        raise Not_Exist(f"Table {tokens[3]} does not exist")

    ## No Join
    if length == 5:
        table = databases[tokens[3]]
        if tokens[4] != ';' and tokens[4].startswith('WHERE'):
            where_tokens = validateWhere([],tokens[3],tokens[4],False)
            if len(where_tokens) != 3 and len(where_tokens) != 7:
                raise Syntax_Error("Syntax Error: Invalid Where Condition Syntax")
            if len(where_tokens) == 3:
                table = single_where(table,where_tokens)
            if len(where_tokens) == 7:
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
    
    where_tokens = []

    if length > 5:
        join_condition = validateJoin(tokens)
        table_a = databases[tokens[3]]
        table_b = databases[tokens[5]]
        cols = []
        if '(' in tokens[1]:
            if ')' in tokens[1]:
                cols = validateAggregateFunction(tokens[1],[tokens[3],tokens[5]])

        else:
            cols = validateJoinColumns(tokens[1],[tokens[3],tokens[5]])

        if tokens[8] != ';' and tokens[8].startswith('WHERE'):
            where_tokens = validateWhere([tokens[3],tokens[5]],"",tokens[8],True)
            if len(where_tokens) != 3 and len(where_tokens) != 7:
                raise Syntax_Error("Syntax Error: Invalid Where condition")
            if len(where_tokens) == 3:
                name_1 = where_tokens[0].split('.')[0]
                col_1 = where_tokens[0].split('.')[1]
                optr = where_tokens[1]
                if "." not in where_tokens[2]:
                    if name_1 == tokens[3]:
                        table_a = single_where(table_a,[col_1,optr,where_tokens[2]])
                    if name_1 == tokens[5]:
                        table_b = single_where(table_b,[col_1,where_tokens[1],where_tokens[2]])
                else:
                    name_2 = where_tokens[2].split('.')[0]
                    col_2 = where_tokens[2].split('.')[1]
                    if name_1 == name_2:
                        if name_1 == tokens[3]:
                            table_a = single_where(table_a,[col_1,optr,col_2])
                        if name_1 == tokens[5]:
                            table_b = single_where(table_b,[col_1,optr,col_2])

            if len(where_tokens) == 7:
                name_1 = where_tokens[0].split('.')[0]
                col_1 = where_tokens[0].split('.')[1]
                name_2 = where_tokens[4].split('.')[0]
                col_2 = where_tokens[4].split('.')[1]
                val1 = where_tokens[2]
                val2 = where_tokens[6]
                optr1 = where_tokens[1]
                optr2 = where_tokens[5]
                log = where_tokens[3]
                if log == 'AND':
                    if name_1 == name_2 and "." not in where_tokens[2] and "." not in where_tokens[6]:
                        if name_1 == tokens[3]:
                            table_a = double_where(table_a,[col_1,optr1,val1,log,col_2,optr2,val2])
                        if name_1 == tokens[5]:
                            table_b = double_where(table_b,[col_1,optr1,val1,log,col_2,optr2,val2])

                    else:
                        if "." not in where_tokens[2]:
                            if name_1 == tokens[3]:
                                table_a = single_where(table_a,[col_1,optr1,val1])
                            if name_1 == tokens[5]:
                                table_b = single_where(table_b,[col_1,optr1,val1])
                        else:
                            name_3 = val1.split('.')[0]
                            col_3 = val1.split('.')[1]
                            if name_1 == name_3:
                                if name_1 == tokens[3]:
                                    table_a = single_where(table_a,[col_1,optr1,col_3])
                                if name_1 == tokens[5]:
                                    table_b = single_where(table_b,[col_1,optr1,col_3])
                        if "." not in where_tokens[6]:
                            if name_2 == tokens[3]:
                                table_a = single_where(table_a,[col_2,optr2,val2])
                            if name_2 == tokens[5]:
                                table_b = single_where(table_b,[col_2,optr2,val2])
                        else:
                            name_3 = val2.split('.')[0]
                            col_3 = val2.split('.')[1]
                            if name_2 == name_3:
                                if name_2 == tokens[3]:
                                    table_a = single_where(table_a,[col_2,optr2,col_3])
                                if name_2 == tokens[5]:
                                    table_b = single_where(table_b,[col_2,optr2,col_3])
                if log == 'OR':
                    if name_1 == name_2:
                        if name_1 == tokens[3]:
                            if "." not in val1 and "." not in val2:
                                table_a = double_where(table_a,[col_1,optr1,val1,log,col_2,optr2,val2])
                            elif "." not in val1:
                                name_3 = val2.split('.')[0]
                                col_3 = val2.split('.')[1]
                                if name_1 == name_3:
                                    table_a = double_where(table_a,[col_1,optr1,val1,log,col_2,optr2,col_3])
                            elif "." not in val2:
                                name_3 = val1.split('.')[0]
                                col_3 = val1.split('.')[1]
                                if name_1 == name_3:
                                    table_a = double_where(table_a,[col_1,optr1,col_3,log,col_2,optr2,val2])
                            else:
                                name_3 = val1.split('.')[0]
                                col_3 = val1.split('.')[1]
                                name_4 = val2.split('.')[0]
                                col_4 = val2.split('.')[1]
                                if name_1 == name_3 and name_1 == name_4:
                                    table_a = double_where(table_a,[col_1,optr1,col_3,log,col_2,optr2,col_4])
                        if name_1 == tokens[5]:
                            if "." not in val1 and "." not in val2:
                                table_b = double_where(table_b,[col_1,optr1,val1,log,col_2,optr2,val2])
                            if "." not in val1:
                                name_3 = val1.split('.')[0]
                                col_3 = val1.split('.')[1]
                                if name_1 == name_3:
                                    table_b = double_where(table_b,[col_1,optr1,col_3,log,col_2,optr2,val2])
                            if "." not in val2:
                                name_3 = val2.split('.')[0]
                                col_3 = val2.split('.')[1]
                                if name_1 == name_3:
                                    table_b = double_where(table_b,[col_1,optr1,val1,log,col_2,optr2,col_3])
                            else:
                                name_3 = val1.split('.')[0]
                                col_3 = val1.split('.')[1]
                                name_4 = val2.split('.')[0]
                                col_4 = val2.split('.')[1]
                                if name_1 == name_3 and name_1 == name_4:
                                    table_b = double_where(table_b,[col_1,optr1,col_3,log,col_2,optr2,col_4])

        table = join(table_a,table_b,join_condition[0],join_condition[1])

        # verify the result and filter the where-clause that can be optimized
        if tokens[8] == ';':
            if '(' in tokens[1]:
                aggregate_select(cols,table)
            else:
                simple_select(cols,table)
            return
        if tokens[8] != ';' and tokens[8].startswith('WHERE'):
            if len(where_tokens) == 3:
                table = single_where(table,where_tokens)
            elif len(where_tokens) == 7:
                table = double_where(table,where_tokens)
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
        raise Unsupported_Functionality("Unsupported Functionality: Only join is supported")
    if ',' in tokens[5]:
        raise Unsupported_Functionality("Unsupported Functionality: SELECT from multiple tables is not supported")
    if tokens[5] not in databases:
        raise Not_Exist(f"Table {tokens[5]} does not exist")
    if(tokens[6] != 'ON'):
        raise Syntax_Error('Syntax Error: Invalid join syntax')
    if('=' not in tokens[7]):
        raise Syntax_Error('Syntax Error: Invalid join syntax, only support equal join')
    if tokens[3] == tokens[5]:
        raise Syntax_Error('Syntax Error: Should not run equal join on two table that are same')

    joining_tables = [tokens[3],tokens[5]]
            
    # validate the join condition
    joinPairs = [token.strip() for token in tokens[7].strip().split('=') if token.strip()]
    if len(joinPairs) != 2:
        raise Syntax_Error("Syntax Error: Invalid JOIN syntax")
    if " " in joinPairs[1]:
        raise Syntax_Error("Syntax Error: Invalid JOIN syntax")

    tabs = []
    cols = []

    for pair in joinPairs:
        if '.' not in pair:
            raise Syntax_Error("Syntax Error: Ambiguous column names")

        table, column = pair.strip().split('.')
        
        if(table not in joining_tables):
            raise Syntax_Error(f'Syntax Error: {table} not exist ')
        
        if(column not in databases[table].column_data):
            raise Syntax_Error('Syntax Error: Column ' + column + ' does not exist')
        
        tabs.append(table)
        cols.append(column)

    if(databases[tabs[0]].column_data[cols[0]][0] != databases[tabs[1]].column_data[cols[1]][0]):
        raise Invalid_Type('Invalid Type: Cannot join on columns of different types')

    return [tabs,cols]


def validateWhere(joining_tables, table_name, where_clause, join):

    numChars = len(where_clause)
    numConditions = 0
    cols = []

    cleanClause = where_clause[6:numChars-1] #removing where and semi-colon
    numConditions = cleanClause.count('AND') + cleanClause.count('OR')

    if(numConditions > 1):
        raise Unsupported_Functionality('Unsupported functionality: can only support single two-clause logical conjunction or disjunction')
    
    if(join):
        #isolating column names using regex
        pattern = r'(\w+)\.(\w+)\s[=!><]=?\s[^ANDOR\s]+\b'
        tables_and_columns = re.findall(pattern, cleanClause)
        tokens = [item.strip() for item in re.split(r'\s*([=!<>]+|AND|OR)\s*', cleanClause) if item.strip()]
        tokens = [item.strip("'") for item in tokens]

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
    
def validateAggregateFunction(func,table_name):

    pattern = r'(\w+|\([^)]*\))'

    if(',' in func):
        raise Unsupported_Functionality('Unsupported Functionality: cannot select multiple attributes when using an aggregate function') 

    tokens = [token for token in re.split(pattern,func) if token.strip()]
    if tokens[0].upper() not in Aggregate:
        raise Unsupported_Functionality('Unsupported Functionality: cannot select multiple attributes when using an aggregate function') 
    tokens[1] = tokens[1][1:len(tokens[1])-1]

    if '.' not in tokens[1] and isinstance(table_name,list):
        raise Syntax_Error(f"Syntax Error: Column {tokens[1]} is ambiguous")
    
    if '.' not in tokens[1]:
        validateColumns(tokens[1],table_name)
        return tokens
    
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
        start()
        eval_query()
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

### END OF MAIN ###
    