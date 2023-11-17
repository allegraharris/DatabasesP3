# Create, show, describe
# Assume one input at each time

import sqlparse
import time
# from test import eval_query
# import test
# import getch

SQLCommand = ['CREATE','SHOW','DESCRIBE'] # keyWord is not case sensitive
Datatype = ['INT','STRING']
sql_query = "CREATE TABLE people { key_no INT NOT NULL, first_name INT NOT NULL, last_name INT NOT NULL, primary_key (key_no)};"
query_tokens = []
quitting = False 
PROMPT = "-> "
PROMPT2 = "> "

tables = {}

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

## Begin evaluation functions
def find_operation():
    try:
        return SQLCommand.index(query_tokens[0])
    except ValueError:
        return -1
        

def eval_query():
    optr = find_operation()
    print(optr)
    return


while quitting == False:
    readInput()
    filter()
    print(sql_query)
    print(query_tokens)
    if query_tokens[0] == "quit":
        break
    eval_query()
    



