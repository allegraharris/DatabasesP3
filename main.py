# Create, show, describe
# Assume one input at each time

import sqlparse
import time
# import getch

SQLCommand = ['CREATE','SHOW','DESCRIBE'] # keyWord is not case sensitive
Datatype = ['INT','STRING']
sql_query = "CREATE TABLE people { key_no INT NOT NULL, first_name INT NOT NULL, last_name INT NOT NULL, primary_key (key_no)};"
query_tokens = []
quitting = False
PROMPT = "-> "
PROMPT2 = "> "

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
    global query_tokens
    query_tokens = []
    parsed = sqlparse.parse(sql_query)
    for statement in parsed:
        for token in statement.tokens:
            if token.value.strip():
                query_tokens.append(token.value)

def isValidCommand():
    global SQLCommand
    global parsedList
    for command in SQLCommand:
        if command == parsedList[0].upper():
            return True
    return False

def Create():
    if parsedList[1].upper() != "TABLE":
        print("")
    return
    
def Show():
    return

def Describe():
    return

def eval():
    global parsedList
    if not isValidCommand():
        print("Invalid SQL syntax")
        return
    pos = SQLCommand.index(parsedList[0].upper())
    if pos == 0:
        Create()
    if pos == 1:
        Show()
    if pos == 2:
        Describe()


while quitting == False:
    readInput()
    filter()
    print(sql_query)
    print(query_tokens)
    if query_tokens[0] == "quit":
        break
    



