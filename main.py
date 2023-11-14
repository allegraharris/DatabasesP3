# Create, show, describe
# Assume one input at each time

import sqlparse
import time
# import getch

SQLCommand = ['CREATE','SHOW','DESCRIBE'] # keyWord is not case sensitive
Datatype = ['INT','STRING']
Command = ['CREATE','SHOW','DESCRIBE']
userInput = "CREATE TABLE people { key_no INT NOT NULL, first_name INT NOT NULL, last_name INT NOT NULL, primary_key (key_no)};"
parsed = sqlparse.parse(userInput)
parsedList = []
quitting = False
PROMPT = "-> "
PROMPT2 = "> "

def readInput():
    global userInput
    global parsedList
    parsedList = []
    userInput = ""
    userinput = input(PROMPT)
    for c in userinput:
        if c ==';':
            userInput += c
            return
        userInput +=c
    readInput2()
    return

def readInput2():
    global userInput
    userinput = input(PROMPT2)
    for c in userinput:
        if c == ';':
            userInput += c
            return
        userInput += c
    readInput2()
    return

def filterInput():
    global userInput
    global parsedList
    parsed = sqlparse.parse(userInput)
    for stmt in parsed:
        for token in stmt.tokens:
            if token.value.strip():
                parsedList.append(token.value)
                # print(f"Token: {token}")
    return

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


# printInput(parsed)
while quitting == False:
    readInput()
    filterInput()
    print(parsedList)
    if parsedList[0] == "quit":
        break
    eval()
    



