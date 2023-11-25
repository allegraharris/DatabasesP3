import unittest
from unittest.mock import patch, Mock
from io import StringIO
from main import *


def test_create_table(qry): 
    global sql_query, query_tokens
    sql_query = qry
    filter()
    print("QUERY TOKENS:", query_tokens)
    result = eval_query()
    print_table(result)

    
    #query_tokens = ["CREATE", "TABLE", "test_table", "col1", "INT,", "col2", "STRING,", "primary key", "(col1);"]
    #print(query_tokens)

def test_show_table():
    global sql_query, query_tokens
    sql_query = "SHOW TABLES;"
    filter()
    result = eval_query()
    print_table(result)

# Call your test functions

#global sql_query, query_tokens
query_tokens = []
sql_query = "create table students (id int, first_name string, last_name string, primary key (id) );"
test_create_table(sql_query)
test_show_table()