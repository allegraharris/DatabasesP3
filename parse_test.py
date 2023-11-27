
import re
import sqlparse
str = "(1,2,3) (23,4) (2,342)"
# tokens = [token.strip() for token in re.split(r',', str) if token.strip()]
tokens = [value.strip() for value in re.split(r',(?![^()]*\))',str)]
print(tokens)
print(len(tokens))

sql_query = 'select a,b,c from test join test2 on test.a=test.b where test.a = 5 and test.b = 6;'
sql_query = sqlparse.format(sql_query,reindent=False, keyword_case='upper') # reformat user input
query_tokens = []
parsed = sqlparse.parse(sql_query) 
for statement in parsed:
    for token in statement.tokens:
        if token.value.strip():
            query_tokens.append(token.value)

for token in query_tokens:
    print(token)