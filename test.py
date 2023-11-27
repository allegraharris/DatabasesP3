import re
str = input("-> ")
tokens = [token for token in re.split(r'(\w+|\([^)]*\))',str) if token.strip()]
print(tokens)