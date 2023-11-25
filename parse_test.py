
import re
str = "(1,2,3) (23,4) (2,342)"
# tokens = [token.strip() for token in re.split(r',', str) if token.strip()]
tokens = [value.strip() for value in re.split(r',(?![^()]*\))',str)]
print(tokens)
print(len(tokens))