import re
str = input("-> ")
tokens = [token for token in re.split(r'(\w+|\([^)]*\))',str) if token.strip()]
pattern = r'\b(\w+)\s[=!><]=?\s[^ANDOR\s]+\b'
cols = re.findall(pattern, str)
tokens = [token for token in re.findall(r'\b\w+\b|[=<>!ANDOR]+|.', str) if token.strip() and token.strip() != "'"]
print(tokens)