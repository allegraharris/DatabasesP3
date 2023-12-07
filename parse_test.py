import re

input_string = input("-> ")
# Split the string using the pattern
# parsed_strings = re.split(r'\s*([=!<>]+|and|or)\s*', input_string)

# Remove any leading or trailing whitespace
parsed_strings = [item.strip() for item in re.split(r'\s*([=!<>]+|AND|OR)\s*', input_string) if item.strip()]
parsed_strings = [item.strip("'") for item in parsed_strings]

print("Parsed strings:", parsed_strings)
