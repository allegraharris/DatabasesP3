<<<<<<< HEAD
print("Hello World")
print("sarah")
print("hello again")
=======
# # database = {}
# # table = {}
# # columns = {'key_no':('INT',1),'first_name':('STRING',0),'last_name':('STRING',0)} # 1: indicate primary key
# # # type = ['INT','STRING','STRING']
# # item1 = (1,'Charlie','Mei')
# # item2 = (2,'Andy','Han')
# # table = [columns]
# # print(columns['key_no'][1])
# # print(table[0]['key_no'][0])
# # table.append(item1)
# # print(table[1])
# # database['table1'] = table
# # # database.append(table)
# # print(database)

# database = {}
# table = []
# database['table1'] = table
# columns = {'key_no':('INT',1),'first_name':('STRING',0),'last_name':('STRING',0)} 
# item1 = (1,'Charlie','Mei')
# table.append(columns)
# table.append(item1)
# for key in table[0].keys():
#     print(key)
# select * from table1
# return database['table1']
# select first_name from table
# output first_name':('STRING',0)

# # research on BTree
import re

# input_string = "(1,2,3), (2,3,4), (2,4,5)"

# # Use regular expression to split by commas outside parentheses
# parsed_strings = re.split(r',\s*(?![^()]*\))', input_string)

# # Remove leading and trailing whitespaces from each parsed string
# parsed_strings = [s.strip() for s in parsed_strings]

# print(parsed_strings)

# tuple1 = {1,2,'efew'}
# tuple2 = {2,1,'efew'}
# if tuple1 == tuple2:
#     print("they are same")
# import re

# input_string = "one, 'two, three', four, 'five, six', seven"

# # Use regular expression to split by commas outside quotes
# parsed_strings = re.split(r',\s*(?=(?:[^\'"]*[\'"][^\'"]*[\'"])*[^\'"]*$)', input_string)

# # Remove leading and trailing single or double quotes
# parsed_strings = [re.sub(r'^[\'"]|[\'"]$', '', s) for s in parsed_strings]

# print(parsed_strings)
tuple1 = tuple({1, 2, 3})
tuple2 = tuple({2,3,4})
if tuple1 == tuple2:
    print("they are same")
>>>>>>> main
