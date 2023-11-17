database = {}
table = {}
columns = {'key_no':('INT',1),'first_name':('STRING',0),'last_name':('STRING',0)} # 1: indicate primary key
# type = ['INT','STRING','STRING']
item1 = (1,'Charlie','Mei')
item2 = (2,'Andy','Han')
table = [columns]
print(columns['key_no'][1])
print(table[0]['key_no'][0])
table.append(item1)
print(table[1])
database['table1'] = table
# database.append(table)
print(database)



