from tabulate import tabulate as tb
import time

data = []

column = ['col1','col2','col3','col4']

for i in range (0,10000):
    data.append([i,i,i,i])

# Print the DataFrame
start_time = time.time()
# print(data)
print(tb(data,column,tablefmt='outline'))
end_time = time.time()
print(f"Time: {end_time-start_time:.3f}s")
