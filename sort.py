my_data = {
    1: {'attribute': 'x', 'info': 3},
    2: {'attribute': 'y', 'info': 1},
    3: {'attribute': 'z', 'info': 4},
    4: {'attribute': 'x', 'info': 2},
}

# Sort by the second element of the 'info' tuple (e.g., sorting by the string values)
sorted_data = sorted(my_data.items(), key=lambda x: x[1]['info'])
print(sorted_data[0][1]['info'])

# for item in sorted_data:
#     print(item[0])
#     print(item[1])

# for key, value in sorted_data:
#     print(f"Key: {key}, Info: {value['info']}")

list1 = [1, 3, 5, 7, 7, 9]
list2 = [3, 5, 7, 7, 9, 10]

common_elements = []

i, j = 0, 0

while i < len(list1) and j < len(list2):
    if list1[i] == list2[j]:
        common_elements.append(list1[i])
        i += 1
        j += 1
    elif list1[i] < list2[j]:
        i += 1
    else:
        j += 1

print("Common elements:", common_elements)

