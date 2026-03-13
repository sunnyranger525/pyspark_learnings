# define list
list_02  = [ x for x in range(16)]
print(list_02)

# add element at end of list
list_02.append(30)
print(list_02)

# add element at particular index
list_02.insert(5, 25)
print(list_02)

# add list of elements at end of list
list_02.extend([10,20,50,35])
print(list_02)

# update element based on index
list_02[10] = 100
print(list_02)

# remove element at end
list_02.pop()
print(list_02)

#remove element at index
list_02.pop(4)
print(list_02)

# remove element at on value
list_02.remove(25)
print(list_02)

# remove some list of element
del list_02[3:8]
print(list_02)

# concate both lists
print(list_02 + list_02)

# multi list
print(list_02 * 2)

# iterate list
for i in list_02:
    print(i, end=",")

# find index
print(list_02.index(100))

# count elements times
print(list_02.count(10))

# lenth of elements
print(len(list_02))

# membership check
print( 100 in list_02)

# value check based
list_num = [21,30,17,14,27]

for i in list_num:
    if i in list_02:
        print(f"{i} is available")
    else:
        print(f"{i} is not available")

even_list = []
odd_list = []
for i in list_02:
    if i % 2 == 0:
        even_list.append(i)
    else:
        odd_list.append(i)
print(even_list)
print(odd_list)

