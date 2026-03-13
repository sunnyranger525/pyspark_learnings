# List Data type,

# where add multiple items into single variable
"""
it is ordered , followed insertion order
it is mutable,
it is iterable
defined with [] braces
define with 'list' key ward as well

"""
print("Creating Raw List")
a = [ x for x in range(1, 11)]
print(a)

print("====Elements Access Methods=====")
print(a[2]) # direct index
print(a[-2]) # Index revers order
print(a[3:7]) # access multiple items
print(a[3:]) # slicing method
print(a[:7])
print(a[1:8:2])
print("=======Access with for loop======")
for a1 in a:
    print(a1)

# Dealing modify existing list, adding items
a.append("Chandu") # append method to add item last

# add value at specified index,
# current index value move to next and on
a.insert(2, True)

a.extend(["Ram", "20.5"])
print(a)
a[1] = 22 # replace or modify index 1
print(a)

# Operation on list

print(a.index(6)) # to find index of existing item
print(a.count(22)) # item how many time available
print(len(a))

# Extrac only numbers
numeric_data = [x for x in a if isinstance(x,(int, float, bool))]
print(numeric_data)
print(min(numeric_data))
print(max(numeric_data))
print(sum(numeric_data))

#  Remove items
print("========Remove Methods=======")
print(a)
a.pop()
print(a)
a.pop(1)
print(a)
a.remove(4)
print(a)
del a[7]
print(a)

# sort Methods
print("==========Sort Operations=====")
numeric_data.sort()
print(numeric_data)
numeric_data.sort(reverse=True)
print(numeric_data)
numeric_data.reverse()
print(numeric_data)



