dict02 = {
    "name" : "Samsu",
    "age" : 23,
    "village" : "ponugondla"
}

# add key and value
dict02["mandal"] = "kothaguda"
print(dict02)

# use iterate with for loop

for dict_key in dict02:
    print(dict_key)

for dict_key02 in dict02 :
    print(dict_key02,"\t:", dict02[dict_key02])

# extrac only keys and values, using keys(), values()
keys = dict02.keys()
values = dict02.values()
print(keys)
print(values)

# use items() method

print(dict02.items())

# using for loop to get key and value
for k, v in dict02.items():
    print(k, "-----", v)

# adding items

dict02["dist"] = "Mahabuabad"
print(dict02)
dict02.update({"state" : "Telangana"})
print(dict02)

# for loop
for k1, v1 in dict02.items():
    print(k1,"====\t",v1)


# removed items
dict02.pop("state")
del_vlaue = dict02.popitem()
print(del_vlaue)
del dict02["mandal"]
print(dict02)
print(dict02.keys())
print(dict02.values())
print(dict02.items())

# enumerate(iterable, start =index)

for ind, value3 in enumerate(dict02, 1):
    print(ind, ":", value3)

for ind, value3 in enumerate(dict02, 1):
    print(ind, ":", dict02[value3])

for ind, (k2, v2) in enumerate(dict02.items(), 1):
    print(ind, ":", k2, ":::", v2)

for ind, value4 in enumerate(dict02.keys(), 1):
    print(ind, ":", value4)

for ind, value5 in enumerate(dict02.values(), 1):
    print(ind, ":", value5)

for ind, value6 in enumerate(dict02.items(), 1):
    print(ind, ":", value6[0])
    print(ind, ":", value6[1])



