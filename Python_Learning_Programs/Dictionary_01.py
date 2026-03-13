# Dictionaries Deals with key value pair data
"""
Keys must be unique → If you insert a duplicate key,
the old value will be replaced.

Keys must be immutable → You can use strings,
numbers, tuples as keys. (Lists or sets cannot be keys.)

Values can be anything → Numbers, strings,
lists, other dictionaries, etc.

Dictionaries are unordered (before Python 3.7).
From Python 3.7+, insertion order is preserved.

"""
# Dict is defined with curl braces, key and value pair
dict01 = {
    "name" : "Samsu",
    "age" : 23,
    "village" : "ponugondla"
}
# print data
print("=============Dictionary ===========")
print(type(dict01))
print(dict01)

# below method, raise error if not find key
# access to value
print(dict01["name"]) # to access value with key
print(dict01["age"]) # return value of key age

# method to access key, key and get() method
print(dict01.get("city")) # doen't raise error, return None'
print(dict01.get("name")) # return value
print(dict01.get("village"))

# adding new value and update existin value
# key and update() method
dict01["age"] = 25 # update value using key
dict01["mandal"] = "kothaguda" # adding new value
print(dict01) # printing the new data after adding
dict01["dist"] = "mahabubabad"
print(dict01)

# adding list of data at once, use curl braces to add list of data
dict01.update({"dept" : "home", "state" : "telangana"})
print(dict01)

# remove data from dict , use pop(), del, clear
ele_remove = dict01.pop("state")
print(ele_remove)
print(dict01)

del dict01["mandal"]
print(dict01)

#dict01.clear()






