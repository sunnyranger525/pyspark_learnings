# Map function with lambda
"""
syntax lambda

lambda Arguments : condition

syntax for map funtion

map(function, iterable)
"""
print("==============Map with Lambda=========")
list01 = [ x for x in range(10, 31)]
print("=====Raw list======")
print(list01)
# addign to 10 to each element
map_list01 = list(map(lambda x : x + 10, list01))
print(map_list01)
map_list02 = list(map(lambda x : "even" if x % 2 ==0 else "odd", map_list01))
print(map_list02)
map_list03 = list(
    map(
        lambda x, y : x+ "~"+str(y), map_list02, map_list01
    )
)
print(map_list03)
map_list04 = list(map(lambda x : x.replace("~", "+"), map_list03))
print(map_list04)
map_list05 = list(map(lambda x : x.split("+"), map_list04))
print(map_list05)
from itertools import *
map_list06 = list(map(lambda x : x, chain.from_iterable(map_list05)))
print(map_list06)
map_list07 = list(map(lambda x :x, sum(map_list05, [])))
print(map_list07)
flat_list = [x for y in map_list05 for x in y]
print(flat_list)
flat_list01 = []
for x in map_list05:
    for y in x:
        flat_list01.append(y)
print(flat_list01)
map_list08 = list(filter(lambda x : isinstance(x, str) and x.isdigit(), map_list07))
print(map_list08)
map_list09 = list(map(lambda x : int(x), map_list08))
print(map_list09)
map_list10 = list(filter(lambda x : isinstance(x, str) and not x.isdigit(), map_list07))
print(map_list10)
map_list11 = list(map(lambda x : x[0], map_list05))
print(map_list11)
map_list12 = list(map(lambda x: x[1], map_list05))
print(map_list12)
map_list13 = list(filter(lambda x : x[0] == "odd", map_list05))
print(map_list13)