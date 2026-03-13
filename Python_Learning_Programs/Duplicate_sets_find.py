from enum import unique

name = "sammaiah"
print(name)

print(name[::-1])
print(list(iter(name)))
print(".".join(iter(name)))
print("".join(reversed(name)))
data = ["level", "madam", "ram"]
for word in data:
    if word.lower() == word[::-1].lower():
        print(f"{word} is polindrom")
    else:
        print(f"{word} is not polindrom")
data01 = ['hello', 'world', 'python']
iter_data01= iter(data01)
print(list(iter_data01))
print("".join(data01))

value_01 = "listEN"
print(value_01)
value_01_lower = value_01.lower()
print(value_01_lower)
value_01_sorted = sorted(value_01_lower)
print(value_01_sorted)
value_01_join = "".join(value_01_sorted)
print(value_01_join)

if value_01_join == "".join(sorted("stenli".lower())):
    print("It is Anagram")
else:
    print("It is not anagram")

list_nums_list01 = [1, 2, 3, 2, 4, 5, 1,2,3,3]

dup_list = []
unique_list = []
for num in list_nums_list01:
    if num in unique_list:
        dup_list.append(num)
    else:
        unique_list.append(num)
print(set(dup_list))
print(unique_list)
dup_count_method = {}
no_dup_list = []
for num in list_nums_list01:
    count_valu = list_nums_list01.count(num)
    if count_valu > 1:
        dup_count_method[num] = count_valu
    else:
        no_dup_list.append(num)
print(dup_count_method)
print(max(dup_count_method))
print(no_dup_list)

# counter method

from collections import Counter
num_list02 = [1, 2, 3, 2, 4, 1, 1]
print(Counter(num_list02))

for value, count_value in Counter(num_list02).items():
    print(value, count_value)

list_dup = {
    item:count for item, count in Counter(num_list02).items()
    if count > 1
}
print(list_dup)
print(type(list_dup))
print(max(list_dup))
print(max(list_dup, key = list_dup.get))
print(max(list_dup, key= list_dup.get),list_dup[max(list_dup, key = list_dup.get)])

num_list02.reverse()
print(num_list02)
num_list02.sort()
print(num_list02)











