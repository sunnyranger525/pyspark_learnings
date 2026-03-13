# Fiding most repeated element in list or tuple

list01 = [1, 2, 2, 3, 3, 3, 4]

# using Counter method
from collections import Counter

# creating counter dict
most_repeated = Counter(list01)
print(most_repeated)

# find most repeated first element and count
print(most_repeated.most_common(1))

# finding most repeated first two elements
print(most_repeated.most_common(2))

# filter repeated set more than 1 time

count_dict = {item:value for item, value in most_repeated.items() if value >1}
print(count_dict)

# without Conter method

lst = [1, 2, 2, 3, 3, 3, 4,4,4,4,4,4,4,4,4]

count_key = {}
for value in lst:
    if value in count_key:
        count_key[value] += 1
    else:
        count_key[value] = 1
print(count_key)

dup_keys = [key for key, value in count_key.items() if value > 1]
print(dup_keys)

max_key = max(count_key, key= count_key.get)
print(max_key)
value = count_key[max_key]
print(value)

dup_keys_lst = set()
for i in lst:
    count_key_cnt = lst.count(i)
    if count_key_cnt > 1:
        dup_keys_lst.add(i)
print(dup_keys_lst)

words = ["apple", "banana", "cat"]

rev_words = [word[::-1] for word in words]
print(rev_words)
for word in words:
    test_word = word.lower()
    check_word = ''.join(reversed(word.lower()))
    if test_word == check_word:
        print("Anagram")
    else:
        print(f"{test_word} with {check_word}")

def mai_dec(fun):
    def wrapper(*value, **kwarg):
        print("before call function")
        fun(*value, **kwarg)
        print("after call function")
    return wrapper


def sam():
    print("hey, sam good morning")
obj = mai_dec(sam)
obj()

def gen_call(value):
    for v in value:
        yield  v
def get_call2(value):
    yield  value
li = [x for x in range(2, 10)]
obj2 = gen_call(li)
print(next(obj2))
print(next(obj2))
print(next(obj2))
print(next(obj2))
print(next(obj2))
for i in li:
    x = get_call2(i)
    print(next(x))

def reverse_word(value):
    for i in value:
        print( i[::-1] )
words = ["apple", "banana", "cat"]
reverse_word(words)


