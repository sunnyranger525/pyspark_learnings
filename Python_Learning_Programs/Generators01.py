# Generators
"""
Generator is special type function.
it doesn't return all the result at once
it return one by one
we need to use yield keyword instead of
return
we need to use next method as well

"""

# creating Generator function
def generator_test(list1):
    for i in list1:
        yield  i

# Creating list
list01 = [x for x in range(1,10)]
print("=========Raw List============")
print(list01)

gen_var1 = generator_test(list01)
# print(gen_var1)
# print(next(gen_var1))
# print(next(gen_var1))
# print(next(gen_var1))
# print(next(gen_var1))
# print(next(gen_var1))
# print(next(gen_var1))
# print(next(gen_var1))
# print(next(gen_var1))
# print(next(gen_var1))

# method 2

def gen_test(x):
    yield x
gen_var2 = gen_test(list01)
print(next(gen_var2))