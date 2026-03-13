# Fibnoccises Series, sum of two preceding values
# define a and b

a, b = 0, 1
c = 0
# apply for loop

for i in range(10):
    print(a)
    a, b=b, a+b
    c = b, 2*b+a
print(c)
print("Applied successfull")


