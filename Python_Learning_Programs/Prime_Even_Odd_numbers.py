# Extract Even, odd and prime numbers

list1 = [ x for x in range(1,101)]
print("=============Raw list================")
print(list1)
def even_list(num):
    if num % 2 == 0:
        return num
    else:
        return num
print("============Even list===========")
even_list1 = []
odd_list1 = []
for num1 in list1:
    if num1 % 2 == 0:
        even_list1.append(num1)
    else :
        odd_list1.append(num1)
print(even_list1)
print("===============Odd list==========")
print(odd_list1)

print("==============Prime List=========")
from math import *
prime_list1 = []
Non_prime_list1 = []
for num2 in list1:
    if num2 <= 2 :
        Non_prime_list1.append(num2)
        continue
    is_prime = True
    for i in range(2, ceil(num2/2)+1):
        if num2 % i == 0:
            is_prime =False
            break
    if is_prime == True:
        prime_list1.append(num2)
    else:
        Non_prime_list1.append(num2)
print(prime_list1)
print(Non_prime_list1)


