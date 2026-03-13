# define list
# creating raw list
raw_list = [x for x in range(101)]
print("=======Raw List==============")
print(raw_list)

even_list = []
odd_list = []
# define programe to extract even and odd list
for i in raw_list:
    if (i % 2) == 0:
        even_list.append(i)
    else:
        odd_list.append(i)
print("==========even list ===========")
print(even_list)
print("=========== Odd list ==========")
print(odd_list)

# define program to extract prime program

prime_list = []
non_prime_list = []

for num in raw_list:
    check_prime = 1
    if num > 1:
        for div in range (2, round(num/2) + 1):
            if (num % div) == 0:
                check_prime = 0
                break
    else:
        check_prime = 0
    if check_prime == 1:
        prime_list.append(num)
    else:
        non_prime_list.append(num)
print("===========prime list ===========")
print(prime_list)
print(f" Number of prime within 100 : {len(prime_list)}")
print("============Non Prime list ===========")
print(non_prime_list)
print(f" Number of non prime within 100 : {len(non_prime_list)}")


# define prime program

def prime_call(num):
    x = 1
    if num > 1:
        for d in range(2, round(num/2)+1):
            if (num % d) ==0:
                x = 0
                break
    else:
        x = 0
    return x

num1 = int(input("Enter number start :>>: "))
num2 = int(input("Enter number end :>>: "))
prime_pre_list = []
non_prime_pre_list = []
for value in range(num1, num2):
    check_value = prime_call(value)
    if check_value == 1:
        prime_pre_list.append(value)
    else:
        non_prime_pre_list.append(value)
print("========= predefined prime list==========")
print(prime_pre_list)
print("=========== Predefined non prime list ====")
print(non_prime_pre_list)
