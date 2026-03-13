# Factiorial Program

# First Program

print("==========with for loop================")
number1 = int(input("Enter number :>>: "))
fact1 = 1
for i in range(1,number1+1):
    fact1 *=i
print(f"factorial of {number1} is : {fact1}")

print("============While loop=================")
number2 = int(input("Enter Number :>>: "))
fact2 = 1
while number2>=1:
    fact2 *=number2
    number2 -=1
print(f"Factorial of {number2} is : {fact2}")

print("===========with function================")
def fact(num):
    fact3 = 1
    while num >=1:
        fact3 *=num
        num -=1
    return fact3
num1 = int(input("Enter Number :>>:: "))
fact4 = fact(num1)
print(f"Factorial of {num1} is : {fact4}")

print("=========Find the Factorial number========")
num2 = int(input("Enter number between 1 to 10 :>>:: "))
for num3 in range(1, num2+1):
    fact5 = fact(num3)
    print(f"Factorial of {num3} is \t: {fact5}")


