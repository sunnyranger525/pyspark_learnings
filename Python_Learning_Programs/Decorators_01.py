# Decorators

def deco(func):
    def wrapper(*pos, **kwrg):
        print("Before function call,", func.__name__)
        func(*pos, **kwrg)
        print("After function call,", func.__name__)
    return wrapper

@deco
def say_hello(name):
    print(f"Hello, {name}")

say_hello("sam")
@deco
def greet(wish):
    print("hello,", wish)
greet("Good Morning")
