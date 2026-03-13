# File handling with read() function, lets go with read mode
"""
file handling with object or variable
read(), read(10), readline(), readlines()
syntax -- method 01
object = opent("file path + name ", "mode")
Syntax -- method 02
with open("file path", "mode")
"""
# create object
object_content = open("textefile01.text", "r")

# store data into variable by usign read()
content01 = object_content.read()
print(content01)

# to find cursor location using tell()
print("current loc",object_content.tell())

# to change cursor location use seek()
object_content.seek(0)

# finding cursor location after using seek
print("current loc", object_content.tell())

# to read number of char
content02 = object_content.read(5)
print(content02)

# set cursor to zero
object_content.seek(0)
content03 = object_content.readline()
print(content03)

# set cursor to zero
print("====readlines as like list =======")
object_content.seek(0)
content04 = object_content.readlines()
print(content04)

# read data using for loop
print("=========read with for loop=====")
for line in content04:
    print(line)


# close open file using close() function
object_content.close()

# check whether file is beign opened or closed
print(object_content.closed)