# file handling with write file
"""

write method use to create file

"""

# open file with write mode
f1 = open("textfile02.text", "w")

# check wheather opened for write or not write
print(f1.writable())

# write method to write in file
f1.write("Hello Python \n")
f1.write("welcome yearrrrrr \n")

# write method to write in file
print("This line added using print ", file= f1)

#writelines method to write in file
multilines = [
    "hello sam \n",
    "Hi Manohar \n",
    "welcome to big daa \n",
    "Great, thanks\n"
]

f1.writelines(multilines)

# write lines method to wrie mode
for line in multilines:
    print(line, file=f1)
    f1.write(line)


print(f1.closed)

f1.close()

print(f1.closed)
