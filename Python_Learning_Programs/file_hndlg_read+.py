"""
read + mode file must be existed. file will not create
automatically

"""

with open("textefile01.text", "r+") as f1:
    f1.write("this is read +  mode \n")
    f1.seek(0, 2)
    f1.write("this is read +  mode \n")
    print(f1.tell())

    print("====read mode at see(0)====")

    f1.seek(0)
    print(f1.tell())

    cnt01 = f1.read()
    print(cnt01)

    f1.seek(0, 2)
    print(f1.tell())

print(f1.closed)
print("======Completed read + mode=====")