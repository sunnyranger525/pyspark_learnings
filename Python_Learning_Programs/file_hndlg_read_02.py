# using existing textfile01.text
# open file usign with open

with open("textefile01.text", "r") as f1:
    cnt01 = f1.read()
    print(cnt01)

    # check cursor location
    print(f1.tell())
    f1.seek(0)
    print(f1.tell())

    # read certain bytes
    cnt02 = f1.read(13)
    print(cnt02)

    #read first line
    f1.seek(0)
    cnt03 = f1.readline()
    print(cnt03)

    #read data  as list
    f1.seek(0)
    cnt04 = f1.readlines()
    print(cnt04)

    # read data using for loop
    print("====== using for loop====")
    for line in cnt04:
        print(line)

# check file closed or not

print(f1.closed)