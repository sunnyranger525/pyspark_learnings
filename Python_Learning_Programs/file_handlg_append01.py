# file handling append mode
from asyncore import write

with open("textfile02.text", "a") as f1:
    f1.write("=======this is append data=====\n")
    f1.write("you are working with append data \n ")
    f1.write("working with write() method\n")

    multiline = [
        "Hello, samsu \n",
        "what are you doing \n",
        "writelien method() \n"
    ]
    f1.writelines(multiline)

    for line in multiline:
        f1.write(line)
    for line in multiline:
        print(line, file = f1)
print(f1.closed)
print("=====Appended data ======")



