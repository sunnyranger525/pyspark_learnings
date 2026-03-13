# file handling with write mode

# using 'with open()

with open("textwrite01.text", "w") as f1:
    # method write() to write content in file
    # do not miss to write to end of file \n
    f1.write("Hello Susmith, Very Good Morning\n")
    f1.write("you are my wife and love \n")

    multilines = [
        "Hey , Susmitha \n",
        "you are my darling \n",
        "How are you doing my love\n"
    ]
    # method writlines() to write content
    f1.writelines(multilines)

    # method print(" ", file = obj) to write
    print("you are a such a good girl", file = f1)
    print(f1.closed)
print(f1.closed)
print("=====Write mode completed=====")
