"""
Dearling with binary file

"""

with open("output.bin", "wb") as f:
    f.write(b'\x41\x42\x43')


import pickle

data = {"id": 101, "name": "Alice", "score": 90}
with open("student.pkl", "wb") as f:
    pickle.dump(data, f)
with open("student.pkl", "rb") as f:
    obj = pickle.load(f)
    print(obj)