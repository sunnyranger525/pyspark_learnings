# inheritance Single

class Parent:
    def __init__(self,name):
        self.name = name
    def show(self):
        print(f"I am parent, Name is {self.name}")
        print(f"relationship with myself")
class Child1(Parent):
    def child1data(self, child1name, relation):
        print(f"My name is {child1name}")
        print(f"My {relation} name is {self.name}")
parent_01 = Parent("raj")
parent_01.show()
child_01 = Child1("raju")
child_01.child1data("sam", "father")
child_01.show()