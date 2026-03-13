# Parent class or base class or
class Human:
    def eat(self):
        print("I can eat")
    def work(self):
        print("I can work")
class Person1(Human):
    def work(self):
        super().work()
        print("I can code")

person01 = Person1()
person01.eat()
person01.work()

class Employee:
    def __init__(self, id, name):
        print("==============================")
        self.id = id
        self.name = name
    def show(self):
        print(f"empoyee Id is  \t\t:{self.id}")
        print(f"employee name is \t: {self.name}")
class Department(Employee):
    def show(self, dept):
        super().show()
        print(f"Department Id is \t: {dept}")
emp1 = Department("70229", "Sam")
emp1.show("Design")
emp2 = Department("70230", "Manohar")
emp2.show("FEA")
emp3 = Employee("70231", "Prashanth")
emp3.show()

class Cyient:
    def __init__(self, id, name):
        print("=====================================")
        self.id = id
        self.name = name
    def display(self):
        print(f"Cyient id : {self.id}")
        print(f"Cyient employee name : {self.name}")
class Tower1(Cyient):
    def __init__(self, dept, id1, name1):
        super().__init__(id1, name1)
        self.dept = dept
    def getdata(self):
        super().display()
        print(f"Employee Department is :{self.dept}")
cyient1 = Tower1("DataEngg", "70229", "sam")
cyient1.getdata()
cyient1.display()