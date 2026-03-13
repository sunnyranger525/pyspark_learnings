# class define
class Vehicle: # Defining class Vehicle
    print("==========Base Class=======")
car = Vehicle() # creating object based on class
car.name = "suziki" # Defining name ,it not simple vairable,its attribute
tractor = Vehicle()
tractor.name = "Kubota"
print(car.name)
print(tractor.name)

# class define
# class People:
#     def __init__(self, name, gender):
#         print("Creating object")
#         self.name = name
#         self.gender = gender
#         self.followers = 0
#
# person1 = People("Sunny", "male")
# print(person1.name)
# print(person1.gender)
# print(person1.followers)
# person2 = People("Sager", "Male")
# print(person2.name)
# print(person2.gender)

# class define
class People:
    followers = 0
    def __init__(self, name, gender):
        print("=========================")
        print("Creating object")
        self.name = name
        self.gender = gender
    def show(self, profession):
        print("Hello", self.name)
        print(f"i am doing {profession}")
    def update_follower(self, name):
        print("name of last follower is ",name)
        self.followers +=5

person1 = People("Sunny", "male")
person1.show("Data Engineering")
print(person1.gender)
print(person1.followers)
person1.update_follower("sunny")
print(person1.followers)
person2 = People("Sager", "Male")
person2.show("Farming at home")
print(person2.gender)
person2.update_follower("Susmi")
print(person2.followers)

# defining the Car Class
class Car:
    """
    it is Car class, wheels is class attribute
    shared with all object
    """
    wheels = 4
    def __init__(self, name, brand):
        print("=============New Object===========")
        self.name = name
        self.brand = brand
    def display(self, city):
        print(f"Available at showromm {city}")
car1 = Car("sonet", "Kia")
print(car1.name)
print(car1.brand)
print(car1.wheels)
car1.display("Ponugondla")
car2 = Car("Creta", "Hyndai")
print(car2.name)
print(car2.brand)
print(car2.wheels)
car2.display("Gangaram")
print("====Overridding Class Attribute ==")
Car.wheels = 6
print(Car.wheels)
car3 = Car("nextgen", "TATA")
print(car3.name)
print(car3.wheels)

