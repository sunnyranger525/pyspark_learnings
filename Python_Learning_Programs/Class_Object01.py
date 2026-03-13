# class Define

class Vehicle:
    """
    Creating class Vehicle, building constructor
    __init__ is special type of method which initialize the
    attributes and methods
    """
    wheels = "4 Wheels" # class attribute
    def __init__(self, type, brand, hp):
        print("====================================")
        self.type = type #instance Attribute
        self.brand = brand #instance Attribute
        self.hp = hp #instance Attribute
    def display(self): # Method
        print(f"Vehicle Type is\t : {self.type}")
        print(f"Brand is \t\t : {self.brand}")
        print(f"Capacity in hp \t : {self.hp}")
car1 = Vehicle("Car", "Kia", "60 hp")
car1.display() # calling the object when create object with vehicle
car2 = Vehicle("Tractor", "Kubota", "45 hp")
car2.display() # calling the object when create object with vehicle
print(car1.brand)
print(car1.wheels)
print(Vehicle.wheels) # access class attribute

