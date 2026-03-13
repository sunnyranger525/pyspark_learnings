# Class Mutliple inheritance
class Parent:
    def __init__(self):
        print("====================")
    def call_parent(self):
        print("I am parent")
class Asset:
    def son_asset(self):
        print("This Assert belongs to Son")
    def daughter_asset(self):
        print("This Asset Belongs to Daughter")
class Son(Parent, Asset):
    def call_son(self):
        print("I am Son")
class Daughter(Parent, Asset):
    def call_daughter(self):
        print("I am Daughter")

son = Son()
son.call_son()
son.call_parent()
son.son_asset()
daughter = Daughter()
daughter.call_daughter()
daughter.call_parent()
daughter.daughter_asset()

class Distic:
    def __init__(self, dist):
        print("==========================")
        self.dist = dist
    def call_dist(self):
        print("This is my distic :", self.dist)
class Mandal(Distic):
    def __init__(self,distm, mandal):
        super().__init__(distm)
        self.mandal = mandal
    def call_mandal(self):
        print("This is Mandal", self.mandal)
class Village(Mandal):
    def __init__(self,distv,mandalv, village):
        super().__init__(distv, mandalv)
        self.village = village
    def call_village(self):
        print("This is Village", self.village)
class People(Village):
    def __init__(self,distp, mandalp, villagep, name):
        super().__init__(distp, mandalp, villagep)
        self.name = name
    def call_peopler(self):
        print("I am person :", self.name)
person3 = People(
    "Mahabuabab",
    "gangaram",
    "ponugondal",
    "Sammaiah"
)
person3.call_dist()
person3.call_mandal()
person3.call_village()
person3.call_peopler()