"""
dealign with datetime module

module datetime ,

"""
from datetime import datetime, date, time, timedelta
from time import strftime

now = datetime.now()
print("Current time is :", now, "\v")

today = date.today()
print("Today date is :", today)

# date is class, created objec date_01 or instance
date_01 = date(2025, 9, 20)

# converting into isoformate date
print("ISO Date",date_01.isoformat())
print("Weekday number", date_01.isoweekday())
print("Day name", date_01.isoweekday())
print("Day name", date_01.weekday())

# extract date, year, month , day
print("===========Extracting Year, Month, Day ========")
print("Year", date_01.year)
print("Month", date_01.month)
print("Day", date_01.day)

# custome date format
d = date(2025, 8, 10)
print("Custom format :",d.strftime("%A, %d %B %Y"))
print("Cust, Format :", strftime("%Y %B %d %A"))

print("Format : ", d.isoformat())
print("Week day Iso:", d.isoweekday())
print("Week Day : ", d.weekday())
print("Year :", d.year)
print("Month :", d.month)
print("Day :", d.day)
print("Custom Format :", d.strftime("%Y %B %d %A"))
print("is calender :", d.isocalendar())
print("Week Numebr", d.isocalendar()[1])
print("to Ordinal:", d.toordinal())
print("Tuple :", d.timetuple())





