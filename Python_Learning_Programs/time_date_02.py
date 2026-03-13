"""
Module datetime

date → Deals with year, month, day (calendar date only).

time → Deals with hour, minute, second, microsecond (time of day only).

datetime → Combines both date and time into one object.

timedelta → Represents the difference between two dates or times.

tzinfo → Handles timezone information.

"""
from datetime import date,time

d = date.today()
print("Today date :", d)
print("ISO Date Format :", d.isoformat())
print("ISO Weekday :",d.isoweekday())
print("Weekday  :", d.weekday())
print("Custom Format :", d.strftime("%Y, %B %d, %A"))
print("Year:", d.year)
print("Month :", d.month)
print("Day :", d.day)
print("year week number :", d.isocalendar()[1])
print("Tuple:", d.timetuple())
print("To Ordinal :", d.toordinal())
print("Today Day Name :", d.strftime("%A"))

# Dealing with Time

t = time(5, 30, 20, 12356)

print(" ")
print("==========Dealin with time=========")
print("Total time :", t)
print("Hour :",t.hour )
print("Minute :", t.minute)
print("Seconds :", t.second)
print("Micro seconds:", t.microsecond)

print("===========Time Delta===========")
from datetime import timedelta, datetime, time

td1 = timedelta(days = 5, hours=5, minutes=30,seconds=30)
td2 = timedelta(days =3, hours=3, minutes=20, seconds=40)
print("Days of  td1 :", td1.days)
print("Days of td2 :", td2.days)
print("Time 1 : ", td1)
print("Time 2 :", td2)
print("Total :", td1 + td2)
print("time diff :", td1 - td2)
print("Seconds : ", td1.seconds)
print("Hours :", td1.seconds // 3600)
print("Minuts : ", (td1.seconds % 3600) // 60)
print("Micro Seconds :", td1.microseconds)

print("=========from to time to timestmp===")

t3 = datetime.now()
print("time :", t3)

dt = datetime.fromtimestamp(t3)
print(dt)







