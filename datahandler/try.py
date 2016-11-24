
from datetime import datetime
import datetime as dt
import time
'''
dt_obj = datetime(2008, 2, 10, 17, 53, 59)
date_str = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
print date_str

str = "20150218224500"

dt = list(str)
year = int("".join(dt[0:4]))
print year

month = int("".join(dt[4:6]))
print month

day = int("".join(dt[6:8]))
print day

hour = int("".join(dt[8:10]))
print hour

minute = int("".join(dt[10:12]))
print minute

sec = int("".join(dt[12:14]))
print sec


str = '2016-02-13 1:35:30'
print(datetime.now().strftime("%Y%m%d%H%M%S"))

str2 = '20161013215606'
date = datetime.strptime(str2, "%Y%m%d%H%M%S")
print date
end_date = date - dt.timedelta(days=10)
print end_date


print( time.mktime(time.strptime(datetime.strptime(str2, "%Y%m%d%H%M%S").strftime("%Y%m%d%H%M%S"),"%Y%m%d%H%M%S")))

timestamp = 1226527167.595983
time_tuple = time.localtime(timestamp)
print repr(time_tuple)

print(time.mktime(time.strptime(datetime.now().strftime("%Y%m%d%H%M%S"),"%Y%m%d%H%M%S")))
'''
'''
print(int("00ff5d",16))
print(hex(65373))


print(int("ffff00",16))
print(str(hex(16776960)).replace("0x",""))


a1 = 16711680
b1 = 16776960
x = -60 + 0.3
val = (((b1-a1)*(x-(-101)))/(1-(-100))) + a1

print(str(hex(int(val))))

'''
'''
from calendar import monthrange

dt = datetime.now()
dt = dt.replace(month=1)

last_month = dt.month-1
last_year = dt.year
if last_month == 0:
  last_month = 12
  last_year = last_year - 1


print last_month
print last_year

last_month_days = monthrange(last_year, last_month)[1]

time_tuple = (last_year, last_month, last_month_days, 0, 0, 0)
dt_lat_month_end = datetime(*(last_year, last_month, last_month_days, 0, 0, 0))
print dt_lat_month_end
'''

color_range_neg = ["#ff0000","#ff3200","#ff4800","#ff5d00","#ff8c00","#ff9d00","#ffbb00","#ffc300","#ffd400","#ffe500"]
print color_range_neg[-8]

str2 = '20161001000000'
date = datetime.strptime(str2, "%Y%m%d%H%M%S")
print date
print date - dt.timedelta(days=1)


