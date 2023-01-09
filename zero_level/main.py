import datetime
from itertools import chain

import pandas as pd



files=["data/Le-seigneur-des-anneaux-tome-1_1.txt","data/Le-seigneur-des-anneaux-tome-1.txt"]

str_long=[]
for file in files:
    with open(file) as f:
        lines = f.readlines()
        str_long.append(lines)
tab_words=[]

start_time=datetime.datetime.now()
for x in str_long:
    for k in x:
        tab_temp=k.split(" ")
        for y in tab_temp:
            if y !="," and y!="\n"and y!="." and y!="":
                tab_words.append(y.lower())
print(len (tab_words))

tab_res=[]
i=0
"""
for x in tab_words:
    if (i%1000==0):
        print(i)
    if x not in tab_res:
        tab_res.append([x,tab_words.count(x)])
    i+=1
"""

for x in tab_words:
    if (x not in chain(*tab_res)):
        tab_res.append([x,tab_words.count(x)])

end_time=datetime.datetime.now()

print(end_time-start_time)



