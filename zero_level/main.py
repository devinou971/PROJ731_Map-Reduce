from itertools import chain

import pandas as pd



files=["data/Le-seigneur-des-anneaux-tome-1_1.txt"]


for file in files:
    with open(file) as f:
        lines = f.readlines()
tab_words=[]
for x in lines:
    tab_temp=x.split(" ")
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


print(tab_res)


