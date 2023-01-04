from itertools import chain
import re
import pandas as pd



files=["data/text_1.txt"]


for file in files:
    with open(file,encoding="utf8") as f:
        lines = f.readlines()
tab_words=[]
for x in lines:
    x.replace(",", " ")
    x.replace(".", " ")
    x.replace("\\n"," ")
    x.replace("\n"," ")
    x.replace(":", " ")
    x.replace("!"," ")
    tab_tamp=x.split(" ")
    for y in tab_tamp :
        if(y!="\\n" and y!=":\n"):
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
    if(i%10000==0):
        print(i)
    if (x not in chain(*tab_res)):
        tab_res.append([x,tab_words.count(x)])
    i+=1

print(tab_res)
tab_res.sort(key=lambda x:x[1],reverse=True)
print(tab_res)


