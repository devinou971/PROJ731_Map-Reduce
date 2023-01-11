import datetime
import random
from time import time
import multiprocessing
import json
import os
import collections

from queue import Queue
from threading import Thread

"""
Il y a environ 2 220 000

"""


files = [ "../data/combat_zip/mon_combat_gros.txt"]


def mapper():
    # Chaque thread va recuperer un element de la Queue
    data=q.get()

    file_content=data[0]
    nb_reducer=data[1]
    mapper_id=data[2]

    file_content = file_content.replace(",", " ")
    file_content = file_content.replace(".", " ")
    file_content = file_content.replace("\\n", " ")
    file_content = file_content.replace("\n", " ")
    file_content = file_content.replace("\t", " ")
    file_content = file_content.replace("\\t", " ")
    file_content = file_content.replace("!", " ")
    file_content = file_content.lower()
    result = [{} for i in range(nb_reducer)]
    possibility_reducer=[x for x in range(nb_reducer)]
    #rand_int = random.randint(2, nb_reducer)


    for word in file_content.split(" "):
        #reducer_id = len(word)*rand_int % nb_reducer
        """var_temp=''.join(str(ord(c)) for c in word)
        if(var_temp==''):
            reducer_id=nb_reducer-1
        else:
            reducer_id= int(var_temp) % nb_reducer"""
        reducer_id=len(word)%nb_reducer
   
        #reducer_id=ord(word[0])%nb_reducer
        """if(reducer_id>nb_reducer):
            reducer_id=nb_reducer"""
        #reducer_id=random.randint(0,nb_reducer-1)
        #random.shuffle(possibility_reducer)
        #reducer_id=possibility_reducer[0]
        """reducer_id=int(bin(ord(word[0]))[2:])
        reducer_id=str(reducer_id).count("1")
        reducer_id=reducer_id%nb_reducer"""



        if word in result[reducer_id]:
            result[reducer_id][word] += 1
        else:
            result[reducer_id][word] = 1

    for i in range(nb_reducer):
        with open(f"mappers/m{mapper_id}_r{i}.json", "w", encoding="utf8") as res_file:
            res_file.write(json.dumps(result[i]))

    #On dit a la Queue que le thread est terminé
    q.task_done()


def reducer():
    #Meme fonctionnement que pour le mapper
    id_reducer=q.get()

    file_names = []
    for file in os.listdir("mappers/"):
        if file.endswith(f"_r{id_reducer}.json"):
            file_names.append(file)

    contents = []
    for file_name in file_names:
        with open("mappers/"+file_name, "r", encoding="utf8") as file:
            contents.append(json.load(file))
    result = {}
    for c in contents:
        for word in c.keys():
            if word in result:
                result[word] += c[word]
            else:
                result[word] = c[word]
    """
    #with open(f"reducers/r_{id_reducer}.json", "w", encoding="utf8") as res_file:
        res_file.write(json.dumps(result))
    """
    res_temp.append(result)
    q.task_done()



if __name__ == "__main__":
    file_names = []
    for file in os.listdir("reducers/"):
            os.remove("reducers/"+file)
    for file in os.listdir("mappers/"):
        os.remove("mappers/" + file)
    star_time=datetime.datetime.now()
    # On définie nos parametres
    NUM_THREADS = 10
    # On va utiliser le module Queue pour faire passer des données vers les threads
    q = Queue()
    q2=Queue()
    nb_mapper =10
    nb_reducer = 10
    incr = 0

    files_content = []
    res_temp=[]

    for f in files:
        with open(f, "r",  encoding="ISO-8859-1") as file:
            files_content += file.readlines()

    nb_line = len(files_content)

    # On met nos données dans la Queue
    for x in range(NUM_THREADS) :
        q.put([" ".join(files_content[int(nb_line/nb_mapper * x):int(nb_line/nb_mapper * (x+1))]),nb_reducer,x])

    #On cree les threads
    for t in range(NUM_THREADS):
        worker = Thread(target=mapper)
        worker.daemon = True
        worker.start()


    #On attend que tous les threads soit terminé
    q.join()

    #On fait la meme chose ici
    for x in range(NUM_THREADS) :
        q.put(x)
    for t in range(NUM_THREADS):
        worker = Thread(target=reducer)
        worker.daemon = True
        worker.start()


    q.join()




    end_time=datetime.datetime.now()
    file_names = []
    """for file in os.listdir("reducers/"):
            file_names.append(file)"""

    """contents = []
    for file_name in file_names:
        with open("reducers/"+file_name, "r", encoding="utf8") as file:
            contents.append(json.load(file))"""

    #On fait la meme chose que pour le multiprocessing
    dict_res={}

    for x in res_temp:
        dict_res.update(x)

    dict_finale = collections.OrderedDict(sorted(dict_res.items(), key=lambda x: x[1], reverse=True))

    with open(f"res.txt", "w", encoding="utf8") as res_file:
        for word in dict_finale.items():
            res_file.write(f"{word[0]} {word[1]} \n")

    print(end_time-star_time)
