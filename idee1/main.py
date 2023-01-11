from time import time
import multiprocessing
import json
import os
import collections
start = time()

files = ["../data/combat_zip/mon_combat_gros.txt"]

def mapper(file_content, nb_reducer, mapper_id):
    file_content = file_content.replace(",", " ")
    file_content = file_content.replace(".", " ")
    file_content = file_content.replace("\\n"," ")
    file_content = file_content.replace("\n"," ")
    file_content = file_content.replace("\t"," ")
    file_content = file_content.replace("\\t"," ")
    file_content = file_content.replace("!"," ")
    file_content = file_content.lower()
    result = [{} for i in range(nb_reducer)]
    for word in file_content.split(" "):
        reducer_id = len(word) % nb_reducer
        if word in result[reducer_id]:
            result[reducer_id][word] += 1
        else : 
            result[reducer_id][word] = 1
    
    for i in range(nb_reducer):
       
        with open(f"m{mapper_id}_r{i}.json", "w", encoding="utf8") as res_file : 
            res_file.write(json.dumps(result[i])) 




def reducer(id_reducer):
    file_names = []
    for file in os.listdir("."):
        if file.endswith(f"_r{id_reducer}.json"):
            file_names.append(file)
    contents = []
    for file_name in file_names:
        with open(file_name, "r", encoding="utf8") as file:
            contents.append(json.load(file))
    result = {}
    for c in contents:
        for word in c.keys():
            if word in result:
                result[word] += c[word]
            else:
                result[word] = c[word]
    
    return result

def sinchronyze(results):
    res_temp.append(results)

if __name__ == "__main__":
    #On récupere le nomnbre de coeur de l'appareil
    ctx = multiprocessing.get_context("spawn")
    res_temp=[]
    nb_mapper = 4
    nb_reducer = 3
    incr = 0
    
    files_content = []

    for f in files:
        with open(f, "r", encoding="ISO-8859-1") as file:
            files_content += file.readlines()

    nb_line = len(files_content)

    # On cree une pool de processus qui va les creer
    with ctx.Pool(processes=nb_mapper) as p:
        for x in range(0, nb_mapper):

            files_content[int(nb_line/nb_mapper * x):int(nb_line/nb_mapper * (x+1))]
            #Chaque processus va appeller la fonction mapper en async
            p.apply_async(mapper, [" ".join(files_content), nb_reducer, x])
            incr += 1
        #On attend la fin de tous les processus et on les ferme
        p.close()
        p.join()
        p.terminate()

    #On refait la meme chose pour les reducer
    with ctx.Pool(processes=nb_reducer) as p:
        for x in range(0, nb_reducer):
            p.apply_async(reducer, [x],callback=sinchronyze)
            incr += 1
        p.close()
        p.join()
        p.terminate()

    #On rassemble les dico des reducers en un seul et on le trie
    dict_res={}
    for x in res_temp:
        dict_res.update(x)
    dict_finale=collections.OrderedDict(sorted(dict_res.items(), key=lambda x: x[1],reverse=True))

    with open(f"res.txt", "w", encoding="utf8") as res_file : 
        for word in dict_finale.items():
            res_file.write(f"{word[0]} {word[1]} \n") 

    end = time()
    print("Time taken :", end - start)