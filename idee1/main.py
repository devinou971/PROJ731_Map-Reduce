from time import time
import multiprocessing
import json
import os
import collections


files = ["../data/the-full-bee-movie-script.txt", "../data/test1.txt", "../data/Le-seigneur-des-anneaux-tome-1.txt", "../data/Le-seigneur-des-anneaux-tome-1_1.txt", "../data/bible.txt"]

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
    for file in os.listdir("."):
        if file.endswith(".json"):
            os.remove(file)

    start = time()
    ctx = multiprocessing.get_context("spawn")
    res_temp=[]
    nb_mapper = 3
    nb_reducer = 3
    incr = 0
    
    files_content = []

    for f in files:
        with open(f, "r", encoding="utf8") as file:
            files_content += file.readlines()

    nb_line = len(files_content)
    print(nb_line)
    
    with ctx.Pool(processes=nb_mapper) as p:
        for x in range(0, nb_mapper):
            print(nb_line/nb_mapper * x)
            files_content[int(nb_line/nb_mapper * x):int(nb_line/nb_mapper * (x+1))]
            p.apply_async(mapper, [" ".join(files_content), nb_reducer, x])
            incr += 1
        p.close()
        p.join()
        p.terminate()

    with ctx.Pool(processes=nb_reducer) as p:
        for x in range(0, nb_reducer):
            p.apply_async(reducer, [x],callback=sinchronyze)
            incr += 1
        p.close()
        p.join()
        p.terminate()
    dict_res={}
    for x in res_temp:
        dict_res.update(x)
    dict_finale=collections.OrderedDict(sorted(dict_res.items(), key=lambda x: x[1],reverse=True))

    with open(f"res.txt", "w", encoding="utf8") as res_file : 
        for word in dict_finale.items():
            res_file.write(f"{word[0]} {word[1]} \n") 

    end = time()
    print("Time taken :", end - start)