# PROJ731_Map-Reduce
On fait le projet en Python

Afin d'étudier le Map-Reduce, nous en avons faits plusieurs versions différentes.

Dans chaque version ( excepter la 1ère ) , vous pouvez changer à votre guise le nombre de mappeur et de reducer.

---
## Idée 0 :
Ne pas faire de map-reduce : 
Juste compter les mots en faisant une grosse boucle sur le fichier d'origine pour compter le nombre d'itérations des mots. 

La version la plus simple.

---
## Idée 1 : 

Le faire juste sur une machine en multiprocessing :  \
1 fichier de base : et on le divise en autant de mappers que l'on a. \
On fait autant de processus que de mappers et ils comptes les mots dans le fichier. \
Les mappers font ensuite le hashage des mots avec le modulo : length(mot) % nb_reducer \
et mettes les résultats dans des fichiers json \
m1_r1 \
m1_r2 \
-- \
m2_r1 \
m2_r2 \
etc. 

Une fois que tous les mappers ont finis, le manager lance les reducers.

Ils vont lire les fichiers qui leurs correspondent  et mettre le résultat dans une liste.

Enfin,une fois que tous les reducers ont finis, on rassemble les dictionnaire ,on trie par ordre décroissant et on écrit le résultat dans un fichier texte.

---
## Idée 2 :
Le faire toujours sur une machine, mais cette fois ci, 
au lieu de faire le multiprocessing , on va faire du multithreading. Néanmoins, cela n'est pas vraiment possible en python. \
En fait, un processus Python ne peut pas réellement exécuter des threads en parallèle, mais il peut les exécuter "simultanément" grâce au changement de contexte pendant les opérations liées aux E/S. \
En python,le multiprocessing utilise le parallélisme, le multithreading utilise la concurrence.

Une chose intéressante à noter est que le multi-threading est plus rapide que le multi-processing, ce qui n'est normalement pas logique en python. Il faudrait en chercher la cause.

--- 
# Idée 3 : Sockets

Il y a 1 Socket Server qui sert de manager.

X sockets pour les mappers.
Y sockets pour les reducers.

Le manager utilise du mutlithreading pour gérer les socket clients.

Voila les interactions avec un mapper : 

*Mapper* envoie `mapper` au **Manager**  \
**Manager** envoie id au *Mapper*  \
*Mapper* envoie `nbreducers` au **Manager** \
**Manager** envoie le nombre de reducer au *Mapper* \
*Mapper* envoie `text_length` au **Manager** \
**Manager** envoie la longueur du texte au *Mapper* \
*Mapper* envoie `text` au **Manager** \
**Manager** envoie la partie du texte au *Mapper* 

*Mapper* fait sa map et le shuffle 

*Mapper* envoie `finished` au **Manager** \
**Manager** envoie `go` au *Mapper* \
*Mapper* envoie `map_size` au **Manager** \
*Mapper* envoie la taille de la map au **Manager** \
**Manager** envoie `ok` au *Mapper* \
*Mapper* envoie toute sa map au **Manager** \
**Manager** envoie `ok` au *Mapper* 

--> Fin de tache du Mapper

Voila les interactions avec un reducer : 

*Reducer* envoie `reducer` au **Manager** \
**Manager** envoie id au *Reducer* 

**Manager** attends que les mappers soient fini 

**Manager** envoie la taille des maps au *Reducers* \
*Reducer* envoie `ok` au **Manager** \
**Manager** envoie les maps au *Reducer* 

*Reducer* fait le reduce

*Reducer* envoie la taille du résultat au **Manager**
**Manager** envoie `ok` au *Reducer*
*Reducer* envoie tout le résultat au **Manager** 

--> Fin de tache du Reducer


