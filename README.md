# PROJ731_Map-Reduce
On fait le projet en Python

---
## Idée 0 :
Ne pas faire de map-reduce : 
Juste compter les mots en faisant une grosse boucle sur le fichier d'originie pour compter le nombre d'itérations des mots. 

---
## Idée 1 : 

Le faire juste sur 1 machine en multiprocessing :  \
1 fichier de base : et on le divise en autant de mappers que l'on a. \
On fait autant de processus que de matppers et ils comptes les mots dans le fichier. \
Les mappers font ensuite le hashage des mots avec le modulo : length % nb_reducer \
et mettes les résultats dans des fichiers \
m1_r1 \
m1_r2 \
-- \
m2_r1 \
m2_r2 \
etc. 

Une fois que tous les mappers ont finis, le manager lance les reducers.

---
## Idée 2 :
Le faire toujours sur 1 machine, mais cette fois ci, 
au lieu de faire le multiprocessing dans 1 fichier python, on va implémenter 1 manager qui lance d'autres fichiers python pour les mappers et reducers pour voir si ça change quoi que ce soit au niveaux du temps.   

--- 
# Idée 3 : Sockets

Il y a 1 Socket Server qui sert de manager.

X sockets pour les mappers.
Y sockets pour les reducers.

Le manager utilise du mutlithreading pour génrer les socket clients.

Voila les interactions avec un mapper : 

*Mapper* envoie `mapper` au **Manager**
**Manager** envoie id au *Mapper*
*Mapper* envoie `nbreducers` au **Manager**
**Manager** envoie le nombre de reducer au *Mapper*
*Mapper* envoie `text_length` au **Manager**
**Manager** envoie la longueur du texte au *Mapper*
*Mapper* envoie `text` au **Manager**
**Manager** envoie la partie du texte au *Mapper*

*Mapper* fait sa map et le shuffle

*Mapper* envoie `finished` au **Manager**
**Manager** envoie `go` au *Mapper*
*Mapper* envoie `map_size` au **Manager**
*Mapper* envoie la taille de la map au **Manager**
**Manager** envoie `ok` au *Mapper*
*Mapper* envoie toute sa map au **Manager**
**Manager** envoie `ok` au *Mapper*

--> Fin de tache du Mapper

Voila les interactions avec un reducer : 

*Reducer* envoie `reducer` au **Manager**
**Manager** envoie id au *Reducer*

**Manager** attends que les mappers soient fini

**Manager** envoie la taille des maps au *Reducers* 
*Reducer* envoie `ok` au **Manager**
**Manager** envoie les maps au *Reducer*

*Reducer* fait le reduce

--> Fin de tache du Reducer



---
## Idée 4 : reducers variables
=> Calculer le nombre de mapper et reducers necessaires
