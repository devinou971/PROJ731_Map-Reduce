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
## Idée 3 : reducers variables
=> Calculer le nombre de mapper et reducers necessaires
