# Analyse des alertes AVICENNE

Les alertes AVICENNE sont répertoriées dans un fichier EXCEL®, comportant plusieurs onglets nécessaires au traitement :
- Un onglet par établissement (actuellement le Centre Hospitalier de Lunéville et le CHRU de Nancy)
- La base des algorithmes, leur numéro, nom, et catégories
- La base des services, leur nom, numéro et catégorie

Le traitement des données statistique est ensuite réalisé avec le langage de programmation Python avec pour bibliothèque principale pandas. Cette bibliothèque de structuration et d’analyse de données est particulièrement utilisé pour l’analyse très rapide d’échantillon de données de taille moyenne. En effet le point fort de pandas est aussi son point faible : les traitements sont entièrement faits en mémoire vive, beaucoup plus rapide mais aussi plus limité en taille.
Actuellement le script d’analyse est séparé en 5 fichiers :
- main.py : programme principale appelant les autres
- constantes.py : permet de définir les constantes et de les modifier dans un seul fichier
- statsAlgo.py : traite les alertes par algorithmes utilisés
- statEtab.py : traite les alertes par établissement 
- statServices.py : traite les alertes par catégorie de services


![avicenne](https://user-images.githubusercontent.com/126063118/226878647-384fe9b1-f4c6-4cf0-9183-37c4cb401ab4.png)
