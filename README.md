# Gestion des Sinistres - Transformation de Données avec PySpark

Ce projet utilise **PySpark** pour transformer une table de sinistres liée aux contrats d'assurance, en réorganisant les informations pour faciliter leur utilisation dans des analyses ou systèmes de gestion. La transformation restructure les données de sinistres d'une structure multi-colonnes vers une table simplifiée.

![PySpark](https://img.shields.io/badge/PySpark-3.3.0-brightgreen)
![License](https://img.shields.io/badge/License-MIT-blue.svg)

## Table des Matières

- [Contexte](#contexte)
- [Installation](#installation)
- [Structure des Données](#structure-des-données)
- [Objectif](#objectif)
- [Instructions d'Exécution](#instructions-dexécution)
- [Exemple de Résultat](#exemple-de-résultat)
- [Contribuer](#contribuer)
- [Licence](#licence)
- [Contact](#contact)

## Contexte

Dans ce projet, nous travaillons avec des données de sinistres extraites de contrats d'assurance. Chaque contrat contient des informations sur plusieurs sinistres. Cependant, ces données sont initialement organisées dans un format avec plusieurs colonnes pour chaque sinistre, ce qui complique les analyses et les rapports. Ce projet vise à restructurer ces données en une table simplifiée pour une meilleure gestion et utilisation.

## Installation

1. **Cloner le dépôt pour accéder aux fichiers du projet** :

Installer PySpark : Assurez-vous d’avoir PySpark installé. Vous pouvez l’installer avec :

pip install pyspark

 ```bash
   git clone https://github.com/Mouhamadoub977/Mouhamad_Project.git
   cd Mouhamad_Project

## Structure des Données
Données d'Entrée (df_sin_in)

Les données d'entrée se présentent sous forme de fichier .parquet avec les colonnes suivantes :

POL_NumCnt : Numéro de contrat
CLA_Num_sinX_CP : Numéro de sinistre pour le sinistre X (X = 1 à 10)
CLA_DtSurv_sinX_CP : Date de survenance du sinistre X
CLA_Etat_sinX_CP : État du sinistre X
CLA_TopCorpMat_sinX_CP : Nature du sinistre X
Note : Placez le fichier d'entrée sinistre_input_test.parquet dans le même dossier que le script ou configurez le chemin d'accès dans le code.

Données de Sortie (df_sin_out)
Le DataFrame de sortie df_sin_out contient les colonnes suivantes :

POL_NumCnt : Numéro de contrat
num_sin : Numéro de sinistre
dt_sin : Date de survenance
etat_sin : État du sinistre
nature_sin : Nature du sinistre

## Objectif

L'objectif est de générer une structure de données simple et tabulaire pour chaque sinistre, facilitant ainsi les analyses et rapports.
La structure finale regroupe chaque sinistre dans une seule ligne avec les informations essentielles.

## Instructions d'Exécution
Exécuter le script de transformation :

Dans un notebook ou un environnement PySpark, chargez le script de transformation Sinistre.py (ou le code dans ce README).
Exécutez le code pour générer df_sin_out.
Vérifier le résultat : Le DataFrame df_sin_out s’affichera avec chaque sinistre représenté par une ligne unique.

Exemple de Résultat
Exemple du tableau final df_sin_out :

POL_NumCnt	num_sin	dt_sin	etat_sin	nature_sin
0000003475	0000005438074773	2023-01-01	Closed	Accident
0000003475	000000520114973	2023-01-10	Open	Theft


## Contribuer
Les contributions sont les bienvenues ! Veuillez suivre les étapes ci-dessous pour contribuer :

Fork le dépôt.
Créez une nouvelle branche (git checkout -b feature/NouvelleFonctionnalité).
Commitez vos modifications (git commit -m 'Ajout d'une nouvelle fonctionnalité').
Poussez vers la branche (git push origin feature/NouvelleFonctionnalité).
Ouvrez une Pull Request.

## Licence
Distribué sous la licence MIT. Voir LICENSE pour plus d'informations.

## Contact
Mouhamad - mouhamadoub977@gmail.com

Lien du projet : https://github.com/Mouhamadoub977/Mouhamad_Project.git
