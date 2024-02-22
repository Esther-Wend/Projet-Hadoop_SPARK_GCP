# Agrégations sur les transactions enregistrées par une banque



Créer un script Spark sur un échantillon de données

L'objectif de cette première étape est de réaliser un script Spark qui va permettre de décrire pas-à-pas les différentes étapes pour obtenir la table de sortie en se basant sur les contraintes de cette dernière (potentiellement dans un Jupyter Notebook ou avec un Workspace Spark Java). Dans cette première tâche, on n'utilisera qu'un échantillon des données pour éviter d'alourdir les calculs.

Paramétrer le script Spark et écrire la table de sortie vers un système cible

En ayant pris soin de stocker les données sur un système de stockage d'objets (AWS S3, GCS ou HDFS), le script doit être adapté afin de pouvoir prendre en compte les paramètres DESTINATION, DATE_START et DATE_END lors du lancement du job Spark. Le script doit également être adapté pour enregistrer la table de sortie au format CSV au chemin DESTINATION spécifié en paramètre.

Exécuter le job Spark en conditions réelles

En lançant un cluster Hadoop dans le Cloud, tester le job Spark en l'exécutant avec DATE_START et DATE_END en ne prenant que quelques jours d'historique.
