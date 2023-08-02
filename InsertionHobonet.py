'''
@Author: Élaine Soucy
@Version : Juillet 2023

Ce script permet de faire l'insertion des données des .csv exportés de l'interface HOBOlink dans la base de données.
Si un ou des fichiers .csv se trouvent dans le dossier D:\SFTPRoot\CETAB_Hobonet, alors le script s'occupera de 
lire les fichiers et d'insérer les données dans la base de données. Après la lecture et l'insertion des données, 
les fichiers seront supprimés du dossier D:\SFTPRoot\CETAB_Hobonet.

Il est possible de rouler directement ce fichier pour insérer immédiatement les données dans la base de données si 
des fichiers de données sont disponibles dans le dossier D:\SFTPRoot\CETAB_Hobonet.
Autrement, une tâche planifiée s'exécutant à toutes les nuits s'occupe d'exécuter ce script.

S'il y a un problème lors de l'exécution de ce script, un fichier log est créé. Ce fichier porte le nom 
errorInsertionHobonet.log et se trouve dans le même dossier que ce fichier. 

Considérations importantes : Les fichiers de données exportés de l'interface HOBOlink sont dans un temps UTC+0.
Ainsi, lorsqu'on fait la procédure pour l'ajout de données Hobonet (voir page OneNote à cet effet), si on veut 
changer l'intervalle de temps lors de l'exportation des fichiers de données, il est important de prendre en 
considération qu'il y a un décalage de quatre heures entre l'heure de Montréal et le fuseau UTC. 

Ce script prend en compte que les données de temps sont dans un fuseau UTC+0. 
'''  

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import csv
from os import listdir, remove
import re
import logging
import datetime as dt
from datetime import timezone
import sys


if __name__ == '__main__':
    try:
        client = None
        # Fichier qui contient le dossier contenant tous les fichiers .csv d'Hobonet
        path_to_data = 'D:\SFTPRoot\CETAB_Hobonet'
                   
        # On obtient la liste de tous les fichiers .csv disponible dans le répertoire
        csv_files_list = [csv_file for csv_file in listdir(path_to_data) if csv_file.endswith('.csv')]
        
        # Si on a des fichiers de disponibles pour insertion
        if len(csv_files_list) > 0:
            # Le nom du bucket pour les données d'Hobonet dans la base de données InfluxDB est appelé Hobonet
            BUCKET_NAME = 'Hobonet'
            
            # On se connecte à la base de données en utilisant les informations de connexion disponibles dans le config.ini
            client = InfluxDBClient.from_config_file("config.ini")
            org_id = client.org
            api_write = client.write_api(write_options=SYNCHRONOUS)
            
            log_filename = 'errorInsertionHobonet.log'
            measurement = 'metrics'
            
            # Lignes de capture pour REGEX
            pattern1 = r'\((.*?)\)' #--> Capture tout ce qu'il y a dans la parenthèse
            pattern2 = r' RX3000 Serre froide '

            # Trouver les buckets présents dans la base de donnée
            buckets_api = client.buckets_api()
            buckets = buckets_api.find_buckets().buckets

            buckets_name_list = []
            for b in buckets:
                buckets_name_list.append(b.name)

            """
            Créer le bucket si pas déjà existant
            """
            if not BUCKET_NAME in buckets_name_list:
                buckets_api.create_bucket(bucket_name=BUCKET_NAME, org_id=org_id)
            
            # Pour chaque fichier dans le dossier
            for csv_file in csv_files_list:
                csv_reader = None
                
                # Ouverture du fichier en mode lecture seule
                namefile = path_to_data + "\\" + csv_file
                with open(namefile, 'r') as f:   
                    csv_reader = list(csv.DictReader(f))
                
                # Pour chaque ligne de données dans le fichier csv en question
                for row in csv_reader:
                    date_to_array = None
                    unix_time = None
                    field_name = None
                    # Le field est le nom de la colonne, la value est la valeur de la colonne
                    for field, value in row.items():
                        if field is not None:

                            if value != '' and value is not None:
                                value = value.lstrip().rstrip()
                                
                                if re.search(pattern2, field) is not None:
                                    # Manipulations permettant d'aller chercher le nom du tag et du field pour l'insertion dans InfluxDB
                                    field = re.sub(pattern1, '', field)
                                    field = re.sub(pattern2, '', field)
                                    split_field_virgule = re.split(",", field)
                                    type_capteur = split_field_virgule[0].lstrip().rstrip()
                                    type_capteur = re.sub(" ", "-", type_capteur)
                                    
                                    split_serre_nom = re.split(' ', split_field_virgule[-1])
                                    tag_serre = split_serre_nom[0].lstrip().rstrip()
                                    nom_capteur = split_serre_nom[-1].lstrip().rstrip()
                                    
                                    field_name = nom_capteur + "_" + type_capteur
                                    
                                    # Le système HOBOlink est un système anglais, donc les milliers ont des virgules.
                                    # On veut enlever la virgule (1,031 --> 1031) 
                                    if re.search(",", value) is not None:
                                        value = re.sub(",", "", value)
                                
                                    if unix_time is None:
                                        raise Exception(f"Problème avec unix time {csv_file}. Le programme se termine ... ")
                                    
                                    dict_structure = {
                                        "measurement": measurement,
                                        "tags": {"Serre": tag_serre},
                                        "fields": {field_name : float(value)},
                                        "time": unix_time
                                    }
                                    
                                    point = Point.from_dict(dict_structure, WritePrecision.S)
                                    # Insertion de la donnée dans la base de données
                                    api_write.write(bucket=BUCKET_NAME, record=point) 
                                
                                elif field == 'Date':
                                    date_to_array = re.split("-", value)  
                                
                                elif field == "Time" :
                                    
                                    if date_to_array is None:
                                        raise Exception(f"Problème de conversion de temps dans le fichier {csv_file}. Le programme se termine ... ")
                                    
                                    temps = re.split(":", value)
                                    heure = int(temps[0])
                                    minute = int(temps[1])
                                    seconde = int(temps[2])
                                    
                                    jour = int(date_to_array[2])
                                    mois = int(date_to_array[1])
                                    annee = int('20' + date_to_array[0])
                                    
                                    # Convertir le temps en type date en indiquant au système d'utiliser le timezone UTC
                                    local_time = dt.datetime(annee, mois, jour, heure, minute, seconde).replace(tzinfo=timezone.utc)
                                    ## Convertir le temps en unix time (unix time en seconde)
                                    unix_time = int(local_time.timestamp())
                # On enlève le fichier afin de libérer l'espace
                remove(namefile)
                    
    except Exception as e:
        time = dt.datetime.now()
        logging.basicConfig(filename=log_filename, filemode='a', format='%(levelname)s - %(message)s')
        exc_type, exc_obj, exc_tb = sys.exc_info()
        logging.error(f"{time} - ligne {exc_tb.tb_lineno} - Type d'exception : {exc_type} \n{repr(e)}\n********************")
    # Ce bloc s'exécute peu importe qu'il y ait eu une exception ou non
    finally:
        if len(csv_files_list) > 0:
            client.close()
    
    

    
    