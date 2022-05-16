#!/usr/bin/env python3.9
import pymongo
from Modules.connect import NoSQLConnect
import datetime

startChrono = datetime.datetime.now()
db = NoSQLConnect()

#---------------Création des collections-----------------
#Collection historique
try:
    db.create_collection("historique")
    db.historique.create_index("stdate")
except:
    raise
#Réinitialisation du txt
txt = open("Log/historique.txt",encoding="utf-8",mode ="w")
txt.truncate()
txt.close
#Instanciation du dictionnaires
historique = {}
#Dictionnaire temporaire
temp_historique = {}
#---------------Lecture collection adsb------------------
#Lit le document en entier
for doc in db.adsb.find({}).sort("tm",pymongo.ASCENDING):
    #Liste des stations ayant reçu le message
    sts = str(doc["st"]).split(",")
    #Icao
    icao = str(doc["icao"])
    #--------Date---------
    tm = str(doc["tm"])
    #Année
    year = int(tm[:4])
    #Mois
    month = int(tm[5:7])
    #Jour
    day = int(tm[8:10])
    #Heure
    hour= int(tm[11:13])
    #Minutes
    minute = int(tm[14:16])
#--------------------Gestion des variables--------------------
    #----------------------historique------------------------
    #Gestion des minutes
    if minute < 15:
        minute = 0
    elif minute < 30:
        minute = 15
    elif minute < 45:
        minute = 30
    elif minute <= 59:
        minute = 45
    #Date mini historique
    date_mini = datetime.datetime(year,month,day,hour,minute)
    for st in sts:
        if len(st) > 5:
            continue
        #Nom du dictionnaire historique
        name_dict_histo = str(st)+" "+str(date_mini).replace("datetime.datetime(","").replace(")","")
#----------------------------Dictionnaires---------------------------
    #----------------------historique----------------------------
        #Si la station et la date sont dans le dictionnaire historique et que l'ICAO n'y est pas
        if (name_dict_histo in historique) and (icao not in historique[name_dict_histo][st][date_mini]):
            historique[name_dict_histo][st][date_mini].append(icao)
        #Si name_dict_histo n'est pas dans le dictionnaire
        if name_dict_histo not in historique:
            temp_historique = {name_dict_histo:{st:{
                    date_mini:
                        [icao]
                }
            }}
            historique.update(temp_historique)
        
#historique        
txt = open("Log/historique.txt",encoding="utf-8",mode ="a")
txt.write(str(historique))
txt.close
#-------------------------Insertion------------------------
#-------------------------historique-----------------
for row in historique.values():
    row = str(row)
    row = row.split(":")
    st = row[0].replace("{'","").replace("'","")
    #----------------------------DATE-----------------------------------------------------
    date_mini = row[1].replace("{datetime.datetime(","").replace(")","").strip().split(",")
    #Annee
    year = int(date_mini[0])
    #Mois
    month = int(date_mini[1])
    #Jour
    day = int(date_mini[2])
    #Heure
    hour= int(date_mini[3])
    #Minutes
    minute = int(date_mini[4])
    #Intervalle de 15 minutes
    date_min = datetime.datetime(year,month,day,hour,minute)
    date_max = date_min+datetime.timedelta(minutes=15)
    #Liste ICAO
    icao = row[2].replace("}}","").replace("'","").replace("[","").replace("]","").replace(" ","").split(",")
    #Nombre d'icao
    nbicao = len(icao)
    try:
        insert_historique = db.historique.insert_one({"stdate":{"st":st,"date":{"min":date_min,"max":date_max}},"icao":icao,"nbicao":nbicao,"type":"q"})
    except:
        txt = open('Log/error_histo.txt',encoding='utf-8',mode ='a')
        text = st+":"+str(historique[name_dict_histo])
        txt.write(text+"\n")
        txt.close
        continue
print(datetime.datetime.now()-startChrono)