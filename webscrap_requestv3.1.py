#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 30 14:21:55 2019

@author: Tarik, Sondra & Marceline
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os

from sqlalchemy import create_engine
from sqlalchemy.sql import text
import argparse
#~ import creds
import os,configparser # credentials: 
import sys

start_time = time.time()

#IMPORTANT!!!
#Pour lancer le programme faire le commande dans le terminale 
#Ajouter le nom de programme (le fichier.py) plus le deux arguments qui definir la region et le mot clé. Etre sur que cette commande est executer dans le directory de fichier.py
#example: nom de programme nom de fichier arg1 arg2 ...... python3 webscrap_requestv3.py 24R sql......(24R) pour la region EN MAJUSCULE et le mot clé (ex:sql) en miniscule. Si plusieurs mot clés separer avec une virgule.

    



#Par convention, le 1er paramètre est la région, le 2me le mot clef

#try:
region=sys.argv[1]
keyword=sys.argv[2]
#except:
#    region='24R'
#    keyword='data'

print("Région: "+region+", kwd: "+keyword)



#launch url
url = 'https://candidat.pole-emploi.fr/offres/recherche?lieux='+region+'&motsCles='+keyword+'&offresPartenaires=true&range=0-9&rayon=10&tri=0'
base_url="https://candidat.pole-emploi.fr"

#Critères : données + centre val de loire
req = requests.get(url)
soup = BeautifulSoup(req.text, "lxml")

#Nombre d'offres
titre = soup.find('h1', class_ ='title')
nb_offres = int(titre.next_element.replace('\n', '').replace(' offres', ''))
print("il ya ",nb_offres,"Offres pour votre recherche")


if (nb_offres>150):
    nbpage= (nb_offres//150)+1
    url2=150
else:
    url2=nb_offres
    nbpage=1
url1=0

#Creation de liste
NumOffre=[]
title=[]
Publication=[]
Experiences=[]
Localisation=[]
listeallUrl=[]
Description=[]
Contrat=[]
Horaires=[]
Salaire=[]
Savoirsfaire=[]
Savoir_etre=[]
Formation=[]
Qualifications=[]
Secteur=[]
Entreprise=[]
Partenaire=[]
Datefirst=[]
Datelast=[]

#Connexion et implementation BDD




config = configparser.ConfigParser()
config.read_file(open(os.path.expanduser("/home/perrot/.datalab.cnf")))
print(config.sections())



DB='BDD_Sondra_PE?charset=utf8' #.
mySQLengine = create_engine("mysql://%s:%s@%s/%s" % (config['myBDD']['user'], config['myBDD']['password'], config['myBDD']['host'], DB))
print(mySQLengine)

    


                    

mySQLengine.execute("""
CREATE TABLE IF NOT EXISTS `BDD_Tarik:Web_scraping`.`POLE_EMPLOI` (
  
  NumOffre VARCHAR(250) NOT NULL,
  title VARCHAR(261) NULL DEFAULT NULL,
  Localisation VARCHAR(250) NULL DEFAULT NULL,
  Experiences VARCHAR(250) NULL DEFAULT NULL,
  Date_publication VARCHAR(250) NULL DEFAULT NULL,
  Description VARCHAR(6000) NULL DEFAULT NULL,
  Contrat VARCHAR(250) NULL DEFAULT NULL,
  Horaires_Contrat VARCHAR(250) NULL DEFAULT NULL,
  Salaire VARCHAR(250) NULL DEFAULT NULL,
  Savoirsfaire VARCHAR(3000) NULL DEFAULT NULL,
  Savoir_etre VARCHAR(1000) NULL DEFAULT NULL,
  Formation VARCHAR(250) NULL DEFAULT NULL,
  Qualifications VARCHAR(250) NULL DEFAULT NULL,
  Secteur VARCHAR(250) NULL DEFAULT NULL,
  Entreprise VARCHAR(250) NULL DEFAULT NULL,
  Partenaire VARCHAR(250) NULL DEFAULT NULL,
  Datefirst VARCHAR(10) ,
  Datelast VARCHAR(10),
 PRIMARY KEY (`NumOffre`))
ENGINE = InnoDB
AUTO_INCREMENT = 0
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_bin;
""")



print(NumOffre)
  

for i in range(nbpage):


    print("------------",i,"---------") 
    
    url='https://candidat.pole-emploi.fr/offres/recherche?lieux='+region+'&motsCles='+keyword+'&offresPartenaires=true&range='+str(url1)+'-'+str(url2)+'&rayon=10&tri=0'
    uc=nb_offres-url2
    
    if (uc>150):
        url1+=150
        if (url1==url2):
            url1+=1
        url2+=150
    else:
        url1=url2
        url2=nb_offres
        
    req = requests.get(url)
    soup = BeautifulSoup(req.text, "lxml")
    
    for liens in soup.find_all('h2', class_ ='t4 media-heading'):
        allUrl=(liens.find('a', class_ ='btn-reset')['href'])
        listeallUrl.append(base_url+allUrl)
        offreUrl=base_url+allUrl
        print(base_url+allUrl)
        
        
            
        requete=requests.get(offreUrl)
        soup[i] = BeautifulSoup(requete.text, "lxml")
        
        #print(soup[i])
        #try:
        #Ajout des numéros des offres
        try:
            for num_offre in soup[i].find('span', {'itemprop' : 'value'}):
                Offre= num_offre
                #print(num_offre)
                NumOffre.append(num_offre) 
        except:
            
            print("L'offre n'est plus disponible")
           
        
        #Ajout Titre des annonces
        try:
            for titre in soup[i].find_all('h1', {'itemprop' : 'title'}):
                Titre=titre.text.replace('\n', '')
                #print(Titre)
                title.append(Titre)
        except:
            
            print("L'offre n'est plus disponible")

        try:
            for date in soup[i].find_all('span', {'itemprop' : 'datePosted'}):
                Date=date.next_element.replace('\n', '')
                #print(Date)
                Publication.append(Date)
        except:
            
            print("L'offre n'est plus disponible")
        
        try:
            for experience in soup[i].find_all('span', {'itemprop' : 'experienceRequirements'}):
                exp=experience.next_element.replace('\n', '')
                print(exp)
                Experiences.append(exp)
        except:
            
            print("L'offre n'est plus disponible")
            
        try:
            for city in soup[i].find('span', {'itemprop' : 'name'}):
                Ville=city
                #print(Ville)
                Localisation.append(Ville)  
        except:
            
            print("L'offre n'est plus disponible")
   
        try:
            description=soup[i].find("div", {"itemprop": "description"}).get_text().replace(';','')
            #print(description)
            Description.append(description)
        except:
            print("il ya pas de description")            
            description='il ya pas de description'
        try:
            contrat=soup[i].find("dd").get_text()
            print(contrat)
            Contrat.append(contrat)
        except:
            Contrat.append("il ya pas de type de contrat renseigné")
            print("il ya pas de type de contrat renseigné")            
            contrat='il ya pas de type de contrat renseigné'
        try:
            contratheures=soup[i].find("dd", {"itemprop": "workHours"}).get_text()
            #print(contratheures)
            Horaires.append(contratheures)
        except:
            Horaires.append("il ya pas de type d'horraires de contrat renseigné")
            print("il ya pas de type d'horraires de contrat renseigné")
            contratheures='il ya pas de type d\'horraires de contrat renseigné'
        try:
            salaire = soup[i].findAll('dd')[2].text
            s=salaire.replace('\n', '').replace('\xa0', '')
                
            #print(s)
            Salaire.append(s)
        except:
            Salaire.append("il ya pas de salaire renseigné")
            print("il ya pas de salaire renseigné")
            s="il ya pas de salaire renseigné"
        
        sfaire=[]
                      
        for savoirsfaire in soup[i].find_all('span', {'itemprop' : 'skills'}):
               
            sfaire.append(savoirsfaire.text)
        if not sfaire:
            print("il ya pas de savoirsfaire renseigné")
            Savoirsfaire.append("il ya pas de savoirs faire renseigné")
            strsfaire="il ya pas de savoirsfaire renseigné"
        else:
            print(sfaire)
            Savoirsfaire.append(str(sfaire))
            strsfaire=str(sfaire)

               
        setre=[]
        try:              
            savoir_etre = soup[i].find('h4', class_= 't6 skill-subtitle',text='Savoir-être professionnels').next_sibling
            #print(savoir_etre)
            for savoirE in savoir_etre.find_all('span', class_ ='skill-name'):
                
                setre.append(savoirE.text)
        
            Savoir_etre.append(str(setre))
            strsetre=str(setre)
        except:
            print("il ya pas de Savoir-être professionnels renseigné")
            Savoir_etre.append("il ya pas de Savoir-être professionnels renseigné")
            strsetre="il ya pas de Savoir-être professionnels renseigné"
        try:
            formation = soup[i].find('span', {'itemprop' : 'educationRequirements'}).get_text()
            Formation.append(formation)
            print(formation)
        except:
            print("il ya pas de formation renseigné")
            Formation.append("il ya pas de formation renseigné") 
            formation="il ya pas de formation renseigné"
        
        try:
            qualifications = soup[i].find('span', {'itemprop' : 'qualifications'}).get_text()
            Qualifications.append(qualifications)
            print(qualifications)
        except:
            print("il ya pas de qualifications renseigné")
            Qualifications.append("il ya pas de qualifications renseigné")
            qualifications = "il ya pas de qualifications renseigné"

        try:
            secteuractv = soup[i].find('span', {'itemprop' : 'industry'}).get_text()
            Secteur.append(secteuractv)
            print(secteuractv)
        except:
            print("il ya pas de secteur renseigné")
            Secteur.append("il ya pas de secteur renseigné")        
            secteuractv ="il ya pas de secteur renseigné"



        try:
            entreprise = soup[i].find('h4', {'class' : 't4 title'}).get_text()
            Entreprise.append(entreprise)
            print(entreprise)
        except:
            
            print("il ya pas d'entreprise renseigné")
            Entreprise.append("il ya pas d'entreprise renseigné")
            entreprise ="il ya pas d'entreprise renseigné"

        try:
            partenaire = soup[i].find('ul',class_='partner-list').find('a')['href'] 
            Partenaire.append(partenaire)
            print(partenaire)
        except:
            print("il ya pas de partenaire renseigné")
            Partenaire.append("il ya pas de partenaire renseigné") 
            partenaire ="il ya pas de partenaire renseigné"
            
            
            #Export to database
        statment=text("""
                   INSERT INTO POLE_EMPLOI(NumOffre,title,Localisation,Experiences,Date_publication,Description,Contrat,Horaires_Contrat,Salaire,Savoirsfaire,Savoir_etre,Formation,Qualifications,Secteur,Entreprise,Partenaire,Datefirst,Datelast)
                   VALUES(:NumOffre,:title,:Localisation,:Experiences,:Publication,:Description,:Contrat,:Horaires,:Salaire,:Savoirsfaire,:Savoir_etre,:Formation,:Qualifications,:Secteur,:Entreprise,:Partenaire, CURRENT_DATE(), CURRENT_DATE())
                   ON DUPLICATE KEY                   
                   UPDATE
                    
                    DateLast = CURRENT_DATE()
                """)

        param= {'NumOffre':num_offre,'title':Titre,'Localisation':Ville,'Experiences':exp,'Publication':Date,'Description':description,
                   'Contrat':contrat,'Horaires':contratheures,'Salaire':s,'Savoirsfaire':strsfaire,'Savoir_etre':strsetre,
                   'Formation':formation,'Qualifications':qualifications,'Secteur':secteuractv,'Entreprise':entreprise,'Partenaire':partenaire}
        print(param)
        mySQLengine.execute(statment,param)




print("il ya ",nb_offres,"Offres pour votre recherche")
print("--- %s seconds ---" % (time.time() - start_time))

