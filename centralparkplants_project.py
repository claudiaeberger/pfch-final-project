import requests
from bs4 import BeautifulSoup
import csv
import numpy as np 
import pandas as pd
import json


#Webscrape of NAEB modified from the in class webscraping example, and expanded to include the second column in the table

page_counter = 1


all_plants = []


while page_counter <= 4:

    url = f"http://naeb.brit.org/uses/search/filtered/?use_category=&tribe=62&string=&page={page_counter}"
    
    r = requests.get(url)
    soup = BeautifulSoup(r.text, features="html.parser")

    all_trs = soup.find_all("tr")
    
    

    for tr in all_trs:

        plant = {
            "common_name" : None,
            "sci_name" : None,
            "usda_symbol":None,
            "usda_link": None,
            "use_cat" : None,
            "use_link" : None,
            "all_use_link":None,
            "use_note":None,
            "use_source":None
        }

        all_tds = tr.find_all("td")
        first_td = all_tds[0]
        second_td = all_tds[1]

        # first TD, grab both A links
        first_td_a_links = first_td.find_all("a")
        sci_name_a_link = first_td_a_links[0]

        plant['all_use_link'] = "http://naeb.brit.org" + sci_name_a_link['href']

        # if it is len 1 that means that there is no UDSA link
        if len(first_td_a_links) == 2:
            plant['sci_name'] = sci_name_a_link.text
            usda_a_link = first_td_a_links[1]
            plant['usda_symbol'] = usda_a_link.text
            plant['usda_link'] = usda_a_link['href']
            plant['usda_symbol'] = plant['usda_symbol'].replace("USDA ","")
        
        else: 
            plant['sci_name'] = sci_name_a_link.text
            

        # problem with BS parsing, jsut find all elements in the TD including text nodes
        # and the 2nd will be the common name if it is there
        firsttd_elements = first_td.find_all(text=True)

        common_name = firsttd_elements[1].strip()

        if len(common_name) > 0:
            plant['common_name'] = common_name

        second_td_a_links = second_td.find_all("a")
        use_cat_a_link = second_td_a_links[0]

        plant['use_cat'] = use_cat_a_link.text
        plant['use_cat'] = plant['use_cat'].replace("Delaware Drug, ","")
        plant['use_link'] = "http://naeb.brit.org" + use_cat_a_link["href"]
        
        secondtd_elements = second_td.find_all(text=True)
        use_note = secondtd_elements[1].strip()

        if len(use_note) > 0:
            plant['use_note'] = use_note

        use_source = secondtd_elements[2].strip()

        if len(use_source) > 0:
            plant['use_source'] = use_source

    
        all_plants.append(plant)

       
    page_counter = page_counter + 1
  
#converting json to csv - needed to manually fix a few comma errors in the JSON before this would work     
out_file = open("NAEB_plants.json", 'w')
json.dump(all_plants, out_file, indent = 6)
out_file.close()

with open ('NAEB_plants.json') as naeb_json_file:
    data = json.load(naeb_json_file)
    plant_data = data
    data_file = open ('naeb_data_file.csv', 'w')
    csv_writer = csv.writer(data_file)
    count = 0
    for plant in plant_data:
         if count == 0:
             header = plant.keys()
             csv_writer.writerow(header)
             count += 1
         csv_writer.writerow(plant.values())

#webscrape of Central Park Bloom Guide to a json file

page_counter = 1

while page_counter <= 15:
    centralpark_url = f"https://www.centralparknyc.org/plants.json?q=&filters=&page={page_counter}"

    r = requests.get(centralpark_url)
    
    soup = BeautifulSoup(r.text, features="html.parser")
    all_plants = json.loads(soup.text)

    with open ('centralparkplants.json', 'a') as outfile:
        json.dump(all_plants, outfile, indent=2)


    page_counter = page_counter + 1

#converting json to a csv, first had to manually debug a few issues in json file, mostly comma issues
with open ('centralparkplants.json') as json_file:
    data = json.load(json_file)

    plant_data = data['data']

    data_file = open ('data_file.csv', 'w')

    csv_writer = csv.writer(data_file)

    count = 0

    for plant in plant_data:
        if count == 0:
            header = plant.keys()
            csv_writer.writerow(header)
            count += 1
        
        csv_writer.writerow(plant.values())

    data_file.close()


#combining tidied CSVs with other datasets using pandas. Using left join to enrich data using scientific names as the main key
#First did some manual and OpenRefine tidying of the Central Park data to make it one row per location, rather than one row per plant

centralpark_data = pd.read_csv("data_file_openrefine.csv")

locations = pd.read_csv("centralpark_locations.csv")
location_merged = pd.merge(centralpark_data, locations, how='left', on=["Location"])

audubon = pd.read_csv("Audabon_plantlist_birds.csv")
wildlife_merged = pd.merge(location_merged, audubon, how='left', on=["sci_name"])

NAEB_uses = pd.read_csv("naeb_data_file_openrefine.csv")
NAEB_merged = pd.merge(wildlife_merged, NAEB_uses, how='left', on=['sci_name'])

floralatlas = pd.read_csv("NYFA-SearchResults-20210412-153881.csv")
floralatlas_merged = pd.merge(NAEB_merged, floralatlas, how='left', on=['sci_name'])

welikia = pd.read_csv('Welikia_CentralPark_MostLikely.csv')
welikia_merged = pd.merge(floralatlas_merged, welikia, how='left', on=['sci_name'])

welikia_merged.to_csv("merged.csv")







