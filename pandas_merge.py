import numpy as np 
import pandas as pd
import csv

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

