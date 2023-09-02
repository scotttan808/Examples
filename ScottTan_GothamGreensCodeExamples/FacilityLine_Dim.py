#!/usr/bin/env python
# coding: utf-8

# In[2]:


import numpy as np
import pyodbc
import datetime as DT
from datetime import date
from datetime import datetime
import yaml
import os
import copy
import re
import pandas as pd
import xlrd, xlwt
import xlsxwriter



# This section loads all neccessary data from GG_EDW

with open("./config.yml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.SafeLoader)
uid = cfg['databasename']['uid']
pwd = cfg['databasename']['pwd']

cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                        "Server=127.0.0,1;"
                        "Database=databasename;"
                        "UID=" + uid + ";"
                        "PWD=" + pwd + ";"
                    )

cnxn_cursor = cnxn.cursor()

# pull Facility dimension - 9 possible facilities

facility_id_list = list()
location_name_list = list()
sage_reference_string_list = list()
legacy_location_name_list = list()
gg_region_list = list()
city_short_code_list= list()
latitude_list = list()
longitude_list= list()

sql = "SELECT FacilityID, LocationName,SageReferenceString,LegacyLocationName,Region,CityShortCode, Latitude, Longitude FROM Facilities_Dim"
cnxn_cursor.execute(sql) 
row = cnxn_cursor.fetchone()

facility_id_list += [row[0]]
#location_name_list += [row[1]]
location_name_str = row[1].rstrip()
# if location_name_str[-1].isdigit() is False:
#     location_name_str += '1'
location_name_list += [location_name_str]

sage_reference_string_list += [row[2]]
legacy_location_name_list += [row[3]]
gg_region_list += [row[4]]
city_short_code_list += [row[5]]
latitude_list += [row[6]]
longitude_list += [row[7]]

while row is not None:
    row = cnxn_cursor.fetchone()
    if row is not None:
        facility_id_list += [row[0]]
        
        #location_name_list += [row[1]]
        location_name_str = row[1].rstrip()
#         if location_name_str[-1].isdigit() is False:
#             location_name_str += '1'
        location_name_list += [location_name_str]
        
        sage_reference_string_list += [row[2]]
        legacy_location_name_list += [row[3]]
        gg_region_list += [row[4]]
        city_short_code_list += [row[5]]
        latitude_list += [row[6]]
        longitude_list += [row[7]]
        
# pull distinct facility lines from daily harvest forecast

dhf_facility_id_list = list()
dhf_line_list= list()

#sql = "SELECT DISTINCT FacilityID, Line FROM DailyHarvestForecast ORDER BY FacilityID, Line"
sql = "SELECT DISTINCT Facility, FinishingLine FROM CropScheduleFacts_T WHERE Facility IS NOT NULL AND FinishingLine IS NOT NULL ORDER BY Facility, FinishingLine"
cnxn_cursor.execute(sql) 
row = cnxn_cursor.fetchone()

dhf_facility_id_list  += [row[0]]
dhf_line_list += [row[1]]

while row is not None:
    row = cnxn_cursor.fetchone()
    if row is not None:
        dhf_facility_id_list  += [row[0]]
        dhf_line_list += [row[1]]
        

# write FacilityLine_Dim
# refresh table  in the database
sql = "DROP TABLE FacilityLine_Dim"
cnxn_cursor.execute(sql)
            
sql = """CREATE TABLE FacilityLine_Dim
        (FacilityLineID INT NOT NULL,
        FacilityLine NVARCHAR(MAX) NOT NULL,
        FacilityID INT NOT NULL,
        Line INT NOT NULL,
        LocationName NVARCHAR(MAX) NOT NULL,
        SageReferenceString NVARCHAR(MAX) NOT NULL,
        LegacyLocationName NVARCHAR(MAX) NOT NULL,
        GGRegion NVARCHAR(MAX) NOT NULL,
        CityShortCode NVARCHAR(MAX) NOT NULL);"""

cnxn_cursor.execute(sql)

#write to FacilityLine_Dim
sql_write = """
INSERT INTO FacilityLine_Dim
VALUES (?,?,?,?,?,?,?,?,?);
""" 
    
for i in range(len(dhf_facility_id_list)):
    facility_line_idx_write = i+1
    facility_id_write = dhf_facility_id_list[i]
    line_write = dhf_line_list[i]
    fs_idx = facility_id_list.index(facility_id_write)
    location_name_write = location_name_list[fs_idx]
    facility_line_write = location_name_write + '_' + str(line_write)
    sage_reference_string_write = sage_reference_string_list[fs_idx]
    legacy_location_name_write = legacy_location_name_list[fs_idx]
    gg_region_write = gg_region_list[fs_idx]
    city_short_code_write = city_short_code_list[fs_idx]
    
    cnxn_cursor.execute(sql_write, (facility_line_idx_write, facility_line_write, facility_id_write, line_write, location_name_write, sage_reference_string_write, legacy_location_name_write, gg_region_write, city_short_code_write))

cnxn.commit()
cnxn_cursor.close()
cnxn.close()

print('pau')


# In[ ]:




