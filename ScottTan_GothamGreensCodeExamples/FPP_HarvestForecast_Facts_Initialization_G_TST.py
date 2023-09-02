#!/usr/bin/env python
# coding: utf-8

# In[8]:


# Gotham Greens Forecasting + Production Planning- HarvestForecast_Facts
# Written by Scott Tan
# Last Updated 3/30/2021


# Goal: Table HarvestForecast_Facts containing growing measure and metric predictions for Gotham Greens

# Inputs:
#   Dimensions:
#     1. Crop_Dim
#     3. Facilities_Dim
#     5. FacilityLine_Dim

#   Facts:
#     1. GreenhouseYields_Facts
#     2. CropSchedule_Facts
# 
# Output:
#    Table name: HarvestForecast_Facts
#         HarvestForecastID INT NOT NULL: unique ID in table HarvestForecast_Facts
#         HarvestDate DATE NOT NULL: date of harvest forecast
#         FacilityID INT NOT NULL: facility ID in Facilities_Dim
#         FacilityLineID INT NOT NULL: facility line ID in FacilityLine_Dim
#         CropID INT NOT NULL: Crop ID in Crop_Dim
#         ExpectedPlantSites INT: Forecasted plant sites from CropSchedule_Facts
#         ExpectedWholeGrams FLOAT: Forecasted whole plant biomass
#         ExpectedLooseGrams FLOAT: Forecasted loose plant biomass
#         ExpectedClamshells INT: Forecasted clamshells
#         Expected12Pack INT: Forecasted 12 packs of clamshells
#         WholeSpatialPrecision INT: spatial precision for whole plant biomass(0=line, 1=greenhouse, 2=region, 3=nation)
#         LooseSpatialPrecision INT: spatial precision for loose plant biomass(0=line, 1=greenhouse, 2=region, 3=nation)
#         AvgHeadweight FLOAT: average whole plant biomass grams per plant site
#         PlantSitesPerClam FLOAT: average plant sites per clamshell
#         LooseGramsPerPlantSite FLOAT: average loose grams per plant site
#         OptimizedTrailLengthAvgHeadweight INT: number of days in trailing average for avg headweight (0=year over year)
#         OptimizedTrailLengthPSPC INT: number of days in trailing average for plant sites per clam (0=year over year)
#         LoadDate DATETIME: load date into HarvestForecast_Facts
#         ToDate DATETIME: to date in HarvestForecast_Facts
#         IsActive INT: active tag in HarvestForecast_Facts

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
import GothamFunctions

print('functions loaded')


# In[2]:


# Load all neccessary data from the enterprise data warehouse

with open("./config.yml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.SafeLoader)
uid = cfg['databasename']['uid']
pwd = cfg['databasename']['pwd']

cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                        "Server=127.0.0.1;" # ,21443
                        "Database=databasename;"
                        "UID=" + uid + ";"
                        "PWD=" + pwd + ";"
                    )

cnxn_cursor = cnxn.cursor()

###########################################################################################

# DIMENSIONS
        
            
# pull from Crop_Dim
crop_id_list = list()
sage_crop_code_list = list()
crop_description_list = list()
default_generic_item_number_list = list()
sql = "SELECT CropID, SageCropCode, CropDescription, DefaultGenericItemNumber FROM Crop_Dim ORDER BY CropID"
cnxn_cursor.execute(sql) 
row = cnxn_cursor.fetchone()

crop_id_list += [row[0]]
sage_crop_code_list += [row[1]]
crop_description_list += [row[2]]
default_generic_item_number_list += [row[3]]

while row is not None:
    row = cnxn_cursor.fetchone()
    if row is not None:
        crop_id_list += [row[0]]
        sage_crop_code_list += [row[1]]
        crop_description_list += [row[2]]
        default_generic_item_number_list += [row[3]]

# pull from Facilities_Dim
facility_list = list()
location_name_list = list()
region_list = list()
sql = "SELECT FacilityID, LocationName,Region FROM Facilities_Dim"
cnxn_cursor.execute(sql) 
row = cnxn_cursor.fetchone()

facility_list += [row[0]]
location_name_str = row[1].rstrip()
location_name_list += [location_name_str]
region_list += [row[2]]

while row is not None:
    row = cnxn_cursor.fetchone()
    if row is not None:
        facility_list += [row[0]]        
        location_name_str = row[1].rstrip()
        location_name_list += [location_name_str]     
        region_list += [row[2]]    
        

# pull from FacilityLine_Dim
fld_facility_line_id_list = list()
fld_facility_line_list = list()
sql = "SELECT FacilityLineID, FacilityLine FROM FacilityLine_Dim"
cnxn_cursor.execute(sql) 
row = cnxn_cursor.fetchone()

fld_facility_line_id_list += [row[0]]
fld_facility_line_list += [row[1]]

while row is not None:
    row = cnxn_cursor.fetchone()
    if row is not None:
        fld_facility_line_id_list += [row[0]]
        fld_facility_line_list += [row[1]]  


print('Crop_Dim, Facilities_Dim, FacilityLine_Dim loaded')   


# FACTS:

# GreenhouseYields

# pull from GreenhouseYields_Facts
gy_harvest_date_list = list()
gy_year_list = list()
gy_week_list = list()
gy_facility_list = list()
gy_line_number_list = list()
gy_item_number_list = list()
gy_crop_id_list = list()
gy_avg_headweight_list = list()
gy_plant_spots_per_clam_list = list()

sql = "SELECT HarvestDate, Facilities_Dim.LocationName, LineNumber, SageProducts_Dim.ItemNo, AvgHeadweight, PlantSpotsPerClam  FROM GreenhouseYields_Facts INNER JOIN Facilities_Dim ON GreenhouseYields_Facts.FacilityID = Facilities_Dim.FacilityID INNER JOIN SageProducts_Dim ON GreenhouseYields_Facts.ItemNumberID = SageProducts_Dim.ItemID WHERE (LEFT(SageProducts_Dim.ItemNo, 10) COLLATE DATABASE_DEFAULT IN (SELECT LEFT(DefaultGenericItemNumber, 10) FROM Crop_Dim)) AND (PlantSpotsPerClam > 0 OR AvgHeadweight > 0) ORDER BY HarvestDate, LocationName, LineNumber"

cnxn_cursor.execute(sql) 
row = cnxn_cursor.fetchone()
gy_harvest_date_list += [row[0]]
gy_year_list += [row[0].year]
gy_week_list += [row[0].isocalendar()[1]]
gy_facility_str = row[1].rstrip()
gy_facility_list += [gy_facility_str]
gy_line_number_list += [str(row[2])]
gy_item_number_list += [row[3].rstrip()]
gy_avg_headweight_list += [row[4]]
gy_plant_spots_per_clam_list += [row[5]]

# get the crop ID
full_item_number_str = row[3].rstrip()
gy_sage_crop_code = full_item_number_str[3:7]
if gy_sage_crop_code in sage_crop_code_list:
    sage_crop_code_idx = sage_crop_code_list.index(gy_sage_crop_code)
else:
    sage_crop_code_idx = 20
crop_id = crop_id_list[sage_crop_code_idx]
gy_crop_id_list +=[crop_id]
    
while row is not None:
    row = cnxn_cursor.fetchone()
    if row is not None:
        gy_harvest_date_list += [row[0]]
        gy_year_list += [row[0].year]
        gy_week_list += [row[0].isocalendar()[1]]
        gy_facility_str = row[1].rstrip()
        gy_facility_list += [gy_facility_str]
        
        gy_line_number_list += [str(row[2])]
        gy_item_number_list += [row[3].rstrip()]
        gy_avg_headweight_list += [row[4]]
        gy_plant_spots_per_clam_list += [row[5]]
        
        # get the crop ID
        full_item_number_str = row[3].rstrip()
        gy_sage_crop_code = full_item_number_str[3:7]
        if gy_sage_crop_code in sage_crop_code_list:
            sage_crop_code_idx = sage_crop_code_list.index(gy_sage_crop_code)
        else:
            sage_crop_code_idx = 20
        
        crop_id = crop_id_list[sage_crop_code_idx]
        gy_crop_id_list +=[crop_id]
        
print('GreenhouseYields_Facts loaded')
        
# 4. CropScheduleFacts_T - current plant sites that have been seeded

# pull crop schedule from CropScheduleFacts_T
csf_harvest_date_list = list()
csf_year_list = list()
csf_week_list = list()
csf_facility_list = list()
csf_finishing_line_list = list()
csf_crop_id_list = list()
csf_total_plant_sites_list = list()

#sql = "SELECT HarvestDate, Facility,FinishingLine, CropID, TotalPlantSites, SeedDate FROM CropScheduleFacts_T WHERE HarvestDate >= GETDATE() AND SeedDate <= GETDATE() AND TotalPlantSites IS NOT NULL ORDER BY HarvestDate"
sql = "SELECT HarvestDate, Facility,FinishingLine, CropID, TotalPlantSites, SeedDate FROM CropSchedule_Facts WHERE HarvestDate >= CONVERT(NVARCHAR(MAX),DATEADD(DAY,-1,GETDATE()),126) AND SeedDate <= DATEADD(DAY,42,GETDATE()) AND TotalPlantSites IS NOT NULL AND HarvestDate < '2300-12-31' AND CropID IS NOT NULL ORDER BY HarvestDate"
cnxn_cursor.execute(sql) 
row = cnxn_cursor.fetchone()

csf_harvest_date_list += [row[0]]
csf_year_list += [row[0].year]
csf_week_list += [row[0].isocalendar()[1]]
csf_facility_list += [row[1]]
csf_finishing_line_list += [row[2]]
csf_crop_id_list += [row[3]]
csf_total_plant_sites_list += [row[4]]

while row is not None:
    row = cnxn_cursor.fetchone()
    if row is not None:
        csf_harvest_date_list += [row[0]]
        csf_year_list += [row[0].year]
        csf_week_list += [row[0].isocalendar()[1]]
        csf_facility_list += [row[1]]
        csf_finishing_line_list += [row[2]]
        csf_crop_id_list += [row[3]]
        csf_total_plant_sites_list += [row[4]]

print('CropSchedule_Facts loaded')


cnxn.commit()
cnxn_cursor.close()
cnxn.close()


        
print('Data is loaded and ready!')


# In[7]:


# Optimized Harvest Forecast

# build plant sites dictionary
# expected_ps_dict[harvest_date][facility_line][crop_id] = total_plant_sites
expected_ps_dict = {}

for csf_idx in range(len(csf_harvest_date_list)):
    csf_harvest_date = csf_harvest_date_list[csf_idx]
    csf_facility = csf_facility_list[csf_idx]
    csf_finishing_line = csf_finishing_line_list[csf_idx]
    csf_crop_id = csf_crop_id_list[csf_idx]
    csf_total_plant_sites = csf_total_plant_sites_list[csf_idx]
    csf_location_name = location_name_list[facility_list.index(csf_facility)]
    csf_facility_line = csf_location_name + '_' + str(csf_finishing_line)
         
    key_str = csf_crop_id
    val_str = csf_total_plant_sites

    if csf_harvest_date in expected_ps_dict.keys():
        csf_harvest_date_dictionary_value = expected_ps_dict[csf_harvest_date]
        if csf_facility_line in csf_harvest_date_dictionary_value.keys():
            csf_facility_line_dictionary_value = expected_ps_dict[csf_harvest_date][csf_facility_line]
            if key_str in csf_facility_line_dictionary_value.keys():
                expected_ps_dict[csf_harvest_date][csf_facility_line][key_str] += val_str
            else:
                expected_ps_dict[csf_harvest_date][csf_facility_line][key_str] = val_str
        else:
            expected_ps_dict[csf_harvest_date][csf_facility_line] = {key_str:val_str}
    else:
        expected_ps_dict[csf_harvest_date] = {csf_facility_line:{key_str:val_str}}

# build average headweight dictionary
# avg_headweight_dict[facility_line][crop_id][year_week] = [avg_headweight, avg_headweight2,...]
avg_headweight_dict = {}

for gy_idx in range(len(gy_harvest_date_list)):
    gy_harvest_date = gy_harvest_date_list[gy_idx]
    gy_year = gy_year_list[gy_idx]
    gy_week = gy_week_list[gy_idx]
    gy_facility = gy_facility_list[gy_idx]
    gy_line_number = gy_line_number_list[gy_idx]
    gy_crop_id = gy_crop_id_list[gy_idx]
    gy_avg_headweight = gy_avg_headweight_list[gy_idx]
    if gy_avg_headweight != None:
        gy_facility_line = gy_facility + '_' + gy_line_number
        key_str = str(gy_year) + '_' + str(gy_week)
        
        if gy_facility_line in avg_headweight_dict.keys():
            gy_facility_line_dictionary_value = avg_headweight_dict[gy_facility_line]
            if gy_crop_id in gy_facility_line_dictionary_value.keys():
                gy_crop_id_dictionary_value = avg_headweight_dict[gy_facility_line][gy_crop_id]
                if key_str in gy_crop_id_dictionary_value.keys():
                    avg_headweight_dict[gy_facility_line][gy_crop_id][key_str] += [gy_avg_headweight]
                else:
                    avg_headweight_dict[gy_facility_line][gy_crop_id][key_str] = [gy_avg_headweight]
            else:
                avg_headweight_dict[gy_facility_line][gy_crop_id] = {key_str:[gy_avg_headweight]}
        else:
            avg_headweight_dict[gy_facility_line] = {gy_crop_id:{key_str:[gy_avg_headweight]}}

# build plant sites per clam dictionary
# pspc_dict[facility_line][crop_id][year_week] = [pspc1, pspc2,...]
pspc_dict = {}

for gy_idx in range(len(gy_harvest_date_list)):
    gy_harvest_date = gy_harvest_date_list[gy_idx]
    gy_year = gy_year_list[gy_idx]
    gy_week = gy_week_list[gy_idx]
    gy_facility = gy_facility_list[gy_idx]
    gy_line_number = gy_line_number_list[gy_idx]
    gy_crop_id = gy_crop_id_list[gy_idx]
    gy_plant_spots_per_clam = gy_plant_spots_per_clam_list[gy_idx]
    if gy_plant_spots_per_clam != None:
        gy_facility_line = gy_facility + '_' + gy_line_number
        key_str = str(gy_year) + '_' + str(gy_week)
        
        if gy_facility_line in pspc_dict.keys():
            gy_facility_line_dictionary_value = pspc_dict[gy_facility_line]
            if gy_crop_id in gy_facility_line_dictionary_value.keys():
                gy_crop_id_dictionary_value = pspc_dict[gy_facility_line][gy_crop_id]
                if key_str in gy_crop_id_dictionary_value.keys():
                    pspc_dict[gy_facility_line][gy_crop_id][key_str] += [gy_plant_spots_per_clam]
                else:
                    pspc_dict[gy_facility_line][gy_crop_id][key_str] = [gy_plant_spots_per_clam]
            else:
                pspc_dict[gy_facility_line][gy_crop_id] = {key_str:[gy_plant_spots_per_clam]}
        else:
            pspc_dict[gy_facility_line] = {gy_crop_id:{key_str:[gy_plant_spots_per_clam]}}
            
            
# Create a list of crop lines that are active in the crop schedule

facility_line_crop_id_list = []
count1 = 0
count5 = 0
l2_1 = 0
l2_5 = 0

for csf_idx in range(len(csf_harvest_date_list)):

    csf_harvest_date = csf_harvest_date_list[csf_idx]
    csf_facility = csf_facility_list[csf_idx]
    csf_finishing_line = csf_finishing_line_list[csf_idx]
    csf_crop_id = csf_crop_id_list[csf_idx]
    csf_total_plant_sites = csf_total_plant_sites_list[csf_idx]
    csf_location_name = location_name_list[facility_list.index(csf_facility)]
    csf_facility_line = csf_location_name + '_' + str(csf_finishing_line)

    facility_line_crop_id = csf_facility_line + '_' + str(csf_crop_id)

    if facility_line_crop_id not in facility_line_crop_id_list:
        facility_line_crop_id_list += [facility_line_crop_id]

# Compare 1-day and 5-day trailing averages:
trail_lengths_list = [1, 5]

[optimal_trail_length, total_crop_lines, max_percent_of_crop_lines, use_one_list_loose, remaining_list_loose] = GothamFunctions.optimalTrailingLength(trail_lengths_list, facility_line_crop_id_list, pspc_dict)
[optimal_trail_length, total_crop_lines, max_percent_of_crop_lines, use_one_list_whole, remaining_list_whole] = GothamFunctions.optimalTrailingLength(trail_lengths_list, facility_line_crop_id_list, avg_headweight_dict)

# Compare 5-day and 6-day trailing averages:
trail_lengths_list = [5, 6]

[optimal_trail_length, total_crop_lines, max_percent_of_crop_lines, use_five_list_loose, use_six_list_loose] = GothamFunctions.optimalTrailingLength(trail_lengths_list, remaining_list_loose, pspc_dict)
[optimal_trail_length, total_crop_lines, max_percent_of_crop_lines, use_five_list_whole, use_six_list_whole] = GothamFunctions.optimalTrailingLength(trail_lengths_list, remaining_list_loose, avg_headweight_dict)

# Compare year over year averages to optimal trailing averages:
use_yoy_list_loose = GothamFunctions.optimalYearOverYear(pspc_dict,facility_line_crop_id_list,use_five_list_loose,use_six_list_loose)
use_yoy_list_whole = GothamFunctions.optimalYearOverYear(avg_headweight_dict,facility_line_crop_id_list,use_five_list_whole,use_six_list_whole)

for flci in use_yoy_list_loose:
    if flci in use_one_list_loose:
        del use_one_list_loose[use_one_list_loose.index(flci)]
    if flci in use_five_list_loose:
        del use_five_list_loose[use_five_list_loose.index(flci)]
    if flci in use_six_list_loose:
        del use_six_list_loose[use_six_list_loose.index(flci)]
        
for flci in use_yoy_list_whole:
    if flci in use_one_list_whole:
        del use_one_list_whole[use_one_list_whole.index(flci)]
    if flci in use_five_list_whole:
        del use_five_list_whole[use_five_list_whole.index(flci)]
    if flci in use_six_list_whole:
        del use_six_list_whole[use_six_list_whole.index(flci)]

# add crop lines not yet in pspc dict to use_one_list
for flci in facility_line_crop_id_list:
    if flci not in use_one_list_loose and flci not in use_five_list_loose and flci not in use_six_list_loose:
        use_one_list_loose += [flci]
    if flci not in use_one_list_whole and flci not in use_five_list_whole and flci not in use_six_list_whole:
        use_one_list_whole += [flci]
        

# Set control limits for plant_sites_per_clam
ucl_factor = 3
lcl_factor = 3

        
# Build dictionaries for total expected whole and loose biomass based on the crop schedule using optimal approximators

# expected_whole_plant_biomass_dict[harvest_date][facility_line][crop_id] = expected_whole_plant_biomass (g)
expected_whole_plant_biomass_dict = {}
expected_whole_plant_biomass_trail_dict = {}
expected_whole_plant_biomass_trail_std_dict = {}
trail_five_avg_headweight_dict = {}

for csf_idx in range(len(csf_harvest_date_list)):
    
    csf_harvest_date = csf_harvest_date_list[csf_idx]
    csf_facility = csf_facility_list[csf_idx]
    csf_finishing_line = csf_finishing_line_list[csf_idx]
    csf_crop_id = csf_crop_id_list[csf_idx]
    csf_total_plant_sites = csf_total_plant_sites_list[csf_idx]
    csf_location_name = location_name_list[facility_list.index(csf_facility)]
    csf_facility_line = csf_location_name + '_' + str(csf_finishing_line)
    if csf_facility_line in avg_headweight_dict.keys():
        if csf_crop_id in avg_headweight_dict[csf_facility_line]:
            last_year_week = list(avg_headweight_dict[csf_facility_line][csf_crop_id].keys())[-1]
            last_avg_headweight = list(avg_headweight_dict[csf_facility_line][csf_crop_id][last_year_week])[-1]
            
            crop_line_optimal_trail = 1
            if csf_facility_line in use_five_list_whole:
                crop_line_optimal_trail = 5
            if csf_facility_line in use_six_list_whole:
                crop_line_optimal_trail = 6
            if csf_facility_line in use_yoy_list_whole:
                crop_line_optimal_trail = 0
            
            if crop_line_optimal_trail != 0:
                [trail_five_avg_headweight,trail_five_std_headweight] = GothamFunctions.trailingAverage(avg_headweight_dict, crop_line_optimal_trail, csf_facility_line, csf_crop_id)

            if crop_line_optimal_trail == 0:
                [trail_five_avg_headweight,trail_five_std_headweight] = GothamFunctions.yearOverYearAverage(avg_headweight_dict,csf_facility_line,csf_crop_id,csf_harvest_date)
                
            if last_avg_headweight != None and last_avg_headweight != 0:
                # compute grams = PS * g/PS
                csf_total_expected_grams = csf_total_plant_sites * float(last_avg_headweight)
                csf_total_expected_grams_trail_five = csf_total_plant_sites * float(trail_five_avg_headweight)
                csf_std_expected_grams_trail_five = csf_total_plant_sites * float(trail_five_std_headweight)
                
                key_str = csf_crop_id
            
                if csf_harvest_date in expected_whole_plant_biomass_dict.keys():
                    csf_harvest_date_dictionary_value = expected_whole_plant_biomass_dict[csf_harvest_date]
                    if csf_facility_line in csf_harvest_date_dictionary_value.keys():
                        csf_facility_line_dictionary_value = expected_whole_plant_biomass_dict[csf_harvest_date][csf_facility_line]
                        if key_str in csf_facility_line_dictionary_value.keys():
                            expected_whole_plant_biomass_dict[csf_harvest_date][csf_facility_line][key_str] += csf_total_expected_grams
                            expected_whole_plant_biomass_trail_dict[csf_harvest_date][csf_facility_line][key_str] += csf_total_expected_grams_trail_five
                            expected_whole_plant_biomass_trail_std_dict[csf_harvest_date][csf_facility_line][key_str] += csf_std_expected_grams_trail_five
                            trail_five_avg_headweight_dict[csf_harvest_date][csf_facility_line][key_str] = trail_five_avg_headweight
                        else:
                            expected_whole_plant_biomass_dict[csf_harvest_date][csf_facility_line][key_str] = csf_total_expected_grams
                            expected_whole_plant_biomass_trail_dict[csf_harvest_date][csf_facility_line][key_str] = csf_total_expected_grams_trail_five
                            expected_whole_plant_biomass_trail_std_dict[csf_harvest_date][csf_facility_line][key_str] = csf_std_expected_grams_trail_five
                            trail_five_avg_headweight_dict[csf_harvest_date][csf_facility_line][key_str] = trail_five_avg_headweight
                    else:
                        expected_whole_plant_biomass_dict[csf_harvest_date][csf_facility_line] = {key_str:csf_total_expected_grams}
                        expected_whole_plant_biomass_trail_dict[csf_harvest_date][csf_facility_line] = {key_str:csf_total_expected_grams_trail_five}
                        expected_whole_plant_biomass_trail_std_dict[csf_harvest_date][csf_facility_line] = {key_str:csf_std_expected_grams_trail_five}
                        trail_five_avg_headweight_dict[csf_harvest_date][csf_facility_line] = {key_str:trail_five_avg_headweight}
                else:
                    expected_whole_plant_biomass_dict[csf_harvest_date] = {csf_facility_line:{key_str:csf_total_expected_grams}}
                    expected_whole_plant_biomass_trail_dict[csf_harvest_date] = {csf_facility_line:{key_str:csf_total_expected_grams_trail_five}}
                    expected_whole_plant_biomass_trail_std_dict[csf_harvest_date] = {csf_facility_line:{key_str:csf_std_expected_grams_trail_five}}
                    trail_five_avg_headweight_dict[csf_harvest_date] = {csf_facility_line:{key_str:trail_five_avg_headweight}}

# expected_loose_plant_biomass_dict[harvest_date][facility_line][crop_id] = expected_loose_plant_biomass (g)
expected_loose_plant_biomass_dict = {}
expected_loose_plant_biomass_trail_dict = {}
expected_loose_plant_biomass_trail_std_dict = {}
trail_five_pspc_dict = {}

for csf_idx in range(len(csf_harvest_date_list)):
    
    csf_harvest_date = csf_harvest_date_list[csf_idx]
    csf_facility = csf_facility_list[csf_idx]
    csf_finishing_line = csf_finishing_line_list[csf_idx]
    csf_crop_id = csf_crop_id_list[csf_idx]
    csf_total_plant_sites = csf_total_plant_sites_list[csf_idx]
    csf_location_name = location_name_list[facility_list.index(csf_facility)]
    csf_facility_line = csf_location_name + '_' + str(csf_finishing_line)
        
    
    if csf_facility_line in pspc_dict.keys():
        if csf_crop_id in pspc_dict[csf_facility_line]:
            last_year_week = list(pspc_dict[csf_facility_line][csf_crop_id].keys())[-1]
            last_pspc = list(pspc_dict[csf_facility_line][csf_crop_id][last_year_week])[-1]
            
            crop_line_optimal_trail = 1
            if csf_facility_line in use_five_list_loose:
                crop_line_optimal_trail = 5
            if csf_facility_line in use_six_list_loose:
                crop_line_optimal_trail = 6
            if csf_facility_line in use_yoy_list_loose:
                crop_line_optimal_trail = 0
            
            if crop_line_optimal_trail != 0:
                [trail_pspc,trail_std_pspc] = GothamFunctions.trailingAverage(pspc_dict, crop_line_optimal_trail, csf_facility_line, csf_crop_id)

            if crop_line_optimal_trail == 0:
                [trail_pspc,trail_std_pspc] = GothamFunctions.yearOverYearAverage(pspc_dict,csf_facility_line,csf_crop_id,csf_harvest_date)

            # check that the optimal trail falls within the control limits
            val_list = list()
            for year_week in pspc_dict[csf_facility_line][csf_crop_id].keys():
                val_list += pspc_dict[csf_facility_line][csf_crop_id][year_week]
            if len(val_list) > 1:
                [trail_baseline_pspc,trail_baseline_std_pspc] = GothamFunctions.trailingAverageSkip(pspc_dict, 5, csf_facility_line, csf_crop_id)
                [trail_twenty_pspc,trail_twenty_std_pspc] = GothamFunctions.trailingAverageSkip(pspc_dict, 20, csf_facility_line, csf_crop_id)
                ucl_pspc = trail_twenty_pspc + ucl_factor * trail_twenty_std_pspc # define guide rails
                lcl_pspc = trail_twenty_pspc - lcl_factor * trail_twenty_std_pspc

                if (trail_pspc > ucl_pspc) or (trail_pspc < lcl_pspc):
                    [trail_pspc,trail_five_std_pspc] = [trail_baseline_pspc,trail_baseline_std_pspc]
                    crop_line_optimal_trail = 5
                   
            if last_pspc != None and last_pspc != 0:
                # compute grams = PS * g/PS
                g_per_clam = 128
                if csf_crop_id == 1:
                    g_per_clam = 114 # arugula
                if csf_crop_id == 3:
                    g_per_clam = 35.4 # basil
                
                csf_total_expected_grams = csf_total_plant_sites * float(1/last_pspc) * g_per_clam
                csf_total_expected_grams_trail = csf_total_plant_sites * float(1/trail_pspc) * g_per_clam
                csf_total_expected_grams_trail_std = csf_total_plant_sites * float(trail_std_pspc) * g_per_clam
                
                key_str = csf_crop_id
            
                if csf_harvest_date in expected_loose_plant_biomass_dict.keys():
                    csf_harvest_date_dictionary_value = expected_loose_plant_biomass_dict[csf_harvest_date]
                    if csf_facility_line in csf_harvest_date_dictionary_value.keys():
                        csf_facility_line_dictionary_value = expected_loose_plant_biomass_dict[csf_harvest_date][csf_facility_line]
                        if key_str in csf_facility_line_dictionary_value.keys():
                            expected_loose_plant_biomass_dict[csf_harvest_date][csf_facility_line][key_str] += csf_total_expected_grams
                            expected_loose_plant_biomass_trail_dict[csf_harvest_date][csf_facility_line][key_str] += csf_total_expected_grams_trail
                            expected_loose_plant_biomass_trail_std_dict[csf_harvest_date][csf_facility_line][key_str] += csf_total_expected_grams_trail_std
                            trail_five_pspc_dict[csf_harvest_date][csf_facility_line][key_str] = trail_pspc
                        else:
                            expected_loose_plant_biomass_dict[csf_harvest_date][csf_facility_line][key_str] = csf_total_expected_grams
                            expected_loose_plant_biomass_trail_dict[csf_harvest_date][csf_facility_line][key_str] = csf_total_expected_grams_trail
                            expected_loose_plant_biomass_trail_std_dict[csf_harvest_date][csf_facility_line][key_str] = csf_total_expected_grams_trail_std
                            trail_five_pspc_dict[csf_harvest_date][csf_facility_line][key_str] = trail_pspc
                    else:
                        expected_loose_plant_biomass_dict[csf_harvest_date][csf_facility_line] = {key_str:csf_total_expected_grams}
                        expected_loose_plant_biomass_trail_dict[csf_harvest_date][csf_facility_line] = {key_str:csf_total_expected_grams_trail}
                        expected_loose_plant_biomass_trail_std_dict[csf_harvest_date][csf_facility_line] = {key_str:csf_total_expected_grams_trail_std}
                        trail_five_pspc_dict[csf_harvest_date][csf_facility_line] = {key_str:trail_pspc}
                else:
                    expected_loose_plant_biomass_dict[csf_harvest_date] = {csf_facility_line:{key_str:csf_total_expected_grams}}
                    expected_loose_plant_biomass_trail_dict[csf_harvest_date] = {csf_facility_line:{key_str:csf_total_expected_grams_trail}}
                    expected_loose_plant_biomass_trail_std_dict[csf_harvest_date] = {csf_facility_line:{key_str:csf_total_expected_grams_trail_std}}
                    trail_five_pspc_dict[csf_harvest_date] = {csf_facility_line:{key_str:trail_pspc}}

                    
#########################################################################

# Write table HarvestForecast_Facts

# # connect to database
with open("./config.yml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.SafeLoader)
uid = cfg['databasename']['uid']
pwd = cfg['databasename']['pwd']

cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                        "Server=127.0.0.1,31443;"
                        "Database=databasename;"
                        "UID=" + uid + ";"
                        "PWD=" + pwd + ";"
                    )

cnxn_cursor = cnxn.cursor()

# initialize table

# sql = "DROP TABLE HarvestForecast_Facts"
# cnxn_cursor.execute(sql)

sql = """CREATE TABLE  HarvestForecast_Facts
        (HarvestForecastID INT NOT NULL,
        HarvestDate DATE NOT NULL,
        FacilityID INT NOT NULL,
        FacilityLineID INT NOT NULL,
        CropID INT NOT NULL,
        ExpectedPlantSites INT,
        ExpectedWholeGrams FLOAT,
        ExpectedLooseGrams FLOAT,
        ExpectedClamshells INT,
        Expected12Pack INT,
        WholeSpatialPrecision INT,
        LooseSpatialPrecision INT,
        AvgHeadweight FLOAT,
        PlantSitesPerClam FLOAT,
        LooseGramsPerPlantSite FLOAT,
        OptimizedTrailLengthAvgHeadweight INT,
        OptimizedTrailLengthPSPC INT,
        LoadDate DATETIME,
        ToDate DATETIME,
        IsActive INT);"""

cnxn_cursor.execute(sql)

#########################################################################
# this section is for change data capture when new data is loaded

# hf_list1 = list()
# hf_list2 = list()
# hf_list3 = list()
# hf_list4 = list()
# hf_list5 = list()
# hf_list6 = list()
# hf_list7 = list()
# hf_list8 = list()
# hf_list9 = list()
# hf_list10 = list()
# hf_list11 = list()
# hf_list12 = list()
# hf_list13 = list()
# hf_list14 = list()
# hf_list15 = list()
# hf_list16 = list()
# hf_list17 = list()
# hf_list18 = list()


# sql = "SELECT * FROM HarvestForecast_Facts WHERE IsActive = 1 ORDER BY HarvestForecastID;"
# cnxn_cursor.execute(sql) 
# row = cnxn_cursor.fetchone()

# hf_list1 += [row[0]]
# hf_list2 += [row[1]]
# hf_list3 += [row[2]]
# hf_list4 += [row[3]]
# hf_list5 += [row[4]]
# hf_list6 += [row[5]]
# hf_list7 += [row[6]]
# hf_list8 += [row[7]]
# hf_list9 += [row[8]]
# hf_list10 += [row[9]]
# hf_list11 += [row[10]]
# hf_list12 += [row[11]]
# hf_list13 += [row[12]]
# hf_list14 += [row[13]]
# hf_list15 += [row[14]]
# hf_list16 += [row[15]]
# hf_list17 += [row[16]]
# hf_list18 += [row[17]]


# while row is not None:
#     row = cnxn_cursor.fetchone()
#     if row is not None:
#         hf_list1 += [row[0]]
#         hf_list2 += [row[1]]
#         hf_list3 += [row[2]]
#         hf_list4 += [row[3]]
#         hf_list5 += [row[4]]
#         hf_list6 += [row[5]]
#         hf_list7 += [row[6]]
#         hf_list8 += [row[7]]
#         hf_list9 += [row[8]]
#         hf_list10 += [row[9]]
#         hf_list11 += [row[10]]
#         hf_list12 += [row[11]]
#         hf_list13 += [row[12]]
#         hf_list14 += [row[13]]
#         hf_list15 += [row[14]]
#         hf_list16 += [row[15]]
#         hf_list17 += [row[16]]
#         hf_list18 += [row[17]]


# # delete entries with IsActive = 1

# sql = "DELETE FROM HarvestForecast_Facts WHERE IsActive = 1;"
# cnxn_cursor.execute(sql)

# # write back old avtive entries to HarvestForecast_Facts with IsActive = 0 and current time ToDate
# to_date_to_write = DT.datetime.now()

# sql = """
# INSERT INTO HarvestForecast_Facts
# VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
# """ 

# for i in range(len(hf_list1)):    
#     tuple_to_write = (hf_list1[i],hf_list2[i],hf_list3[i],hf_list4[i],hf_list5[i],hf_list6[i],hf_list7[i],hf_list8[i],hf_list9[i],hf_list10[i],hf_list11[i],hf_list12[i],hf_list13[i],hf_list14[i],hf_list15[i],hf_list16[i],hf_list17[i],to_date_to_write,0)
#     cnxn_cursor.execute(sql, tuple_to_write)


#########################################################################

# # # write new entries with IsActive = 1
# max_old_id = max(hf_list1)
max_old_id = 0
harvest_forecast_id_to_write = max_old_id + 1
# load_date_to_write = to_date_to_write
load_date_to_write = DT.datetime.now()
to_date_to_write = DT.datetime.strptime('2099-12-31 00:00:00.000000', '%Y-%m-%d %H:%M:%S.%f')
is_active_to_write = 1


# loop through expected plant sites and write new entries
for date_tomorrow_idx in range(len(list(expected_ps_dict.keys()))-1):
    date_tomorrow = list(expected_ps_dict.keys())[date_tomorrow_idx]
    print(date_tomorrow)
    harvest_date_to_write = date_tomorrow.date()
    ps_active_lines = list(expected_ps_dict[date_tomorrow].keys())
    for active_line in ps_active_lines:
        facility_line_to_write = active_line
        facility_line_id_to_write = fld_facility_line_id_list[fld_facility_line_list.index(active_line)]
        #location_name_to_write = active_line.split('_')[0]
        facility_id_to_write = facility_list[location_name_list.index(active_line.split('_')[0])]
        #city_to_write = facility_line_to_write[0:3]
        line_to_write = int(active_line.split('_')[1])
        ps_active_line_crop_id_list = list(expected_ps_dict[date_tomorrow][active_line].keys())
        active_line_expected_ps_list = list(expected_ps_dict[date_tomorrow][active_line].values())
        #region_to_write = region_list[location_name_list.index(active_line.split('_')[0])]
        
        if date_tomorrow in list(expected_whole_plant_biomass_dict.keys()):
            if active_line in list(expected_whole_plant_biomass_dict[date_tomorrow].keys()):
                whole_active_line_crop_id_list = list(expected_whole_plant_biomass_dict[date_tomorrow][active_line].keys())
                #active_line_expected_whole_biomass_list = list(expected_whole_plant_biomass_dict[date_tomorrow][active_line].values())
                active_line_expected_whole_biomass_list = list(expected_whole_plant_biomass_trail_dict[date_tomorrow][active_line].values())
                active_line_avg_headweight_list = list(trail_five_avg_headweight_dict[date_tomorrow][active_line].values())
            else:
                whole_active_line_crop_id_list = list(expected_ps_dict[date_tomorrow][active_line].keys())
                active_line_expected_whole_biomass_list = [0] * len(list(expected_ps_dict[date_tomorrow][active_line].values()))
                active_line_avg_headweight_list = [0] * len(list(expected_ps_dict[date_tomorrow][active_line].values()))
        else:
            whole_active_line_crop_id_list = list(expected_ps_dict[date_tomorrow][active_line].keys())
            active_line_expected_whole_biomass_list = [0] * len(list(expected_ps_dict[date_tomorrow][active_line].values()))
            active_line_avg_headweight_list = [0] * len(list(expected_ps_dict[date_tomorrow][active_line].values()))
            
        if date_tomorrow in list(expected_loose_plant_biomass_dict.keys()):
            if active_line in list(expected_loose_plant_biomass_dict[date_tomorrow].keys()):
                loose_active_line_crop_id_list = list(expected_loose_plant_biomass_dict[date_tomorrow][active_line].keys())                
                #active_line_expected_loose_biomass_list = list(expected_loose_plant_biomass_dict[date_tomorrow][active_line].values())
                active_line_expected_loose_biomass_list = list(expected_loose_plant_biomass_trail_dict[date_tomorrow][active_line].values())
                active_line_pspc_list = list(trail_five_pspc_dict[date_tomorrow][active_line].values())
            else:
                loose_active_line_crop_id_list = list(expected_ps_dict[date_tomorrow][active_line].keys())
                active_line_expected_loose_biomass_list = [0] * len(list(expected_ps_dict[date_tomorrow][active_line].values()))
                active_line_pspc_list = [0] * len(list(expected_ps_dict[date_tomorrow][active_line].values()))
        else:
            loose_active_line_crop_id_list = list(expected_ps_dict[date_tomorrow][active_line].keys())
            active_line_expected_loose_biomass_list = [0] * len(list(expected_ps_dict[date_tomorrow][active_line].values()))
            active_line_pspc_list = [0] * len(list(expected_ps_dict[date_tomorrow][active_line].values()))
        
        
        line_tuple = (harvest_date_to_write, facility_id_to_write, facility_line_id_to_write)
        
        # for given date, facility, and line, loop through the crops
        for idx in range(len(ps_active_line_crop_id_list)):
            crop_id_to_write = ps_active_line_crop_id_list[idx]
            sage_crop_code_to_write = sage_crop_code_list[crop_id_list.index(crop_id_to_write)]
            crop_description_to_write = crop_description_list[crop_id_list.index(crop_id_to_write)]
            default_generic_item_number_to_write = default_generic_item_number_list[crop_id_list.index(crop_id_to_write)]
            
            expected_plant_sites_to_write = active_line_expected_ps_list[idx]
            expected_whole_grams_to_write = 0
            expected_loose_grams_to_write = 0
            whole_spatial_precision_to_write = 4
            loose_spatial_precision_to_write = 4
            if crop_id_to_write in whole_active_line_crop_id_list:
                whole_idx = whole_active_line_crop_id_list.index(crop_id_to_write)
                expected_whole_grams_to_write = active_line_expected_whole_biomass_list[whole_idx]
                avg_headweight_to_write = active_line_avg_headweight_list[whole_idx]
                whole_spatial_precision_to_write = 0
            if crop_id_to_write in loose_active_line_crop_id_list:            
                loose_idx = loose_active_line_crop_id_list.index(crop_id_to_write)
                expected_loose_grams_to_write = active_line_expected_loose_biomass_list[loose_idx]
                pspc_to_write = active_line_pspc_list[loose_idx]
                loose_spatial_precision_to_write = 0
            # compute at lower spatial precision if expected biomass is zero
            if expected_whole_grams_to_write == 0:
                # use cropAverages to compute predictions for any missing entries
                crop_averages_list = GothamFunctions.cropAverages(avg_headweight_dict,active_line,ps_active_line_crop_id_list[idx])
                idx_to_try = 0
                while idx_to_try < 3 and expected_whole_grams_to_write == 0:
                    conversion_factor = crop_averages_list[idx_to_try]
                    #print(type(expected_whole_grams_to_write))
                    if conversion_factor != conversion_factor:
                        expected_whole_grams_to_write = 0
                        conversion_factor = 0
                    #print(expected_whole_grams_to_write)
                    if conversion_factor != 0:
                        expected_whole_grams_to_write = expected_plant_sites_to_write * float(conversion_factor)
                        avg_headweight_to_write = conversion_factor
                        #print(expected_whole_grams_to_write)
                        whole_spatial_precision_to_write = idx_to_try+1
                        # add entry to avg_headweight_dict
                        # new_avg_headweight_dict[facility_line_to_write] = {crop_id_to_write:{'0000_00':conversion_factor}}
                        
                        avg_headweight_value = conversion_factor
                        facility_line = facility_line_to_write
                        crop_id = crop_id_to_write
                        year_week = '0000_00'
                        if facility_line in avg_headweight_dict.keys():
                            facility_line_dictionary_value = avg_headweight_dict[facility_line]
                            if crop_id in facility_line_dictionary_value.keys():
                                crop_id_dictionary_value = avg_headweight_dict[facility_line][crop_id]
                                if year_week in crop_id_dictionary_value.keys():
                                    avg_headweight_dict[facility_line][crop_id][year_week] += [avg_headweight_value]
                                else:
                                    avg_headweight_dict[facility_line][crop_id][year_week] = [avg_headweight_value]
                            else:
                                avg_headweight_dict[facility_line][crop_id] = {year_week:[avg_headweight_value]}
                        else:
                            avg_headweight_dict[facility_line] = {crop_id:{year_week:[avg_headweight_value]}}
                        
                        # add value to expected_whole_plant_biomass_dict
                        # new_expected_whole_plant_biomass_dict[date_tomorrow][facility_line_to_write] = {crop_id_to_write:[expected_whole_grams_to_write]}
                        total_expected_grams = expected_whole_grams_to_write
                        harvest_date = date_tomorrow
                        facility_line = facility_line_to_write
                        crop_id = crop_id_to_write
                        if harvest_date in expected_whole_plant_biomass_dict.keys():
                            harvest_date_dictionary_value = expected_whole_plant_biomass_dict[harvest_date]
                            if facility_line in harvest_date_dictionary_value.keys():
                                facility_line_dictionary_value = expected_whole_plant_biomass_dict[harvest_date][facility_line]
                                if crop_id in facility_line_dictionary_value.keys():
                                    expected_whole_plant_biomass_dict[harvest_date][facility_line][crop_id] += total_expected_grams
                                else:
                                    expected_whole_plant_biomass_dict[harvest_date][facility_line][crop_id] = total_expected_grams
                            else:
                                expected_whole_plant_biomass_dict[harvest_date][facility_line] = {crop_id:total_expected_grams}
                        else:
                            expected_whole_plant_biomass_dict[harvest_date] = {facility_line:{crop_id:total_expected_grams}}
                        
                        idx_to_try = 3
                    idx_to_try += 1
            if expected_loose_grams_to_write == 0:
                crop_averages_list = GothamFunctions.cropAverages(pspc_dict,active_line,ps_active_line_crop_id_list[idx])
                idx_to_try = 0
                while idx_to_try < 3 and expected_loose_grams_to_write == 0:
                    conversion_factor = crop_averages_list[idx_to_try]
                    if conversion_factor != conversion_factor:
                        expected_loose_grams_to_write = 0
                        conversion_factor = 0
                    #print(expected_loose_grams_to_write)
                    if conversion_factor != 0:
                        g_per_clam = 128
                        if crop_id_to_write == 1:
                            g_per_clam = 114 # arugula
                        if crop_id_to_write == 3:
                            g_per_clam = 35.4 # basil
                        expected_loose_grams_to_write = expected_plant_sites_to_write * float(1/conversion_factor) * g_per_clam
                        pspc_to_write = conversion_factor
                        #print(expected_loose_grams_to_write)
                        loose_spatial_precision_to_write = idx_to_try+1
                        # add entry to pspc_dict
                        #new_pspc_dict[facility_line_to_write] = {crop_id_to_write:{'0000_00':conversion_factor}}
                        pspc_value = conversion_factor
                        facility_line = facility_line_to_write
                        crop_id = crop_id_to_write
                        year_week = '0000_00'
                        if facility_line in pspc_dict.keys():
                            facility_line_dictionary_value = pspc_dict[facility_line]
                            if crop_id in facility_line_dictionary_value.keys():
                                crop_id_dictionary_value = pspc_dict[facility_line][crop_id]
                                if year_week in crop_id_dictionary_value.keys():
                                    pspc_dict[facility_line][crop_id][year_week] += [pspc_value]
                                else:
                                    pspc_dict[facility_line][crop_id][year_week] = [pspc_value]
                            else:
                                pspc_dict[facility_line][crop_id] = {year_week:[pspc_value]}
                        else:
                            pspc_dict[facility_line] = {crop_id:{year_week:[pspc_value]}}
                        
                        #new_expected_loose_plant_biomass_dict[date_tomorrow][facility_line_to_write] = {crop_id_to_write:[expected_loose_grams_to_write]}                                            
                        total_expected_grams = expected_loose_grams_to_write
                        harvest_date = date_tomorrow
                        facility_line = facility_line_to_write
                        crop_id = crop_id_to_write
                        if harvest_date in expected_loose_plant_biomass_dict.keys():
                            harvest_date_dictionary_value = expected_loose_plant_biomass_dict[harvest_date]
                            if facility_line in harvest_date_dictionary_value.keys():
                                facility_line_dictionary_value = expected_loose_plant_biomass_dict[harvest_date][facility_line]
                                if crop_id in facility_line_dictionary_value.keys():
                                    expected_loose_plant_biomass_dict[harvest_date][facility_line][crop_id] += total_expected_grams
                                else:
                                    expected_loose_plant_biomass_dict[harvest_date][facility_line][crop_id] = total_expected_grams
                            else:
                                expected_loose_plant_biomass_dict[harvest_date][facility_line] = {crop_id:total_expected_grams}
                        else:
                            expected_loose_plant_biomass_dict[harvest_date] = {facility_line:{crop_id:total_expected_grams}}                      
                        idx_to_try = 3
                    idx_to_try += 1
            
            g_per_clam = 128
            if crop_id_to_write == 1:
                g_per_clam = 114 # arugula
            if crop_id_to_write == 3:
                g_per_clam = 35.4 # basil
            expected_clamshells_to_write = int(expected_loose_grams_to_write/g_per_clam)
            expected_12_pack_to_write = int(expected_clamshells_to_write / 12)
            loose_grams_per_plant_site_to_write = float(g_per_clam / float(pspc_to_write))
            
            
            facility_line_crop_id = facility_line_to_write + '_' + str(crop_id_to_write)

            optimized_trail_length_avg_headweight_to_write = 1
            optimized_trail_length_pspc_to_write = 1
                
            if facility_line_crop_id in use_five_list_whole:
                optimized_trail_length_avg_headweight_to_write = 5
            if facility_line_crop_id in use_five_list_loose:
                optimized_trail_length_pspc_to_write = 5    

            if facility_line_crop_id in use_six_list_whole:
                optimized_trail_length_avg_headweight_to_write = 6
            if facility_line_crop_id in use_six_list_loose:
                optimized_trail_length_pspc_to_write = 6      

            if facility_line_crop_id in use_yoy_list_whole:
                optimized_trail_length_avg_headweight_to_write = 0
            if facility_line_crop_id in use_yoy_list_loose:
                optimized_trail_length_pspc_to_write = 0
            
            tuple_to_write = (harvest_forecast_id_to_write,) + line_tuple + (crop_id_to_write, expected_plant_sites_to_write, expected_whole_grams_to_write, expected_loose_grams_to_write,expected_clamshells_to_write, expected_12_pack_to_write, whole_spatial_precision_to_write,loose_spatial_precision_to_write, avg_headweight_to_write, pspc_to_write, loose_grams_per_plant_site_to_write, optimized_trail_length_avg_headweight_to_write, optimized_trail_length_pspc_to_write, load_date_to_write, to_date_to_write, is_active_to_write)
            
            #write to HarvestForecast_Facts 
            sql = """
            INSERT INTO HarvestForecast_Facts
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
            """ 
            cnxn_cursor.execute(sql, tuple_to_write)
            #print(tuple_to_write)
            harvest_forecast_id_to_write += 1
            
cnxn.commit()
cnxn_cursor.close()
cnxn.close()

                    
print('HarvestForecast_Facts done')                    


                


# In[ ]:




