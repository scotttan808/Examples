#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Gotham Greens Forecasting + Production Planning: OrderForecast_Facts
# Written by Scott Tan
# Last Updated 4/7/2021

# Goal: Table OrderForecast_Facts containing sales order predictions for Gotham Greens

# Inputs:
#   Dimensions:
#     1. Facilities_Dim
#     2. Customers_Dim
#     3. SageProducts_Dim
#
#   Facts:
#     1. LiveSales_Facts
#     2. InvoicedSales_Facts
#
# Output:
#    Table name: OrderForecast_Facts
#         OrderForecastID INT NOT NULL: unique ID in table OrderForecast_Facts
#         OrderDate DATE NOT NULL: date of order forecast
#         FacilityID INT NOT NULL: facility ID in Facilities_Dim
#         CustomersID INT NOT NULL: customer ID in Customers_Dim
#         ItemID INT NOT NULL: item ID in SageProducts_Dim
#         ExpectedOrderQty INT: Forecasted order quantity of case packs
#         StdExpectedOrderQty FLOAT: Standard deviation of the forecasted order quantity of case packs
#         LiveSalesOrderQty INT: Actual order quantity of case packs if a real order has been placed in LiveSales_Facts
#         OrderAllocationDate DATE: date that the order is to be allocated on
#         LoadDate DATETIME: load date into OrderForecast_Facts
#         ToDate DATETIME: to date in OrderForecast_Facts
#         IsActive INT: active tag in OrderForecast_Facts

import numpy as np
import pyodbc
import socket
import sys
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

HOSTNAME = socket.gethostname()

if HOSTNAME == 'servername':
    CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server}; 
                            Server=127.0.0.1;
                            Database=databasename;
                            trusted_connection=yes""" # use windows auth on DB01
else:
    with open(os.path.join(sys.path[0], "config.yml"), 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.SafeLoader)
    uid = cfg['databasename']['uid']
    pwd = cfg['databasename']['pwd']

    CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server};
                            Server=127.0.0.1;
                            Database=databasename;
                            UID=%s;
                            PWD=%s;""" % (uid, pwd) # use config.yml on local machine
                            
cnxn = pyodbc.connect(CONNECTIONSTRING)   
cnxn_cursor = cnxn.cursor()

###########################################################################################

# DIMENSIONS

# pull SageProducts_Dim
spd_item_no_list = list()
spd_eaches_per_unit_list = list()
spd_product_name_list = list()
spd_packed_weight_conversion_grams_list = list()
spd_item_id_list = list()

sql_orders = "SELECT ItemNo, ChildEachesPerUnit, ParentEachesPerUnit,ProductName, PackedWeightConversionGrams, ItemID FROM SageProductsDim"
#sql_orders = "SELECT ItemNo, ChildEachesPerUnit, ParentEachesPerUnit,ProductName, PackedWeightConversionGrams FROM SageProductsDim WHERE Active = 1 AND IsInvoiced = 1 AND PackedWeightConversionGrams IS NOT NULL"
cnxn_cursor.execute(sql_orders)
row = cnxn_cursor.fetchone()

spd_item_no_list += [row[0].rstrip()]

eaches_per_unit = 1
if row[1] != '':
    eaches_per_unit = row[1]
if row[2] != '':
    eaches_per_unit = row[2]
spd_eaches_per_unit_list+=[eaches_per_unit]
spd_product_name_list += [row[3]]
if row[4] is None:
    spd_packed_weight_conversion_grams_to_add = 1530.88 # conversion factor for leafy greens retail set as default
    if row[0].rstrip()[3:10] == 'BTHDBBY':
        spd_packed_weight_conversion_grams_to_add = 12 # placeholder value of 12 whole heads
        if row[0].rstrip()[-1] == '6':
            spd_packed_weight_conversion_grams_to_add = 6    
if row[4] is not None:
    spd_packed_weight_conversion_grams_to_add = row[4]
    if row[0].rstrip()[3:10] == 'BTHDBBY':
        spd_packed_weight_conversion_grams_to_add = 12 
        if row[0].rstrip()[-1] == '6':
            spd_packed_weight_conversion_grams_to_add = 6
            
spd_packed_weight_conversion_grams_list += [spd_packed_weight_conversion_grams_to_add]
spd_item_id_list += [row[5]]

while row is not None:  
    row = cnxn_cursor.fetchone()
    if row is not None:
        spd_item_no_list += [row[0].rstrip()]
        #spd_sku_type_short_name_list += [row[1].rstrip()]
        eaches_per_unit = 1
        if row[1] != '':
            eaches_per_unit = row[1]
        if row[2] != '':
            eaches_per_unit = row[2]
        spd_eaches_per_unit_list+=[eaches_per_unit]
        spd_product_name_list += [row[3]]
        if row[4] is None:
            spd_packed_weight_conversion_grams_to_add = 1530.88 # conversion factor for leafy greens retail set as default
            if row[0].rstrip()[3:10] == 'BTHDBBY':
                spd_packed_weight_conversion_grams_to_add = 12 
                if row[0].rstrip()[-1] == '6':
                    spd_packed_weight_conversion_grams_to_add = 6    
        if row[4] is not None:
            spd_packed_weight_conversion_grams_to_add = row[4]
            if row[0].rstrip()[3:10] == 'BTHDBBY':
                spd_packed_weight_conversion_grams_to_add = 12 
                if row[0].rstrip()[-1] == '6':
                    spd_packed_weight_conversion_grams_to_add = 6
        spd_packed_weight_conversion_grams_list += [spd_packed_weight_conversion_grams_to_add]
        spd_item_id_list += [row[5]]
        

# pull Facilities_Dim
facility_id_list = list()
location_code_list = list()
region_list = list()
sql = "SELECT FacilityID, LocationName,Region FROM Facilities_Dim"
cnxn_cursor.execute(sql) 
row = cnxn_cursor.fetchone()

facility_id_list += [row[0]]
location_code_str = row[1].rstrip()
location_code_list += [location_code_str]
region_list += [row[2]]

while row is not None:
    row = cnxn_cursor.fetchone()
    if row is not None:
        facility_id_list += [row[0]]
        location_code_str = row[1].rstrip()
        location_code_list += [location_code_str]
        region_list += [row[2]]    
        

# pull Customers_Dim
customers_id_list = list()
sage_customer_id_list = list()

sql = "SELECT CustomersID, SageCustomerID FROM Customers_Dim"
cnxn_cursor.execute(sql) 
row = cnxn_cursor.fetchone()

customers_id_list += [row[0]]
sage_customer_id_list += [row[1].rstrip()]

while row is not None:
    row = cnxn_cursor.fetchone()
    if row is not None:
        customers_id_list += [row[0]]
        sage_customer_id_list += [row[1].rstrip()]        

print('SageProducts_Dim, Facilities_Dim, Customers_Dim loaded')   


# FACTS:

# 1. pull LiveSales_Facts

# Allocation Classes (see architecture C1-C8)
# 1. OrderType = 'Food Service’ AND ItemNo NOT LIKE '%LOS%’
# 2. OrderType = 'Retail' AND SkuTypeShortName = 'Baby Butterhead’
# 3. OrderType = 'Food Service’ AND ItemNo LIKE '%LOS%’
# 4. OrderType = 'Retail' AND ProductTypeDesc = 'Leafy Greens' AND SkuTypeShortName != 'Gourmet Medley' AND SkuTypeShortName != 'Ugly Greens' AND SkuTypeShortName != 'Baby Butterhead’
# 5. OrderType = 'Retail'AND SkuTypeShortName = 'Gourmet Medley’
# 6. OrderType = 'Retail' AND ProductTypeDesc = 'Herbs’
# 7. OrderType = 'Retail'AND (ProductTypeDesc = 'Sauces' OR ProductTypeDesc = 'Dressings & Dips' OR ProductTypeDesc = 'Prepared Foods')
# 8. OrderType = 'Retail' AND SkuTypeShortName = 'Ugly Greens’

allocation_classes_str_list = ["SageProducts_Dim.OrderType = 'Food Service' AND SageProducts_Dim.ItemNo NOT LIKE '%LOS%'",
                               "SageProducts_Dim.OrderType = 'Retail' AND SageProducts_Dim.SkuTypeShortName = 'Baby Butterhead'",
                               "SageProducts_Dim.OrderType = 'Food Service' AND SageProducts_Dim.ItemNo LIKE '%LOS%'",
                               "SageProducts_Dim.OrderType = 'Retail' AND SageProducts_Dim.ProductTypeDesc = 'Leafy Greens' AND SageProducts_Dim.SkuTypeShortName != 'Gourmet Medley' AND SageProducts_Dim.SkuTypeShortName != 'Ugly Greens' AND SageProducts_Dim.SkuTypeShortName != 'Baby Butterhead'",
                               "SageProducts_Dim.OrderType = 'Retail' AND SageProducts_Dim.SkuTypeShortName = 'Gourmet Medley'",
                               "SageProducts_Dim.OrderType = 'Retail' AND SageProducts_Dim.ProductTypeDesc = 'Herbs'",
                               "SageProducts_Dim.OrderType = 'Retail' AND (SageProducts_Dim.ProductTypeDesc = 'Sauces' OR SageProducts_Dim.ProductTypeDesc = 'Dressings & Dips' OR SageProducts_Dim.ProductTypeDesc = 'Prepared Foods')",
                               "SageProducts_Dim.OrderType = 'Retail' AND SageProducts_Dim.SkuTypeShortName = 'Ugly Greens'"]

# pull all upcoming orders in the next 6 weeks from LiveSales_Facts
lsd_date_list = list()
lsd_item_no_list = list()
lsd_original_qty_list = list()
lsd_sage_customer_id_list = list()
lsd_location_code_list = list()
lsd_order_number_list = list()
lsd_allocation_class_list = list()

allocation_class = 1
for allocation_class_str in allocation_classes_str_list:
    sql_orders = "SELECT LiveSales_Facts.OrderDate,SageProducts_Dim.ItemNo, OriginalQty, Customers_Dim.SageCustomerID, SageLocations_Dim.LocationName, OpenOrders_Dim.OrderNumber FROM LiveSales_Facts INNER JOIN SageLocations_Dim ON LiveSales_Facts.FacilityID = SageLocations_Dim.ID INNER JOIN SageProducts_Dim ON LiveSales_Facts.ItemID = SageProducts_Dim.ItemID INNER JOIN Customers_Dim ON LiveSales_Facts.CustomersID = Customers_Dim.CustomersID INNER JOIN OpenOrders_Dim ON LiveSales_Facts.OpenOrderID = OpenOrders_Dim.OpenOrderID WHERE (LiveSales_Facts.OrderDate BETWEEN GETDATE() AND DATEADD(WEEK,6,GETDATE())) AND SageProducts_Dim.ItemNo LIKE 'FNG%' AND CurrentRecord = 1 AND " + allocation_class_str + " ORDER BY LiveSales_Facts.OrderDate"
    cnxn_cursor.execute(sql_orders) 
    row = cnxn_cursor.fetchone()

    if row is not None:
        lsd_date_list += [row[0]]
        lsd_item_no_list += [row[1].rstrip()]
        lsd_original_qty_list += [row[2]]
        lsd_sage_customer_id_list += [row[3].rstrip()]
        lsd_location_code_list +=[row[4].rstrip()]
        lsd_order_number_list += [row[5].rstrip()]
        lsd_allocation_class_list += [allocation_class]
    
    while row is not None:
        row = cnxn_cursor.fetchone()
        if row is not None:
            lsd_date_list += [row[0]]
            lsd_item_no_list += [row[1].rstrip()]
            lsd_original_qty_list += [row[2]]
            lsd_sage_customer_id_list += [row[3].rstrip()]
            lsd_location_code_list +=[row[4].rstrip()]
            lsd_order_number_list += [row[5].rstrip()]
            lsd_allocation_class_list += [allocation_class]
    
    allocation_class += 1
    
    
print('LiveSales_Facts loaded')

# pull InvoicedSales_Facts
fs_order_date_list = list()
fs_item_no_list = list()
fs_original_qty_list = list()
fs_sage_customer_id_list = list()
fs_location_code_list = list()
fs_allocation_class_list = list()

allocation_class = 1
for allocation_class_str in allocation_classes_str_list:

    sql_orders = "SELECT OrderDate, SageProducts_Dim.ItemNo, OriginalQty, Customers_Dim.SageCustomerID, SageLocations_Dim.LocationCode FROM InvoicedSales_Facts INNER JOIN SageLocations_Dim ON InvoicedSales_Facts.FacilityID = SageLocations_Dim.ID INNER JOIN SageProducts_Dim ON InvoicedSales_Facts.ItemID = SageProducts_Dim.ItemID INNER JOIN Customers_Dim ON InvoicedSales_Facts.CustomersID = Customers_Dim.CustomersID WHERE CurrentRecord = 1 AND SageLocations_Dim.LocationName IS NOT NULL AND InvoicedSales_Facts.OrderDate BETWEEN DATEADD(WEEK,-6,GETDATE()) AND GETDATE() AND SageProducts_Dim.ItemNo LIKE 'FNG%' AND "+ allocation_class_str + " ORDER BY InvoicedSales_Facts.OrderDate"

    cnxn_cursor.execute(sql_orders) 
    row = cnxn_cursor.fetchone()

    fs_order_date_list += [row[0]]
    fs_item_no_list += [row[1].rstrip()]
    fs_original_qty_list += [row[2]]
    fs_sage_customer_id_list += [row[3].rstrip()]
    fs_location_code_list +=[row[4].rstrip()]
    fs_allocation_class_list += [allocation_class]
    
    while row is not None:
        row = cnxn_cursor.fetchone()
        if row is not None:
            fs_order_date_list += [row[0]]
            fs_item_no_list += [row[1].rstrip()]
            fs_original_qty_list += [row[2]]
            fs_sage_customer_id_list += [row[3].rstrip()]
            fs_location_code_list +=[row[4].rstrip()]        
            fs_allocation_class_list += [allocation_class]
    
    allocation_class += 1
    
print('InvoicedSales_Facts loaded')


cnxn.commit()
cnxn_cursor.close()
cnxn.close()


        
print('Data is loaded and ready!')


# In[3]:


# order forecast


# connect to database
HOSTNAME = socket.gethostname()

if HOSTNAME == 'servername':
    CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server}; 
                            Server=127.0.0.1,1443;
                            Database=databasename;
                            trusted_connection=yes""" # use windows auth on DB01
else:
    with open(os.path.join(sys.path[0], "config.yml"), 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.SafeLoader)
    uid = cfg['databasename']['uid']
    pwd = cfg['databasename']['pwd']

    CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server};
                            Server=127.0.01,31443;
                            Database=databasename;
                            UID=%s;
                            PWD=%s;""" % (uid, pwd) # use config.yml on local machine
                            
cnxn = pyodbc.connect(CONNECTIONSTRING)   
cnxn_cursor = cnxn.cursor()



#########################################################################
# this section is for change data capture when new data is loaded

hf_list1 = list()
hf_list2 = list()
hf_list3 = list()
hf_list4 = list()
hf_list5 = list()
hf_list6 = list()
hf_list7 = list()
hf_list8 = list()
hf_list9 = list()
hf_list10 = list()



sql = "SELECT * FROM OrderForecast_Facts WHERE IsActive = 1 ORDER BY OrderForecastID;"
cnxn_cursor.execute(sql) 
row = cnxn_cursor.fetchone()

hf_list1 += [row[0]]
hf_list2 += [row[1]]
hf_list3 += [row[2]]
hf_list4 += [row[3]]
hf_list5 += [row[4]]
hf_list6 += [row[5]]
hf_list7 += [row[6]]
hf_list8 += [row[7]]
hf_list9 += [row[8]]
hf_list10 += [row[9]]


while row is not None:
    row = cnxn_cursor.fetchone()
    if row is not None:
        hf_list1 += [row[0]]
        hf_list2 += [row[1]]
        hf_list3 += [row[2]]
        hf_list4 += [row[3]]
        hf_list5 += [row[4]]
        hf_list6 += [row[5]]
        hf_list7 += [row[6]]
        hf_list8 += [row[7]]
        hf_list9 += [row[8]]
        hf_list10 += [row[9]]


# delete entries with IsActive = 1

sql = "DELETE FROM OrderForecast_Facts WHERE IsActive = 1;"
cnxn_cursor.execute(sql)

# write back old avtive entries to OrderForecast_Facts with IsActive = 0 and current time ToDate
to_date_to_write = DT.datetime.now()

sql = """
INSERT INTO OrderForecast_Facts
VALUES (?,?,?,?,?,?,?,?,?,?,?,?);
""" 

for i in range(len(hf_list1)):    
    tuple_to_write = (hf_list1[i],hf_list2[i],hf_list3[i],hf_list4[i],hf_list5[i],hf_list6[i],hf_list7[i],hf_list8[i],hf_list9[i],hf_list10[i],to_date_to_write,0)
    cnxn_cursor.execute(sql, tuple_to_write)


#########################################################################

# # # write new entries with IsActive = 1
max_old_id = max(hf_list1)
# max_old_id = 0
order_forecast_id_to_write = max_old_id + 1
load_date_to_write = to_date_to_write
# load_date_to_write = DT.datetime.now()
to_date_to_write = DT.datetime.strptime('2099-12-31 00:00:00.000000', '%Y-%m-%d %H:%M:%S.%f')
is_active_to_write = 1
#######################################################

# build order forecast date list
date_today = date.today()
datetime_today = datetime(date_today.year, date_today.month, date_today.day)

number_of_days = 504

order_forecast_date_list = []
for day in range(number_of_days)[1:]:
    next_date = (datetime_today + DT.timedelta(days = day))
    order_forecast_date_list.append(next_date)
    
# build list of lists for order forecast
lsd_list_of_lists = [lsd_date_list, lsd_item_no_list, lsd_original_qty_list, lsd_sage_customer_id_list, lsd_location_code_list, lsd_order_number_list, lsd_allocation_class_list]
fs_list_of_lists = [fs_order_date_list, fs_item_no_list, fs_original_qty_list, fs_sage_customer_id_list, fs_location_code_list, fs_allocation_class_list]


for a in [1,2,3,4,5,6,7,8]:

    allocation_class = a

    # order forecast
    c_tuple = GothamFunctions.orderForecast(allocation_class,lsd_list_of_lists, fs_list_of_lists, order_forecast_date_list)
    actual_orders_lists = c_tuple[0]
    expected_orders_dict = c_tuple[1]
    std_expected_orders_dict = c_tuple[2]
    live_orders_dict = c_tuple[3]


    sql_write = """
    INSERT INTO OrderForecast_Facts
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?);
    """ 

    # write expected_orders_dict to OrderForecast_Facts
    for order_date in expected_orders_dict.keys():
        order_date_to_write = order_date.date()
        weekday = order_date_to_write.weekday()
        days_before_allocation = 1
        if order_date_to_write.weekday() == 0:
            days_before_allocation = 3
        if order_date_to_write.weekday() == 6:
            days_before_allocation = 2

        order_allocation_date_to_write = order_date_to_write - DT.timedelta(days = days_before_allocation)

        for location_name in expected_orders_dict[order_date].keys():
            city = location_name
            facility_id_to_write = facility_id_list[location_code_list.index(location_name)]
            for sage_customer_id in expected_orders_dict[order_date][city].keys():
                dc = sage_customer_id
                customers_id_to_write = customers_id_list[sage_customer_id_list.index(sage_customer_id)]
                for crop_id in expected_orders_dict[order_date][city][dc].keys():
                    for item_number in expected_orders_dict[order_date][city][dc][crop_id].keys():
                        item_id_to_write = spd_item_id_list[spd_item_no_list.index(item_number)]
                        expected_order_qty_to_write = int(np.ceil(expected_orders_dict[order_date][city][dc][crop_id][item_number]))
                        std_expected_order_qty_to_write = round(std_expected_orders_dict[order_date][city][dc][crop_id][item_number],2)
                        live_order_qty_to_write = int(live_orders_dict[order_date][city][dc][crop_id][item_number])
                        if expected_order_qty_to_write > 0:
                            tuple_to_insert = (order_forecast_id_to_write, order_date_to_write, facility_id_to_write, customers_id_to_write, item_id_to_write, expected_order_qty_to_write, std_expected_order_qty_to_write, live_order_qty_to_write, order_allocation_date_to_write, load_date_to_write, to_date_to_write, is_active_to_write)
                            # write to database
                            cnxn_cursor.execute(sql_write, tuple_to_insert)
                            order_forecast_id_to_write += 1
                            

cnxn.commit()
cnxn_cursor.close()
cnxn.close()
                            


# In[2]:


print('pau')


# In[ ]:




