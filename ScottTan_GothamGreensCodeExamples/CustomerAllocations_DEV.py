#!/usr/bin/env python
# coding: utf-8

# ![](Gotham_Greens_Logo.png)
# 
# 
# 
# # CustomerAllocations_DEV
# 
# ## Written by Scott Tan
# 
# ## Last updated: 4/21/2022
# 
# 
# # Overview
# 
# ### This script performs allocations of available inventory to expected demand, expected harvest to expected demand, expected harvest to available inventory for hierarchical products defined by Operations and hierarchical customers defined by Sales. This script also calculates transfers for scheduled routes defined by Operations to maximize fill rate of top-tier customers.
# 
# 
# ## Inputs:
# 
# 
# 
# #### Descriptions from Sales:
# 1. Invoiced Orders (InvoicedSales_Facts)
# 2. Live Sales (LiveSales_Facts)
# 3. Customer Hierarchy (CustomerFillGoal_Dim, Customers_Dim)
# 
# 
# 
# 
# #### Descriptions from Operations:
# 1. Inventory of Finished Goods (Inventory_Facts, inactive InventoryAllocation_Facts)
# 2. Historical production of Finished Goods (GreenhouseYields_Facts)
# 3. Product Hierarchy (Products_Dim)
# 4. Location Hierarchy (Greenhouses_Dim)
# 5. Planned Transfers (PlannedTransfers_Facts)
# 
# 
# 
# #### Descriptions from Growing:
# 1. Crop Schedule (CropSchedule_Facts)
# 2. Crop Categories (Crop_Dim)
# 
# 
# 
# #### Predictions for Sales and Operations:
# 1. Order Forecast (OrderForecast_Facts)
# 2. Customer Demand Forecast (CustomerDemandForecast_Facts)
# 
# #### Predictions for Growing and Operations:
# 1. Harvest Forecast (HarvestForecastSeasonality_Facts)
# 
# 
# ## Algorithm:
# 1. Import functions
# 2. Check if customer allocations have yet been completed and if actual initial Inventory has been uploaded for the day
# When initial Inventory is up to date and Customer Allocations are to be completed:
#     1. Load data
#     2. Initialize inventory and first stop sell
#     3. Main Loop - for demand date, customer allocation tier, and product allocation tier
#         - Roll allocated harvest to starting inventory (starting Day 2 on)
#         - Determine stop sell (final customer tier)
#         - Allocate Inventory to Product Sold (write CustomerInventoryAllocation_Facts)
#         - Allocate Harvest to Product Sold (write CustomerHarvestAllocation_Facts)
#         - Allocate Harvest to Inventory (write CustomerHarvestAllocation_Facts, CustomerShortDemand_Facts)
#         - Allocate Harvest from prior days to Product Sold
#     4. Pending Loop with Calculated Transfers - for demand date, customer allocation tier, and product allocation tier
#         - Roll allocated harvest to starting inventory (starting Day 2 on)
#         - Determine stop sell (final customer tier)
#         - Allocate Inventory to Product Sold (write CustomerInventoryAllocation_Facts)
#         - Allocate Harvest to Product Sold (write CustomerHarvestAllocation_Facts)
#         - Allocate Harvest to Inventory (write CustomerHarvestAllocation_Facts, CustomerShortDemand_Facts)
#         - Allocate Harvest from prior days to Product Sold
#         - Calculate transfers for remaining Product Sold
# 
# 
# ## Outputs:
# 
# #### Prescriptions:
# 1. Inventory allocation to product sold (CustomerInventoryAllocation_Facts)
# 2. Harvest allocation product sold (CustomerHarvestAllocation_Facts)
# 3. Harvest allocation to future inventory (CustomerHarvestAllocation_Facts)
# 4. Calculated transfers (CalculatedTransfers_Facts)
# 
# #### Predictions:
# 1. Short Demand (CustomerShortDemand_Facts)
# 2. Inventory Forecast (StopSell_Facts, Allocated_Facts)

# In[1]:



# import functions

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
from timezonefinder import TimezoneFinder
from pytz import timezone, utc
import time
#import GothamFunctions

debug_status = 0

#CustomerInventoryAllocation_Facts
def customerInventoryAllocation(forecast_date, inventory_out_LoL, demand_in_LoL, facilities_LoL,inv_transfers_LoL, tier_count):
    '''
    #### Inputs:
    - forecast_date: date to compare to the demand allocation date
    - inventory_out_LoL: list of four lists containing expected inventory that is not expiring
        1. List of greenhouse IDs
        2. List of enjoy-by-dates
        3. List of product IDs
        4. List of inventory quantities cooresponding to each combination of greenhouse/product/enjoy-by-date
    - demand_in_LoL: list of five lists containing demand for the customer tier
        1. List of demand dates
        2. List of demand allocation dates
        3. List of greenhouse IDs
        4. List of product IDs
        5. List of customer IDs
        6. List of demand quantities
    - facilities_LoL: list of two lists cooresponding to greenhouses dimension
        1. List of greenhouse IDs
        2. List of city abbreviations
     - inv_transfers_LoL: transfers information as a list of seven lists
        1. List of ship dates
        2. List of arrival dates
        3. List of ship greenhouse IDs
        4. List of arrival greenhouse IDs
        5. List of product IDs
        6. List of enjoy-by-dates
        7. List of transfer quantites
    - tier_count: integer tier count for inventory allocation

        
    #### Algorithm:
    - load inputs
    - initialize outputs
    - loop through customer demand forecast and allocate from inventory if demand allocation date equals forecast date
        - check for products in the city and build lists of relevant inventory quantities
        - sort relevant inventory quantites by accending enjoy-by-date
        - while demand quantity is positive and there is inventory to allocate
            - check the first entry of sorted relevant inventory quantites and allocate to demand
            - if there is still remaining demand, allocate the entire quantity to partially fulfill demand
            - if there is no remaining demand, allocate inventory quantity partially to fulfill demand partially or fully
        - if we run out of items in inventory to fulfill the demand, add the order to the lists of remaining demand
    - return the output tuple
    
    #### Output: (inventory_allocation_out_LoL, inventory_demand_out_LoL)
    - inventory_allocation_out_LoL: list of seven lists containing inventory allocation
        1. List of greenhouse IDs
        2. List of product IDs
        3. List of enjoy-by-dates
        4. List of customer IDs
        5. List of start-of-day quantities cooresponding to each combination of greenhouse/product/enjoy-by-date
        6. List of allocated quantities cooresponding to each combination of greenhouse/product/enjoy-by-date/customer
        7. List of end-of-day quantities cooresponding to each combination of greenhouse/product/enjoy-by-date
    - inventory_demand_out_LoL: list of seven lists containing remaining demand after inventory allocation
        1. List of demand date
        2. List of demand allocation date
        3. List of greenhouse IDs
        4. List of product IDs
        5. List of customer IDs
        6. List of short demand quantities cooresponding to each combination of demand date/greenhouse/product/customer
        7. List of production priorities cooresponding to each combination of demand date/greenhouse/product/customer
    '''
    

    
    # initialize lists for pre-allocation inventory
    iaf_inventory_facility_id_list = inventory_out_LoL[0]
    iaf_product_id_list = inventory_out_LoL[1]
    iaf_enjoy_by_date_list = inventory_out_LoL[2]
    iaf_start_of_day_qty_list = inventory_out_LoL[3]


    # initialize end of day qty list
    iaf_end_of_day_qty_list = list()
    for i in range(len(iaf_start_of_day_qty_list)):
        iaf_end_of_day_qty_list += [iaf_start_of_day_qty_list[i]]
    
    # initialize lists to be updated as we allocate products from inventory
    # greenhouse/product/enjoy-by-date can have multiple customers (multiple rows in CustomerInventoryAllocation_Facts)
    # customer IDs and allocated qty will be stored as list within list
    iaf_customer_id_list = [0] * len(iaf_end_of_day_qty_list)
    iaf_allocated_qty_list = [0] * len(iaf_end_of_day_qty_list)
    
    # initialize lists for demand 
    df_demand_date_list = demand_in_LoL[0]
    df_demand_allocation_date_list = demand_in_LoL[1]
    df_facility_id_list = demand_in_LoL[2]
    df_product_id_list = demand_in_LoL[3]
    df_customer_id_list = demand_in_LoL[4]
    df_demand_qty_list = demand_in_LoL[5]
    df_rollover_qty_list = demand_in_LoL[6]
    df_safety_stock_qty_list = demand_in_LoL[7]
    
    # initialize lists for ShortDemand_Facts
    sdf_demand_date_list = list()
    sdf_demand_allocation_date_list = list()
    sdf_demand_facility_id_list = list()
    sdf_product_id_list = list()
    sdf_customer_id_list = list()
    sdf_short_demand_qty_list = list()
    sdf_roll_qty_list = list()
    sdf_production_priority_list = list()
    
    # initialize facilities dimension
    fd_facility_id_list = facilities_LoL[0]
    fd_city_short_code_list = facilities_LoL[1]
    
    #transfers
    tsf_ship_date_list = inv_transfers_LoL[0]
    tsf_arrival_date_list = inv_transfers_LoL[1]
    tsf_ship_facility_id_list = inv_transfers_LoL[2]
    tsf_arrival_facility_id_list = inv_transfers_LoL[3]
    tsf_product_id_list = inv_transfers_LoL[4]
    tsf_enjoy_by_date_list = inv_transfers_LoL[5]
    tsf_transfer_qty_list = inv_transfers_LoL[6]
    
    # planned transfers
    if tier_count == 1:
        
        # planned transfers on ship date
        tsf_sd_indices = [i for i, x in enumerate(tsf_ship_date_list) if x == forecast_date]
        # loop through planned transfers to allocate from inventory
        for tsf_idx in tsf_sd_indices:
            tsf_facility_id = tsf_ship_facility_id_list[tsf_idx]
            tsf_product_id = tsf_product_id_list[tsf_idx]
            tsf_enjoy_by_date = tsf_enjoy_by_date_list[tsf_idx]
            tsf_transfer_qty = tsf_transfer_qty_list[tsf_idx]

            iaf_fid_indices = [i for i, x in enumerate(iaf_inventory_facility_id_list) if fd_city_short_code_list[fd_facility_id_list.index(x)] == fd_city_short_code_list[fd_facility_id_list.index(tsf_facility_id)]]        
            iaf_pid_indices = [i for i, x in enumerate(iaf_product_id_list) if x == tsf_product_id]
            iaf_ebd_indices = [i for i, x in enumerate(iaf_enjoy_by_date_list) if x == tsf_enjoy_by_date]

            iaf_fpe_indices = list(set(iaf_fld_indices) & set(iaf_pid_indices) & set(iaf_ebd_indices))
            
            if len(iaf_pfe_indices) > 0:
                # allocate the inventory to the planned transfer
                cccif_idx = iaf_pfe_indices[0]
                cccif_qty = iaf_end_of_day_qty_list[cccif_idx]
                newif_qty = cccif_qty - tsf_transfer_qty
                
                if newif_qty >= 0:

                    iaf_allocated_qty_list[cccif_idx] = [tsf_transfer_qty]
                    iaf_end_of_day_qty_list[cccif_idx] = [newif_qty]
                    iaf_customer_id_list[cccif_idx] = [None]
                    
                if newif_qty < 0:
                    print('Failed planned transfer for Product: ', tsf_product_id, 'Enjoy-By: ', tsf_enjoy_by_date, 'qty: ', tsf_transfer_qty, 'inv: ', cccif_qty)

            if len(iaf_fpe_indices) == 0:
                # print warning for unavailable planned transfer
                print('Failed planned transfer for Product: ', tsf_product_id, 'Enjoy-By: ', tsf_enjoy_by_date, 'qty: ', tsf_transfer_qty)

            ### TO DO ####

    # loop through demand forecast and allocate from inventory
    for df_idx in range(len(df_demand_date_list)):
        demand_date = df_demand_date_list[df_idx]
        demand_allocation_date = df_demand_allocation_date_list[df_idx]
        demand_facility_id = df_facility_id_list[df_idx]
        product_id = df_product_id_list[df_idx]
        customer_id = df_customer_id_list[df_idx]
        total_demand_qty = df_demand_qty_list[df_idx]
        rollover_qty = df_rollover_qty_list[df_idx]
        safety_stock_qty = df_safety_stock_qty_list[df_idx]

#         if product_id == 74 and demand_facility_id == 7 and customer_id == 636:
#             print('debug', total_demand_qty, rollover_qty, safety_stock_qty)
        
        
        if demand_allocation_date == forecast_date:

            # allocate only to product sold from inventory
            roll_qty = rollover_qty + safety_stock_qty
            demand_qty = total_demand_qty - roll_qty
        
            
            product_id_indices = [j for j, x in enumerate(iaf_product_id_list) if x == product_id]
            # inventory facility ID's matching demand_facility_id
            #inventory_facility_id_indices = [k for k, y in enumerate(cccif_facility_id_list) if y == demand_facility_id] 
            inventory_facility_id_indices = [k for k, y in enumerate(iaf_inventory_facility_id_list) if fd_city_short_code_list[fd_facility_id_list.index(y)] == fd_city_short_code_list[fd_facility_id_list.index(demand_facility_id)]]
 
            
            product_facility_indices = list(set(product_id_indices) & set(inventory_facility_id_indices))
 
            check_enjoy_by_date_list = list()
            check_quantity_list = list()
            check_cccif_indices_list = list()


            for check_idx in product_facility_indices:
                if iaf_end_of_day_qty_list[check_idx] != 0:        
                    check_enjoy_by_date_list += [iaf_enjoy_by_date_list[check_idx]]
                    check_quantity_list += [iaf_end_of_day_qty_list[check_idx]]
                    check_cccif_indices_list += [check_idx]
                    
            
            # sort relevant inventory by accending enjoy by date
            sorted_check_enjoy_by_date_list = list(np.sort(check_enjoy_by_date_list))
            sorted_check_quantity_list = list([x for _,x in sorted(zip(check_enjoy_by_date_list,check_quantity_list))])
            sorted_check_cccif_indices_list = list([y for _,y in sorted(zip(check_enjoy_by_date_list,check_cccif_indices_list))])

            
            # try allocation while there is demand and inventory
            while demand_qty > 0 and len(sorted_check_enjoy_by_date_list) > 0:
                
                
                # check the first entry in the check lists
                inventory_qty = sorted_check_quantity_list[0]
                enjoy_by_date = sorted_check_enjoy_by_date_list[0]
                cccif_idx = sorted_check_cccif_indices_list[0]
                
                # check the qty is non-zero
                if inventory_qty == 0:
                    # delete the entry from inventory lists
                    del sorted_check_quantity_list[0]
                    del sorted_check_enjoy_by_date_list[0]
                    del sorted_check_cccif_indices_list[0]
                    
                    
                    
                # allocate inventory to demand
                new_demand_qty = demand_qty - inventory_qty
                if new_demand_qty > 0 and inventory_qty > 0:
                    # there is still remaining demand, so allocate the entire quantity to partially fulfill demand

                    #iaf_allocated_qty_list[cccif_idx] += inventory_qty
                    #original_starting_qty = iaf_start_of_day_qty_list[cccif_idx]
                    original_starting_qty = inventory_qty
                        
                    
                    # already an allocation for the inventory qty
                    if type(iaf_allocated_qty_list[cccif_idx]) == list:
                        iaf_allocated_qty_list[cccif_idx] += [original_starting_qty]
                        iaf_end_of_day_qty_list[cccif_idx] = 0
                        iaf_customer_id_list[cccif_idx] += [customer_id]
                        
                     # first allocation for the inventory qty
                    if iaf_allocated_qty_list[cccif_idx] == 0:
                        iaf_allocated_qty_list[cccif_idx] = [original_starting_qty]
                        iaf_end_of_day_qty_list[cccif_idx] = 0
                        iaf_customer_id_list[cccif_idx] = [customer_id]
                    
                    # delete the entry from inventory lists
                    del sorted_check_quantity_list[0]
                    del sorted_check_enjoy_by_date_list[0]
                    del sorted_check_cccif_indices_list[0]
                    
                    # update variables
                    demand_qty = new_demand_qty
                    inventory_qty = 0

                if new_demand_qty <= 0 and inventory_qty > 0:
                    # there is no remaining demand, so allocate inventory quantity partially to fulfill demand partially or fully
                    remaining_inventory = -new_demand_qty # remaining_inventory will be 0 if demand quantity is met exactly
                    sorted_check_quantity_list[0] = remaining_inventory
                    
                    
                    # already an allocation for the inventory qty
                    if type(iaf_allocated_qty_list[cccif_idx]) == list:                    
                        iaf_allocated_qty_list[cccif_idx] += [demand_qty]
                        iaf_end_of_day_qty_list[cccif_idx] = remaining_inventory
                        iaf_customer_id_list[cccif_idx] += [customer_id]
                        
                    # first allocation for the inventory qty
                    if iaf_allocated_qty_list[cccif_idx] == 0:
                        iaf_allocated_qty_list[cccif_idx] = [demand_qty]
                        iaf_end_of_day_qty_list[cccif_idx] = remaining_inventory
                        iaf_customer_id_list[cccif_idx] = [customer_id]
                    
                    # stop allocation loop since allocation is complete for the demand 
                    demand_qty = 0
                    inventory_qty = remaining_inventory
                    
                # while loop will continue as long as demand_qty is positive and there are entries in check_enjoy_by_date_list

            #add the order to lists for harvest allocation
            sdf_demand_date_list += [demand_date]
            sdf_demand_allocation_date_list += [demand_allocation_date]
            sdf_demand_facility_id_list += [demand_facility_id]
            sdf_product_id_list += [product_id]
            sdf_customer_id_list += [customer_id]
            sdf_short_demand_qty_list += [demand_qty]
            sdf_roll_qty_list += [roll_qty]
            sdf_production_priority_list += [pd_production_priority_list[pd_product_id_list.index(product_id)]]

    inventory_allocation_out_LoL = [iaf_inventory_facility_id_list,
                                    iaf_product_id_list,
                                    iaf_enjoy_by_date_list,
                                    iaf_customer_id_list,
                                    iaf_start_of_day_qty_list,
                                    iaf_allocated_qty_list,
                                    iaf_end_of_day_qty_list]
    
    # compress short demand by demand date, facility, product, customer
    csdf_demand_date_list = list()
    csdf_demand_allocation_date_list = list()
    csdf_demand_facility_id_list = list()
    csdf_product_id_list = list()
    csdf_customer_id_list = list()
    csdf_short_demand_qty_list = list()
    csdf_roll_qty_list = list()
    csdf_production_priority_list = list()
    
    sdf_key_list = list()
    
    for sdf_idx in range(len(sdf_demand_date_list)):
        sdf_dd = sdf_demand_date_list[sdf_idx]
        sdf_fid = sdf_demand_facility_id_list[sdf_idx]
        sdf_pid = sdf_product_id_list[sdf_idx]
        sdf_cid = sdf_customer_id_list[sdf_idx]
        
        sdf_key = str(sdf_dd) + '_' + str(sdf_fid)  + '_' + str(sdf_pid)  + '_' + str(sdf_cid)
        
        if sdf_key in sdf_key_list:
            csdf_short_demand_qty_list[sdf_key_list.index(sdf_key)] += sdf_short_demand_qty_list[sdf_idx]
            csdf_roll_qty_list[sdf_key_list.index(sdf_key)] += sdf_roll_qty_list[sdf_idx]
        if sdf_key not in sdf_key_list:
            
            sdf_key_list += [sdf_key]
            
            csdf_demand_date_list += [sdf_dd]
            csdf_demand_allocation_date_list += [sdf_demand_allocation_date_list[sdf_idx]]
            csdf_demand_facility_id_list += [sdf_fid]
            csdf_product_id_list += [sdf_pid]
            csdf_customer_id_list += [sdf_cid]
            csdf_short_demand_qty_list += [sdf_short_demand_qty_list[sdf_idx]]
            csdf_roll_qty_list += [sdf_roll_qty_list[sdf_idx]]
            csdf_production_priority_list += [sdf_production_priority_list[sdf_idx]]
    

    inventory_demand_out_LoL =  [csdf_demand_date_list,
                                 csdf_demand_allocation_date_list,
                                 csdf_demand_facility_id_list,
                                 csdf_product_id_list,
                                 csdf_customer_id_list,
                                 csdf_short_demand_qty_list,
                                 csdf_roll_qty_list,
                                 csdf_production_priority_list
                                 ]
            
    return (inventory_allocation_out_LoL, inventory_demand_out_LoL)


def writeCustomerInventoryAllocation(forecast_date,inventory_allocation_out_LoL,tier_count, is_pending = 0):
    '''
    #### Inputs:
    - forecast_date: date of the forecast
    - inventory_allocation_out_LoL: list of six lists containing inventory allocation
        1. List of greenhouse IDs
        2. List of product IDs
        3. List of enjoy-by-dates
        4. List of customer IDs
        5. List of start-of-day quantities cooresponding to each combination of greenhouse/product/enjoy-by-date
        6. List of allocated quantities cooresponding to each combination of greenhouse/product/enjoy-by-date/customer
        7. List of end-of-day quantities cooresponding to each combination of greenhouse/product/enjoy-by-date
    - tier_count: integer tier count for inventory allocation
    - is_pending: integer boolean 1 for Pending table or 0 for baseline table
        
    #### Algorithm:
    - load inputs
    - loop through inventory allocation lists and insert entries into CustomerInventoryAllocation_Facts
    - return string indicating completion
    #### Output: string indicating completion
    '''

    # connect to database
    HOSTNAME = socket.gethostname()

    if HOSTNAME == 'hostname':
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server}; 
                                Server=127.0.0.1,1443;
                                Database=databasename;
                                trusted_connection=yes""" # use windows auth on DB01

    else:
        with open(os.path.join(sys.path[0], "config.yml"), 'r') as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.SafeLoader)
    #    uid = cfg['databasename']['uid']
        uid = 'sa'
        pwd = cfg['databasename']['pwd'][:-3]
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server};
                                Server=hostname\MSSQLSERVER1;
                                Database=databasename;
                                UID=%s;
                                PWD=%s;""" % (uid, pwd) # use config.yml on local machine



    cnxn = pyodbc.connect(CONNECTIONSTRING)   
    cnxn_cursor = cnxn.cursor()
    
    sql = """
    SELECT MAX(CustomerInventoryAllocationID) FROM CustomerInventoryAllocation_Facts
    """
    cnxn_cursor.execute(sql)
    
    max_old_id = cnxn_cursor.fetchone()[0]
    if max_old_id is None:
        max_old_id = 0
    inventory_allocation_id = max_old_id + 1


    load_date = DT.datetime.now()
    to_date = DT.datetime.strptime('2099-12-31 00:00:00.000000', '%Y-%m-%d %H:%M:%S.%f')
    is_active = 1

    sql = """
    INSERT INTO CustomerInventoryAllocation_Facts
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);
    """ 
    
    if is_pending == 1:
        
        sql = """
        SELECT MAX(CustomerInventoryAllocationPendingID) FROM CustomerInventoryAllocationPending_Facts
        """
        cnxn_cursor.execute(sql)

        max_old_id = cnxn_cursor.fetchone()[0]
        if max_old_id is None:
            max_old_id = 0
        inventory_allocation_id = max_old_id + 1

        sql = """
        INSERT INTO CustomerInventoryAllocationPending_Facts
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);
        """ 

    iaf_inventory_facility_id_list = inventory_allocation_out_LoL[0]
    iaf_product_id_list = inventory_allocation_out_LoL[1]
    iaf_enjoy_by_date_list = inventory_allocation_out_LoL[2]
    iaf_customer_id_list = inventory_allocation_out_LoL[3]
    iaf_start_of_day_qty_list = inventory_allocation_out_LoL[4]
    iaf_allocated_qty_list = inventory_allocation_out_LoL[5]
    iaf_end_of_day_qty_list = inventory_allocation_out_LoL[6]

    for iaf_idx in range(len(iaf_inventory_facility_id_list)):        
        inventory_facility_id = iaf_inventory_facility_id_list[iaf_idx]
        product_id = iaf_product_id_list[iaf_idx]                
        enjoy_by_date = iaf_enjoy_by_date_list[iaf_idx]
        customer_id = iaf_customer_id_list[iaf_idx]
        start_of_day_qty = iaf_start_of_day_qty_list[iaf_idx]
        allocated_qty = iaf_allocated_qty_list[iaf_idx]
        end_of_day_qty = iaf_end_of_day_qty_list[iaf_idx]
        
        if allocated_qty == 0:
            tuple_to_write = (inventory_allocation_id,
                              forecast_date,
                              inventory_facility_id,
                              product_id,
                              enjoy_by_date,
                              customer_id,
                              start_of_day_qty,
                              allocated_qty,
                              end_of_day_qty,
                              tier_count,
                              load_date,
                              to_date,
                              is_active)
            cnxn_cursor.execute(sql, tuple_to_write)
            inventory_allocation_id += 1
        if type(allocated_qty) == list:
            for aq_idx in range(len(allocated_qty)):
                aq = allocated_qty[aq_idx]
                cid = customer_id[aq_idx]
                tuple_to_write = (inventory_allocation_id,
                                  forecast_date,
                                  inventory_facility_id,
                                  product_id,
                                  enjoy_by_date,
                                  cid,
                                  start_of_day_qty,
                                  aq,
                                  end_of_day_qty,
                                  tier_count,
                                  load_date,
                                  to_date,
                                  is_active)
                cnxn_cursor.execute(sql, tuple_to_write)
                inventory_allocation_id += 1

                

    cnxn.commit()
    cnxn_cursor.close()
    cnxn.close()
    
    return 'CustomerInventoryAllocation_Facts for ' + str(forecast_date) + ' pau'



#     #CustomerHarvestAllocation_Facts
def customerHarvestAllocation(forecast_date, harvest_in_LoL, inventory_demand_out_LoL, facilities_LoL,allocated_crops_LoL, har_transfers_LoL, products_LoL, inventory_allocation_LoL, tier_count):
    '''
    #### Inputs:
    - forecast_date: date of the forecast
    - harvest_in_LoL: list of seven lists cooresponding to harvest
        1. List of harvest dates
        2. List of greenhouse IDs
        3. List of greenhouse line IDs
        4. List of crop IDs
        5. List of expected plant sites
        6. List of average headweights
        7. List of loose grams per plant site
    - inventory_demand_out_LoL: list of six lists cooresponding to remaining demand to allocate towards from harvest
        1. List of demand dates
        2. List of demand allocation dates
        3. List of greenhouse IDs
        4. List of product IDs
        5. List of customer IDs
        6. List of short demand quantities cooresponding to each combination of demand date/greenhouse/product/customer
        7. List of production priorities cooresponding to each combination of demand date/greenhouse/product
    - facilities_LoL: list of two lists cooresponding to greenhouses dimension
        1. List of greenhouse IDs
        2. List of city abbreviations
    - allocated_crops_LoL: list of three lists for mid-allocation tracking
        1. List of date/crop/greenhouse key combinations that have an allocation
        2. List of starting plant sites for each date/crop/greenhouse key combination
        3. List of allocated plant sites for each date/crop/greenhouse key combination
        4. List of date/crop/greenhouse key combinations that have been fully allocated
    - har_transfers_LoL: transfers information as a list of seven lists
        1. List of ship dates
        2. List of arrival dates
        3. List of ship greenhouse IDs
        4. List of arrival greenhouse IDs
        5. List of product IDs
        6. List of enjoy-by-dates
        7. List of transfer quantites
    - products_LoL: products information as list of five lists
        1. List of product IDs
        2. List of stop sell days
        3. List of crop IDs
        4. List of net weight grams
        5. List of is whole boolean flags
    - inventory_allocation_LoL: list of six lists containing inventory allocation
        1. List of greenhouse IDs
        2. List of product IDs
        3. List of enjoy-by-dates
        4. List of customer IDs
        5. List of start-of-day quantities cooresponding to each combination of greenhouse/product/enjoy-by-date
        6. List of allocated quantities cooresponding to each combination of greenhouse/product/enjoy-by-date/customer
        7. List of end-of-day quantities cooresponding to each combination of greenhouse/product/enjoy-by-date
    - tier_count: allocation tier for the harvest allocation from harvest city to customer  
    
    #### Algorithm:
    - load inputs
    - initialize outputs
    - loop through scheduled transfers
        - compute transfer allocation day for the transfer ship date
        - check if transfer allocation day for the transfer ship date matches the forecast date
        - check if harvest is available to allocate for the date/crop/greenhouse on the transfer allocation day 
        - allocate available harvest to outbound transfer
    - loop through production priorities
        - loop through remaining demand where demand allocation date equals forecast date and demand production priority equals production priority
            - check if the remaining demand is still positive
            - if production priority is 5 (Gotham Foods), incorporate lead time into demand allocation date
            - if demand greenhouse is not main cooler greenhouse for the city, change the demand greenhouse to the main cooler greenhouse (e.g. change NYC2 to NYC3)
            - check if allocation for the date/crop/greenhouse cooresponding to the demand has harvest available to allocate
            - compute the mean grams per plant site to expect for the date/crop/greenhouse
            - convert the demand quantity into the equivalent plant sites required to meet the demand
            - compute remaining plant sites that are available considering all allocations thusfar (including potential allocations to products of the same date/crop/greenhouse/production priority)
            - attempt to allocate remaining demand from remaining plant sites that are available
            - if demand exceeds harvest, allocate partially to all products of the same production priority with the same harvest allocation ratio
                - compute all short demand plant sites for all products cooresponding to the same date/crop/greenhouse/production priority
                - compute all available plant sites considering no allocations to any products of the same production priority
                - compute harvest allocation ratio: all available plant sites / short demand plant sites
                - allocate demand multiplied by the harvest allocation ratio for each product with to the same date/crop/greenhouse/production priority
                - compute remaining short demand quantities after partial allocations
            - if harvest can meet the demand, add date/product/greenhouse key and the demand quantity to mid-allocation tracking lists
        - after looping through all remaining demand for the production priority, allocate fully to the demand for each date/product/greenhouse in mid-allocation tracking lists
    - return output tuple
    
    #### Output: (harvest_allocation_out_LoL,harvest_unallocated_out_LoL, short_demand_out_LoL)
    - harvest_allocation_out_LoL: list of twelve lists cooresponding to harvest allocations
        1. List of demand allocation dates
        2. List of demand dates
        3. List of harvest greenhouse IDs
        4. List of demand greenhouse IDs
        5. List of crop IDs
        6. List of product IDs
        7. List of customer IDs
        8. List of forecasted grams per plant site values
        9. List of allocated plant sites
        10. List of allocated grams
        11. List of allocated quantities
        12. List of full packout boolean flags
    - harvest_unallocated_out_LoL: list of three lists for mid-allocation tracking
        1. List of date/crop/greenhouse key combinations that have an allocation
        2. List of allocated plant sites for each date/crop/greenhouse key combination
        3. List of date/crop/greenhouse key combinations that have been fully allocated
    - short_demand_out_LoL: list of seven lists cooresponding to short demand
        1. List of demand dates
        2. List of demand allocation dates
        3. List of greenhouse IDs
        4. List of product IDs
        5. List of customer IDs
        6. List of short demand quantities cooresponding to each combination of demand date/greenhouse/product
        7. List of production priorities cooresponding to each combination of demand date/greenhouse/product
    '''

    # input lists from harvest
    hfsf_harvest_date_list = harvest_in_LoL[0]
    hfsf_facility_id_list = harvest_in_LoL[1]
    hfsf_facility_line_id_list = harvest_in_LoL[2]
    hfsf_crop_id_list = harvest_in_LoL[3]
    hfsf_expected_plant_sites_list = harvest_in_LoL[4]
    hfsf_avg_headweight_list = harvest_in_LoL[5]
    hfsf_loose_grams_per_plant_site_list = harvest_in_LoL[6]
    
    # input lists from demand
    sdf_demand_date_list = inventory_demand_out_LoL[0]
    sdf_demand_allocation_date_list = inventory_demand_out_LoL[1]
    sdf_demand_facility_id_list = inventory_demand_out_LoL[2]
    sdf_product_id_list = inventory_demand_out_LoL[3]
    sdf_customer_id_list = inventory_demand_out_LoL[4]
    sdf_short_demand_qty_list = inventory_demand_out_LoL[5]
    sdf_roll_qty_list = inventory_demand_out_LoL[6]
    sdf_production_priority_list = inventory_demand_out_LoL[7]
    
    # product dimension
    pd_product_id_list = products_LoL[0]
    pd_shelf_life_guarantee_list = products_LoL[1]
    pd_crop_id_list = products_LoL[2]
    pd_net_weight_grams_list = products_LoL[3]
    pd_is_whole_list = products_LoL[4]
    
    # facilities dimension
    fd_facility_id_list = facilities_LoL[0]
    fd_city_short_code_list = facilities_LoL[1]
    
    # mid-allocation tracking lists
    allocated_date_crop_facility_key_list = allocated_crops_LoL[0]
    allocated_starting_ps_list = allocated_crops_LoL[1]
    allocated_plant_sites_list = allocated_crops_LoL[2]
    complete_crop_allocation_key_list = allocated_crops_LoL[3]
    
    #transfers
    tsf_ship_date_list = har_transfers_LoL[0]
    tsf_arrival_date_list = har_transfers_LoL[1]
    tsf_ship_facility_id_list = har_transfers_LoL[2]
    tsf_arrival_facility_id_list = har_transfers_LoL[3]
    tsf_product_id_list = har_transfers_LoL[4]
    tsf_enjoy_by_date_list = har_transfers_LoL[5]
    tsf_transfer_qty_list = har_transfers_LoL[6]
    
    # remaining inventory
    iaf_inventory_facility_id_list = inventory_allocation_out_LoL[0]
    iaf_product_id_list = inventory_allocation_out_LoL[1]
    iaf_enjoy_by_date_list = inventory_allocation_out_LoL[2]
    iaf_customer_id_list = inventory_allocation_out_LoL[3]
    iaf_end_of_day_qty_list = inventory_allocation_out_LoL[6]

    ri_key_list = list()
    ri_qty_list = list()
    for iaf_idx in range(len(iaf_inventory_facility_id_list)):        
        inventory_facility_id = iaf_inventory_facility_id_list[iaf_idx]
        product_id = iaf_product_id_list[iaf_idx]                
        end_of_day_qty = iaf_end_of_day_qty_list[iaf_idx]
        
        ri_key = str(inventory_facility_id) + '_' + str(product_id)
        if ri_key in ri_key_list:
            ri_qty_list[ri_key_list.index(ri_key)] += end_of_day_qty
        if ri_key not in ri_key_list:
            ri_key_list += [ri_key]
            ri_qty_list += [end_of_day_qty]
            
    # these lists will track the delta of harvest lists through the allocation process
    
    allocated_date_product_facility_key_list = list()
    allocated_gpps_list = list()
    allocated_qty_list = list()
    allocated_product_plant_sites_list = list()
    
    complete_product_allocation_key_list = list()
    complete_customer_allocation_key_list = list()
    
    allocated_date_product_facility_customer_key_list = list()
    allocated_customer_gpps_list = list()
    allocated_customer_qty_list = list()
    allocated_customer_plant_sites_list = list()
    allocated_customer_roll_qty_list = list()
    allocated_customer_demand_date_list = list()
      
    # initialize lists for CustomerHarvestAllocation_Facts
    haf_demand_allocation_date_list = list()
    haf_demand_date_list = list()
    haf_harvest_facility_id_list = list()
    haf_demand_facility_id_list = list()
    haf_crop_id_list = list()
    haf_product_id_list = list()
    haf_customer_id_list = list()
    haf_forecasted_gpps_list = list()
    haf_allocated_plant_sites_list = list()
    haf_allocated_grams_list = list()
    haf_allocated_qty_list = list()
    haf_full_packout_list = list()
    

    # initialize lists for new ShortDemand_Facts
    new_sdf_demand_date_list = list()
    new_sdf_demand_allocation_date_list = list()
    new_sdf_demand_facility_id_list = list()
    new_sdf_product_id_list = list()
    new_sdf_customer_id_list = list()
    new_sdf_short_demand_qty_list = list()
    new_sdf_production_priority_list = list()
    
    # list to track completed short demand allocations
    sdf_idx_to_skip_list = list()

    # get distinct production priorities
    production_priority_list = [1,2,3,4,5]

    # begin main loop to compute the harvest allocation
    # start by looping through possible demand dates
    #for demand_date_idx in range(len(distinct_demand_date_list)):
    # demand_date_idx = 0

    # demand_date = distinct_demand_date_list[demand_date_idx]
    # demand_allocation_date = distinct_demand_allocation_date_list[demand_date_idx]
    demand_allocation_date_indices = [d for d, g in enumerate(sdf_demand_allocation_date_list) if g == forecast_date]
    
    
    ########################
    # transfer allocations
    if tier_count == 1:
        # allocate harvest to outbound transfers
        for tsf_idx in range(len(tsf_ship_date_list)):
            tsf_ship_date = tsf_ship_date_list[tsf_idx]
            # allocate from harvest to transfers a day before ship date
            tsf_ship_allocation_date = tsf_ship_date - DT.timedelta(days = 1)
            if tsf_ship_allocation_date.weekday() == 6:
                tsf_ship_allocation_date -= DT.timedelta(days = 2)

            if tsf_ship_allocation_date == forecast_date:
                # allocate outbound transfer
                harvest_date = tsf_ship_allocation_date
                demand_date = tsf_arrival_date_list[tsf_idx] # demand date defined as when transfer will arrive
                harvest_facility_id = tsf_ship_facility_id_list[tsf_idx]
                demand_facility_id = tsf_arrival_facility_id_list[tsf_idx] # demand facility defined as arrival facility
                product_id = tsf_product_id_list[tsf_idx]
                customer_id = None

                transfer_qty = tsf_transfer_qty_list[tsf_idx]

                # get values from Products_Dim
                pd_idx = pd_product_id_list.index(product_id)
                crop_id = pd_crop_id_list[pd_idx]
                net_weight_grams = pd_net_weight_grams_list[pd_idx]
                is_whole = pd_is_whole_list[pd_idx]

                # keys for allocation tracking
                allocated_date_crop_facility_key = str(harvest_date) + '_' + str(crop_id) + '_' + str(harvest_facility_id)
                allocated_date_product_facility_key = str(harvest_date) + '_' + str(product_id) + '_' + str(harvest_facility_id)
                allocated_date_product_facility_customer_key = str(harvest_date) + '_' + str(product_id) + '_' + str(harvest_facility_id) + '_' + str(customer_id)

                # checkpoint: we still can allocate for this date/crop/facility
                if allocated_date_crop_facility_key not in complete_crop_allocation_key_list:
                    # get plant sites already allocated for the facility
                    already_allocated_plant_sites = 0
                    if allocated_date_crop_facility_key in allocated_date_crop_facility_key_list:
                        already_allocated_plant_sites = allocated_plant_sites_list[allocated_date_crop_facility_key_list.index(allocated_date_crop_facility_key)]

                    # get other values from HarvestForecastSeasonality_Facts
                    harvest_date_indices =  [i for i, x in enumerate(hfsf_harvest_date_list) if x == harvest_date]
                    harvest_crop_id_indices =  [j for j, y in enumerate(hfsf_crop_id_list) if y == crop_id]
                    harvest_facility_id_indices = [k for k, z in enumerate(hfsf_facility_id_list)
                                                 if fd_city_short_code_list[fd_facility_id_list.index(z)]
                                                    == fd_city_short_code_list[fd_facility_id_list.index(harvest_facility_id)]]
                    harvest_date_crop_facility_indices = list(set(harvest_date_indices) & set(harvest_crop_id_indices) & set(harvest_facility_id_indices))
                    #print(len(harvest_date_crop_facility_indices))
                    # multiple lines are potentially available to allocate for each crop
                    # from the harvest forecast, determine if the short demand can be fulfilled or not
                    harvest_facility_line_id_list = list()
                    harvest_expected_plant_sites_list = list()
                    harvest_avg_headweight_list = list()
                    harvest_whole_gpps_list = list()
                    harvest_loose_gpps_list = list()
                    for hfsf_idx in harvest_date_crop_facility_indices:
                        # get available lines
                        harvest_facility_line_id_list += [hfsf_facility_line_id_list[hfsf_idx]]
                        # get total plant sites
                        harvest_expected_plant_sites_list += [hfsf_expected_plant_sites_list[hfsf_idx]]
                        # get whole grams per plant site
                        harvest_whole_gpps_list += [hfsf_avg_headweight_list[hfsf_idx]]
                        harvest_loose_gpps_list += [hfsf_loose_grams_per_plant_site_list[hfsf_idx]]
                    #print(allocated_date_crop_facility_key)
                    # checkpoint: there is available harvest
                    if len(harvest_expected_plant_sites_list) > 0:

                        # compute mean grams per plant site for the facility and round to two decimals
                        #harvest_facility_mean_whole_gpps = round(float(np.mean(harvest_whole_gpps_list)),2)
                        harvest_facility_std_whole_gpps = round(float(np.std(harvest_whole_gpps_list)),2)
                        #harvest_facility_mean_loose_gpps = round(float(np.mean(harvest_loose_gpps_list)),2)
                        harvest_facility_std_loose_gpps = round(float(np.std(harvest_loose_gpps_list)),2)

                        # GPPS normalized by expected plant sites
                        whole_numerator = 0
                        loose_numerator = 0
                        total_ps = 0
                        for harvest_idx in range(len(harvest_whole_gpps_list)):
                            whole_numerator += harvest_whole_gpps_list[harvest_idx] * harvest_expected_plant_sites_list[harvest_idx]
                            loose_numerator += harvest_loose_gpps_list[harvest_idx] * harvest_expected_plant_sites_list[harvest_idx]
                            total_ps += harvest_expected_plant_sites_list[harvest_idx]

                        harvest_facility_mean_whole_gpps = 0
                        harvest_facility_mean_loose_gpps = 0 
                        if total_ps != 0:
                            harvest_facility_mean_whole_gpps = round(float(whole_numerator/total_ps),2)
                            harvest_facility_mean_loose_gpps = round(float(loose_numerator/total_ps),2)

                        # choose conversion factor
                        harvest_facility_mean_gpps = harvest_facility_mean_loose_gpps 
                        if is_whole == 1:
                            harvest_facility_mean_gpps = harvest_facility_mean_whole_gpps

                        # compute net_plant_sites for the transfer
                        net_plant_sites = 0
                        if harvest_facility_mean_gpps != 0:
                            net_plant_sites = int(np.ceil(float(transfer_qty * net_weight_grams / harvest_facility_mean_gpps)))

                        # compute net plant sites for the facility
                        harvest_facility_net_plant_sites = int(sum(harvest_expected_plant_sites_list))
                        #print(harvest_expected_plant_sites_list)

                        # compute remaining plant sites for allocation considering the mid allocation checkpoint
                        harvest_facility_pre_plant_sites = harvest_facility_net_plant_sites - already_allocated_plant_sites

                        # ATTEMPT THE ALLOCATION
                        harvest_facility_post_plant_sites = harvest_facility_pre_plant_sites - net_plant_sites


                        # first deal with what to do when we cannot satisfy the transfer
                        if harvest_facility_post_plant_sites < 0:
                            # oh no! this transfer exceeds our harvest for the facility
                            print("WARNING: transfer request exceeds harvest capacity")
                            # set full_packout to true
                            full_packout = 1

                            forecasted_gpps = round(float(harvest_facility_mean_gpps),2)
                            allocated_product_plant_sites = harvest_facility_pre_plant_sites
                            allocated_qty = int(np.floor(harvest_facility_pre_plant_sites * forecasted_gpps / net_weight_grams))
                            allocated_grams = round(allocated_product_plant_sites * forecasted_gpps,2)

                            # write to lists for CustomerHarvestAllocation_Facts
                            haf_demand_allocation_date_list += [harvest_date]
                            haf_demand_date_list += [demand_date]
                            haf_harvest_facility_id_list += [harvest_facility_id]
                            haf_demand_facility_id_list += [demand_facility_id]
                            haf_crop_id_list += [crop_id]
                            haf_product_id_list += [product_id]
                            haf_customer_id_list += [customer_id]
                            haf_forecasted_gpps_list += [forecasted_gpps]
                            haf_allocated_plant_sites_list += [allocated_product_plant_sites]
                            haf_allocated_grams_list += [allocated_grams]
                            haf_allocated_qty_list += [allocated_qty]
                            haf_full_packout_list += [full_packout]

                            # update the allocation_lists
                            if allocated_date_crop_facility_key in allocated_date_crop_facility_key_list:
                                allocated_plant_sites_list[allocated_date_crop_facility_key_list.index(allocated_date_crop_facility_key)] += allocated_product_plant_sites
                            if allocated_date_crop_facility_key not in allocated_date_crop_facility_key_list:
                                allocated_date_crop_facility_key_list += [allocated_date_crop_facility_key]
                                allocated_plant_sites_list += [allocated_product_plant_sites]
                                allocated_starting_ps_list += [harvest_facility_net_plant_sites]

                            if allocated_date_product_facility_key in allocated_date_product_facility_key_list:
                                del allocated_gpps_list[allocated_date_product_facility_key_list.index(allocated_date_product_facility_key)]
                                del allocated_qty_list[allocated_date_product_facility_key_list.index(allocated_date_product_facility_key)]
                                del allocated_product_plant_sites_list[allocated_date_product_facility_key_list.index(allocated_date_product_facility_key)]
                                del allocated_date_product_facility_key_list[allocated_date_product_facility_key_list.index(allocated_date_product_facility_key)]

                            # customer allocation
                            if allocated_date_product_facility_customer_key in allocated_date_product_facility_customer_key_list:
                                del allocated_date_product_facility_customer_key_list[allocated_date_product_facility_customer_key_list.index(allocated_date_product_facility_customer_key)]

                            # mark allocation as complete
                            #complete_crop_allocation_key_list += [allocated_date_crop_facility_key]
                            #complete_product_allocation_key_list += [allocated_date_product_facility_key]
                            #complete_customer_allocation_key_list += [allocated_date_product_facility_customer_key]
                            # mark allocation as complete
                            if allocated_date_crop_facility_key not in complete_crop_allocation_key_list:
                                complete_crop_allocation_key_list += [allocated_date_crop_facility_key]
                            if allocated_date_product_facility_key not in complete_product_allocation_key_list:
                                complete_product_allocation_key_list += [allocated_date_product_facility_key]
                            if allocated_date_product_facility_customer_key not in complete_customer_allocation_key_list:
                                complete_customer_allocation_key_list += [allocated_date_product_facility_customer_key]


                        # next deal with what to do when we can successfully satisfy the transfer
                        if harvest_facility_post_plant_sites >= 0:
                            # yay! we have enough plant sites to cover the demand (so far)
                            # update the allocation lists
                            # add to mid-allocation tracker if they key exists
                            #print("yay")
                            if allocated_date_crop_facility_key in allocated_date_crop_facility_key_list:
                                allocated_plant_sites_list[allocated_date_crop_facility_key_list.index(allocated_date_crop_facility_key)] += net_plant_sites
                            # add to mid-allocation tracker if its a new key
                            if allocated_date_crop_facility_key not in allocated_date_crop_facility_key_list:
                                allocated_date_crop_facility_key_list += [allocated_date_crop_facility_key]
                                allocated_plant_sites_list += [net_plant_sites]
                                allocated_starting_ps_list += [harvest_facility_net_plant_sites]

                            # write transfer to CustomerHarvestAllocation_Facts
                            full_packout = 0

                            forecasted_gpps = round(float(harvest_facility_mean_gpps),2)
                            allocated_product_plant_sites = net_plant_sites
                            allocated_qty = transfer_qty
                            allocated_grams = round(allocated_product_plant_sites * forecasted_gpps,2)

                            # write to lists for HarvestAllocation_Facts
                            haf_demand_allocation_date_list += [harvest_date]
                            haf_demand_date_list += [demand_date]
                            haf_harvest_facility_id_list += [harvest_facility_id]
                            haf_demand_facility_id_list += [demand_facility_id]
                            haf_crop_id_list += [crop_id]
                            haf_product_id_list += [product_id]
                            haf_customer_id_list += [customer_id]
                            haf_forecasted_gpps_list += [forecasted_gpps]
                            haf_allocated_plant_sites_list += [allocated_product_plant_sites]
                            haf_allocated_grams_list += [allocated_grams]
                            haf_allocated_qty_list += [allocated_qty]
                            haf_full_packout_list += [full_packout]
                    else:
                        print('WARNING: no expected harvest for date_crop_facility: ', allocated_date_crop_facility_key)

    ###############################
    # demand allocations

    # loop through the production priorities
    
    # next loop through the production priorities
    for production_priority in production_priority_list:
        short_priority_indices = [c for c, f in enumerate(sdf_production_priority_list) if f == production_priority]
        date_priority_indices = list(set(demand_allocation_date_indices) & set(short_priority_indices))

        # next loop through the date_priority indicies in the short demand
        dp_roll_key_list = list()
        dp_roll_qty_list = list()
        dp_roll_ps_list = list()
        
        for sdf_idx in date_priority_indices:
            # checkpoint: we have not allocated this short demand yet
            if sdf_idx not in sdf_idx_to_skip_list:

                # get values from short demand
                product_id = sdf_product_id_list[sdf_idx]
                demand_date = sdf_demand_date_list[sdf_idx]
                demand_allocation_date = sdf_demand_allocation_date_list[sdf_idx]
                demand_facility_id = sdf_demand_facility_id_list[sdf_idx]
                short_demand_qty = sdf_short_demand_qty_list[sdf_idx]
                customer_id = sdf_customer_id_list[sdf_idx]
                roll_qty = sdf_roll_qty_list[sdf_idx]
                
                # get values from Products_Dim
                pd_idx = pd_product_id_list.index(product_id)
                crop_id = pd_crop_id_list[pd_idx]
                net_weight_grams = pd_net_weight_grams_list[pd_idx]
                is_whole = pd_is_whole_list[pd_idx]

                # adjust allocation date for production priority 5 (Gotham Foods) to incorporate lead time
                sdf_demand_allocation_date = demand_allocation_date
                if production_priority == 5:
                    lead_time_in_days = int(pd_lead_time_in_days_list[pd_idx])
                    demand_allocation_date = demand_allocation_date - DT.timedelta(days=(lead_time_in_days))
                    
#                 # consider demand from one facility ID per city short code
                if demand_facility_id in [1,2,9]:
                    demand_facility_id = 3 # set NYC1, NYC2, and NYC4 to NYC3
#                 # set harvest facility ID to demand facility ID
                if demand_facility_id == 4:
                    demand_facility_id = 7 # set CHI1 to CHI2
                
                # set harvest facility ID to demand facility ID
                harvest_facility_id = demand_facility_id
                #harvest_city_short_code = fd_city_short_code_list[fd_facility_id_list.index(harvest_facility_id)]
                
                # create key for mid-allocation check
                allocated_date_crop_facility_key = str(demand_allocation_date) + '_' + str(crop_id) + '_' + str(harvest_facility_id)
                allocated_date_product_facility_key = str(demand_allocation_date) + '_' + str(product_id) + '_' + str(harvest_facility_id)
                allocated_date_product_facility_customer_key = str(demand_allocation_date) + '_' + str(product_id) + '_' + str(harvest_facility_id) + '_' + str(customer_id)
                # complete_crop_allocation_key_list contains date/crop/facility combinations that are already fully allocated
                


                # get plant sites already allocated for the facility
                already_allocated_plant_sites = 0

                if allocated_date_crop_facility_key in allocated_date_crop_facility_key_list: 
                    already_allocated_plant_sites = allocated_plant_sites_list[allocated_date_crop_facility_key_list.index(allocated_date_crop_facility_key)]

                # get other values from HarvestForecastSeasonality_Facts
                harvest_date_indices =  [i for i, x in enumerate(hfsf_harvest_date_list) if x == demand_allocation_date]
                harvest_crop_id_indices =  [j for j, y in enumerate(hfsf_crop_id_list) if y == crop_id]
                #harvest_facility_id_indices = [k for k, z in enumerate(hfsf_facility_id_list) if z == harvest_facility_id]
                harvest_facility_id_indices = [k for k, z in enumerate(hfsf_facility_id_list)
                                            if fd_city_short_code_list[fd_facility_id_list.index(z)]
                                               == fd_city_short_code_list[fd_facility_id_list.index(harvest_facility_id)]]
                harvest_date_crop_facility_indices = list(set(harvest_date_indices) & set(harvest_crop_id_indices) & set(harvest_facility_id_indices))
                #print(len(harvest_date_crop_facility_indices))
                # multiple lines are potentially available to allocate for each crop
                # from the harvest forecast, determine if the short demand can be fulfilled or not
                harvest_facility_line_id_list = list()
                harvest_expected_plant_sites_list = list()
                harvest_avg_headweight_list = list()
                harvest_whole_gpps_list = list()
                harvest_loose_gpps_list = list()

                for hfsf_idx in harvest_date_crop_facility_indices:
                    # get available lines
                    harvest_facility_line_id_list += [hfsf_facility_line_id_list[hfsf_idx]]
                    # get total plant sites
                    harvest_expected_plant_sites_list += [hfsf_expected_plant_sites_list[hfsf_idx]]
                    # get whole grams per plant site
                    harvest_whole_gpps_list += [hfsf_avg_headweight_list[hfsf_idx]]
                    harvest_loose_gpps_list += [hfsf_loose_grams_per_plant_site_list[hfsf_idx]]

                # no co-packers in DEN
                if production_priority == 5 and demand_facility_id == 8:
                    harvest_expected_plant_sites_list = list()
                #print(allocated_date_crop_facility_key)

                # checkpoint: there is available harvest
                if len(harvest_expected_plant_sites_list) > 0:
                
                    # compute mean grams per plant site for the facility and round to two decimals
                    #harvest_facility_mean_whole_gpps = round(float(np.mean(harvest_whole_gpps_list)),2)
                    harvest_facility_std_whole_gpps = round(float(np.std(harvest_whole_gpps_list)),2)
                    #harvest_facility_mean_loose_gpps = round(float(np.mean(harvest_loose_gpps_list)),2)
                    harvest_facility_std_loose_gpps = round(float(np.std(harvest_loose_gpps_list)),2)

                    # GPPS normalized by expected plant sites
                    whole_numerator = 0
                    loose_numerator = 0
                    total_ps = 0
                    for harvest_idx in range(len(harvest_whole_gpps_list)):
                        whole_numerator += harvest_whole_gpps_list[harvest_idx] * harvest_expected_plant_sites_list[harvest_idx]
                        loose_numerator += harvest_loose_gpps_list[harvest_idx] * harvest_expected_plant_sites_list[harvest_idx]
                        total_ps += harvest_expected_plant_sites_list[harvest_idx]

                    harvest_facility_mean_whole_gpps = 0
                    harvest_facility_mean_loose_gpps = 0
                    if total_ps != 0:
                        harvest_facility_mean_whole_gpps = round(float(whole_numerator/total_ps),2)
                        harvest_facility_mean_loose_gpps = round(float(loose_numerator/total_ps),2)

                    # choose conversion factor
                    harvest_facility_mean_gpps = harvest_facility_mean_loose_gpps 
                    if is_whole == 1:
                        harvest_facility_mean_gpps = harvest_facility_mean_whole_gpps

                    # compute net_plant_sites for the short demand
                    #net_plant_sites = int(short_demand_qty * np.ceil(float(net_weight_grams / harvest_facility_mean_gpps)))
                    net_plant_sites = 0
                    
                    if harvest_facility_mean_gpps != 0:
                        net_plant_sites = int(np.ceil(float(short_demand_qty * net_weight_grams / harvest_facility_mean_gpps)))

                            
                    # add to short demand if harvest is completely allocated (no harvest is available)
                    if allocated_date_crop_facility_key in complete_crop_allocation_key_list:
                        if short_demand_qty > 0:
                            new_sdf_demand_date_list += [sdf_demand_date_list[sdf_idx]]
                            new_sdf_demand_allocation_date_list += [sdf_demand_allocation_date_list[sdf_idx]]
                            new_sdf_demand_facility_id_list += [sdf_demand_facility_id_list[sdf_idx]]
                            new_sdf_product_id_list += [sdf_product_id_list[sdf_idx]]
                            new_sdf_customer_id_list += [sdf_customer_id_list[sdf_idx]]
                            new_sdf_short_demand_qty_list += [sdf_short_demand_qty_list[sdf_idx]]
                            new_sdf_production_priority_list += [pd_production_priority_list[pd_product_id_list.index(sdf_product_id_list[sdf_idx])]]

                            

                                
                                
                    # checkpoint: we still need to allocate for this date/crop/facility
                    if allocated_date_crop_facility_key not in complete_crop_allocation_key_list:

                        # compute net plant sites for the facility
                        harvest_facility_net_plant_sites = int(sum(harvest_expected_plant_sites_list))
                        #print(harvest_expected_plant_sites_list)

                        # compute remaining plant sites for allocation considering the mid allocation checkpoint
                        harvest_facility_pre_plant_sites = harvest_facility_net_plant_sites - already_allocated_plant_sites

                        # ATTEMPT THE ALLOCATION
                        harvest_facility_post_plant_sites = harvest_facility_pre_plant_sites - net_plant_sites
                        #print('post allocation: ', harvest_facility_post_plant_sites)

                        # first deal with what to do when we cannot satisfy the demand
                        if harvest_facility_post_plant_sites < 0:
                            # oh no! this demand exceeds our harvest for the facility
                            #print("oh no")
                            # set full_packout to true
                            full_packout = 1

                            # compute total short demand in the same product priority        
                            short_date_indices =  [a for a, d in enumerate(sdf_demand_allocation_date_list) if d == sdf_demand_allocation_date]
                            #short_facility_id_indices =  [b for b, e in enumerate(sdf_demand_facility_id_list) if e == demand_facility_id]
                            short_facility_id_indices =  [b for b, e in enumerate(sdf_demand_facility_id_list)
                                                 if fd_city_short_code_list[fd_facility_id_list.index(e)]
                                                    == fd_city_short_code_list[fd_facility_id_list.index(demand_facility_id)]]
                            short_date_facility_priority_indices = list(set(short_date_indices) & set(short_facility_id_indices) & set(short_priority_indices))

                            # accumulate short_demand_plant_sites for all products of the same product priority
                            short_demand_plant_sites = 0
                            for short_idx in short_date_facility_priority_indices:
                                product_id = sdf_product_id_list[short_idx]
                                short_demand_qty = sdf_short_demand_qty_list[short_idx]
                                s_roll_qty = sdf_roll_qty_list[short_idx]
                                #####
                                # get other values from Products_Dim
                                pd_idx = pd_product_id_list.index(product_id)
                                short_crop_id = pd_crop_id_list[pd_idx]

                                if short_crop_id == crop_id:

                                    net_weight_grams = pd_net_weight_grams_list[pd_idx]
                                    is_whole = pd_is_whole_list[pd_idx]

                                    # choose conversion factor
                                    harvest_facility_mean_gpps = harvest_facility_mean_loose_gpps 
                                    if is_whole == 1:
                                        harvest_facility_mean_gpps = harvest_facility_mean_whole_gpps
                                    # compute net_plant_sites for the short demand
                                    net_plant_sites = int(np.ceil(float(short_demand_qty * net_weight_grams / harvest_facility_mean_gpps)))
                                    short_demand_plant_sites += net_plant_sites
                                    roll_net_plant_sites = int(np.ceil(float(s_roll_qty * net_weight_grams / harvest_facility_mean_gpps)))
                                    # aggregate roll qty for date and priority
                                    dp_roll_key = str(demand_facility_id) + '_' + str(product_id)
                                    if roll_qty > 0:
                                        if dp_roll_key in dp_roll_key_list:
                                            dp_roll_qty_list[dp_roll_key_list.index(dp_roll_key)] += s_roll_qty
                                            dp_roll_ps_list[dp_roll_key_list.index(dp_roll_key)] += roll_net_plant_sites
                                        if dp_roll_key not in dp_roll_key_list:
                                            dp_roll_key_list += [dp_roll_key]
                                            dp_roll_qty_list += [s_roll_qty]
                                            dp_roll_ps_list += [roll_net_plant_sites]
                                            
                            # now that we have how many plant sites we are short across all products
                            # accumulate harvest_priority_plant_sites available for all products of the same priority
                            harvest_priority_plant_sites = harvest_facility_pre_plant_sites
                            for check_idx in range(len(allocated_date_product_facility_key_list)):
                                check_key = allocated_date_product_facility_key_list[check_idx]
                                check_demand_allocation_date = DT.datetime.strptime(str(check_key.split("_")[0]), '%Y-%m-%d').date()
                                check_product_id = int(check_key.split("_")[1])
                                check_facility_id = int(check_key.split("_")[2])
                                #check_city_short_code = check_key.split("_")[2]
                                check_priority = pd_production_priority_list[pd_product_id_list.index(check_product_id)]
                                check_crop_id = pd_crop_id_list[pd_product_id_list.index(check_product_id)]
                                check_date_crop_facility_key = str(check_demand_allocation_date) + '_' + str(check_crop_id) + '_' + str(check_facility_id)
                                if check_demand_allocation_date == demand_allocation_date and check_facility_id == harvest_facility_id and check_priority == production_priority and check_crop_id == crop_id:
                                #if check_demand_allocation_date == demand_allocation_date and check_city_short_code == fd_city_short_code_list[fd_facility_id_list.index(harvest_facility_id)] and check_priority == production_priority and check_crop_id == crop_id:
                                    check_allocated_product_plant_sites = allocated_product_plant_sites_list[check_idx]
                                    # add the plant sites for the harvest_demand_ratio
                                    harvest_priority_plant_sites += check_allocated_product_plant_sites
                                    # subtract the plant sites from the overall allocation tracking list since the new value multiplied by harvest_demand_ratio will be added back
                                    allocated_plant_sites_list[allocated_date_crop_facility_key_list.index(check_date_crop_facility_key)] -= check_allocated_product_plant_sites


                            # compute harvest_demand_ratio (less than 1 in this case)
                            harvest_demand_ratio = float(harvest_priority_plant_sites / short_demand_plant_sites)
                            #harvest_demand_ratio = float(harvest_facility_pre_plant_sites / short_demand_plant_sites)

                            # allocate to every product ID of the same product priority where shorts exist
                            for short_idx in short_date_facility_priority_indices:
                                short_demand_date = sdf_demand_date_list[short_idx]
                                product_id = sdf_product_id_list[short_idx]
                                customer_id = sdf_customer_id_list[short_idx]
                                short_demand_qty = sdf_short_demand_qty_list[short_idx]
                                #####
                                # get other values from Products_Dim
                                pd_idx = pd_product_id_list.index(product_id)
                                short_crop_id = pd_crop_id_list[pd_idx]

                                if short_crop_id == crop_id:

                                    net_weight_grams = pd_net_weight_grams_list[pd_idx]
                                    is_whole = pd_is_whole_list[pd_idx]

                                    s_allocated_date_product_facility_key = str(demand_allocation_date) + '_' + str(product_id) + '_' + str(harvest_facility_id)
                                    s_allocated_date_product_facility_customer_key = str(demand_allocation_date) + '_' + str(product_id) + '_' + str(harvest_facility_id)+ '_' + str(customer_id)

                                    # choose conversion factor
                                    harvest_facility_mean_gpps = harvest_facility_mean_loose_gpps 
                                    if is_whole == 1:
                                        harvest_facility_mean_gpps = harvest_facility_mean_whole_gpps

                                    # apply harvest_demand_ratio to allocated_qty
                                    allocated_qty = int(np.floor(short_demand_qty * harvest_demand_ratio))

                                    forecasted_gpps = round(float(harvest_facility_mean_gpps),2)
                                    allocated_product_plant_sites = int(np.ceil(allocated_qty * net_weight_grams / forecasted_gpps))
                                    allocated_grams = round(allocated_product_plant_sites * forecasted_gpps,2)

                                    if allocated_qty > 0:
                                        # write to lists for HarvestAllocation_Facts
                                        haf_demand_allocation_date_list += [demand_allocation_date]
                                        haf_demand_date_list += [short_demand_date]
                                        haf_harvest_facility_id_list += [harvest_facility_id]
                                        haf_demand_facility_id_list += [demand_facility_id]
                                        haf_crop_id_list += [crop_id]
                                        haf_product_id_list += [product_id]
                                        haf_customer_id_list += [customer_id]
                                        haf_forecasted_gpps_list += [forecasted_gpps]
                                        haf_allocated_plant_sites_list += [allocated_product_plant_sites]
                                        haf_allocated_grams_list += [allocated_grams]
                                        haf_allocated_qty_list += [allocated_qty]
                                        haf_full_packout_list += [full_packout]


                                        # update the allocation_lists
                                        if allocated_date_crop_facility_key in allocated_date_crop_facility_key_list:        
                                            allocated_plant_sites_list[allocated_date_crop_facility_key_list.index(allocated_date_crop_facility_key)] += allocated_product_plant_sites

                                        if allocated_date_crop_facility_key not in allocated_date_crop_facility_key_list:
                                            allocated_date_crop_facility_key_list += [allocated_date_crop_facility_key]
                                            allocated_plant_sites_list += [allocated_product_plant_sites]
                                            allocated_starting_ps_list += [harvest_facility_net_plant_sites]                          

                                    if s_allocated_date_product_facility_key in allocated_date_product_facility_key_list:
                                        del allocated_gpps_list[allocated_date_product_facility_key_list.index(s_allocated_date_product_facility_key)]
                                        del allocated_qty_list[allocated_date_product_facility_key_list.index(s_allocated_date_product_facility_key)]
                                        del allocated_product_plant_sites_list[allocated_date_product_facility_key_list.index(s_allocated_date_product_facility_key)]
                                        del allocated_date_product_facility_key_list[allocated_date_product_facility_key_list.index(s_allocated_date_product_facility_key)]

                                    # customer allocation
                                    if s_allocated_date_product_facility_customer_key in allocated_date_product_facility_customer_key_list:
                                        adpfc_key_idx = allocated_date_product_facility_customer_key_list.index(s_allocated_date_product_facility_customer_key)
                                        del allocated_date_product_facility_customer_key_list[adpfc_key_idx]
                                        del allocated_customer_gpps_list[adpfc_key_idx]
                                        del allocated_customer_qty_list[adpfc_key_idx]
                                        del allocated_customer_plant_sites_list[adpfc_key_idx]
                                        del allocated_customer_roll_qty_list[adpfc_key_idx]
                                        del allocated_customer_demand_date_list[adpfc_key_idx]
                                        

                                    # mark allocation as complete
#                                     complete_crop_allocation_key_list += [allocated_date_crop_facility_key]
#                                     complete_product_allocation_key_list += [allocated_date_product_facility_key]
#                                     complete_customer_allocation_key_list += [allocated_date_product_facility_customer_key]
                                    # mark allocation as complete
                                    if allocated_date_crop_facility_key not in complete_crop_allocation_key_list:
                                        complete_crop_allocation_key_list += [allocated_date_crop_facility_key]
                                    if s_allocated_date_product_facility_key not in complete_product_allocation_key_list:
                                        complete_product_allocation_key_list += [s_allocated_date_product_facility_key]
                                    if s_allocated_date_product_facility_customer_key not in complete_customer_allocation_key_list:
                                        complete_customer_allocation_key_list += [s_allocated_date_product_facility_customer_key]


                                    # new short demand lists
                                    new_short_demand_qty = short_demand_qty - allocated_qty

                                    if new_short_demand_qty > 0:

                                        new_sdf_demand_date_list += [short_demand_date]
                                        new_sdf_demand_allocation_date_list += [demand_allocation_date]
                                        new_sdf_demand_facility_id_list += [demand_facility_id]
                                        new_sdf_product_id_list += [product_id]
                                        new_sdf_customer_id_list += [customer_id]
                                        new_sdf_short_demand_qty_list += [new_short_demand_qty]
                                        new_sdf_production_priority_list += [pd_production_priority_list[pd_product_id_list.index(product_id)]]

                                    # this short demand has now been allocated towards so add short idx to the list to skip
                                    sdf_idx_to_skip_list += [short_idx]  
                        
                        # next deal with what to do when we can successfully satisfy the demand
                        if harvest_facility_post_plant_sites >= 0 and net_plant_sites > 0:
                            # yay! we have enough plant sites to cover the demand (so far)
                            # update the allocation lists
                            # add to mid-allocation tracker if they key exists
                            #print("yay")
                            if allocated_date_crop_facility_key in allocated_date_crop_facility_key_list:
                                allocated_plant_sites_list[allocated_date_crop_facility_key_list.index(allocated_date_crop_facility_key)] += net_plant_sites

                            # add to mid-allocation tracker if its a new key
                            if allocated_date_crop_facility_key not in allocated_date_crop_facility_key_list:
                                allocated_date_crop_facility_key_list += [allocated_date_crop_facility_key]
                                allocated_plant_sites_list += [net_plant_sites]
                                allocated_starting_ps_list += [harvest_facility_net_plant_sites]

                            if allocated_date_product_facility_key in allocated_date_product_facility_key_list:
                                allocated_qty_list[allocated_date_product_facility_key_list.index(allocated_date_product_facility_key)] += short_demand_qty
                                allocated_product_plant_sites_list[allocated_date_product_facility_key_list.index(allocated_date_product_facility_key)] += net_plant_sites

                            if allocated_date_product_facility_key not in allocated_date_product_facility_key_list:   
                                allocated_date_product_facility_key_list += [allocated_date_product_facility_key]
                                allocated_gpps_list += [round(float(harvest_facility_mean_gpps),2)]
                                allocated_qty_list += [short_demand_qty]
                                allocated_product_plant_sites_list += [net_plant_sites]

                            if allocated_date_product_facility_customer_key in allocated_date_product_facility_customer_key_list:
                                allocated_customer_qty_list[allocated_date_product_facility_customer_key_list.index(allocated_date_product_facility_customer_key)] += [short_demand_qty]
                                allocated_customer_plant_sites_list[allocated_date_product_facility_customer_key_list.index(allocated_date_product_facility_customer_key)] += [net_plant_sites]
                                allocated_customer_roll_qty_list[allocated_date_product_facility_customer_key_list.index(allocated_date_product_facility_customer_key)] += [roll_qty]
                                allocated_customer_demand_date_list[allocated_date_product_facility_customer_key_list.index(allocated_date_product_facility_customer_key)] += [demand_date]
                            if allocated_date_product_facility_customer_key not in allocated_date_product_facility_customer_key_list:   
                                allocated_date_product_facility_customer_key_list += [allocated_date_product_facility_customer_key]
                                allocated_customer_gpps_list += [round(float(harvest_facility_mean_gpps),2)]
                                allocated_customer_qty_list += [[short_demand_qty]]
                                allocated_customer_plant_sites_list += [[net_plant_sites]] 
                                allocated_customer_roll_qty_list += [[roll_qty]]
                                allocated_customer_demand_date_list += [[demand_date]]


                if len(harvest_expected_plant_sites_list) == 0:
                    #if demand_allocation_date > load_date.date():
                        #print('no expected harvest for date_crop_facility: ', allocated_date_crop_facility_key)
                    nh_demand_qty = sdf_short_demand_qty_list[sdf_idx]
                    
                    if nh_demand_qty > 0:

                        new_sdf_demand_date_list += [sdf_demand_date_list[sdf_idx]]
                        new_sdf_demand_allocation_date_list += [sdf_demand_allocation_date_list[sdf_idx]]
                        new_sdf_demand_facility_id_list += [sdf_demand_facility_id_list[sdf_idx]]
                        new_sdf_product_id_list += [sdf_product_id_list[sdf_idx]]
                        new_sdf_customer_id_list += [sdf_customer_id_list[sdf_idx]]
                        new_sdf_short_demand_qty_list += [nh_demand_qty]
                        new_sdf_production_priority_list += [pd_production_priority_list[pd_product_id_list.index(sdf_product_id_list[sdf_idx])]]

                            
        # after going through all short demand for the production priority
       
        # set full_packout to false
        full_packout = 0
        # write all complete allocations to customerHarvestAllocation_Facts for the remaining products
        for allocated_idx in range(len(allocated_date_product_facility_customer_key_list)):
            allocated_date_product_facility_customer_key = allocated_date_product_facility_customer_key_list[allocated_idx]
            # only write the keys that are not yet completed
            if allocated_date_product_facility_customer_key not in complete_customer_allocation_key_list:

                customer_id = int(allocated_date_product_facility_customer_key.split("_")[3])
                
                harvest_facility_id = int(allocated_date_product_facility_customer_key.split("_")[2])
                #harvest_city_short_code = int(allocated_date_product_facility_key.split("_")[2])
                product_id = int(allocated_date_product_facility_customer_key.split("_")[1])
                               
                # get values from Products_Dim
                pd_idx = pd_product_id_list.index(product_id)
                crop_id = pd_crop_id_list[pd_idx]
                net_weight_grams = pd_net_weight_grams_list[pd_idx]
                
                demand_facility_id = harvest_facility_id
                
                
                allocated_customer_qty_sub_list = allocated_customer_qty_list[allocated_idx]
                forecasted_gpps = allocated_customer_gpps_list[allocated_idx]
                allocated_customer_plant_sites_sub_list = allocated_customer_plant_sites_list[allocated_idx]
                demand_date_sub_list = allocated_customer_demand_date_list[allocated_idx]
                roll_qty_sub_list = allocated_customer_roll_qty_list[allocated_idx]
                
                
                for sub_idx in range(len(demand_date_sub_list)):
                    allocated_customer_qty = allocated_customer_qty_sub_list[sub_idx]
                    allocated_customer_plant_sites = allocated_customer_plant_sites_sub_list[sub_idx]
                    demand_date = demand_date_sub_list[sub_idx]
                    roll_qty = roll_qty_sub_list[sub_idx]
                    
                    allocated_grams = round(allocated_customer_plant_sites * forecasted_gpps,2)

                    # write to lists for CustomerHarvestAllocation_Facts
                    haf_demand_allocation_date_list += [demand_allocation_date]
                    haf_demand_date_list += [demand_date]
                    haf_harvest_facility_id_list += [harvest_facility_id]
                    haf_demand_facility_id_list += [demand_facility_id]
                    haf_crop_id_list += [crop_id]
                    haf_product_id_list += [product_id]
                    haf_customer_id_list += [customer_id]
                    haf_forecasted_gpps_list += [forecasted_gpps]
                    haf_allocated_plant_sites_list += [allocated_customer_plant_sites]
                    haf_allocated_grams_list += [allocated_grams]
                    haf_allocated_qty_list += [allocated_customer_qty]
                    haf_full_packout_list += [full_packout]

                    # add rollover qty

                    roll_net_plant_sites = int(np.ceil(float(roll_qty * net_weight_grams / forecasted_gpps)))
                    # aggregate roll qty for date and priority
                    dp_roll_key = str(demand_facility_id) + '_' + str(product_id)
                    if roll_qty > 0:
                        if dp_roll_key in dp_roll_key_list:
                            dp_roll_qty_list[dp_roll_key_list.index(dp_roll_key)] += roll_qty
                            dp_roll_ps_list[dp_roll_key_list.index(dp_roll_key)] += roll_net_plant_sites
                        if dp_roll_key not in dp_roll_key_list:
                            dp_roll_key_list += [dp_roll_key]
                            dp_roll_qty_list += [roll_qty]
                            dp_roll_ps_list += [roll_net_plant_sites]
                            

        
        # allocate harvest to rollover for the tier, date, and production priority
        ha_roll_key_list = list() 
        ha_roll_product_id_list = list() 
        ha_roll_ps_list  = list() 
        ha_roll_qty_list  = list() 
        for dp_roll_idx in range(len(dp_roll_key_list)):
            dp_roll_key = dp_roll_key_list[dp_roll_idx]
            dp_facility_id = int(dp_roll_key.split('_')[0])
            dp_product_id = int(dp_roll_key.split('_')[1])
            dp_roll_qty = dp_roll_qty_list[dp_roll_idx]
            dp_roll_ps = dp_roll_ps_list[dp_roll_idx]
            
            dp_crop_id = int(pd_crop_id_list[pd_product_id_list.index(dp_product_id)])
            # check remaining inventory
            ri_qty = 0
            ri_ps = 0
            if dp_roll_key in ri_key_list and dp_roll_qty > 0:
                ri_qty = ri_qty_list[ri_key_list.index(dp_roll_key)]
                ri_ps = int(np.floor(ri_qty * dp_roll_ps / dp_roll_qty))
            # harvest allocation rollover plant sites is aggregated rollover plant sites from demand minus remaining inventory in plant sites
            ha_roll_ps = dp_roll_ps - ri_ps
            ha_roll_qty = dp_roll_qty - ri_qty
            
            ha_roll_key = str(demand_allocation_date) + '_' + str(dp_crop_id) + '_' + str(dp_facility_id)
            
            # build harvest allocation lists for rollover
            if ha_roll_ps > 0:
                if ha_roll_key in ha_roll_key_list:
                    ha_roll_idx = ha_roll_key_list.index(ha_roll_key)
                    ha_roll_product_id_list[ha_roll_idx] += [dp_product_id]
                    ha_roll_ps_list[ha_roll_idx] += [ha_roll_ps]
                    ha_roll_qty_list[ha_roll_idx] += [ha_roll_qty]
                if ha_roll_key not in ha_roll_key_list:
                    ha_roll_key_list += [ha_roll_key] 
                    ha_roll_product_id_list += [[dp_product_id]]
                    ha_roll_ps_list += [[ha_roll_ps]]
                    ha_roll_qty_list += [[ha_roll_qty]]
        
        # check aggregated demand and available harvest
        for ha_idx in range(len(ha_roll_key_list)):
            ha_roll_key = ha_roll_key_list[ha_idx]
            ha_roll_ps_sub_list = ha_roll_ps_list[ha_idx]
            ha_roll_product_id_sub_list = ha_roll_product_id_list[ha_idx]
            ha_roll_qty_sub_list = ha_roll_qty_list[ha_idx]

            ha_roll_ps_sum = sum(ha_roll_ps_sub_list)


            roll_demand_allocation_date = DT.datetime.strptime(ha_roll_key.split('_')[0],"%Y-%m-%d").date()
            roll_crop_id = int(ha_roll_key.split('_')[1])
            roll_facility_id = int(ha_roll_key.split('_')[2])

            # set demand date as one day ahead of allocation for rollover qty
            roll_demand_date = roll_demand_allocation_date + DT.timedelta(days = 1)
            
            available_ps = 0
            if ha_roll_key in allocated_date_crop_facility_key_list and ha_roll_key not in complete_crop_allocation_key_list:
                ha_roll_allocated_idx = allocated_date_crop_facility_key_list.index(ha_roll_key)
                
                starting_ps = allocated_starting_ps_list[ha_roll_allocated_idx]

                available_ps = starting_ps - allocated_plant_sites_list[ha_roll_allocated_idx]

            if available_ps > 0:
    
                # attempt allocation
                new_available_ps = available_ps - ha_roll_ps_sum
                
                roll_allocation_ratio = 1
                # check if short on the crop
                if new_available_ps < 0:
                    
                    roll_allocation_ratio = float(available_ps / ha_roll_ps_sum)
                    

                # compute harvest allocation to rollover quantities
                for ha_sub_idx in range(len(ha_roll_product_id_sub_list)):
                    roll_product_id = ha_roll_product_id_sub_list[ha_sub_idx]
                    roll_net_weight_grams = pd_net_weight_grams_list[pd_product_id_list.index(roll_product_id)]
                    roll_prod_priority = pd_production_priority_list[pd_product_id_list.index(roll_product_id)]
                    # compute with allocation ratio
                    roll_plant_sites = int(np.floor(float(ha_roll_ps_sub_list[ha_sub_idx] * roll_allocation_ratio)))
                    roll_qty = int(np.floor(ha_roll_qty_sub_list[ha_sub_idx] * roll_allocation_ratio))
                    roll_grams = round(float(roll_qty * roll_net_weight_grams),2)
                    roll_gpps = 0
                    if roll_plant_sites > 0:
                        roll_gpps = round(float(roll_grams / roll_plant_sites),2)
                    roll_short_demand_qty = ha_roll_qty_sub_list[ha_sub_idx] - roll_qty
                    # update allocation tracking
                    full_packout = 0
                    if new_available_ps <= 0:
                        complete_crop_allocation_key_list += [ha_roll_key]
                        full_packout = 1
                        new_available_ps = 0
                    #allocated_plant_sites_list[ha_roll_allocated_idx] = new_available_ps
                    
                    # write to lists for CustomerHarvestAllocation_Facts
                    if roll_qty > 0:
                        haf_demand_allocation_date_list += [roll_demand_allocation_date]
                        haf_demand_date_list += [roll_demand_date]
                        haf_harvest_facility_id_list += [roll_facility_id]
                        haf_demand_facility_id_list += [roll_facility_id]
                        haf_crop_id_list += [roll_crop_id]
                        haf_product_id_list += [roll_product_id]
                        haf_customer_id_list += [0]
                        haf_forecasted_gpps_list += [roll_gpps]
                        haf_allocated_plant_sites_list += [roll_plant_sites]
                        haf_allocated_grams_list += [roll_grams]
                        haf_allocated_qty_list += [roll_qty]
                        haf_full_packout_list += [full_packout]
                    
                        # allocation tracking
                        allocated_plant_sites_list[allocated_date_crop_facility_key_list.index(ha_roll_key)] += roll_plant_sites
                        

 
                    if roll_short_demand_qty > 0:
                        
                        # update short demand
                        new_sdf_demand_date_list += [roll_demand_date]
                        new_sdf_demand_allocation_date_list += [roll_demand_allocation_date]
                        new_sdf_demand_facility_id_list += [roll_facility_id]
                        new_sdf_product_id_list += [roll_product_id]
                        new_sdf_customer_id_list += [0]
                        new_sdf_short_demand_qty_list += [roll_short_demand_qty]
                        new_sdf_production_priority_list += [roll_prod_priority]
                        
            # update short demand when no plant sites are available
            if available_ps == 0:
                for ha_sub_idx in range(len(ha_roll_product_id_sub_list)):
                    roll_product_id = ha_roll_product_id_sub_list[ha_sub_idx]
                    roll_qty = ha_roll_qty_sub_list[ha_sub_idx]
                    roll_prod_priority = pd_production_priority_list[pd_product_id_list.index(roll_product_id)]
                    
                    # update short demand
                    new_sdf_demand_date_list += [roll_demand_date]
                    new_sdf_demand_allocation_date_list += [roll_demand_allocation_date]
                    new_sdf_demand_facility_id_list += [roll_facility_id]
                    new_sdf_product_id_list += [roll_product_id]
                    new_sdf_customer_id_list += [0]
                    new_sdf_short_demand_qty_list += [roll_qty]
                    new_sdf_production_priority_list += [roll_prod_priority]
                


        # reset complete_product_allocation_key_list
        complete_product_allocation_key_list = list()
        # reset mid-allocation tracking list
        allocated_date_product_facility_key_list = list()
        allocated_gpps_list = list()
        allocated_qty_list = list()
        allocated_product_plant_sites_list = list()
        
        allocated_date_product_facility_customer_key_list = list()
        allocated_customer_gpps_list = list()
        allocated_customer_qty_list = list()
        allocated_customer_plant_sites_list = list()
        allocated_customer_roll_qty_list =list()
        allocated_customer_demand_date_list = list()

    # harvest allocation output
    harvest_allocation_out_LoL = [haf_demand_allocation_date_list,
                                  haf_demand_date_list,
                                  haf_harvest_facility_id_list,
                                  haf_demand_facility_id_list,
                                  haf_crop_id_list,
                                  haf_product_id_list,
                                  haf_customer_id_list,
                                  haf_forecasted_gpps_list,
                                  haf_allocated_plant_sites_list,
                                  haf_allocated_grams_list,
                                  haf_allocated_qty_list,
                                  haf_full_packout_list]
    
    

    # harvest unallocated output
    harvest_unallocated_out_LoL = [allocated_date_crop_facility_key_list,
                                   allocated_starting_ps_list,
                                   allocated_plant_sites_list,
                                   complete_crop_allocation_key_list,
                                  ]

    # initialize lists for new ShortDemand_Facts
    short_demand_out_LoL = [new_sdf_demand_date_list,
                            new_sdf_demand_allocation_date_list,
                            new_sdf_demand_facility_id_list,
                            new_sdf_product_id_list,
                            new_sdf_customer_id_list,
                            new_sdf_short_demand_qty_list,
                            new_sdf_production_priority_list]
    
    return (harvest_allocation_out_LoL,harvest_unallocated_out_LoL, short_demand_out_LoL)
        
    

def writeCustomerHarvestAllocation(forecast_allocation_date,harvest_allocation_out_LoL, tier_count, is_pending = 0):
    '''
    #### Inputs:
    - forecast_allocation_date: date of the forecast
    - harvest_allocation_out_LoL: list of twelve lists cooresponding to harvest allocations
        1. List of demand allocation dates
        2. List of demand dates
        3. List of harvest greenhouse IDs
        4. List of demand greenhouse IDs
        5. List of crop IDs
        6. List of product IDs
        7. List of customer IDs
        7. List of forecasted grams per plant site values
        8. List of allocated plant sites
        9. List of allocated grams
        10. List of allocated quantities
        11. List of full packout boolean flags
    - tier_count: allocation tier for the harvest allocation from harvest city to customer
    - is_pending: integer boolean 1 for Pending table or 0 for baseline table
        
    #### Algorithm:
    - load inputs
    - loop through harvest allocation lists and insert entries into CustomerHarvestAllocation_Facts
    - return string indicating completion
    #### Output: string indicating completion
    '''
    
    
    # connect to database
    HOSTNAME = socket.gethostname()

    if HOSTNAME == 'hostname':
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server}; 
                                Server=127.0.0.1,1443;
                                Database=databasename;
                                trusted_connection=yes""" # use windows auth on DB01
    else:
        with open(os.path.join(sys.path[0], "config.yml"), 'r') as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.SafeLoader)
    #    uid = cfg['databasename']['uid']
        uid = 'sa'
        pwd = cfg['databasename']['pwd'][:-3]
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server};
                                Server=hostname\MSSQLSERVER1;
                                Database=databasename;
                                UID=%s;
                                PWD=%s;""" % (uid, pwd) # use config.yml on local machine



    cnxn = pyodbc.connect(CONNECTIONSTRING)   
    cnxn_cursor = cnxn.cursor()
    
    haf_demand_allocation_date_list = harvest_allocation_out_LoL[0]
    haf_demand_date_list = harvest_allocation_out_LoL[1]
    haf_harvest_facility_id_list = harvest_allocation_out_LoL[2]
    haf_demand_facility_id_list = harvest_allocation_out_LoL[3]
    haf_crop_id_list = harvest_allocation_out_LoL[4]
    haf_product_id_list = harvest_allocation_out_LoL[5]
    haf_customer_id_list = harvest_allocation_out_LoL[6]
    haf_forecasted_gpps_list = harvest_allocation_out_LoL[7]
    haf_allocated_plant_sites_list = harvest_allocation_out_LoL[8]
    haf_allocated_grams_list = harvest_allocation_out_LoL[9]
    haf_allocated_qty_list = harvest_allocation_out_LoL[10]
    haf_full_packout_list = harvest_allocation_out_LoL[11]
                                
    # initizlize index
    sql = """
    SELECT MAX(CustomerHarvestAllocationID) FROM CustomerHarvestAllocation_Facts
    """
    cnxn_cursor.execute(sql)
    
    max_old_id = cnxn_cursor.fetchone()[0]
    if max_old_id is None:
        max_old_id = 0
    harvest_allocation_id = max_old_id + 1
    
    # initialize timestamp
    load_date = DT.datetime.now()
    to_date = DT.datetime.strptime('2099-12-31 00:00:00.000000', '%Y-%m-%d %H:%M:%S.%f')
    is_active = 1

    # insert
    haf_sql = """
    INSERT INTO CustomerHarvestAllocation_Facts
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
    """ 
    
    if is_pending == 1:
        

        # initizlize index
        sql = """
        SELECT MAX(CustomerHarvestAllocationPendingID) FROM CustomerHarvestAllocationPending_Facts
        """
        cnxn_cursor.execute(sql)

        max_old_id = cnxn_cursor.fetchone()[0]
        if max_old_id is None:
            max_old_id = 0
        harvest_allocation_id = max_old_id + 1
    
        haf_sql = """
        INSERT INTO CustomerHarvestAllocationPending_Facts
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
        """ 

    for haf_idx in range(len(haf_demand_allocation_date_list)):

        # get values from the lists
        demand_allocation_date = haf_demand_allocation_date_list[haf_idx]
        demand_date = haf_demand_date_list[haf_idx]
        harvest_facility_id = haf_harvest_facility_id_list[haf_idx]
        demand_facility_id = haf_demand_facility_id_list[haf_idx]
        crop_id = haf_crop_id_list[haf_idx]
        product_id = haf_product_id_list[haf_idx]
        customer_id = haf_customer_id_list[haf_idx]
        forecasted_gpps = haf_forecasted_gpps_list[haf_idx]
        allocated_plant_sites= haf_allocated_plant_sites_list[haf_idx]
        allocated_grams = haf_allocated_grams_list[haf_idx]
        allocated_qty = haf_allocated_qty_list[haf_idx]
        full_packout = haf_full_packout_list[haf_idx]
        
        #if forecast_allocation_date == demand_allocation_date:
        # write to HarvestAllocation_Facts
        tuple_to_write = (harvest_allocation_id,
                         demand_allocation_date,
                         demand_date,
                         harvest_facility_id,
                         demand_facility_id,
                         crop_id,
                         product_id,
                         customer_id,
                         forecasted_gpps,
                         allocated_plant_sites,
                         allocated_grams,
                         allocated_qty,
                         full_packout,
                         tier_count,
                         load_date,
                         to_date,
                         is_active)
        cnxn_cursor.execute(haf_sql, tuple_to_write)
        harvest_allocation_id += 1                            

    cnxn.commit()
    cnxn_cursor.close()
    cnxn.close()    
    
    return 'CustomerHarvestAllocation_Facts for ' + str(forecast_allocation_date) + ' pau'




def writeHarvestUnallocated(harvest_in_LoL, harvest_unallocated_out_LoL, facilities_LoL, is_pending = 0):
    '''
    #### Inputs:
    - harvest_in_LoL: list of seven lists cooresponding to harvest
        1. List of harvest dates
        2. List of greenhouse IDs
        3. List of greenhouse line IDs
        4. List of crop IDs
        5. List of expected plant sites
        6. List of average headweights
        7. List of loose grams per plant site
    - harvest_unallocated_out_LoL: list of three lists for mid-allocation tracking
        1. List of date/crop/greenhouse key combinations that have an allocation
        2. List of allocated plant sites for each date/crop/greenhouse key combination
    - is_pending: integer boolean 1 for Pending table or 0 for baseline table
        
    #### Algorithm:
    - load inputs
    - loop through List of date/crop/greenhouse key combinations that have an allocation
        - compute total expected harvest plant sites
        - compute unallocated expected harvest plant sites as total expected harvest minus allocated harvest plant sites
        - insert entries into HarvestUnallocated_Facts
    - return string indicating completion
    #### Output: string indicating completion
    '''
    
        
    # connect to database
    HOSTNAME = socket.gethostname()

    if HOSTNAME == 'hostname':
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server}; 
                                Server=127.0.0.1,1443;
                                Database=databasename;
                                trusted_connection=yes""" # use windows auth on DB01
    else:
        with open(os.path.join(sys.path[0], "config.yml"), 'r') as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.SafeLoader)
    #    uid = cfg['databasename']['uid']
        uid = 'sa'
        pwd = cfg['databasename']['pwd'][:-3]
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server};
                                Server=hostname\MSSQLSERVER1;
                                Database=databasename;
                                UID=%s;
                                PWD=%s;""" % (uid, pwd) # use config.yml on local machine

    cnxn = pyodbc.connect(CONNECTIONSTRING)   
    cnxn_cursor = cnxn.cursor()

    allocated_date_crop_facility_key_list = harvest_unallocated_out_LoL[0]
    allocated_plant_sites_list = harvest_unallocated_out_LoL[2]
    
    # input lists from harvest
    hfsf_harvest_date_list = harvest_in_LoL[0]
    hfsf_facility_id_list = harvest_in_LoL[1]
    hfsf_facility_line_id_list = harvest_in_LoL[2]
    hfsf_crop_id_list = harvest_in_LoL[3]
    hfsf_expected_plant_sites_list = harvest_in_LoL[4]
    hfsf_avg_headweight_list = harvest_in_LoL[5]
    hfsf_loose_grams_per_plant_site_list = harvest_in_LoL[6]
    
    # facilities dimension
    fd_facility_id_list = facilities_LoL[0]
    fd_city_short_code_list = facilities_LoL[1]
    

    # initizlize index
    sql = """
    SELECT MAX(HarvestUnallocatedID) FROM HarvestUnallocated_Facts
    """
    cnxn_cursor.execute(sql)
    
    max_old_id = cnxn_cursor.fetchone()[0]
    if max_old_id is None:
        max_old_id = 0
    harvest_unallocated_id = max_old_id + 1
    
    # initialize timestamp
    load_date = DT.datetime.now()
    to_date = DT.datetime.strptime('2099-12-31 00:00:00.000000', '%Y-%m-%d %H:%M:%S.%f')
    is_active = 1

    # insert
    huf_sql = """
    INSERT INTO HarvestUnallocated_Facts
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?);
    """ 
    
    if is_pending == 1:
        
        
        # initizlize index
        sql = """
        SELECT MAX(HarvestUnallocatedPendingID) FROM HarvestUnallocatedPending_Facts
        """
        cnxn_cursor.execute(sql)

        max_old_id = cnxn_cursor.fetchone()[0]
        if max_old_id is None:
            max_old_id = 0
        harvest_unallocated_id = max_old_id + 1
        
        huf_sql = """
        INSERT INTO HarvestUnallocatedPending_Facts
        VALUES (?,?,?,?,?,?,?,?,?,?,?);
        """ 

    for unallocated_key_idx in range(len(allocated_date_crop_facility_key_list)):
        unallocated_key = allocated_date_crop_facility_key_list[unallocated_key_idx]
        unallocated_date = DT.datetime.strptime(str(unallocated_key.split("_")[0]), '%Y-%m-%d').date()
        unallocated_crop = int(unallocated_key.split("_")[1])
        unallocated_facility = int(unallocated_key.split("_")[2])


        # consider one facility ID per city short code
        if unallocated_facility in [1,2,9]:
            unallocated_facility = 3 # set NYC1, NYC2, and NYC4 to NYC3
        # set harvest facility ID to demand facility ID
        if unallocated_facility == 4:
            unallocated_facility = 7 # set CHI1 to CHI2


        # get starting values from HarvestForecastSeasonality_Facts
        harvest_date_indices =  [i for i, x in enumerate(hfsf_harvest_date_list) if x == unallocated_date]
        harvest_crop_id_indices =  [j for j, y in enumerate(hfsf_crop_id_list) if y == unallocated_crop]
        harvest_facility_id_indices = [k for k, z in enumerate(hfsf_facility_id_list)
                                                 if fd_city_short_code_list[fd_facility_id_list.index(z)]
                                                    == fd_city_short_code_list[fd_facility_id_list.index(unallocated_facility)]]
        #harvest_facility_id_indices = [k for k, z in enumerate(hfsf_facility_id_list) if z == unallocated_facility]

        harvest_date_crop_facility_indices = list(set(harvest_date_indices) & set(harvest_crop_id_indices) & set(harvest_facility_id_indices))

        # multiple lines are potentially available to allocate for each crop
        # from the harvest forecast, determine if the short demand can be fulfilled or not
        harvest_facility_line_id_list = list()
        harvest_expected_plant_sites_list = list()
        harvest_avg_headweight_list = list()
        harvest_whole_gpps_list = list()
        harvest_loose_gpps_list = list()
        for hfsf_idx in harvest_date_crop_facility_indices:
            # get available lines
            harvest_facility_line_id_list += [hfsf_facility_line_id_list[hfsf_idx]]
            # get total plant sites
            harvest_expected_plant_sites_list += [hfsf_expected_plant_sites_list[hfsf_idx]]
            # get whole grams per plant site
            harvest_whole_gpps_list += [hfsf_avg_headweight_list[hfsf_idx]]
            harvest_loose_gpps_list += [hfsf_loose_grams_per_plant_site_list[hfsf_idx]]

        # checkpoint: there is available harvest
        if len(harvest_expected_plant_sites_list) > 0:
            # compute mean grams per plant site for the facility and round to two decimals
            #harvest_facility_mean_whole_gpps = round(float(np.mean(harvest_whole_gpps_list)),2)
            harvest_facility_std_whole_gpps = round(float(np.std(harvest_whole_gpps_list)),2)

            #harvest_facility_mean_loose_gpps = round(float(np.mean(harvest_loose_gpps_list)),2)
            harvest_facility_std_loose_gpps = round(float(np.std(harvest_loose_gpps_list)),2)

            # GPPS normalized by expected plant sites
            whole_numerator = 0
            loose_numerator = 0
            total_ps = 0
            for harvest_idx in range(len(harvest_whole_gpps_list)):
                whole_numerator += harvest_whole_gpps_list[harvest_idx] * harvest_expected_plant_sites_list[harvest_idx]
                loose_numerator += harvest_loose_gpps_list[harvest_idx] * harvest_expected_plant_sites_list[harvest_idx]
                total_ps += harvest_expected_plant_sites_list[harvest_idx]
            
            harvest_facility_mean_whole_gpps = 0
            harvest_facility_mean_loose_gpps = 0
            if total_ps != 0:
                harvest_facility_mean_whole_gpps = round(float(whole_numerator/total_ps),2)
                harvest_facility_mean_loose_gpps = round(float(loose_numerator/total_ps),2)
            
            key_total_plant_sites = int(sum(harvest_expected_plant_sites_list))
            key_allocated_plant_sites = allocated_plant_sites_list[unallocated_key_idx]
            key_unallocated_plant_sites = key_total_plant_sites - key_allocated_plant_sites


            unallocated_plant_sites = key_unallocated_plant_sites
            unallocated_whole_grams = round(harvest_facility_mean_whole_gpps * unallocated_plant_sites,2)
            unallocated_loose_grams = round(harvest_facility_mean_loose_gpps * unallocated_plant_sites,2)

            # qty unit is generic 4 oz. retail qty (127.57275 g)
            g_per_clam = 127.57275
            if unallocated_crop == 1:
                g_per_clam = 114 # arugula
            if unallocated_crop == 3:
                g_per_clam = 35.4 # basil
            
            unallocated_qty = round(unallocated_loose_grams/g_per_clam/12,2)

            tuple_to_write = (harvest_unallocated_id,
                              unallocated_date,
                              unallocated_facility,
                              unallocated_crop,
                              unallocated_plant_sites,
                              unallocated_whole_grams,
                              unallocated_loose_grams,
                              unallocated_qty,
                              load_date,
                              to_date,
                              is_active,
                              unallocated_facility)
            
            if is_pending == 1:
                tuple_to_write = (harvest_unallocated_id,
                  unallocated_date,
                  unallocated_facility,
                  unallocated_crop,
                  unallocated_plant_sites,
                  unallocated_whole_grams,
                  unallocated_loose_grams,
                  unallocated_qty,
                  load_date,
                  to_date,
                  is_active)
            cnxn_cursor.execute(huf_sql, tuple_to_write)
            harvest_unallocated_id += 1

    # add for date_crop_facility with no allocations
    no_allocations_date_list = list()
    no_allocations_facility_id_list = list()
    no_allocations_crop_id_list = list()
    no_allocations_plant_sites_list = list()
    no_allocations_whole_grams_list = list()
    no_allocations_loose_grams_list = list()

    no_allocations_date_crop_facility_key_list = list()

    hfsf_harvest_date_indices =  [i for i, x in enumerate(hfsf_harvest_date_list) if x == date_today]
    for hfsf_idx in hfsf_harvest_date_indices:
        hfsf_harvest_date = hfsf_harvest_date_list[hfsf_idx]
        hfsf_crop_id = hfsf_crop_id_list[hfsf_idx]
        hfsf_facility_id = hfsf_facility_id_list[hfsf_idx]
        
        # consider one facility ID per city short code
        if hfsf_facility_id in [1,2,9]:
            hfsf_facility_id = 3 # set NYC1, NYC2, and NYC4 to NYC3
        # set harvest facility ID to demand facility ID
        if hfsf_facility_id == 4:
            hfsf_facility_id = 7 # set CHI1 to CHI2

        hfsf_date_crop_facility_key = str(hfsf_harvest_date) + '_' + str(hfsf_crop_id) + '_' + str(hfsf_facility_id)

        check_plant_sites = hfsf_expected_plant_sites_list[hfsf_idx]
        check_avg_headweight = hfsf_avg_headweight_list[hfsf_idx]
        check_loose_gpps = hfsf_loose_grams_per_plant_site_list[hfsf_idx]

        check_whole_grams = round(check_avg_headweight * check_plant_sites,2)
        check_loose_grams = round(check_loose_gpps * check_plant_sites,2)

        if hfsf_date_crop_facility_key not in allocated_date_crop_facility_key_list:
            # if we haven't allocated
            if hfsf_date_crop_facility_key in no_allocations_date_crop_facility_key_list:
                # add plant sites, whole grams, and loose grams to no allocations lists
                no_allocations_idx = no_allocations_date_crop_facility_key_list.index(hfsf_date_crop_facility_key)
                no_allocations_plant_sites_list[no_allocations_idx] += check_plant_sites
                no_allocations_whole_grams_list[no_allocations_idx] += check_whole_grams
                no_allocations_loose_grams_list[no_allocations_idx] += check_loose_grams

            if hfsf_date_crop_facility_key not in no_allocations_date_crop_facility_key_list:
                # add to list to no allocations lists
                no_allocations_date_list += [hfsf_harvest_date]
                no_allocations_facility_id_list += [hfsf_facility_id]
                no_allocations_crop_id_list += [hfsf_crop_id]
                no_allocations_plant_sites_list += [check_plant_sites]
                no_allocations_whole_grams_list += [check_whole_grams]
                no_allocations_loose_grams_list += [check_loose_grams]
                no_allocations_date_crop_facility_key_list += [hfsf_date_crop_facility_key]

    # write no allocations lists to HarvestUnallocated_Facts
    for na_idx in range(len(no_allocations_date_list)):
        unallocated_date = no_allocations_date_list[na_idx]
        unallocated_facility = no_allocations_facility_id_list[na_idx]
        unallocated_crop = no_allocations_crop_id_list[na_idx]
        unallocated_plant_sites = no_allocations_plant_sites_list[na_idx]
        unallocated_whole_grams = no_allocations_whole_grams_list[na_idx]
        unallocated_loose_grams = no_allocations_loose_grams_list[na_idx]

        # qty unit is generic 4 oz. retail qty (127.57275 g)
        unallocated_qty = round(unallocated_loose_grams/127.57275,2)

        tuple_to_write = (harvest_unallocated_id,
                          unallocated_date,
                          unallocated_facility,
                          unallocated_crop,
                          unallocated_plant_sites,
                          unallocated_whole_grams,
                          unallocated_loose_grams,
                          unallocated_qty,
                          load_date,
                          to_date,
                          is_active,
                          unallocated_facility)
        if is_pending == 1:
            tuple_to_write = (harvest_unallocated_id,
              unallocated_date,
              unallocated_facility,
              unallocated_crop,
              unallocated_plant_sites,
              unallocated_whole_grams,
              unallocated_loose_grams,
              unallocated_qty,
              load_date,
              to_date,
              is_active)
        cnxn_cursor.execute(huf_sql, tuple_to_write)
        harvest_unallocated_id += 1

    cnxn.commit()
    cnxn_cursor.close()
    cnxn.close()
    
    return 'HarvestUnallocated_Facts pau'
    
    

def writeCustomerShortDemand(forecast_date, short_demand_out_LoL, is_pending = 0):
    '''
    #### Inputs:
    - forecast_date: date of the forecast
    - short_demand_out_LoL: list of seven lists cooresponding to short demand
        1. List of demand dates
        2. List of demand allocation dates
        3. List of greenhouse IDs
        4. List of product IDs
        5. List of customer IDs
        6. List of short demand quantities cooresponding to each combination of demand date/greenhouse/product
        7. List of production priorities cooresponding to each combination of demand date/greenhouse/product
    - is_pending: integer boolean 1 for Pending table or 0 for baseline table
    
    #### Algorithm:
    - load inputs
    - loop through short demand lists and insert entries into CustomerShortDemand_Facts
    - return string indicating completion
    #### Output: string indicating completion
    '''
        
    # connect to database
    HOSTNAME = socket.gethostname()

    if HOSTNAME == 'hostname':
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server}; 
                                Server=127.0.01,1443;
                                Database=databasename;
                                trusted_connection=yes""" # use windows auth on DB01
    else:
        with open(os.path.join(sys.path[0], "config.yml"), 'r') as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.SafeLoader)
    #    uid = cfg['databasename']['uid']
        uid = 'sa'
        pwd = cfg['databasename']['pwd'][:-3]
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server};
                                Server=hostname\MSSQLSERVER1;
                                Database=databasename;
                                UID=%s;
                                PWD=%s;""" % (uid, pwd) # use config.yml on local machine

    cnxn = pyodbc.connect(CONNECTIONSTRING)   
    cnxn_cursor = cnxn.cursor()

    
    new_sdf_demand_date_list = short_demand_out_LoL[0]
    new_sdf_demand_allocation_date_list = short_demand_out_LoL[1]
    new_sdf_demand_facility_id_list = short_demand_out_LoL[2]
    new_sdf_product_id_list = short_demand_out_LoL[3]
    new_sdf_customer_id_list = short_demand_out_LoL[4]
    new_sdf_short_demand_qty_list = short_demand_out_LoL[5]
    #new_sdf_production_priority_list = short_demand_out_LoL[6]
    
    
    # initialize timestamp
    load_date = DT.datetime.now()
    to_date = DT.datetime.strptime('2099-12-31 00:00:00.000000', '%Y-%m-%d %H:%M:%S.%f')
    is_active = 1

    
    # initizlize index
    sql = """
    SELECT MAX(CustomerShortDemandID) FROM CustomerShortDemand_Facts
    """
    cnxn_cursor.execute(sql)
    max_old_id = cnxn_cursor.fetchone()[0]
    if max_old_id is None:
        max_old_id = 0
    short_demand_id = max_old_id + 1
    
    # insert
    sdf_sql = """
    INSERT INTO CustomerShortDemand_Facts
    VALUES (?,?,?,?,?,?,?,?,?,?);
    """ 
    if is_pending == 1:
        
        sql = """
        SELECT MAX(CustomerShortDemandPendingID) FROM CustomerShortDemandPending_Facts
        """
        cnxn_cursor.execute(sql)
        max_old_id = cnxn_cursor.fetchone()[0]
        if max_old_id is None:
            max_old_id = 0
        short_demand_id = max_old_id + 1

        sdf_sql = """
        INSERT INTO CustomerShortDemandPending_Facts
        VALUES (?,?,?,?,?,?,?,?,?,?);
        """ 

    # write new short demand for the day to ShortDemand_Facts
    for nsdf_idx in range(len(new_sdf_demand_date_list)):
        nsdf_demand_date = new_sdf_demand_date_list[nsdf_idx]
        nsdf_demand_allocation_date = new_sdf_demand_allocation_date_list[nsdf_idx]
        nsdf_demand_facility_id = new_sdf_demand_facility_id_list[nsdf_idx]
        nsdf_product_id = new_sdf_product_id_list[nsdf_idx]
        nsdf_customer_id = new_sdf_customer_id_list[nsdf_idx]
        nsdf_short_demand_qty = new_sdf_short_demand_qty_list[nsdf_idx]
       
        
        
        if nsdf_demand_allocation_date == forecast_date and nsdf_short_demand_qty > 0:
            tuple_to_write = (short_demand_id,
                                nsdf_demand_date,
                                nsdf_demand_allocation_date,
                                nsdf_demand_facility_id,
                                nsdf_product_id,
                                nsdf_customer_id,
                                nsdf_short_demand_qty,
                                load_date,
                                to_date,
                                is_active
                                )
            cnxn_cursor.execute(sdf_sql, tuple_to_write)
            short_demand_id += 1                

    cnxn.commit()
    cnxn_cursor.close()
    cnxn.close()
    
    return 'CustomerShortDemand_Facts for ' + str(forecast_date) + ' pau'



def inventoryRollover(evening_date, products_LoL, morning_date, is_pending = 0):
    '''
    #### Inputs:
    - evening_date: date before inventory rollover
    - products_LoL: products information as list of seven lists
        1. List of product IDs
        2. List of stop sell days
        3. List of crop IDs
        4. List of net weight grams
        5. List of is whole boolean flags
        7. List of production priorities
    - is_pending: integer boolean 1 for Pending table or 0 for baseline table
    
    
    #### Algorithm:
    - load input
    - load end-of-day inventory from InventoryAllocation_Facts active records where InventoryDate matches evening_date
    - return output new inventory list of lists
    
    #### Output: list of four lists containing inventory to roll over
        1. List of greenhouse IDs
        2. List of product IDs
        3. List of enjoy-by-dates
        4. List of inventory quantities
    '''
    
        
    # connect to database
    HOSTNAME = socket.gethostname()

    if HOSTNAME == 'databasename':
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server}; 
                                Server=127.0.0.1,1443;
                                Database=databasename;
                                trusted_connection=yes""" # use windows auth on DB01
    else:
        with open(os.path.join(sys.path[0], "config.yml"), 'r') as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.SafeLoader)
    #    uid = cfg['databasename']['uid']
        uid = 'sa'
        pwd = cfg['databasename']['pwd'][:-3]
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server};
                                Server=hostname\MSSQLSERVER1;
                                Database=databasename;
                                UID=%s;
                                PWD=%s;""" % (uid, pwd) # use config.yml on local machine

    cnxn = pyodbc.connect(CONNECTIONSTRING)   
    cnxn_cursor = cnxn.cursor()

    evening_date_str = evening_date.strftime("%Y-%m-%d")
    
    new_inv_inventory_facility_id_list = list()
    new_inv_product_id_list = list()
    new_inv_enjoy_by_date_list = list()
    new_inv_end_of_day_qty_list = list()
    
    sql = """ SELECT InventoryGreenhouseID, ProductID, EnjoyByDate, MIN(HoldQty) AS EndOfDayQty
                FROM CustomerInventoryAllocation_Facts
                WHERE IsActive = 1
                GROUP BY InventoryGreenhouseID, ProductID, EnjoyByDate
                ORDER BY InventoryGreenhouseID, ProductID, EnjoyByDate"""
    if is_pending == 1:
        sql = """ SELECT InventoryGreenhouseID, ProductID, EnjoyByDate, MIN(HoldQty) AS EndOfDayQty
            FROM CustomerInventoryAllocationPending_Facts
            WHERE IsActive = 1
            GROUP BY InventoryGreenhouseID, ProductID, EnjoyByDate
            ORDER BY InventoryGreenhouseID, ProductID, EnjoyByDate"""


    cnxn_cursor.execute(sql) 
    row = cnxn_cursor.fetchone()
    if row is not None:
        end_of_day_qty = row[3]
        
        if end_of_day_qty > 0:
        

            new_inv_inventory_facility_id_list += [row[0]]
            new_inv_product_id_list+= [row[1]]
            new_inv_enjoy_by_date_list += [row[2]]
            new_inv_end_of_day_qty_list += [row[3]]


    while row is not None:
        row = cnxn_cursor.fetchone()
        if row is not None:
            end_of_day_qty = row[3]

            if end_of_day_qty > 0:
                new_inv_inventory_facility_id_list += [row[0]]
                new_inv_product_id_list+= [row[1]]
                new_inv_enjoy_by_date_list += [row[2]]
                new_inv_end_of_day_qty_list += [row[3]]

    # remove out products that have not yet been produced based on Total Shelf Life, Enjoy By Date, and Evening Date
        # total shelf life of products
    pd_product_id_list = products_LoL[0]
    pd_total_shelf_life_list = products_LoL[5]
    pd_production_priority_list = products_LoL[6]
          

    # inventory without products that have not been produced yet
    nn_inv_inventory_facility_id_list = list()
    nn_inv_product_id_list = list()
    nn_inv_enjoy_by_date_list = list()
    nn_inv_end_of_day_qty_list = list()

    for inv_idx in range(len(new_inv_product_id_list)):
        
        product_id = new_inv_product_id_list[inv_idx]
        enjoy_by_date = new_inv_enjoy_by_date_list[inv_idx]
        
        # shelf life
        product_shelf_life = 30
        if product_id in pd_product_id_list:
            product_shelf_life = pd_total_shelf_life_list[pd_product_id_list.index(product_id)]
            production_priority = pd_production_priority_list[pd_product_id_list.index(product_id)]
        # packout date
        packout_date = enjoy_by_date - DT.timedelta(days = product_shelf_life)
        
        # build initial lists for smooth inventory when the inventory
        # the oldest products in inventory were put into boxes on the packout date
        # for a valid inventory entry, the packout date must be before the evening date (or it's done in the future)
        # does not apply for Gotham Foods
        if packout_date < morning_date or production_priority == 5:
            nn_inv_inventory_facility_id_list += [new_inv_inventory_facility_id_list[inv_idx]]
            nn_inv_product_id_list += [product_id]
            nn_inv_enjoy_by_date_list += [enjoy_by_date]
            nn_inv_end_of_day_qty_list += [new_inv_end_of_day_qty_list[inv_idx]]



    # compress inventory to one qty per combination of facility/product/enjoy-by-date
    csi_facility_id_list = list()
    csi_product_id_list = list()
    csi_enjoy_by_date_list = list()
    csi_qty_list = list()
    csi_fpe_key_list = list()
    
    for si_idx in range(len(nn_inv_enjoy_by_date_list)):
        si_facility_id = nn_inv_inventory_facility_id_list[si_idx]
        si_product_id = nn_inv_product_id_list[si_idx]
        si_enjoy_by_date = nn_inv_enjoy_by_date_list[si_idx]
        
        # check the facility/product/enjoy-by-date
        fpe_key = str(si_facility_id) + '_' + str(si_product_id) + '_' + str(si_enjoy_by_date)
        
        si_qty = nn_inv_end_of_day_qty_list[si_idx]
        
        if fpe_key in csi_fpe_key_list:
            # the entry exists, so update qty
            csi_idx = csi_fpe_key_list.index(fpe_key)
            csi_qty_list[csi_idx] += si_qty
        if fpe_key not in csi_fpe_key_list:
            # the entry doesn't exist yet, so create it
            csi_facility_id_list += [si_facility_id]
            csi_product_id_list += [si_product_id]
            csi_enjoy_by_date_list += [si_enjoy_by_date]
            csi_qty_list += [si_qty]
            csi_fpe_key_list +=[fpe_key]
    
    # make output smooth_inv_LoL
    new_inv_LoL = [csi_facility_id_list,
                      csi_product_id_list,
                      csi_enjoy_by_date_list,
                      csi_qty_list]
    
    cnxn.commit()
    cnxn_cursor.close()
    cnxn.close()
    return new_inv_LoL



def smoothRollover(evening_date, inventory_LoL, roll_harvest_LoL, products_LoL, morning_date):
    '''
    #### Inputs:
    - evening_date: date before smooth rollover
    - inventory_LoL: list of four lists containing inventory without allocated smooth quantities
        1. List of greenhouse IDs
        2. List of product IDs
        3. List of enjoy-by-dates
        4. List of inventory quantities
    - products_LoL: products information as list of six lists
        1. List of product IDs
        2. List of stop sell days
        3. List of crop IDs
        4. List of net weight grams
        5. List of is whole boolean flags
        6. list of total shelf life days
#     #### Algorithm:
        1. initialize inputs
        2. add qty allocated to the inventory buffer (rollover qty) to the inventory lists
        3. compress inventory to one row per combination of facility/product/enjoy-by-date
        4. return output
#     #### Output: smooth_inv_LoL
#     - smooth_inv_LoL: list of four lists containing inventory to roll over including allocated smooth quantities
#         1. List of greenhouse IDs
#         2. List of product IDs
#         3. List of enjoy-by-dates
#         4. List of inventory quantities
#     '''
    
    # move cases built for smoothening to the next day
    # returns new_inv_Lol containing the new quantities
    
    # total shelf life of products
    pd_product_id_list = products_LoL[0]
    pd_total_shelf_life_list = products_LoL[5]

    
    # inventory without products that have not been produced yet
    smooth_inv_inventory_facility_id_list = inventory_LoL[0]
    smooth_inv_product_id_list = inventory_LoL[1]
    smooth_inv_enjoy_by_date_list = inventory_LoL[2]
    smooth_inv_end_of_day_qty_list = inventory_LoL[3]            
    
    roll_facility_id_list = roll_harvest_LoL[0]
    roll_product_id_list = roll_harvest_LoL[1]
    roll_qty_list = roll_harvest_LoL[2]

    if morning_date > evening_date:
    
        for roll_idx in range(len(roll_facility_id_list)):

            # get rollover quantity
            roll_facility_id = roll_facility_id_list[roll_idx]
            roll_product_id = roll_product_id_list[roll_idx]
            roll_qty = roll_qty_list[roll_idx]

            # get enjoy by date
            roll_product_shelf_life = 30
            roll_enjoy_by_date = evening_date + DT.timedelta(days = roll_product_shelf_life)
            if roll_product_id in pd_product_id_list:
                roll_product_shelf_life = pd_total_shelf_life_list[pd_product_id_list.index(roll_product_id)]
                if roll_product_shelf_life is not None:
                    roll_enjoy_by_date = evening_date + DT.timedelta(days = roll_product_shelf_life)

                # add to smooth inventory lists
                smooth_inv_inventory_facility_id_list += [roll_facility_id]
                smooth_inv_product_id_list += [roll_product_id]
                smooth_inv_enjoy_by_date_list += [roll_enjoy_by_date]
                smooth_inv_end_of_day_qty_list += [roll_qty]


    # compress smooth inventory to one qty per combination of facility/product/enjoy-by-date
    csi_facility_id_list = list()
    csi_product_id_list = list()
    csi_enjoy_by_date_list = list()
    csi_qty_list = list()
    csi_fpe_key_list = list()
    
    for si_idx in range(len(smooth_inv_enjoy_by_date_list)):
        si_facility_id = smooth_inv_inventory_facility_id_list[si_idx]
        si_product_id = smooth_inv_product_id_list[si_idx]
        si_enjoy_by_date = smooth_inv_enjoy_by_date_list[si_idx]
        
        # check the facility/product/enjoy-by-date
        fpe_key = str(si_facility_id) + '_' + str(si_product_id) + '_' + str(si_enjoy_by_date)
        
        si_qty = smooth_inv_end_of_day_qty_list[si_idx]
        
        if fpe_key in csi_fpe_key_list:
            # the entry exists, so update qty
            csi_idx = csi_fpe_key_list.index(fpe_key)
            csi_qty_list[csi_idx] += si_qty
        if fpe_key not in csi_fpe_key_list:
            # the entry doesn't exist yet, so create it
            csi_facility_id_list += [si_facility_id]
            csi_product_id_list += [si_product_id]
            csi_enjoy_by_date_list += [si_enjoy_by_date]
            csi_qty_list += [si_qty]
            csi_fpe_key_list +=[fpe_key]
    
    # make output smooth_inv_LoL
    smooth_inv_LoL = [csi_facility_id_list,
                      csi_product_id_list,
                      csi_enjoy_by_date_list,
                      csi_qty_list]
    
    
    return smooth_inv_LoL





def inventoryForecast(forecast_date, inventory_LoL, products_LoL, transfers_LoL, tier_count):
    '''
    #### Inputs:
    - forecast_date: date to compare to the stop sell date
    - inventory_LoL: inventory information as a list of lists
        1. List of greenhouse IDs
        2. List of product IDs
        3. List of enjoy-by-dates
        4. List of inventory quantites (rolled over from yesterday)
    - products_LoL: product information as a list of lists
        1. List of product IDs
        2. List of stop sell days for each product
    - transfers_LoL: transfers information as a list of lists
        1. List of ship dates
        2. List of arrival dates
        3. List of ship greenhouse IDs
        4. List of arrival greenhouse IDs
        5. List of product IDs
        6. List of customer IDs
        7. List of enjoy-by-dates
        8. List of transfer quantites
    - tier_count: allocation tier for the harvest allocation from harvest city to customer
        
    #### Algorithm:
    - load inputs
    - initialize outputs
    - looping through each inventory entry:
        - compute stop sell date to compare to forecast date
        - if stop sell date is greater than or equal to forecast date, add the entry to output inventory lists
        - if stop sell date is less than forecast date, add entry to output stop sell lists
    - looping through each transfer entry:
        - if the inbound transfer arrival date is equal to the forecast date and there is greenhouse/product/enjoy-by-date is unique, add the entry to output inventory lists
        - if the inbound transfer arrival date is equal to the forecast date and there is greenhouse/product/enjoy-by-date is not unique, add the transfer quantity to the output inventory quantity list
    - return the output tuple
    #### Output: (inventory_out_LoL, shelf_life_guarantee_out_LoL)
    - inventory_out_LoL: list of four lists containing expected inventory that is not expiring
        1. List of greenhouse IDs
        2. List of enjoy-by-dates
        3. List of product IDs
        4. List of inventory quantities cooresponding to each combination of greenhouse/product/enjoy-by-date
    - shelf_life_guarantee_out_LoL: list of four lists containing expected stop sell (inventory that is expiring on the forecast_date)
        1. List of greenhouse IDs 
        2. List of enjoy-by-dates
        3. List of product IDs
        4. List of stop sell quantities cooresponding to each combination of greenhouse/product/enjoy-by-date
    '''
    
    iaf_inventory_facility_id_list = inventory_LoL[0]
    iaf_product_id_list= inventory_LoL[1]
    iaf_enjoy_by_date_list = inventory_LoL[2]
    iaf_end_of_day_qty_list = inventory_LoL[3]
    
    pd_product_id_list = products_LoL[0]
    pd_shelf_life_guarantee_list = products_LoL[1]
    
    tsf_ship_date_list = transfers_LoL[0]
    tsf_arrival_date_list = transfers_LoL[1]
    tsf_ship_facility_id_list = transfers_LoL[2]
    tsf_arrival_facility_id_list = transfers_LoL[3]
    tsf_product_id_list = transfers_LoL[4]
    tsf_enjoy_by_date_list = transfers_LoL[5]
    tsf_transfer_qty_list = transfers_LoL[6]
    
    # initialize outputs
    inv_out_facility_id_list = list()
    inv_out_product_id_list = list()
    inv_out_enjoy_by_date_list = list()
    inv_out_quantity_list = list()
    
    inv_out_facility_product_date_key_list = list()

    ss_out_facility_id_list = list()
    ss_out_product_id_list = list()
    ss_out_enjoy_by_date_list = list()
    ss_out_quantity_list = list()
    
    # read allocation results and make 1) inventory_LoL and 2) shelf_life_guarantee_LoL
    for iaf_idx in range(len(iaf_inventory_facility_id_list)):

        check_inventory_facility_id = iaf_inventory_facility_id_list[iaf_idx]
        check_product_id = iaf_product_id_list[iaf_idx]                
        check_enjoy_by_date = iaf_enjoy_by_date_list[iaf_idx]
        check_end_of_day_qty = iaf_end_of_day_qty_list[iaf_idx]

        # check enjoy_by_date with forecast_date
        
        check_shelf_life_guarantee_days = pd_shelf_life_guarantee_list[pd_product_id_list.index(check_product_id)]
        shelf_life_guarantee_date = check_enjoy_by_date - DT.timedelta(days = check_shelf_life_guarantee_days)

        if shelf_life_guarantee_date >= forecast_date and check_end_of_day_qty > 0:
            inv_out_facility_id_list += [check_inventory_facility_id]
            inv_out_product_id_list += [check_product_id]
            inv_out_enjoy_by_date_list += [check_enjoy_by_date]
            inv_out_quantity_list += [check_end_of_day_qty]
            
            inv_out_facility_product_date_key_list += str(check_inventory_facility_id) + '_' + str(check_product_id) + '_' + str(check_enjoy_by_date)
        else:
            ss_out_facility_id_list += [check_inventory_facility_id]
            ss_out_product_id_list += [check_product_id]
            ss_out_enjoy_by_date_list += [check_enjoy_by_date]
            ss_out_quantity_list += [check_end_of_day_qty]
    
    
    # inbound transfers
    if tier_count == 1:
        for tsf_idx in range(len(tsf_ship_date_list)):
            tsf_arrival_date = tsf_arrival_date_list[tsf_idx]
            if tsf_arrival_date == forecast_date:
                # add inbound transfer to inventory of arrival facility
                tsf_arrival_facility_id = tsf_arrival_facility_id_list[tsf_idx]
                tsf_arrival_location_name = fd_location_name_list[fd_facility_id_list.index(tsf_arrival_facility_id)]
                tsf_product_id = tsf_product_id_list[tsf_idx]
                tsf_enjoy_by_date = tsf_enjoy_by_date_list[tsf_idx]
                tsf_transfer_qty = tsf_transfer_qty_list[tsf_idx]

                tsf_facility_product_date_key = str(tsf_arrival_facility_id)+ '_' + str(tsf_product_id) + '_' + str(tsf_enjoy_by_date)

                if tsf_facility_product_date_key in inv_out_facility_product_date_key_list:
                    inv_out_idx = inv_out_facility_product_date_key_list.index(tsf_facility_product_date_key)
                    inv_out_quantity_list[inv_out_idx] += tsf_transfer_qty
                if tsf_facility_product_date_key not in inv_out_facility_product_date_key_list:
                    inv_out_facility_id_list += [tsf_arrival_facility_id]
                    inv_out_enjoy_by_date_list += [tsf_enjoy_by_date]
                    inv_out_product_id_list+= [tsf_product_id]
                    inv_out_quantity_list += [tsf_transfer_qty]
                    inv_out_facility_product_date_key_list += [tsf_facility_product_date_key]

    inventory_out_LoL = [inv_out_facility_id_list, inv_out_product_id_list, inv_out_enjoy_by_date_list, inv_out_quantity_list]
    shelf_life_guarantee_out_LoL = [ss_out_facility_id_list,  ss_out_product_id_list, ss_out_enjoy_by_date_list, ss_out_quantity_list]

    return (inventory_out_LoL, shelf_life_guarantee_out_LoL)

    
def writeStopSell(forecast_date,shelf_life_guarantee_LoL, is_pending = 0):
    '''
    #### Inputs:
    - inventory_date: date of the forecast of the stop sell inventory
    - shelf_life_guarantee_out_LoL: list of four lists containing expected stop sell (inventory that is expiring on the forecast_date)
        1. ss_out_facility_id_list: list of greenhouse IDs 
        2. ss_out_enjoy_by_date_list: list of enjoy-by-dates
        3. ss_out_product_id_list: list of product IDs
        4. ss_out_quantity_list: list of  stop sell quantities cooresponding to each combination of greenhouse/product/enjoy-by-date
    - is_pending: integer boolean 1 for Pending table or 0 for baseline table
    
    #### Algorithm:
    - load inputs
    - loop through stop sell lists and insert entries into StopSell_Facts
    - return string indicating completion
    #### Output: string indicating completion
    '''

        
    # connect to database
    HOSTNAME = socket.gethostname()

    if HOSTNAME == 'hostname:
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server}; 
                                Server=127.0.0.1,1443;
                                Database=databasename;
                                trusted_connection=yes""" # use windows auth on DB01
    else:
        with open(os.path.join(sys.path[0], "config.yml"), 'r') as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.SafeLoader)
    #    uid = cfg['databasename']['uid']
        uid = 'sa'
        pwd = cfg['databasename']['pwd'][:-3]
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server};
                                Server=hostname\MSSQLSERVER1;
                                Database=databasename;
                                UID=%s;
                                PWD=%s;""" % (uid, pwd) # use config.yml on local machine
    cnxn = pyodbc.connect(CONNECTIONSTRING)   
    cnxn_cursor = cnxn.cursor()

    
    sql = """
    SELECT MAX(StopSellID) FROM StopSell_Facts
    """
    cnxn_cursor.execute(sql) 
    
    max_old_id = cnxn_cursor.fetchone()[0]
    if max_old_id is None:
        max_old_id = 0
    
    shelf_life_guarantee_id = max_old_id + 1

    load_date = DT.datetime.now()
    to_date = DT.datetime.strptime('2099-12-31 00:00:00.000000', '%Y-%m-%d %H:%M:%S.%f')
    is_active = 1

    sql = """
    INSERT INTO StopSell_Facts
    VALUES (?,?,?,?,?,?,?,?,?,?);
    """ 
    if is_pending == 1:
        
            
        sql = """
        SELECT MAX(StopSellPendingID) FROM StopSellPending_Facts
        """
        cnxn_cursor.execute(sql) 

        max_old_id = cnxn_cursor.fetchone()[0]
        if max_old_id is None:
            max_old_id = 0

        shelf_life_guarantee_id = max_old_id + 1
        
        sql = """
        INSERT INTO StopSellPending_Facts
        VALUES (?,?,?,?,?,?,?,?,?);
        """ 

    # Stop sell
    ss_facility_id_list = shelf_life_guarantee_LoL[0]
    ss_product_id_list = shelf_life_guarantee_LoL[1]
    ss_enjoy_by_date_list = shelf_life_guarantee_LoL[2]
    ss_quantity_list = shelf_life_guarantee_LoL[3]

    for ss_idx in range(len(ss_facility_id_list)):
        ss_facility_id = ss_facility_id_list[ss_idx]
        ss_enjoy_by_date = ss_enjoy_by_date_list[ss_idx]
        ss_product_id = ss_product_id_list[ss_idx]
        ss_quantity = ss_quantity_list[ss_idx]


        tuple_to_write = (shelf_life_guarantee_id,
                          forecast_date,
                          ss_facility_id,
                          ss_product_id,
                          ss_enjoy_by_date,
                          ss_quantity,
                          load_date,
                          to_date,
                          is_active,
                          ss_facility_id)
        if is_pending == 1:
            tuple_to_write = (shelf_life_guarantee_id,
                          forecast_date,
                          ss_facility_id,
                          ss_product_id,
                          ss_enjoy_by_date,
                          ss_quantity,
                          load_date,
                          to_date,
                          is_active)
        
        cnxn_cursor.execute(sql, tuple_to_write)
        shelf_life_guarantee_id += 1
    
    cnxn.commit()
    cnxn_cursor.close()
    cnxn.close()

    return 'StopSell_Facts for ' + str(forecast_date) + ' pau'


def writeAllocated(allocated_crops_LoL, tier_count):
    """
    Write mid allocations to database
    Input: allocated_crops_LoL: list of four lists tracking harvest allocation
        1. Date_crop_facility list
        2. Starting plant sites list
        3. Allocated plant sites list
        4. Completed crop allocation list
    Algorithm:
        1. Change data capture
        2. Write allocated crops to Allocated_Facts
    Output: string indicating completion
    """

        
    # connect to database
    HOSTNAME = socket.gethostname()

    if HOSTNAME == 'hostname':
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server}; 
                                Server=127.0.0.1,1443;
                                Database=databasename;
                                trusted_connection=yes""" # use windows auth on DB01
    else:
        with open(os.path.join(sys.path[0], "config.yml"), 'r') as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.SafeLoader)
    #    uid = cfg['databasename']['uid']
        uid = 'sa'
        pwd = cfg['databasename']['pwd'][:-3]
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server};
                                Server=hostname\MSSQLSERVER1;
                                Database=databasename;
                                UID=%s;
                                PWD=%s;""" % (uid, pwd) # use config.yml on local machine
    cnxn = pyodbc.connect(CONNECTIONSTRING)   
    cnxn_cursor = cnxn.cursor()
    
    # CDC
    sql = """
    UPDATE Allocated_Facts
    SET ToDate = GETDATE(), IsActive = 0
    WHERE IsActive = 1;
    """
    cnxn_cursor.execute(sql)

    
    sql = """
    SELECT MAX(AllocatedID) FROM Allocated_Facts
    """
    cnxn_cursor.execute(sql) 
    
    max_old_id = cnxn_cursor.fetchone()[0]
    if max_old_id is None:
        max_old_id = 0
    
    # new entries
    allocated_id = max_old_id + 1

    load_date = DT.datetime.now()
    to_date = DT.datetime.strptime('2099-12-31 00:00:00.000000', '%Y-%m-%d %H:%M:%S.%f')
    is_active = 1

    
    sql = """
    INSERT INTO Allocated_Facts
    VALUES (?,?,?,?,?,?,?,?,?,?,?);
    """ 

    allocated_date_crop_facility_key_list = allocated_crops_LoL[0]
    allocated_starting_ps_list = allocated_crops_LoL[1]
    allocated_plant_sites_list = allocated_crops_LoL[2]
    complete_crop_allocation_key_list = allocated_crops_LoL[3]

    for a_idx in range(len(allocated_date_crop_facility_key_list)):
        date_crop_facility_key = allocated_date_crop_facility_key_list[a_idx]
        starting_ps = allocated_starting_ps_list[a_idx]
        allocated_ps = allocated_plant_sites_list[a_idx]
        
        is_complete = 0
        if date_crop_facility_key in complete_crop_allocation_key_list:
            is_complete = 1
        
        allocated_date = DT.datetime.strptime(str(date_crop_facility_key.split("_")[0]), '%Y-%m-%d').date()
        allocated_crop_id = int(date_crop_facility_key.split("_")[1])
        allocated_facility_id = int(date_crop_facility_key.split("_")[2])

        tuple_to_write = (
            allocated_id,
            allocated_date,
            allocated_crop_id,
            allocated_facility_id,
            tier_count,
            starting_ps,
            allocated_ps,
            is_complete,
            load_date,
            to_date,
            is_active
        )
        #print(tuple_to_write)
        cnxn_cursor.execute(sql, tuple_to_write)
        allocated_id += 1
    
    cnxn.commit()
    cnxn_cursor.close()
    cnxn.close()

    return 'Allocated_Facts pau'


def readAllocated():
    '''
    #### Inputs: none
    
    
    #### Algorithm:
    - read Allocated_Facts active entries
    
    #### Output: allocated_crops_LoL: list of four lists for allocation tracking
        1. Date_crop_facility list
        2. Starting plant sites list
        3. Allocated plant sites list
        4. Completed crop allocation list
    '''
    
        
    # connect to database
    HOSTNAME = socket.gethostname()

    if HOSTNAME == 'hostname':
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server}; 
                                Server=127.0.0.1,1443;
                                Database=databasename;
                                trusted_connection=yes""" # use windows auth on DB01
    else:
        with open(os.path.join(sys.path[0], "config.yml"), 'r') as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.SafeLoader)
    #    uid = cfg['databasename']['uid']
        uid = 'sa'
        pwd = cfg['databasename']['pwd'][:-3]
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server};
                                Server=hostname\MSSQLSERVER1;
                                Database=databasename;
                                UID=%s;
                                PWD=%s;""" % (uid, pwd) # use config.yml on local machine

    cnxn = pyodbc.connect(CONNECTIONSTRING)   
    cnxn_cursor = cnxn.cursor()

    allocated_date_crop_facility_key_list = list()
    allocated_starting_ps_list = list()
    allocated_plant_sites_list = list()
    complete_crop_allocation_key_list = list()
    
    sql = """ SELECT AllocatedDate, CropID, GreenhouseID, StartingPlantSites, AllocatedPlantSites, IsComplete
                FROM Allocated_Facts
                WHERE IsActive = 1
                ORDER BY AllocatedDate, CropID, GreenhouseID"""

    
    cnxn_cursor.execute(sql) 
    row = cnxn_cursor.fetchone()
    if row is not None:
        a_date = row[0]
        a_crop_id = row[1]
        a_facility_id = row[2]
        
        a_key = str(a_date) + '_' + str(a_crop_id) + '_' + str(a_facility_id)
        
        allocated_date_crop_facility_key_list += [a_key]
        allocated_starting_ps_list += [row[3]]
        allocated_plant_sites_list += [row[4]]
        
        a_is_complete = row[5]
        if a_is_complete == 1:
            complete_crop_allocation_key_list += [a_key]
        
    while row is not None:
        row = cnxn_cursor.fetchone()
        if row is not None:
            a_date = row[0]
            a_crop_id = row[1]
            a_facility_id = row[2]

            a_key = str(a_date) + '_' + str(a_crop_id) + '_' + str(a_facility_id)

            allocated_date_crop_facility_key_list += [a_key]
            allocated_starting_ps_list += [row[3]]
            allocated_plant_sites_list += [row[4]]

            a_is_complete = row[5]
            if a_is_complete == 1:
                complete_crop_allocation_key_list += [a_key]
 
    # make output smooth_inv_LoL
    allocated_crops_LoL = [allocated_date_crop_facility_key_list,
                            allocated_starting_ps_list,
                            allocated_plant_sites_list,
                            complete_crop_allocation_key_list]
    
    cnxn.commit()
    cnxn_cursor.close()
    cnxn.close()
    
    return allocated_crops_LoL

def calculateTransfers(demand_allocation_date, harvest_in_LoL, short_demand_LoL, facilities_LoL, allocated_crops_LoL, products_LoL, transfer_constraints_LoL, calendar_LoL, calc_transfers_LoL, inventory_allocation_out_LoL):
    '''
    #### Inputs:
        - demand_allocation_date: date of the short demand allocation in main loop
        - harvest_in_LoL: harvest
            1. List of harvest dates
            2. List of greenhouse IDs
            3. List of greenhouse line IDs
            4. List of crop IDs
            5. List of expected plant sites
            6. List of average headweights
            7. List of loose grams per plant site
        - short_demand_LoL: remaining demand to allocate towards with transfers
            1. List of demand dates
            2. List of demand allocation dates
            3. List of greenhouse IDs
            4. List of product IDs
            5. List of customer IDs
            6. List of short demand quantities cooresponding to each combination of demand date/greenhouse/product/customer
            7. List of production priorities cooresponding to each combination of demand date/greenhouse/product
        - facilities_LoL: greenhouses dimension
            1. List of greenhouse IDs
            2. List of city abbreviations
        - allocated_crops_LoL:  mid-allocation tracking
            1. List of date/crop/greenhouse key combinations that have an allocation
            2. List of starting plant sites for each date/crop/greenhouse key combination
            3. List of allocated plant sites for each date/crop/greenhouse key combination
            4. List of date/crop/greenhouse key combinations that have been fully allocated
        - products_LoL: products information
            1. List of product IDs
            2. List of stop sell days
            3. List of crop IDs
            4. List of net weight grams
            5. List of is whole boolean flags
            6. List of total shelf life
            7. List of production priority
            8. List of case equivalent multipler
            9. List of cases per pallet
        - transfer_constraints_LoL: transfer constraints
            1. List of ship greenhouse IDs
            2. List of arrival greenhouse IDs
            3. List of ship day of week
            4. List of pack lead time days
            5. List of ship duration days
            6. List of max pallet capacity
            7. List of gfoods transfer boolean flag
        - calendar_LoL: calendar information
            1. List of date day
            2. List of year number
            3. List of week of year
            4. List of year week
            5. List of day of week
        9. calc_transfers_LoL: initial calcualted transfers
            1.List of  sShip dates
            2. List of arrival dates
            3. List of arrival facilites
            4. List of transfer constraints
            5. List of product IDs
            6. List of enjoy-by-dates
            7. List of customer IDs
            8. List of transfer quantiites
            9. List of transfer pallets
            10. List of truck counts
        10.inventory_allocation_out_LoL: remaining inventory
            1. List of greenhouse IDs
            2. List of product IDs
            3. List of enjoy-by-dates
            4. List of customer IDs
            5. List of start-of-day quantities cooresponding to each combination of greenhouse/product/enjoy-by-date
            6. List of allocated quantities cooresponding to each combination of greenhouse/product/enjoy-by-date/customer
            7. List of end-of-day quantities cooresponding to each combination of greenhouse/product/enjoy-by-date
    #### Algorithm:
    - initialize inputs
    - loop through transfer constraints for the demand allocation date
        - check for initial truck capacity
        - organize demand into nested dictionaries 
        - inventory calculated transfers for GFOODS products
        - harvest calculated transfers for Retail products
    - create and return output tuple
    
    #### Output:transfer_tuple
        - inventory_allocation_transfers_LoL
            1. List of greenhouse IDs
            2. List of product IDs
            3. List of enjoy-by-dates
            4. List of customer IDs
            5. List of start-of-day quantities cooresponding to each combination of greenhouse/product/enjoy-by-date
            6. List of allocated quantities cooresponding to each combination of greenhouse/product/enjoy-by-date/customer
            7. List of end-of-day quantities cooresponding to each combination of greenhouse/product/enjoy-by-date
        - harvest_allocation_transfers_LoL: harvest allocation for calculated transfers
            1. List of demand allocation dates
            2. List of demand dates
            3. List of harvest greenhouse IDs
            4. List of demand greenhouse IDs
            5. List of crop IDs
            6. List of product IDs
            7. List of customer IDs
            7. List of forecasted grams per plant site values
            8. List of allocated plant sites
            9. List of allocated grams
            10. List of allocated quantities
            11. List of full packout boolean flags
        - allocated_crops_out_LoL: mid-allocation tracking
            1. List of date/crop/greenhouse key combinations that have an allocation
            2. List of starting plant sites for each date/crop/greenhouse key combination
            3. List of allocated plant sites for each date/crop/greenhouse key combination
            4. List of date/crop/greenhouse key combinations that have been fully allocated
        - short_demand_out_LoL: 
            1. List of demand dates
            2. List of demand allocation dates
            3. List of greenhouse IDs
            4. List of product IDs
            5. List of customer IDs
            6. List of short demand quantities cooresponding to each combination of demand date/greenhouse/product/customer
            7. List of production priorities cooresponding to each combination of demand date/greenhouse/product
        - new_calc_transfers_LoL: final calculated transfers
            1.List of  ship dates
            2. List of arrival dates
            3. List of arrival facilites
            4. List of transfer constraints
            5. List of product IDs
            6. List of enjoy-by-dates
            7. List of customer IDs
            8. List of transfer quantiites
            9. List of transfer pallets
            10. List of truck counts
   
        
    '''


    # input lists from harvest
    hfsf_harvest_date_list = harvest_in_LoL[0]
    hfsf_facility_id_list = harvest_in_LoL[1]
    hfsf_facility_line_id_list = harvest_in_LoL[2]
    hfsf_crop_id_list = harvest_in_LoL[3]
    hfsf_expected_plant_sites_list = harvest_in_LoL[4]
    hfsf_avg_headweight_list = harvest_in_LoL[5]
    hfsf_loose_grams_per_plant_site_list = harvest_in_LoL[6]
    
    # allocated crops
    allocated_date_crop_facility_key_list = allocated_crops_LoL[0]
    allocated_starting_ps_list = allocated_crops_LoL[1]
    allocated_plant_sites_list = allocated_crops_LoL[2]
    complete_crop_allocation_key_list = allocated_crops_LoL[3]
    

    # input lists from demand
    sdf_demand_date_list = short_demand_LoL[0]
    sdf_demand_allocation_date_list = short_demand_LoL[1]
    sdf_demand_facility_id_list = short_demand_LoL[2]
    sdf_product_id_list = short_demand_LoL[3]
    sdf_customer_id_list = short_demand_LoL[4]
    sdf_short_demand_qty_list = short_demand_LoL[5]
    
    ct_sdf_key_list = list()
    ct_qty_list = list()
    for sdf_idx in range(len(sdf_demand_date_list)):
        ct_sdf_key = sdf_demand_date_list[sdf_idx].strftime("%Y-%m-%d") + '_' + str(sdf_demand_facility_id_list[sdf_idx]) + '_' + str(sdf_product_id_list[sdf_idx]) + '_' + str(sdf_customer_id_list[sdf_idx])
        ct_qty = sdf_short_demand_qty_list[sdf_idx]
        if ct_sdf_key in ct_sdf_key_list:
            ct_qty_list[ct_sdf_key_list.index(ct_sdf_key)] += ct_qty
        if ct_sdf_key not in ct_sdf_key_list:
            ct_sdf_key_list += [ct_sdf_key]
            ct_qty_list += [ct_qty]
        
    
    # product dimension
    pd_product_id_list = products_LoL[0]
    pd_shelf_life_guarantee_list = products_LoL[1]
    pd_crop_id_list = products_LoL[2]
    pd_net_weight_grams_list = products_LoL[3]
    pd_is_whole_list = products_LoL[4]
    pd_total_shelf_life_list = products_LoL[5]
    pd_case_equivalent_multiplier_list = products_LoL[7]
    pd_cases_per_pallet_list = products_LoL[8]
    
    # facilities dimension
    fd_facility_id_list = facilities_LoL[0]
    fd_city_short_code_list = facilities_LoL[1]

    # read transfer constraints
    tcf_ship_greenhouse_id_list = transfer_constraints_LoL[0]
    tcf_arrival_greenhouse_id_list = transfer_constraints_LoL[1]
    tcf_ship_day_of_week_list = transfer_constraints_LoL[2]
    tcf_pack_lead_time_days_list = transfer_constraints_LoL[3]
    tcf_ship_duration_days_list = transfer_constraints_LoL[4]
    tcf_max_pallet_capacity_list = transfer_constraints_LoL[5]
    tcf_gfoods_transfer_list = transfer_constraints_LoL[6]
    
    # calendar dimension
    cald_date_day_list = calendar_LoL[0]
    cald_year_number_list = calendar_LoL[1]
    cald_week_of_year_list = calendar_LoL[2]
    cald_year_week_list = calendar_LoL[3]
    cald_year_week_dow_list = calendar_LoL[4]
    
    # initialize calculated transfer lists
    calc_ship_date_list = calc_transfers_LoL[0]
    calc_arrival_date_list  = calc_transfers_LoL[1]
    calc_ship_facility_id_list  = calc_transfers_LoL[2]
    calc_arrival_facility_id_list  = calc_transfers_LoL[3]
    calc_transfer_constraints_id_list = calc_transfers_LoL[4]
    calc_product_id_list = calc_transfers_LoL[5]
    calc_enjoy_by_date_list = calc_transfers_LoL[6]
    calc_customer_id_list = calc_transfers_LoL[7]
    calc_transfer_qty_list = calc_transfers_LoL[8]
    calc_transfer_pallets_list = calc_transfers_LoL[9]
    calc_truck_count_list = calc_transfers_LoL[10]
    
    # remaining inventory
    iaf_inventory_facility_id_list = inventory_allocation_out_LoL[0]
    iaf_product_id_list = inventory_allocation_out_LoL[1]
    iaf_enjoy_by_date_list = inventory_allocation_out_LoL[2]
    # start of day is the end qty after inventory allocation
    iaf_start_of_day_qty_list = inventory_allocation_out_LoL[6]
        
#     # initialize end of day qty list
    iaf_end_of_day_qty_list = list()
    for i in range(len(iaf_start_of_day_qty_list)):
        iaf_end_of_day_qty_list += [iaf_start_of_day_qty_list[i]]
    
#     # initialize lists to be updated as we allocate products from inventory
#     # greenhouse/product/enjoy-by-date can have multiple customers (multiple rows in CustomerInventoryAllocation_Facts)
    # customer IDs and allocated qty will be stored as list within list
    iaf_customer_id_list = [0] * len(iaf_end_of_day_qty_list)
    iaf_allocated_qty_list = [0] * len(iaf_end_of_day_qty_list)

    ri_key_list = list()
    ri_qty_list = list()
    for iaf_idx in range(len(iaf_inventory_facility_id_list)):        
        inventory_facility_id = iaf_inventory_facility_id_list[iaf_idx]
        product_id = iaf_product_id_list[iaf_idx]                
        end_of_day_qty = iaf_end_of_day_qty_list[iaf_idx]
        
        ri_key = str(inventory_facility_id) + '_' + str(product_id)
        if ri_key in ri_key_list:
            ri_qty_list[ri_key_list.index(ri_key)] += end_of_day_qty
        if ri_key not in ri_key_list:
            ri_key_list += [ri_key]
            ri_qty_list += [end_of_day_qty]
            
    # initialize harvest allocation lists for transfers
    # lists for CustomerHarvestAllocation_Facts
    haf_demand_allocation_date_list = list()
    haf_demand_date_list = list()
    haf_harvest_facility_id_list = list()
    haf_demand_facility_id_list = list()
    haf_crop_id_list = list()
    haf_product_id_list = list()
    haf_customer_id_list = list()
    haf_forecasted_gpps_list = list()
    haf_allocated_plant_sites_list = list()
    haf_allocated_grams_list = list()
    haf_allocated_qty_list = list()
    haf_full_packout_list = list()
    
    # initialize trqacking list for attempted allocations
    skip_key_list = list()
    
    # initial date to check ship day
    # add one day since we need at least one day to plan the calculated transfer
    initial_date = hfsf_harvest_date_list[1]
                    
    # these lists will track the delta of harvest lists through the allocation process
    allocated_date_product_facility_key_list = list()
    allocated_gpps_list = list()
    allocated_qty_list = list()
    allocated_product_plant_sites_list = list()

    complete_product_allocation_key_list = list()
    complete_customer_allocation_key_list = list()

    allocated_date_product_facility_customer_key_list = list()
    allocated_customer_gpps_list = list()
    allocated_customer_qty_list = list()
    allocated_customer_plant_sites_list = list()
    allocated_customer_demand_date_list = list()
                
    

        ##########################
        # OLD way to get to the ship day
#         # arrival day of week
#         arrival_dow = ship_dow + ship_duration_days
        
#         # demand allocation date day of week
#         dad_dow = demand_allocation_date.weekday()
#         # week of year of the demand allocation date
#         dad_woy = cald_week_of_year_list[cald_date_day_list.index(demand_allocation_date)]
#         # year of demand allocation date
#         dad_yr = cald_year_number_list[cald_date_day_list.index(demand_allocation_date)]
        
#         # next arrival day of week for the same route
#         # sort days of the week for the truck route
#         tcf_ship_greenhouse_indices = [i for i, x in enumerate(tcf_ship_greenhouse_id_list) if x == ship_greenhouse_id]
#         tcf_arrival_greenhouse_indices = [i for i, x in enumerate(tcf_arrival_greenhouse_id_list) if x == arrival_greenhouse_id]
#         tcf_ship_arrival_indices = list(set(tcf_ship_greenhouse_indices) & set(tcf_arrival_greenhouse_indices))
#         tcf_ship_dow_list = [tcf_ship_day_of_week_list[idx] for idx in tcf_ship_arrival_indices]
#         sorted_tcf_ship_dow_list = list(np.sort(tcf_ship_dow_list))
        

#         # ship day of the week for the transfer in the sorted list
#         this_idx = sorted_tcf_ship_dow_list.index(ship_dow)
#         this_dow = sorted_tcf_ship_dow_list[this_idx]
#         # find arrival date for transfer constraint given the arrival is the same week as demand allocation date
#         this_woy = dad_woy
#         this_yr = dad_yr
#         this_yr_wk_dow = str(this_yr) + '_' + str(this_woy) + '_' + str(this_dow)
#         ship_day = cald_date_day_list[cald_year_week_dow_list.index(this_yr_wk_dow)]
        
        
#         arrival_day = ship_day + DT.timedelta(days = ship_duration_days)
        
#         # weeks of the year in the same year as the demand allocation date
#         this_yr_indices = [i for i, x in enumerate(cald_year_number_list) if x == this_yr]
#         this_yr_woy_list = [cald_week_of_year_list[idx] for idx in this_yr_indices]
        
        
#         # if last day of the week, next ship day is the first day of the week shipping out next week
#         if this_dow == sorted_tcf_ship_dow_list[-1]:
#             next_dow = sorted_tcf_ship_dow_list[0]
#             next_woy = this_woy + 1
#             next_yr = this_yr
#             # if the last week of year, then next ship date is next year
#             if next_woy > max(this_yr_woy_list):
#                 next_woy = 1
#                 next_yr = this_yr + 1    
#             next_yr_wk_dow = str(next_yr) + '_' + str(next_woy) + '_' + str(next_dow)

#         # if not the last day of the week, next ship day is the next day of the week shipping out this week    
#         if this_dow != sorted_tcf_ship_dow_list[-1]:
#             next_dow = sorted_tcf_ship_dow_list[this_idx + 1]
#             next_yr_wk_dow = str(this_yr) + '_' + str(this_woy) + '_' + str(next_dow)

#         # next ship day and arrival day for the same route
#         cald_idx = cald_year_week_dow_list.index(next_yr_wk_dow)
#         next_ship_day = cald_date_day_list[cald_idx]        
#         next_arrival_day = next_ship_day + DT.timedelta(days = ship_duration_days)        

#         # checkpoint: if the demand allocation date is between the arrival date and next arrival date
#         if arrival_day <= demand_allocation_date and demand_allocation_date < next_arrival_day:
        
    ######################
    
    
    # for the constraint indices cooresponding to shipments
    for tcf_idx in range(len(tcf_ship_greenhouse_id_list)):
    #for tcf_idx in [0]:
        # determine if the demand allocation date is between the arrival date and next arrival date
        
    
        #checkpoint: demand_allocation date matches transfer constraint
        
        # NYC gets shipments on Tuesday (from BAL only), Thursday, and Saturday
        # DEN gets shipments on Wednesday and Friday
        
        #demand allocation date day of week (Mon is 0, Tues is 1, etc.)
#         Monday: 3 calculated transfers
#             1. receive in NYC from PVD previous Sat (1)
#             2. receive in NYC from BAL previous Sat (4)
#             3. recieve in DEN from CHI previous Fri (6)
#         Tuesday:
#             4. receive in NYC from PVD previous Sat (1)
#             5. receive in NYC from BAL current Tues (2)
#             6. recieve in DEN from CHI previous Fri (6)
#         Wednesday:
#             7. receive in NYC from PVD previous Sat (1)
#             8. receive in NYC from BAL previous Tues (2)
#             9. recieve in DEN from CHI current Wed (5)
#          Thursday:
#             10. receive in NYC from PVD current Thurs (0)
#             11. receive in NYC from BAL current Thurs (3)
#             12. recieve in DEN from CHI previous Wed (5)       
#         Friday:
#             13. receive in NYC from PVD previous Thurs (0)
#             14. receive in NYC from BAL previous Thurs (3)
#             15. recieve in DEN from CHI current Friday (6)    
#
# GFOODs only
#         Monday: 3 calculated transfers
#             16. receive in PVD from NYC previous Sat (8)
#             17. receive in BAL from NYC previous Sat (10)
#             18. recieve in DEN from CHI previous Sun (12)
#         Tuesday:
#             19. receive in PVD from NYC previous Sat (8)
#             20. receive in BAL from NYC previous Sat (10)
#             21. recieve in DEN from CHI previous Sun (12)
#         Wednesday:
#             22. receive in PVD from NYC previous Sat (8)
#             23. receive in BAL from NYC previous Sat (10)
#             24. recieve in DEN from CHI previous Sun (12)
#          Thursday:
#             25. receive in PVD from NYC current Thurs (7)
#             26. receive in BAL from NYC current Thurs (9)
#             27. recieve in DEN from CHI current Thurs (11)      
#         Friday:
#             28. receive in PVD from NYC previous Thurs (7)
#             29. receive in BAL from NYC previous Thurs (9)
#             30. recieve in DEN from CHI previous Thurs (11)
            
        # python datetime .weekday() starts with Mon as 0, whereas Calendars_dim DayOfWeek starts at 1
        dad_dow = demand_allocation_date.weekday() # note python datetime .weekday() starts with Mon as 0, whereas Calendars_dim DayOfWeek starts at 1
        if len(ct_sdf_key_list) > 0 and ((dad_dow == 0 and tcf_idx in [1,4,6,8,10,12]) or (dad_dow == 1 and tcf_idx in [1,2,6, 8,10,12]) or (dad_dow == 2 and tcf_idx in [1,2,5, 8,10,12]) or (dad_dow == 3 and tcf_idx in [0,3,5,7,9,11]) or (dad_dow == 4 and tcf_idx in [0,3,6,7,9,11])):
        
            #print(demand_allocation_date, tcf_idx)

            # transfer constraints
            arrival_greenhouse_id = tcf_arrival_greenhouse_id_list[tcf_idx]
            ship_greenhouse_id = tcf_ship_greenhouse_id_list[tcf_idx]
            ship_dow = tcf_ship_day_of_week_list[tcf_idx]
            pack_lead_time_days = tcf_pack_lead_time_days_list[tcf_idx]
            ship_duration_days = tcf_ship_duration_days_list[tcf_idx]
            max_pallet_capacity = tcf_max_pallet_capacity_list[tcf_idx]
            gfoods_transfer = tcf_gfoods_transfer_list[tcf_idx]

            # infer ship day based on demand_allocation_date
            
            # 5, 9, 10, 11, 15 demand allocation date matches arrival date
            ship_day = demand_allocation_date -  DT.timedelta(days = ship_duration_days)
            
            # modify ship day when we need to keep it on the shelf for one or more additional days
            if tcf_idx == 0 and dad_dow == 4: # 13 pvd to nyc Wed when its Fri
                ship_day = demand_allocation_date -  DT.timedelta(days = ship_duration_days + 1)
            if tcf_idx == 1 and dad_dow == 0: # 1 pvd to nyc Fri when its Mon
                ship_day = demand_allocation_date -  DT.timedelta(days = ship_duration_days + 2)   
            if tcf_idx == 1 and dad_dow == 1: # 4 pvd to nyc Fri when its Tues
                ship_day = demand_allocation_date -  DT.timedelta(days = ship_duration_days + 3)
            if tcf_idx == 1 and dad_dow == 2: # 7 pvd to nyc Fri when its Wed
                ship_day = demand_allocation_date -  DT.timedelta(days = ship_duration_days + 4)   
            if tcf_idx == 2 and dad_dow == 2: # 8 bal to nyc Mon when its Wed
                ship_day = demand_allocation_date -  DT.timedelta(days = ship_duration_days + 1)
            if tcf_idx == 3 and dad_dow == 4: # 14 bal to nyc Wed when its Fri
                ship_day = demand_allocation_date -  DT.timedelta(days = ship_duration_days + 1)
            if tcf_idx == 4 and dad_dow == 0: # 2 bal to nyc Fri when its Mon
                ship_day = demand_allocation_date -  DT.timedelta(days = ship_duration_days + 2)
            if tcf_idx == 5 and dad_dow == 3: # 12 chi to den Mon when its Thurs
                ship_day = demand_allocation_date -  DT.timedelta(days = ship_duration_days + 1)
            if tcf_idx == 6 and dad_dow == 0: # 3 chi to den Wed when its Mon
                ship_day = demand_allocation_date -  DT.timedelta(days = ship_duration_days + 3)
            if tcf_idx == 6 and dad_dow == 1: # 6 chi to den Wed when its Tues
                ship_day = demand_allocation_date -  DT.timedelta(days = ship_duration_days + 4)

            if tcf_idx == 7 and dad_dow == 4: # 28
                ship_day = demand_allocation_date -  DT.timedelta(days = ship_duration_days + 1)
            if tcf_idx == 8 and dad_dow == 0: # 16
                ship_day = demand_allocation_date -  DT.timedelta(days = ship_duration_days + 2)  
            if tcf_idx == 8 and dad_dow == 1: # 19
                ship_day = demand_allocation_date -  DT.timedelta(days = ship_duration_days + 3)
            if tcf_idx == 8 and dad_dow == 2: # 22
                ship_day = demand_allocation_date -  DT.timedelta(days = ship_duration_days + 4) 
            if tcf_idx == 9 and dad_dow == 4: # 29
                ship_day = demand_allocation_date -  DT.timedelta(days = ship_duration_days + 1)
            if tcf_idx == 10 and dad_dow == 0: # 17
                ship_day = demand_allocation_date -  DT.timedelta(days = ship_duration_days + 2)  
            if tcf_idx == 10 and dad_dow == 1: # 20
                ship_day = demand_allocation_date -  DT.timedelta(days = ship_duration_days + 3)
            if tcf_idx == 10 and dad_dow == 2: # 23
                ship_day = demand_allocation_date -  DT.timedelta(days = ship_duration_days + 4)
            if tcf_idx == 11 and dad_dow == 4: # 30
                ship_day = demand_allocation_date -  DT.timedelta(days = ship_duration_days + 1)
            if tcf_idx == 12 and dad_dow == 0: # 18
                ship_day = demand_allocation_date -  DT.timedelta(days = ship_duration_days + 2)  
            if tcf_idx == 12 and dad_dow == 1: # 21
                ship_day = demand_allocation_date -  DT.timedelta(days = ship_duration_days + 3)
            if tcf_idx == 12 and dad_dow == 2: # 24
                ship_day = demand_allocation_date -  DT.timedelta(days = ship_duration_days + 4)                
                
                
            # checkpoint: ship date is greater than the first date in the harvest forecast
            if ship_day > initial_date:
                

                arrival_day = ship_day + DT.timedelta(days = ship_duration_days)
                # get short demand in arrival facility between arrival day and next arrival day


    #             # indices for all short demand between arrival day and next arrival day for the arrival greenhouse
    #             #sdf_allocation_date_indices = [i for i, x in enumerate(sdf_demand_allocation_date_list) if x >= arrival_day and x < next_arrival_day]
    #             sdf_allocation_date_indices = [i for i, x in enumerate(sdf_demand_allocation_date_list) if x == demand_allocation_date]
    #             sdf_greenhouse_indices = [i for i, x in enumerate(sdf_demand_facility_id_list) if x == arrival_greenhouse_id]                 
    #             sdf_date_greenhouse_indices = list(set(sdf_allocation_date_indices) & set(sdf_greenhouse_indices))

    #             # checkpoint: there is short demand
    #             if len(sdf_date_greenhouse_indices) > 0:
    #                 # loop through short demand indicies cooresponding to the allocation date and arrival greenhouse
    #                 # organize short demand into demand_dict
    #                 # demand_dict[production priority][crop][product][customer][demand date][demand qty]
    #                 demand_dict = {}
    #                 for sdf_idx in sdf_date_greenhouse_indices:

    #                     sdf_demand_date = sdf_demand_date_list[sdf_idx]
    #                     sdf_customer_id = sdf_customer_id_list[sdf_idx]
    #                     sdf_product_id = sdf_product_id_list[sdf_idx]
    #                     sdf_short_demand_qty = sdf_short_demand_qty_list[sdf_idx]

    #                     sdf_crop_id = pd_crop_id_list[pd_product_id_list.index(sdf_product_id)]
    #                     sdf_production_priority = pd_production_priority_list[pd_product_id_list.index(sdf_product_id)]

                    # loop through short demand indicies cooresponding to the allocation date and arrival greenhouse
                    # organize short demand into demand_dict
                    # demand_dict[production priority][crop][product][customer][demand date][demand qty]
                demand_dict = {}

                for ct_idx in range(len(ct_sdf_key_list)):
                    ct_key = ct_sdf_key_list[ct_idx]
                    sdf_demand_date = DT.datetime.strptime(ct_key.split('_')[0],"%Y-%m-%d").date()
                    sdf_greenhouse_id = int(ct_key.split('_')[1])
                    sdf_product_id = int(ct_key.split('_')[2])
                    sdf_customer_id = int(ct_key.split('_')[3])
                    sdf_short_demand_qty = ct_qty_list[ct_idx]

                    sdf_crop_id = pd_crop_id_list[pd_product_id_list.index(sdf_product_id)]
                    sdf_production_priority = pd_production_priority_list[pd_product_id_list.index(sdf_product_id)]

                    if sdf_short_demand_qty != None and sdf_customer_id != 0 and sdf_greenhouse_id == arrival_greenhouse_id:

                        if sdf_production_priority in demand_dict.keys():
                            sdf_production_priority_dictionary_value = demand_dict[sdf_production_priority]
                            if sdf_crop_id in sdf_production_priority_dictionary_value.keys():
                                sdf_crop_id_dictionary_value = demand_dict[sdf_production_priority][sdf_crop_id]
                                if sdf_product_id in sdf_crop_id_dictionary_value.keys():
                                    sdf_product_id_dictionary_value = demand_dict[sdf_production_priority][sdf_crop_id][sdf_product_id]
                                    if sdf_customer_id in sdf_product_id_dictionary_value.keys():
                                        sdf_customer_id_dictionary_value = demand_dict[sdf_production_priority][sdf_crop_id][sdf_product_id][sdf_customer_id]
                                        if sdf_demand_date in sdf_customer_id_dictionary_value.keys():
                                            demand_dict[sdf_production_priority][sdf_crop_id][sdf_product_id][sdf_customer_id][sdf_demand_date] += sdf_short_demand_qty
                                        else:
                                            demand_dict[sdf_production_priority][sdf_crop_id][sdf_product_id][sdf_customer_id][sdf_demand_date] = sdf_short_demand_qty
                                    else:
                                        demand_dict[sdf_production_priority][sdf_crop_id][sdf_product_id][sdf_customer_id] = {sdf_demand_date:sdf_short_demand_qty}
                                else:
                                    demand_dict[sdf_production_priority][sdf_crop_id][sdf_product_id] = {sdf_customer_id:{sdf_demand_date:sdf_short_demand_qty}}
                            else:
                                demand_dict[sdf_production_priority][sdf_crop_id] = {sdf_product_id:{sdf_customer_id:{sdf_demand_date:sdf_short_demand_qty}}}
                        else:
                            demand_dict[sdf_production_priority] = {sdf_crop_id:{sdf_product_id:{sdf_customer_id:{sdf_demand_date:sdf_short_demand_qty}}}}


                # check for initial truck capacity
                calc_ship_date_ind = [i for i, x in enumerate(calc_ship_date_list) if x == ship_day]
                calc_ship_greenhouse_ind = [i for i, x in enumerate(calc_ship_facility_id_list ) if x == ship_greenhouse_id]
                calc_arrival_greenhouse_ind = [i for i, x in enumerate(calc_arrival_facility_id_list) if x == arrival_greenhouse_id]
                calc_all_indices = list(set(calc_ship_date_ind) & set(calc_ship_greenhouse_ind) & set(calc_arrival_greenhouse_ind))

                # get max truck count
                max_truck_count = 1
                for calc_idx in calc_all_indices:
                    # get max truck count
                     if calc_truck_count_list[calc_idx] > max_truck_count:
                        max_truck_count = calc_truck_count_list[calc_idx]

                # get pallets assigned so far
                calc_max_truck_ind = [i for i, x in enumerate(calc_truck_count_list) if x == max_truck_count]
                calc_mt_indices = list(set(calc_all_indices) & set(calc_max_truck_ind))
                calc_mt_pallets_sum = 0
                for calc_mt_idx in calc_mt_indices:
                    # product ID and transfer qty
                    calc_mt_product_id = calc_product_id_list[calc_mt_idx]
                    calc_mt_qty = calc_transfer_qty_list[calc_mt_idx]

                    # compute pallets
                    pd_idx = pd_product_id_list.index(calc_mt_product_id)
                    calc_case_equivalent_multiplier = pd_case_equivalent_multiplier_list[pd_idx]
                    calc_cases_per_pallet = pd_cases_per_pallet_list[pd_idx]
                    calc_mt_pallets = float(calc_mt_qty * calc_case_equivalent_multiplier / calc_cases_per_pallet)
                    calc_mt_pallets_sum += calc_mt_pallets




                # check for inventory (GFOODS only)
                if 5 in demand_dict.keys():
                    if 3 in demand_dict[5].keys():
                        for product_id in list(demand_dict[5][3].keys()):

                            # get total inventory qty for the product
                            ship_ri_key = str(ship_greenhouse_id) + '_' + str(product_id)
                            ship_ri_qty = 0
                            if ship_ri_key in ri_key_list:
                                ship_ri_qty = ri_qty_list[ri_key_list.index(ship_ri_key)]

                            if ship_ri_qty > 0:
                                # sort inventory for the product
                                product_id_indices = [j for j, x in enumerate(iaf_product_id_list) if x == product_id]
                                inventory_facility_id_indices = [k for k, y in enumerate(iaf_inventory_facility_id_list) if y == ship_greenhouse_id]
                                product_facility_indices = list(set(product_id_indices) & set(inventory_facility_id_indices))

                                check_enjoy_by_date_list = list()
                                check_quantity_list = list()
                                check_cccif_indices_list = list()

                                for check_idx in product_facility_indices:
                                    if iaf_end_of_day_qty_list[check_idx] != 0:        
                                        check_enjoy_by_date_list += [iaf_enjoy_by_date_list[check_idx]]
                                        check_quantity_list += [iaf_end_of_day_qty_list[check_idx]]
                                        check_cccif_indices_list += [check_idx]


                                # sort relevant inventory by accending enjoy by date
                                sorted_check_enjoy_by_date_list = list(np.sort(check_enjoy_by_date_list))
                                sorted_check_quantity_list = list([x for _,x in sorted(zip(check_enjoy_by_date_list,check_quantity_list))])
                                sorted_check_cccif_indices_list = list([y for _,y in sorted(zip(check_enjoy_by_date_list,check_cccif_indices_list))])

                                # loop through customers
                                for customer_id in list(demand_dict[5][3][product_id].keys()):
                                    # look through demand dates
                                    for demand_date in list(demand_dict[5][3][product_id][customer_id].keys()):
                                        demand_qty = demand_dict[5][3][product_id][customer_id][demand_date]


                                        # customer inventory allocation
                                        # try allocation while there is demand and inventory
                                        while demand_qty > 0 and len(sorted_check_enjoy_by_date_list) > 0:


                                            # check the first entry in the check lists
                                            inventory_qty = sorted_check_quantity_list[0]
                                            enjoy_by_date = sorted_check_enjoy_by_date_list[0]
                                            cccif_idx = sorted_check_cccif_indices_list[0]

                                            # check the qty is non-zero
                                            if inventory_qty == 0:
                                                # delete the entry from inventory lists
                                                del sorted_check_quantity_list[0]
                                                del sorted_check_enjoy_by_date_list[0]
                                                del sorted_check_cccif_indices_list[0]

                                            # allocate inventory to demand
                                            new_demand_qty = demand_qty - inventory_qty
                                            if new_demand_qty > 0 and inventory_qty > 0:
                                                # there is still remaining demand, so allocate the entire quantity to partially fulfill demand

                                                #iaf_allocated_qty_list[cccif_idx] += inventory_qty
                                                #original_starting_qty = iaf_start_of_day_qty_list[cccif_idx]
                                                original_starting_qty = inventory_qty


                                                # already an allocation for the inventory qty
                                                if type(iaf_allocated_qty_list[cccif_idx]) == list:
                                                    iaf_allocated_qty_list[cccif_idx] += [original_starting_qty]
                                                    iaf_end_of_day_qty_list[cccif_idx] = 0
                                                    iaf_customer_id_list[cccif_idx] += [customer_id]

                                                 # first allocation for the inventory qty
                                                if iaf_allocated_qty_list[cccif_idx] == 0:
                                                    iaf_allocated_qty_list[cccif_idx] = [original_starting_qty]
                                                    iaf_end_of_day_qty_list[cccif_idx] = 0
                                                    iaf_customer_id_list[cccif_idx] = [customer_id]

                                                # delete the entry from inventory lists
                                                del sorted_check_quantity_list[0]
                                                del sorted_check_enjoy_by_date_list[0]
                                                del sorted_check_cccif_indices_list[0]

                                                # compute pallets
                                                pd_idx = pd_product_id_list.index(product_id)
                                                case_equivalent_multiplier = pd_case_equivalent_multiplier_list[pd_idx]
                                                cases_per_pallet = pd_cases_per_pallet_list[pd_idx]
                                                pallets = round(float(inventory_qty * case_equivalent_multiplier / cases_per_pallet),5)

                                                new_calc_mt_pallets_sum = calc_mt_pallets_sum + pallets
                                                # assign to same truck
                                                if new_calc_mt_pallets_sum <= max_pallet_capacity:
                                                    calc_mt_pallets_sum = new_calc_mt_pallets_sum
                                                # assign to new truck
                                                if new_calc_mt_pallets_sum > max_pallet_capacity:
                                                    calc_mt_pallets_sum = pallets
                                                    max_truck_count += 1

                                                # add to calculated transfer lists
                                                calc_ship_date_list += [ship_day]
                                                calc_arrival_date_list  += [arrival_day]
                                                calc_ship_facility_id_list  += [ship_greenhouse_id]
                                                calc_arrival_facility_id_list += [arrival_greenhouse_id]
                                                calc_transfer_constraints_id_list += [tcf_idx+1]
                                                calc_product_id_list += [product_id]
                                                calc_enjoy_by_date_list += [enjoy_by_date]
                                                calc_customer_id_list += [customer_id]
                                                calc_transfer_qty_list += [inventory_qty]
                                                calc_transfer_pallets_list += [pallets]
                                                calc_truck_count_list += [max_truck_count]

                                                # update variables
                                                demand_qty = new_demand_qty
                                                inventory_qty = 0

                                            if new_demand_qty <= 0 and inventory_qty > 0:
                                                # there is no remaining demand, so allocate inventory quantity partially to fulfill demand partially or fully
                                                remaining_inventory = -new_demand_qty # remaining_inventory will be 0 if demand quantity is met exactly
                                                sorted_check_quantity_list[0] = remaining_inventory


                                                # already an allocation for the inventory qty
                                                if type(iaf_allocated_qty_list[cccif_idx]) == list:                    
                                                    iaf_allocated_qty_list[cccif_idx] += [demand_qty]
                                                    iaf_end_of_day_qty_list[cccif_idx] = remaining_inventory
                                                    iaf_customer_id_list[cccif_idx] += [customer_id]

                                                # first allocation for the inventory qty
                                                if iaf_allocated_qty_list[cccif_idx] == 0:
                                                    iaf_allocated_qty_list[cccif_idx] = [demand_qty]
                                                    iaf_end_of_day_qty_list[cccif_idx] = remaining_inventory
                                                    iaf_customer_id_list[cccif_idx] = [customer_id]

                                                # compute pallets
                                                pd_idx = pd_product_id_list.index(product_id)
                                                case_equivalent_multiplier = pd_case_equivalent_multiplier_list[pd_idx]
                                                cases_per_pallet = pd_cases_per_pallet_list[pd_idx]
                                                pallets = round(float(demand_qty * case_equivalent_multiplier / cases_per_pallet),5)

                                                new_calc_mt_pallets_sum = calc_mt_pallets_sum + pallets
                                                # assign to same truck
                                                if new_calc_mt_pallets_sum <= max_pallet_capacity:
                                                    calc_mt_pallets_sum = new_calc_mt_pallets_sum
                                                # assign to new truck
                                                if new_calc_mt_pallets_sum > max_pallet_capacity:
                                                    calc_mt_pallets_sum = pallets
                                                    max_truck_count += 1

                                                # add to calculated transfer lists
                                                calc_ship_date_list += [ship_day]
                                                calc_arrival_date_list  += [arrival_day]
                                                calc_ship_facility_id_list  += [ship_greenhouse_id]
                                                calc_arrival_facility_id_list += [arrival_greenhouse_id]
                                                calc_transfer_constraints_id_list += [tcf_idx+1]
                                                calc_product_id_list += [product_id]
                                                calc_enjoy_by_date_list += [enjoy_by_date]
                                                calc_customer_id_list += [customer_id]
                                                calc_transfer_qty_list += [demand_qty]
                                                calc_transfer_pallets_list += [pallets]
                                                calc_truck_count_list += [max_truck_count]

                                                # stop allocation loop since allocation is complete for the demand 
                                                demand_qty = 0
                                                inventory_qty = remaining_inventory

                                            # while loop will continue as long as demand_qty is positive and there are entries in check_enjoy_by_date_list

                                        #add the short demand (if any)

                                        sdf_key = demand_date.strftime("%Y-%m-%d") + '_' + str(arrival_greenhouse_id) + '_' + str(product_id) + '_' + str(customer_id)
                                        if demand_qty > 0 and sdf_key in ct_sdf_key_list:
                                            ct_qty_list[ct_sdf_key_list.index(sdf_key)] = demand_qty
                                        if demand_qty == 0 and sdf_key in ct_sdf_key_list:
                                            del ct_qty_list[ct_sdf_key_list.index(sdf_key)]
                                            del ct_sdf_key_list[ct_sdf_key_list.index(sdf_key)]

    #                                             new_sdf_demand_date_list += [demand_date]
    #                                             new_sdf_demand_allocation_date_list += [demand_allocation_date]
    #                                             new_sdf_demand_facility_id_list += [arrival_greenhouse_id]
    #                                             new_sdf_product_id_list += [product_id]
    #                                             new_sdf_customer_id_list += [customer_id]
    #                                             new_sdf_short_demand_qty_list += [demand_qty]

                # check for harvest (retail only)
                last_harvest_day = ship_day - DT.timedelta(days = pack_lead_time_days)

                if last_harvest_day >= hfsf_harvest_date_list[0] and gfoods_transfer is None:

                    if 2 in demand_dict.keys():

                        for crop_id in demand_dict[2].keys():
                            for product_id in list(demand_dict[2][crop_id].keys()):


                                # get values from Products_Dim
                                pd_idx = pd_product_id_list.index(product_id)
                                net_weight_grams = pd_net_weight_grams_list[pd_idx]
                                is_whole = pd_is_whole_list[pd_idx]
                                total_shelf_life = pd_total_shelf_life_list[pd_idx]
                                shelf_life_guarantee = pd_shelf_life_guarantee_list[pd_idx]

                                for customer_id in list(demand_dict[2][crop_id][product_id].keys()):
                                    for demand_date in list(demand_dict[2][crop_id][product_id][customer_id].keys()):
                                        #og_key = str(product_id) + '_' + str(customer_id) + '_' + str(demand_date)
                                        og_key = demand_date.strftime("%Y-%m-%d") + '_' + str(arrival_greenhouse_id) + '_' + str(product_id) + '_' + str(customer_id)

                                        short_demand_qty = demand_dict[2][crop_id][product_id][customer_id][demand_date]

                                        #earliest we could harvest for the demand
                                        remaining_shelf_life = total_shelf_life - shelf_life_guarantee
                                        first_harvest_day = demand_date - DT.timedelta(days = remaining_shelf_life)


                                        # get harvest for the crop in the ship greenhouse
                                        transfer_harvest_date_list = list()

                                        harvest_date_indices =  [i for i, x in enumerate(hfsf_harvest_date_list) if x >= first_harvest_day and x<= last_harvest_day]
                                        harvest_crop_id_indices =  [j for j, y in enumerate(hfsf_crop_id_list) if y == crop_id]
                                        harvest_facility_id_indices = [k for k, z in enumerate(hfsf_facility_id_list)
                                                                     if fd_city_short_code_list[fd_facility_id_list.index(z)]
                                                                        == fd_city_short_code_list[fd_facility_id_list.index(ship_greenhouse_id)]]
                                        harvest_date_crop_facility_indices = list(set(harvest_date_indices) & set(harvest_crop_id_indices) & set(harvest_facility_id_indices))

                                        all_harvest_expected_plant_sites_list = list()
                                        all_harvest_whole_gpps_list = list()
                                        all_harvest_loose_gpps_list = list()

                                        # organize harvest by date
                                        distinct_harvest_date_list = list()

                                        for hfsf_idx in harvest_date_crop_facility_indices:

                                            hfsf_harvest_date = hfsf_harvest_date_list[hfsf_idx]
                                            hfsf_expected_plant_sites = hfsf_expected_plant_sites_list[hfsf_idx]
                                            if hfsf_harvest_date in distinct_harvest_date_list and hfsf_expected_plant_sites > 0:

                                                harvest_idx = distinct_harvest_date_list.index(hfsf_harvest_date)
                                                # get total plant sites, whole grams per plant site, and loose grams per plant site
                                                all_harvest_expected_plant_sites_list[harvest_idx] += [hfsf_expected_plant_sites]
                                                all_harvest_whole_gpps_list[harvest_idx] += [hfsf_avg_headweight_list[hfsf_idx]]
                                                all_harvest_loose_gpps_list[harvest_idx] += [hfsf_loose_grams_per_plant_site_list[hfsf_idx]]


                                            if hfsf_harvest_date not in distinct_harvest_date_list and hfsf_expected_plant_sites > 0:
                                                distinct_harvest_date_list += [hfsf_harvest_date]
                                                # get total plant sites, whole grams per plant site, and loose grams per plant site
                                                all_harvest_expected_plant_sites_list += [[hfsf_expected_plant_sites]]
                                                all_harvest_whole_gpps_list += [[hfsf_avg_headweight_list[hfsf_idx]]]
                                                all_harvest_loose_gpps_list += [[hfsf_loose_grams_per_plant_site_list[hfsf_idx]]]


                                        # sort harvest by date
                                        sorted_harvest_date_list = list(np.sort(distinct_harvest_date_list))
                                        sorted_harvest_expected_plant_sites_list = list([x for _,x in sorted(zip(distinct_harvest_date_list,all_harvest_expected_plant_sites_list))])
                                        sorted_harvest_whole_gpps_list = list([y for _,y in sorted(zip(distinct_harvest_date_list,all_harvest_whole_gpps_list))])
                                        sorted_harvest_loose_gpps_list = list([y for _,y in sorted(zip(distinct_harvest_date_list,all_harvest_loose_gpps_list))])


                                        # attempt allocation in reverse chronological order
                                        while len(sorted_harvest_date_list) > 0 and short_demand_qty > 0:
                                            #print(len(sorted_harvest_date_list),short_demand_qty)
                                            harvest_date = sorted_harvest_date_list[-1]

                                            # checkpoint: skip allocation for this product_customer_demanddate_harvestdate since its been exhausted previously by calculated transfers
                                            check_skip_key = og_key + '_' + harvest_date.strftime("%Y-%m-%d") + str(ship_greenhouse_id)

                                            if check_skip_key in skip_key_list:
                                                del sorted_harvest_date_list[-1]
                                                del sorted_harvest_expected_plant_sites_list[-1]
                                                del sorted_harvest_whole_gpps_list[-1]
                                                del sorted_harvest_loose_gpps_list[-1]
                                            if check_skip_key not in skip_key_list:


                                                allocated_date_crop_facility_key = str(harvest_date) + '_' + str(crop_id) + '_' + str(ship_greenhouse_id)

                                                # remove from lists if harvest is completely allocated (no harvest is available)
                                                if allocated_date_crop_facility_key in complete_crop_allocation_key_list:
                                                    del sorted_harvest_date_list[-1]
                                                    del sorted_harvest_expected_plant_sites_list[-1]
                                                    del sorted_harvest_whole_gpps_list[-1]
                                                    del sorted_harvest_loose_gpps_list[-1]

                                                # checkpoint: harvest is available

                                                if allocated_date_crop_facility_key not in complete_crop_allocation_key_list:

                                                    # get plant sites already allocated for the facility on the harvest date
                                                    already_allocated_plant_sites = 0
                                                    if allocated_date_crop_facility_key in allocated_date_crop_facility_key_list: 
                                                        already_allocated_plant_sites = allocated_plant_sites_list[allocated_date_crop_facility_key_list.index(allocated_date_crop_facility_key)]

                                                    # available harvest closes to ship day
                                                    harvest_expected_plant_sites_list = sorted_harvest_expected_plant_sites_list[-1]
                                                    harvest_whole_gpps_list = sorted_harvest_whole_gpps_list[-1]
                                                    harvest_loose_gpps_list = sorted_harvest_loose_gpps_list[-1]

                                                    # GPPS normalized by expected plant sites
                                                    whole_numerator = 0
                                                    loose_numerator = 0
                                                    total_ps = 0
                                                    for harvest_idx in range(len(harvest_whole_gpps_list)):
                                                        whole_numerator += harvest_whole_gpps_list[harvest_idx] * harvest_expected_plant_sites_list[harvest_idx]
                                                        loose_numerator += harvest_loose_gpps_list[harvest_idx] * harvest_expected_plant_sites_list[harvest_idx]
                                                        total_ps += harvest_expected_plant_sites_list[harvest_idx]

                                                    harvest_facility_mean_whole_gpps = 0
                                                    harvest_facility_mean_loose_gpps = 0
                                                    if total_ps != 0:
                                                        harvest_facility_mean_whole_gpps = round(float(whole_numerator/total_ps),2)
                                                        harvest_facility_mean_loose_gpps = round(float(loose_numerator/total_ps),2)

                                                    # choose conversion factor
                                                    harvest_facility_mean_gpps = harvest_facility_mean_loose_gpps 
                                                    if is_whole == 1:
                                                        harvest_facility_mean_gpps = harvest_facility_mean_whole_gpps

                                                    # compute net_plant_sites for the short demand
                                                    #net_plant_sites = int(short_demand_qty * np.ceil(float(net_weight_grams / harvest_facility_mean_gpps)))
                                                    net_plant_sites = 0

                                                    if harvest_facility_mean_gpps != 0:
                                                        net_plant_sites = int(np.ceil(float(short_demand_qty * net_weight_grams / harvest_facility_mean_gpps)))

                                                    # compute net plant sites for the facility
                                                    harvest_facility_net_plant_sites = int(sum(harvest_expected_plant_sites_list))

                                                    # compute remaining plant sites for allocation considering the mid allocation checkpoint
                                                    harvest_facility_pre_plant_sites = harvest_facility_net_plant_sites - already_allocated_plant_sites

                                                    # ATTEMPT THE ALLOCATION
                                                    harvest_facility_post_plant_sites = harvest_facility_pre_plant_sites - net_plant_sites

                                                    # first deal with what to do when we cannot satisfy the demand
                                                    if harvest_facility_post_plant_sites < 0:
                                                        # oh no! this demand exceeds our harvest for the facility
                                                        #print('oh no short on ', crop_id, ship_greenhouse_id, harvest_date, demand_allocation_date)
                                                        # update harvest allocation lists
                                                        del sorted_harvest_date_list[-1]
                                                        del sorted_harvest_expected_plant_sites_list[-1]
                                                        del sorted_harvest_whole_gpps_list[-1]
                                                        del sorted_harvest_loose_gpps_list[-1]

                                                        # set full_packout to true
                                                        full_packout = 1

                                                        # compute total short demand 
                                                        # accumulate short_demand_plant_sites for all products
                                                        short_demand_plant_sites = 0

                                                        for a_product_id in list(demand_dict[2][crop_id].keys()):
                                                            pd_idx = pd_product_id_list.index(a_product_id)
                                                            net_weight_grams = pd_net_weight_grams_list[pd_idx]
                                                            is_whole = pd_is_whole_list[pd_idx]
                                                            # choose conversion factor
                                                            harvest_facility_mean_gpps = harvest_facility_mean_loose_gpps 
                                                            if is_whole == 1:
                                                                harvest_facility_mean_gpps = harvest_facility_mean_whole_gpps
                                                            for a_customer_id in list(demand_dict[2][crop_id][a_product_id].keys()):
                                                                for a_demand_date in list(demand_dict[2][crop_id][a_product_id][a_customer_id].keys()):
                                                                    a_short_demand_qty = demand_dict[2][crop_id][a_product_id][a_customer_id][a_demand_date]
                                                                    # compute net_plant_sites for the short demand
                                                                    net_plant_sites = int(np.ceil(float(a_short_demand_qty * net_weight_grams / harvest_facility_mean_gpps)))
                                                                    short_demand_plant_sites += net_plant_sites


                                                        # accumulate harvest_priority_plant_sites available for all products
                                                        harvest_priority_plant_sites = harvest_facility_pre_plant_sites
                                                        for check_idx in range(len(allocated_date_product_facility_key_list)):
                                                            check_key = allocated_date_product_facility_key_list[check_idx]
                                                            check_harvest_date = DT.datetime.strptime(str(check_key.split("_")[0]), '%Y-%m-%d').date()
                                                            check_product_id = int(check_key.split("_")[1])
                                                            check_facility_id = int(check_key.split("_")[2])
                                                            check_crop_id = pd_crop_id_list[pd_product_id_list.index(check_product_id)]
                                                            check_date_crop_facility_key = str(check_harvest_date) + '_' + str(check_crop_id) + '_' + str(check_facility_id)
                                                            if check_harvest_date == harvest_date and check_facility_id == ship_greenhouse_id and check_crop_id == crop_id:
                                                            #if check_demand_allocation_date == demand_allocation_date and check_city_short_code == fd_city_short_code_list[fd_facility_id_list.index(harvest_facility_id)] and check_priority == production_priority and check_crop_id == crop_id:
                                                                check_allocated_product_plant_sites = allocated_product_plant_sites_list[check_idx]
                                                                # add the plant sites for the harvest_demand_ratio
                                                                harvest_priority_plant_sites += check_allocated_product_plant_sites
                                                                # subtract the plant sites from the overall allocation tracking list since the new value multiplied by harvest_demand_ratio will be added back
                                                                allocated_plant_sites_list[allocated_date_crop_facility_key_list.index(check_date_crop_facility_key)] -= check_allocated_product_plant_sites


                                                        # compute harvest_demand_ratio (less than 1 in this case)
                                                        harvest_demand_ratio = float(harvest_priority_plant_sites / short_demand_plant_sites)
                                                        #print(list(demand_dict[2][crop_id].keys()))
                                                        # allocate to every product ID where shorts exist
                                                        for s_product_id in list(demand_dict[2][crop_id].keys()):
                                                            s_pd_idx = pd_product_id_list.index(s_product_id)
                                                            s_net_weight_grams = pd_net_weight_grams_list[s_pd_idx]
                                                            s_is_whole = pd_is_whole_list[s_pd_idx]
                                                            s_total_shelf_life = pd_total_shelf_life_list[s_pd_idx]
                                                            # choose conversion factor
                                                            s_harvest_facility_mean_gpps = harvest_facility_mean_loose_gpps 
                                                            if s_is_whole == 1:
                                                                s_harvest_facility_mean_gpps = harvest_facility_mean_whole_gpps
                                                            s_forecasted_gpps = round(float(harvest_facility_mean_gpps),2)
                                                            s_allocated_date_product_facility_key = str(harvest_date) + '_' + str(s_product_id) + '_' + str(ship_greenhouse_id)

                                                            for s_customer_id in list(demand_dict[2][crop_id][s_product_id].keys()):
                                                                s_allocated_date_product_facility_customer_key = str(harvest_date) + '_' + str(s_product_id) + '_' + str(ship_greenhouse_id)+ '_' + str(s_customer_id)
                                                                #print(list(demand_dict[2][crop_id][s_product_id][s_customer_id].keys()))
                                                                for s_demand_date in list(demand_dict[2][crop_id][s_product_id][s_customer_id].keys()):

                                                                    s_short_demand_qty = demand_dict[2][crop_id][s_product_id][s_customer_id][s_demand_date]

                                                                    # apply harvest_demand_ratio to allocated_qty
                                                                    allocated_qty = int(np.floor(s_short_demand_qty * harvest_demand_ratio))
                                                                    allocated_product_plant_sites = int(np.ceil(allocated_qty * s_net_weight_grams / s_forecasted_gpps))
                                                                    allocated_grams = round(allocated_product_plant_sites * s_forecasted_gpps,2)
                                                                    #print(s_short_demand_qty, allocated_qty)
                                                                    if allocated_qty > 0:
                                                                        # write to lists for HarvestAllocation_Facts
                                                                        haf_demand_allocation_date_list += [harvest_date]
                                                                        haf_demand_date_list += [s_demand_date]
                                                                        haf_harvest_facility_id_list += [ship_greenhouse_id]
                                                                        haf_demand_facility_id_list += [arrival_greenhouse_id]
                                                                        haf_crop_id_list += [crop_id]
                                                                        haf_product_id_list += [s_product_id]
                                                                        haf_customer_id_list += [s_customer_id]
                                                                        haf_forecasted_gpps_list += [s_forecasted_gpps]
                                                                        haf_allocated_plant_sites_list += [allocated_product_plant_sites]
                                                                        haf_allocated_grams_list += [allocated_grams]
                                                                        haf_allocated_qty_list += [allocated_qty]
                                                                        haf_full_packout_list += [full_packout]

                                                                        # update the allocation_lists
                                                                        if allocated_date_crop_facility_key in allocated_date_crop_facility_key_list:        
                                                                            allocated_plant_sites_list[allocated_date_crop_facility_key_list.index(allocated_date_crop_facility_key)] += allocated_product_plant_sites

                                                                        if allocated_date_crop_facility_key not in allocated_date_crop_facility_key_list:
                                                                            allocated_date_crop_facility_key_list += [allocated_date_crop_facility_key]
                                                                            allocated_plant_sites_list += [allocated_product_plant_sites]
                                                                            allocated_starting_ps_list += [harvest_facility_net_plant_sites]                          

                                                                        # compute pallets
                                                                        pd_idx = pd_product_id_list.index(s_product_id)
                                                                        case_equivalent_multiplier = pd_case_equivalent_multiplier_list[pd_idx]
                                                                        cases_per_pallet = pd_cases_per_pallet_list[pd_idx]
                                                                        pallets = round(float(allocated_qty * case_equivalent_multiplier / cases_per_pallet),5)

                                                                        new_calc_mt_pallets_sum = calc_mt_pallets_sum + pallets
                                                                        # assign to same truck
                                                                        if new_calc_mt_pallets_sum <= max_pallet_capacity:
                                                                            calc_mt_pallets_sum = new_calc_mt_pallets_sum
                                                                        # assign to new truck
                                                                        if new_calc_mt_pallets_sum > max_pallet_capacity:
                                                                            calc_mt_pallets_sum = pallets
                                                                            max_truck_count += 1

                                                                        # update transfer lists                                                            
                                                                        enjoy_by_date =  harvest_date + DT.timedelta(days = s_total_shelf_life)
                                                                        calc_ship_date_list += [ship_day]
                                                                        calc_arrival_date_list  += [arrival_day]
                                                                        calc_ship_facility_id_list  += [ship_greenhouse_id]
                                                                        calc_arrival_facility_id_list += [arrival_greenhouse_id]
                                                                        calc_transfer_constraints_id_list += [tcf_idx+1]
                                                                        calc_product_id_list += [s_product_id]
                                                                        calc_enjoy_by_date_list += [enjoy_by_date]
                                                                        calc_customer_id_list += [s_customer_id]
                                                                        calc_transfer_qty_list += [allocated_qty]
                                                                        calc_transfer_pallets_list += [pallets]
                                                                        calc_truck_count_list += [max_truck_count]

                                                                    if s_allocated_date_product_facility_key in allocated_date_product_facility_key_list:
                                                                        del allocated_gpps_list[allocated_date_product_facility_key_list.index(s_allocated_date_product_facility_key)]
                                                                        del allocated_qty_list[allocated_date_product_facility_key_list.index(s_allocated_date_product_facility_key)]
                                                                        del allocated_product_plant_sites_list[allocated_date_product_facility_key_list.index(s_allocated_date_product_facility_key)]
                                                                        del allocated_date_product_facility_key_list[allocated_date_product_facility_key_list.index(s_allocated_date_product_facility_key)]

                                                                    # customer allocation
                                                                    if s_allocated_date_product_facility_customer_key in allocated_date_product_facility_customer_key_list:
                                                                        adpfc_key_idx = allocated_date_product_facility_customer_key_list.index(s_allocated_date_product_facility_customer_key)
                                                                        del allocated_date_product_facility_customer_key_list[adpfc_key_idx]
                                                                        del allocated_customer_gpps_list[adpfc_key_idx]
                                                                        del allocated_customer_qty_list[adpfc_key_idx]
                                                                        del allocated_customer_plant_sites_list[adpfc_key_idx]
                                                                        del allocated_customer_demand_date_list[adpfc_key_idx]

                                                                    # mark allocation as complete
                                                                    if allocated_date_crop_facility_key not in complete_crop_allocation_key_list:
                                                                        complete_crop_allocation_key_list += [allocated_date_crop_facility_key]
                                                                    if s_allocated_date_product_facility_key not in complete_product_allocation_key_list:
                                                                        complete_product_allocation_key_list += [s_allocated_date_product_facility_key]
                                                                    if s_allocated_date_product_facility_customer_key not in complete_customer_allocation_key_list:
                                                                        complete_customer_allocation_key_list += [s_allocated_date_product_facility_customer_key]

                                                                    # new short demand
                                                                    # continue looking from previous harvest day for the product/customer/demand_date
                                                                    new_short_demand_qty = s_short_demand_qty - allocated_qty

                                                                    #print(new_short_demand_qty)
                                                                    # update dictionary
                                                                    demand_dict[2][crop_id][s_product_id][s_customer_id][s_demand_date] = new_short_demand_qty

                                                                    #short_key = str(s_product_id) + '_' + str(s_customer_id) + '_' + str(s_demand_date)

    #                                                                 if new_short_demand_qty > 0 and short_key == og_key:
    #                                                                     short_demand_qty = new_short_demand_qty
                                                                    #if new_short_demand_qty > 0 and short_key != og_key:



                                                                    s_sdf_key = s_demand_date.strftime("%Y-%m-%d") + '_' + str(arrival_greenhouse_id) + '_' + str(s_product_id) + '_' + str(s_customer_id)

                                                                    #print(og_key, s_sdf_key, new_short_demand_qty)

                                                                    if s_sdf_key == og_key:
                                                                        short_demand_qty = new_short_demand_qty

                                                                    if new_short_demand_qty > 0 and s_sdf_key in ct_sdf_key_list:
                                                                        ct_qty_list[ct_sdf_key_list.index(s_sdf_key)] = new_short_demand_qty                                                               
                                                                    if new_short_demand_qty == 0 and s_sdf_key in ct_sdf_key_list:
                                                                        del ct_qty_list[ct_sdf_key_list.index(s_sdf_key)]
                                                                        del ct_sdf_key_list[ct_sdf_key_list.index(s_sdf_key)]


                                                                    # skip main loop allocation for this product_customer_demanddate_harvestdate_shipgreenhouse
                                                                    skip_key = s_sdf_key + '_' + harvest_date.strftime("%Y-%m-%d") + str(ship_greenhouse_id)
                                                                    skip_key_list += [skip_key]

                                                    # next deal with what to do when we can successfully satisfy the demand
                                                    if harvest_facility_post_plant_sites >= 0 and net_plant_sites > 0:
                                                        # yay! we have enough plant sites to cover the demand (so far)
                                                        # update the allocation lists

                                                        if allocated_date_crop_facility_key in allocated_date_crop_facility_key_list:
                                                            allocated_plant_sites_list[allocated_date_crop_facility_key_list.index(allocated_date_crop_facility_key)] += net_plant_sites

                                                        # add to mid-allocation tracker if its a new key
                                                        if allocated_date_crop_facility_key not in allocated_date_crop_facility_key_list:
                                                            allocated_date_crop_facility_key_list += [allocated_date_crop_facility_key]
                                                            allocated_plant_sites_list += [net_plant_sites]
                                                            allocated_starting_ps_list += [harvest_facility_net_plant_sites]

                                                        # create key for mid-allocation check
                                                        allocated_date_product_facility_key = str(harvest_date) + '_' + str(product_id) + '_' + str(ship_greenhouse_id)
                                                        allocated_date_product_facility_customer_key = str(harvest_date) + '_' + str(product_id) + '_' + str(ship_greenhouse_id) + '_' + str(customer_id)

                                                        if allocated_date_product_facility_key in allocated_date_product_facility_key_list:
                                                            allocated_qty_list[allocated_date_product_facility_key_list.index(allocated_date_product_facility_key)] += short_demand_qty
                                                            allocated_product_plant_sites_list[allocated_date_product_facility_key_list.index(allocated_date_product_facility_key)] += net_plant_sites

                                                        if allocated_date_product_facility_key not in allocated_date_product_facility_key_list:   
                                                            allocated_date_product_facility_key_list += [allocated_date_product_facility_key]
                                                            allocated_gpps_list += [round(float(harvest_facility_mean_gpps),2)]
                                                            allocated_qty_list += [short_demand_qty]
                                                            allocated_product_plant_sites_list += [net_plant_sites]

                                                        if allocated_date_product_facility_customer_key in allocated_date_product_facility_customer_key_list:
                                                            allocated_customer_qty_list[allocated_date_product_facility_customer_key_list.index(allocated_date_product_facility_customer_key)] += [short_demand_qty]
                                                            allocated_customer_plant_sites_list[allocated_date_product_facility_customer_key_list.index(allocated_date_product_facility_customer_key)] += [net_plant_sites]
                                                            allocated_customer_demand_date_list[allocated_date_product_facility_customer_key_list.index(allocated_date_product_facility_customer_key)] += [demand_date]
                                                        if allocated_date_product_facility_customer_key not in allocated_date_product_facility_customer_key_list:   
                                                            allocated_date_product_facility_customer_key_list += [allocated_date_product_facility_customer_key]
                                                            allocated_customer_gpps_list += [round(float(harvest_facility_mean_gpps),2)]
                                                            allocated_customer_qty_list += [[short_demand_qty]]
                                                            allocated_customer_plant_sites_list += [[net_plant_sites]] 
                                                            allocated_customer_demand_date_list += [[demand_date]]

                                                        short_demand_qty = 0



                                        # exit while loop through harvest dates

                                        # short demand remaining
                                        sdf_key = demand_date.strftime("%Y-%m-%d") + '_' + str(arrival_greenhouse_id) + '_' + str(product_id) + '_' + str(customer_id)
                                        if short_demand_qty > 0 and sdf_key in ct_sdf_key_list:                                        
                                            ct_qty_list[ct_sdf_key_list.index(sdf_key)] = short_demand_qty
                                        if short_demand_qty == 0 and sdf_key in ct_sdf_key_list:
                                            del ct_qty_list[ct_sdf_key_list.index(sdf_key)]
                                            del ct_sdf_key_list[ct_sdf_key_list.index(sdf_key)]
                                        #print(sdf_key)
        #                                         new_sdf_demand_date_list += [demand_date]
        #                                         new_sdf_demand_allocation_date_list += [demand_allocation_date]
        #                                         new_sdf_demand_facility_id_list += [arrival_greenhouse_id]
        #                                         new_sdf_product_id_list += [product_id]
        #                                         new_sdf_customer_id_list += [customer_id]
        #                                         new_sdf_short_demand_qty_list += [short_demand_qty]

                        # exit loop through all demand
                        # after going through all short demand

                        # set full_packout to false
                        full_packout = 0
                        # write all complete allocations to customerHarvestAllocation_Facts for the remaining products
                        for allocated_idx in range(len(allocated_date_product_facility_customer_key_list)):
                            allocated_date_product_facility_customer_key = allocated_date_product_facility_customer_key_list[allocated_idx]
                            # only write the keys that are not yet completed
                            if allocated_date_product_facility_customer_key not in complete_customer_allocation_key_list:

                                customer_id = int(allocated_date_product_facility_customer_key.split("_")[3])

                                product_id = int(allocated_date_product_facility_customer_key.split("_")[1])
                                harvest_date_str = allocated_date_product_facility_customer_key.split("_")[0]
                                harvest_date = DT.datetime.strptime(harvest_date_str, '%Y-%m-%d').date()
                                # get values from Products_Dim
                                pd_idx = pd_product_id_list.index(product_id)
                                crop_id = pd_crop_id_list[pd_idx]
                                net_weight_grams = pd_net_weight_grams_list[pd_idx]
                                total_shelf_life = pd_total_shelf_life_list[pd_idx]
                                case_equivalent_multiplier = pd_case_equivalent_multiplier_list[pd_idx]
                                cases_per_pallet = pd_cases_per_pallet_list[pd_idx]

                                allocated_customer_qty_sub_list = allocated_customer_qty_list[allocated_idx]
                                forecasted_gpps = allocated_customer_gpps_list[allocated_idx]
                                allocated_customer_plant_sites_sub_list = allocated_customer_plant_sites_list[allocated_idx]
                                demand_date_sub_list = allocated_customer_demand_date_list[allocated_idx]

                                #print('full fill: ',harvest_date, crop_id, product_id, ship_greenhouse_id)

                                for sub_idx in range(len(demand_date_sub_list)):
                                    allocated_customer_qty = allocated_customer_qty_sub_list[sub_idx]
                                    allocated_customer_plant_sites = allocated_customer_plant_sites_sub_list[sub_idx]
                                    demand_date = demand_date_sub_list[sub_idx]
                                    allocated_grams = round(allocated_customer_plant_sites * forecasted_gpps,2)

                                    # write to lists for CustomerHarvestAllocation_Facts
                                    haf_demand_allocation_date_list += [harvest_date]
                                    haf_demand_date_list += [demand_date]
                                    haf_harvest_facility_id_list += [ship_greenhouse_id]
                                    haf_demand_facility_id_list += [arrival_greenhouse_id]
                                    haf_crop_id_list += [crop_id]
                                    haf_product_id_list += [product_id]
                                    haf_customer_id_list += [customer_id]
                                    haf_forecasted_gpps_list += [forecasted_gpps]
                                    haf_allocated_plant_sites_list += [allocated_customer_plant_sites]
                                    haf_allocated_grams_list += [allocated_grams]
                                    haf_allocated_qty_list += [allocated_customer_qty]
                                    haf_full_packout_list += [full_packout]


                                    # compute pallets


                                    pallets = round(float(allocated_customer_qty * case_equivalent_multiplier / cases_per_pallet),5)

                                    new_calc_mt_pallets_sum = calc_mt_pallets_sum + pallets
                                    # assign to same truck
                                    if new_calc_mt_pallets_sum <= max_pallet_capacity:
                                        calc_mt_pallets_sum = new_calc_mt_pallets_sum
                                    # assign to new truck
                                    if new_calc_mt_pallets_sum > max_pallet_capacity:
                                        calc_mt_pallets_sum = pallets
                                        max_truck_count += 1

                                    # update transfer lists                                                            
                                    enjoy_by_date =  harvest_date + DT.timedelta(days = total_shelf_life)
                                    calc_ship_date_list += [ship_day]
                                    calc_arrival_date_list  += [arrival_day]
                                    calc_ship_facility_id_list  += [ship_greenhouse_id]
                                    calc_arrival_facility_id_list += [arrival_greenhouse_id]
                                    calc_transfer_constraints_id_list += [tcf_idx+1]
                                    calc_product_id_list += [product_id]
                                    calc_enjoy_by_date_list += [enjoy_by_date]
                                    calc_customer_id_list += [customer_id]
                                    calc_transfer_qty_list += [allocated_customer_qty]
                                    calc_transfer_pallets_list += [pallets]
                                    calc_truck_count_list += [max_truck_count]

                                complete_customer_allocation_key_list += [allocated_date_product_facility_customer_key]

    # inventory allocation output
    inventory_allocation_transfers_LoL = [iaf_inventory_facility_id_list,
                                iaf_product_id_list,
                                iaf_enjoy_by_date_list,
                                iaf_customer_id_list,
                                iaf_start_of_day_qty_list,
                                iaf_allocated_qty_list,
                                iaf_end_of_day_qty_list]                     
    # harvest allocation output
    harvest_allocation_transfers_LoL = [haf_demand_allocation_date_list,
        haf_demand_date_list,
        haf_harvest_facility_id_list,
        haf_demand_facility_id_list,
        haf_crop_id_list,
        haf_product_id_list,
        haf_customer_id_list,
        haf_forecasted_gpps_list,
        haf_allocated_plant_sites_list,
        haf_allocated_grams_list,
        haf_allocated_qty_list,
        haf_full_packout_list]  
    
    # allocated crops output
    allocated_crops_out_LoL = [allocated_date_crop_facility_key_list,
        allocated_starting_ps_list,
        allocated_plant_sites_list,
        complete_crop_allocation_key_list]
    
    # short demand
    ct_sdf_demand_date_list = list()
    ct_sdf_demand_allocation_date_list= list()
    ct_sdf_demand_facility_id_list= list()
    ct_sdf_product_id_list= list()
    ct_sdf_customer_id_list= list()
    ct_sdf_short_demand_qty_list= list()
    for ct_idx in range(len(ct_sdf_key_list)):
        ct_key = ct_sdf_key_list[ct_idx]
        ct_sdf_demand_date_list += [DT.datetime.strptime(ct_key.split('_')[0],"%Y-%m-%d").date()]
        ct_sdf_demand_allocation_date_list += [demand_allocation_date]
        ct_sdf_demand_facility_id_list += [int(ct_key.split('_')[1])]
        ct_sdf_product_id_list += [int(ct_key.split('_')[2])]
        ct_sdf_customer_id_list += [int(ct_key.split('_')[3])]
        ct_sdf_short_demand_qty_list += [ct_qty_list[ct_idx]]
    
    short_demand_out_LoL = [
        ct_sdf_demand_date_list,
        ct_sdf_demand_allocation_date_list,
        ct_sdf_demand_facility_id_list,
        ct_sdf_product_id_list,
        ct_sdf_customer_id_list,
        ct_sdf_short_demand_qty_list] 
                         
    new_calc_transfers_LoL = [calc_ship_date_list,
        calc_arrival_date_list,
        calc_ship_facility_id_list,
        calc_arrival_facility_id_list,
        calc_transfer_constraints_id_list,
        calc_product_id_list,
        calc_enjoy_by_date_list,
        calc_customer_id_list,
        calc_transfer_qty_list,
        calc_transfer_pallets_list,
        calc_truck_count_list]
    
    transfer_tuple = (inventory_allocation_transfers_LoL, harvest_allocation_transfers_LoL, allocated_crops_out_LoL, short_demand_out_LoL, new_calc_transfers_LoL)
    
   # print('sl:', len(ct_sdf_short_demand_qty_list))
    return transfer_tuple


def writeCalculatedTransfers(calc_transfers_LoL):
    '''
    #### Inputs:
    - calc_transfers_LoL
        1. List of  ship dates
        2. List of arrival dates
        3. List of arrival facilites
        4. List of transfer constraints
        5. List of product IDs
        6. List of enjoy-by-dates
        7. List of customer IDs
        8. List of transfer quantiites
        9. List of transfer pallets
        10. List of truck counts
    #### Algorithm:
        - load inputs
        - write to CalculatedTransfers_Facts
        - write to CustomerInventoryAllocation_Facts
        - write to CustomerHarvestAllocation_Facts
        - return string indicating completion
    #### Output: string indicating completion
    '''
    
    # connect to database
    HOSTNAME = socket.gethostname()

    if HOSTNAME == 'hostname':
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server}; 
                                Server=127.0.0.1,1443;
                                Database=databasename;
                                trusted_connection=yes""" # use windows auth on DB01
    else:
        with open(os.path.join(sys.path[0], "config.yml"), 'r') as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.SafeLoader)
    #    uid = cfg['databasename']['uid']
        uid = 'sa'
        pwd = cfg['databasename']['pwd'][:-3]
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server};
                                Server=hostname\MSSQLSERVER1;
                                Database=databasename;
                                UID=%s;
                                PWD=%s;""" % (uid, pwd) # use config.yml on local machine
    cnxn = pyodbc.connect(CONNECTIONSTRING)   
    cnxn_cursor = cnxn.cursor()

    
    sql = """
    SELECT MAX(CalculatedTransfersID) FROM CalculatedTransfers_Facts
    """
    cnxn_cursor.execute(sql) 
    
    max_old_id = cnxn_cursor.fetchone()[0]
    if max_old_id is None:
        max_old_id = 0
    
    calc_transfers_id = max_old_id + 1

    load_date = DT.datetime.now()
    to_date = DT.datetime.strptime('2099-12-31 00:00:00.000000', '%Y-%m-%d %H:%M:%S.%f')
    is_active = 1

    sql = """
    INSERT INTO CalculatedTransfers_Facts
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
    """ 

    # write calculated transfers
    calc_ship_date_list = calc_transfers_LoL[0]
    calc_arrival_date_list  = calc_transfers_LoL[1]
    calc_ship_facility_id_list  = calc_transfers_LoL[2]
    calc_arrival_facility_id_list  = calc_transfers_LoL[3]
    calc_transfer_constraints_id_list = calc_transfers_LoL[4]
    calc_product_id_list = calc_transfers_LoL[5]
    calc_enjoy_by_date_list = calc_transfers_LoL[6]
    calc_customer_id_list = calc_transfers_LoL[7]
    calc_transfer_qty_list = calc_transfers_LoL[8]
    calc_transfer_pallets_list = calc_transfers_LoL[9]
    calc_truck_count_list = calc_transfers_LoL[10]
    
    
    for i in range(len(calc_ship_date_list)):
        tuple_to_write = (calc_transfers_id,
                        None,
                        None,
                        calc_ship_date_list[i],
                        calc_arrival_date_list[i],
                        calc_ship_facility_id_list[i],
                        calc_arrival_facility_id_list[i],
                        calc_transfer_constraints_id_list[i],
                        calc_product_id_list[i],
                        calc_enjoy_by_date_list[i],
                        calc_customer_id_list[i],
                        round(float(calc_transfer_qty_list[i]),2),
                        calc_transfer_pallets_list[i],
                        calc_truck_count_list[i],
                        load_date,
                        to_date,
                        is_active)
        
        cnxn_cursor.execute(sql, tuple_to_write)
        calc_transfers_id += 1
    
    cnxn.commit()
    cnxn_cursor.close()
    cnxn.close()
    
    
    return 'CalculatedTransfers_Facts pau'

def priorHarvestAllocation(demand_allocation_date, harvest_in_LoL, short_demand_LoL, facilities_LoL, allocated_crops_LoL, products_LoL):
    '''
    #### Inputs:
        - demand_allocation_date: date of the short demand allocation in main loop
        - harvest_in_LoL: harvest
            1. List of harvest dates
            2. List of greenhouse IDs
            3. List of greenhouse line IDs
            4. List of crop IDs
            5. List of expected plant sites
            6. List of average headweights
            7. List of loose grams per plant site
        - short_demand_LoL: remaining demand to allocate towards with transfers
            1. List of demand dates
            2. List of demand allocation dates
            3. List of greenhouse IDs
            4. List of product IDs
            5. List of customer IDs
            6. List of short demand quantities cooresponding to each combination of demand date/greenhouse/product/customer
            7. List of production priorities cooresponding to each combination of demand date/greenhouse/product
        - facilities_LoL: greenhouses dimension
            1. List of greenhouse IDs
            2. List of city abbreviations
        - allocated_crops_LoL:  mid-allocation tracking
            1. List of date/crop/greenhouse key combinations that have an allocation
            2. List of starting plant sites for each date/crop/greenhouse key combination
            3. List of allocated plant sites for each date/crop/greenhouse key combination
            4. List of date/crop/greenhouse key combinations that have been fully allocated
        - products_LoL: products information
            1. List of product IDs
            2. List of stop sell days
            3. List of crop IDs
            4. List of net weight grams
            5. List of is whole boolean flags
            6. List of total shelf life
            7. List of production priority
            8. List of case equivalent multipler
            9. List of cases per pallet
        - calendar_LoL: calendar information
            1. List of date day
            2. List of year number
            3. List of week of year
            4. List of year week
            5. List of day of week
    #### Algorithm:
    - initialize inputs
    - for the demand allocation date
        - check for initial truck capacity
        - organize demand into nested dictionaries 
        - harvest allocation for Retail products
    - create and return output tuple
    
    #### Output:prior_harvest_tuple
        - harvest_allocation_transfers_LoL: harvest allocation for calculated transfers
            1. List of demand allocation dates
            2. List of demand dates
            3. List of harvest greenhouse IDs
            4. List of demand greenhouse IDs
            5. List of crop IDs
            6. List of product IDs
            7. List of customer IDs
            7. List of forecasted grams per plant site values
            8. List of allocated plant sites
            9. List of allocated grams
            10. List of allocated quantities
            11. List of full packout boolean flags
        - allocated_crops_out_LoL: mid-allocation tracking
            1. List of date/crop/greenhouse key combinations that have an allocation
            2. List of starting plant sites for each date/crop/greenhouse key combination
            3. List of allocated plant sites for each date/crop/greenhouse key combination
            4. List of date/crop/greenhouse key combinations that have been fully allocated
        - short_demand_out_LoL: 
            1. List of demand dates
            2. List of demand allocation dates
            3. List of greenhouse IDs
            4. List of product IDs
            5. List of customer IDs
            6. List of short demand quantities cooresponding to each combination of demand date/greenhouse/product/customer
            7. List of production priorities cooresponding to each combination of demand date/greenhouse/product
   
        
    '''


    # input lists from harvest
    hfsf_harvest_date_list = harvest_in_LoL[0]
    hfsf_facility_id_list = harvest_in_LoL[1]
    hfsf_facility_line_id_list = harvest_in_LoL[2]
    hfsf_crop_id_list = harvest_in_LoL[3]
    hfsf_expected_plant_sites_list = harvest_in_LoL[4]
    hfsf_avg_headweight_list = harvest_in_LoL[5]
    hfsf_loose_grams_per_plant_site_list = harvest_in_LoL[6]
    
    # allocated crops
    allocated_date_crop_facility_key_list = allocated_crops_LoL[0]
    allocated_starting_ps_list = allocated_crops_LoL[1]
    allocated_plant_sites_list = allocated_crops_LoL[2]
    complete_crop_allocation_key_list = allocated_crops_LoL[3]
    

    # input lists from demand
    sdf_demand_date_list = short_demand_LoL[0]
    sdf_demand_allocation_date_list = short_demand_LoL[1]
    sdf_demand_facility_id_list = short_demand_LoL[2]
    sdf_product_id_list = short_demand_LoL[3]
    sdf_customer_id_list = short_demand_LoL[4]
    sdf_short_demand_qty_list = short_demand_LoL[5]
    
    ct_sdf_key_list = list()
    ct_qty_list = list()
    for sdf_idx in range(len(sdf_demand_date_list)):
        ct_sdf_key = sdf_demand_date_list[sdf_idx].strftime("%Y-%m-%d") + '_' + str(sdf_demand_facility_id_list[sdf_idx]) + '_' + str(sdf_product_id_list[sdf_idx]) + '_' + str(sdf_customer_id_list[sdf_idx])
        ct_qty = sdf_short_demand_qty_list[sdf_idx]
        if ct_sdf_key in ct_sdf_key_list:
            ct_qty_list[ct_sdf_key_list.index(ct_sdf_key)] += ct_qty
        if ct_sdf_key not in ct_sdf_key_list:
            ct_sdf_key_list += [ct_sdf_key]
            ct_qty_list += [ct_qty]
        
    
    # product dimension
    pd_product_id_list = products_LoL[0]
    pd_shelf_life_guarantee_list = products_LoL[1]
    pd_crop_id_list = products_LoL[2]
    pd_net_weight_grams_list = products_LoL[3]
    pd_is_whole_list = products_LoL[4]
    pd_total_shelf_life_list = products_LoL[5]
    pd_case_equivalent_multiplier_list = products_LoL[7]
    pd_cases_per_pallet_list = products_LoL[8]
    
    # facilities dimension
    fd_facility_id_list = facilities_LoL[0]
    fd_city_short_code_list = facilities_LoL[1]

    
    # initialize harvest allocation lists for transfers
    # lists for CustomerHarvestAllocation_Facts
    haf_demand_allocation_date_list = list()
    haf_demand_date_list = list()
    haf_harvest_facility_id_list = list()
    haf_demand_facility_id_list = list()
    haf_crop_id_list = list()
    haf_product_id_list = list()
    haf_customer_id_list = list()
    haf_forecasted_gpps_list = list()
    haf_allocated_plant_sites_list = list()
    haf_allocated_grams_list = list()
    haf_allocated_qty_list = list()
    haf_full_packout_list = list()
    
    # initialize trqacking list for attempted allocations
    skip_key_list = list()
    
    # initial date to check ship day
    # add one day since we need at least one day to plan the calculated transfer
    initial_date = hfsf_harvest_date_list[1]
                    
    # these lists will track the delta of harvest lists through the allocation process
    allocated_date_product_facility_key_list = list()
    allocated_gpps_list = list()
    allocated_qty_list = list()
    allocated_product_plant_sites_list = list()

    complete_product_allocation_key_list = list()
    complete_customer_allocation_key_list = list()

    allocated_date_product_facility_customer_key_list = list()
    allocated_customer_gpps_list = list()
    allocated_customer_qty_list = list()
    allocated_customer_plant_sites_list = list()
    allocated_customer_demand_date_list = list()
                
    
    # organize demand into demand_dict

    demand_dict = {}

    for ct_idx in range(len(ct_sdf_key_list)):
        ct_key = ct_sdf_key_list[ct_idx]
        sdf_demand_date = DT.datetime.strptime(ct_key.split('_')[0],"%Y-%m-%d").date()
        sdf_greenhouse_id = int(ct_key.split('_')[1])
        sdf_product_id = int(ct_key.split('_')[2])
        sdf_customer_id = int(ct_key.split('_')[3])
        sdf_short_demand_qty = ct_qty_list[ct_idx]

        sdf_crop_id = pd_crop_id_list[pd_product_id_list.index(sdf_product_id)]
        sdf_production_priority = pd_production_priority_list[pd_product_id_list.index(sdf_product_id)]

        if sdf_short_demand_qty != None and sdf_customer_id != 0:
            if sdf_greenhouse_id in demand_dict.keys():
                sdf_greenhouse_id_dictionary_value = demand_dict[sdf_greenhouse_id]
                if sdf_production_priority in sdf_greenhouse_id_dictionary_value.keys():
                    sdf_production_priority_dictionary_value = demand_dict[sdf_greenhouse_id][sdf_production_priority]
                    if sdf_crop_id in sdf_production_priority_dictionary_value.keys():
                        sdf_crop_id_dictionary_value = demand_dict[sdf_greenhouse_id][sdf_production_priority][sdf_crop_id]
                        if sdf_product_id in sdf_crop_id_dictionary_value.keys():
                            sdf_product_id_dictionary_value = demand_dict[sdf_greenhouse_id][sdf_production_priority][sdf_crop_id][sdf_product_id]
                            if sdf_customer_id in sdf_product_id_dictionary_value.keys():
                                sdf_customer_id_dictionary_value = demand_dict[sdf_greenhouse_id][sdf_production_priority][sdf_crop_id][sdf_product_id][sdf_customer_id]
                                if sdf_demand_date in sdf_customer_id_dictionary_value.keys():
                                    demand_dict[sdf_greenhouse_id][sdf_production_priority][sdf_crop_id][sdf_product_id][sdf_customer_id][sdf_demand_date] += sdf_short_demand_qty
                                else:
                                    demand_dict[sdf_greenhouse_id][sdf_production_priority][sdf_crop_id][sdf_product_id][sdf_customer_id][sdf_demand_date] = sdf_short_demand_qty
                            else:
                                demand_dict[sdf_greenhouse_id][sdf_production_priority][sdf_crop_id][sdf_product_id][sdf_customer_id] = {sdf_demand_date:sdf_short_demand_qty}
                        else:
                            demand_dict[sdf_greenhouse_id][sdf_production_priority][sdf_crop_id][sdf_product_id] = {sdf_customer_id:{sdf_demand_date:sdf_short_demand_qty}}
                    else:
                        demand_dict[sdf_greenhouse_id][sdf_production_priority][sdf_crop_id] = {sdf_product_id:{sdf_customer_id:{sdf_demand_date:sdf_short_demand_qty}}}
                else:
                    demand_dict[sdf_greenhouse_id][sdf_production_priority] = {sdf_crop_id:{sdf_product_id:{sdf_customer_id:{sdf_demand_date:sdf_short_demand_qty}}}}
            else:
                demand_dict[sdf_greenhouse_id] = {sdf_production_priority:{sdf_crop_id:{sdf_product_id:{sdf_customer_id:{sdf_demand_date:sdf_short_demand_qty}}}}}


    last_harvest_day = demand_allocation_date - DT.timedelta(days = 1)
    # loop through each greenhouse
    for greenhouse_id in list(demand_dict.keys()):
        # check if there is short retail products
        if 2 in demand_dict[greenhouse_id].keys():

            for crop_id in demand_dict[greenhouse_id][2].keys():
                for product_id in list(demand_dict[greenhouse_id][2][crop_id].keys()):


                    # get values from Products_Dim
                    pd_idx = pd_product_id_list.index(product_id)
                    net_weight_grams = pd_net_weight_grams_list[pd_idx]
                    is_whole = pd_is_whole_list[pd_idx]
                    total_shelf_life = pd_total_shelf_life_list[pd_idx]
                    shelf_life_guarantee = pd_shelf_life_guarantee_list[pd_idx]
                    stop_sell = total_shelf_life - shelf_life_guarantee
                    for customer_id in list(demand_dict[greenhouse_id][2][crop_id][product_id].keys()):
                        for demand_date in list(demand_dict[greenhouse_id][2][crop_id][product_id][customer_id].keys()):
                            #og_key = str(product_id) + '_' + str(customer_id) + '_' + str(demand_date)
                            og_key = demand_date.strftime("%Y-%m-%d") + '_' + str(greenhouse_id) + '_' + str(product_id) + '_' + str(customer_id)

                            short_demand_qty = demand_dict[greenhouse_id][2][crop_id][product_id][customer_id][demand_date]

                            #earliest we could harvest for the demand
                            first_harvest_day = demand_date - DT.timedelta(days = stop_sell)


                            # get harvest for the crop in the ship greenhouse
                            transfer_harvest_date_list = list()

                            harvest_date_indices =  [i for i, x in enumerate(hfsf_harvest_date_list) if x >= first_harvest_day and x<= last_harvest_day]
                            harvest_crop_id_indices =  [j for j, y in enumerate(hfsf_crop_id_list) if y == crop_id]
                            harvest_facility_id_indices = [k for k, z in enumerate(hfsf_facility_id_list)
                                                         if fd_city_short_code_list[fd_facility_id_list.index(z)]
                                                            == fd_city_short_code_list[fd_facility_id_list.index(greenhouse_id)]]
                            harvest_date_crop_facility_indices = list(set(harvest_date_indices) & set(harvest_crop_id_indices) & set(harvest_facility_id_indices))

                            all_harvest_expected_plant_sites_list = list()
                            all_harvest_whole_gpps_list = list()
                            all_harvest_loose_gpps_list = list()

                            # organize harvest by date
                            distinct_harvest_date_list = list()

                            for hfsf_idx in harvest_date_crop_facility_indices:

                                hfsf_harvest_date = hfsf_harvest_date_list[hfsf_idx]
                                hfsf_expected_plant_sites = hfsf_expected_plant_sites_list[hfsf_idx]
                                if hfsf_harvest_date in distinct_harvest_date_list and hfsf_expected_plant_sites > 0:

                                    harvest_idx = distinct_harvest_date_list.index(hfsf_harvest_date)
                                    # get total plant sites, whole grams per plant site, and loose grams per plant site
                                    all_harvest_expected_plant_sites_list[harvest_idx] += [hfsf_expected_plant_sites]
                                    all_harvest_whole_gpps_list[harvest_idx] += [hfsf_avg_headweight_list[hfsf_idx]]
                                    all_harvest_loose_gpps_list[harvest_idx] += [hfsf_loose_grams_per_plant_site_list[hfsf_idx]]


                                if hfsf_harvest_date not in distinct_harvest_date_list and hfsf_expected_plant_sites > 0:
                                    distinct_harvest_date_list += [hfsf_harvest_date]
                                    # get total plant sites, whole grams per plant site, and loose grams per plant site
                                    all_harvest_expected_plant_sites_list += [[hfsf_expected_plant_sites]]
                                    all_harvest_whole_gpps_list += [[hfsf_avg_headweight_list[hfsf_idx]]]
                                    all_harvest_loose_gpps_list += [[hfsf_loose_grams_per_plant_site_list[hfsf_idx]]]


                            # sort harvest by date
                            sorted_harvest_date_list = list(np.sort(distinct_harvest_date_list))
                            sorted_harvest_expected_plant_sites_list = list([x for _,x in sorted(zip(distinct_harvest_date_list,all_harvest_expected_plant_sites_list))])
                            sorted_harvest_whole_gpps_list = list([y for _,y in sorted(zip(distinct_harvest_date_list,all_harvest_whole_gpps_list))])
                            sorted_harvest_loose_gpps_list = list([y for _,y in sorted(zip(distinct_harvest_date_list,all_harvest_loose_gpps_list))])


                            # attempt allocation in reverse chronological order
                            while len(sorted_harvest_date_list) > 0 and short_demand_qty > 0:
                                #print(len(sorted_harvest_date_list),short_demand_qty)
                                harvest_date = sorted_harvest_date_list[-1]

                                # checkpoint: skip allocation for this product_customer_demanddate_harvestdate since its been exhausted previously by calculated transfers
                                check_skip_key = og_key + '_' + harvest_date.strftime("%Y-%m-%d") + str(greenhouse_id)

                                if check_skip_key in skip_key_list:
                                    del sorted_harvest_date_list[-1]
                                    del sorted_harvest_expected_plant_sites_list[-1]
                                    del sorted_harvest_whole_gpps_list[-1]
                                    del sorted_harvest_loose_gpps_list[-1]
                                if check_skip_key not in skip_key_list:


                                    allocated_date_crop_facility_key = str(harvest_date) + '_' + str(crop_id) + '_' + str(greenhouse_id)

                                    # remove from lists if harvest is completely allocated (no harvest is available)
                                    if allocated_date_crop_facility_key in complete_crop_allocation_key_list:
                                        del sorted_harvest_date_list[-1]
                                        del sorted_harvest_expected_plant_sites_list[-1]
                                        del sorted_harvest_whole_gpps_list[-1]
                                        del sorted_harvest_loose_gpps_list[-1]

                                    # checkpoint: harvest is available

                                    if allocated_date_crop_facility_key not in complete_crop_allocation_key_list:

                                        # get plant sites already allocated for the facility on the harvest date
                                        already_allocated_plant_sites = 0
                                        if allocated_date_crop_facility_key in allocated_date_crop_facility_key_list: 
                                            already_allocated_plant_sites = allocated_plant_sites_list[allocated_date_crop_facility_key_list.index(allocated_date_crop_facility_key)]

                                        # available harvest closes to ship day
                                        harvest_expected_plant_sites_list = sorted_harvest_expected_plant_sites_list[-1]
                                        harvest_whole_gpps_list = sorted_harvest_whole_gpps_list[-1]
                                        harvest_loose_gpps_list = sorted_harvest_loose_gpps_list[-1]

                                        # GPPS normalized by expected plant sites
                                        whole_numerator = 0
                                        loose_numerator = 0
                                        total_ps = 0
                                        for harvest_idx in range(len(harvest_whole_gpps_list)):
                                            whole_numerator += harvest_whole_gpps_list[harvest_idx] * harvest_expected_plant_sites_list[harvest_idx]
                                            loose_numerator += harvest_loose_gpps_list[harvest_idx] * harvest_expected_plant_sites_list[harvest_idx]
                                            total_ps += harvest_expected_plant_sites_list[harvest_idx]

                                        harvest_facility_mean_whole_gpps = 0
                                        harvest_facility_mean_loose_gpps = 0
                                        if total_ps != 0:
                                            harvest_facility_mean_whole_gpps = round(float(whole_numerator/total_ps),2)
                                            harvest_facility_mean_loose_gpps = round(float(loose_numerator/total_ps),2)

                                        # choose conversion factor
                                        harvest_facility_mean_gpps = harvest_facility_mean_loose_gpps 
                                        if is_whole == 1:
                                            harvest_facility_mean_gpps = harvest_facility_mean_whole_gpps

                                        # compute net_plant_sites for the short demand
                                        #net_plant_sites = int(short_demand_qty * np.ceil(float(net_weight_grams / harvest_facility_mean_gpps)))
                                        net_plant_sites = 0

                                        if harvest_facility_mean_gpps != 0:
                                            net_plant_sites = int(np.ceil(float(short_demand_qty * net_weight_grams / harvest_facility_mean_gpps)))

                                        # compute net plant sites for the facility
                                        harvest_facility_net_plant_sites = int(sum(harvest_expected_plant_sites_list))

                                        # compute remaining plant sites for allocation considering the mid allocation checkpoint
                                        harvest_facility_pre_plant_sites = harvest_facility_net_plant_sites - already_allocated_plant_sites

                                        # ATTEMPT THE ALLOCATION
                                        harvest_facility_post_plant_sites = harvest_facility_pre_plant_sites - net_plant_sites

                                        # first deal with what to do when we cannot satisfy the demand
                                        if harvest_facility_post_plant_sites < 0:
                                            # oh no! this demand exceeds our harvest for the facility
                                            #print('oh no short on ', crop_id, ship_greenhouse_id, harvest_date, demand_allocation_date)
                                            # update harvest allocation lists
                                            del sorted_harvest_date_list[-1]
                                            del sorted_harvest_expected_plant_sites_list[-1]
                                            del sorted_harvest_whole_gpps_list[-1]
                                            del sorted_harvest_loose_gpps_list[-1]

                                            # set full_packout to true
                                            full_packout = 1

                                            # compute total short demand 
                                            # accumulate short_demand_plant_sites for all products
                                            short_demand_plant_sites = 0

                                            for a_product_id in list(demand_dict[greenhouse_id][2][crop_id].keys()):
                                                pd_idx = pd_product_id_list.index(a_product_id)
                                                net_weight_grams = pd_net_weight_grams_list[pd_idx]
                                                is_whole = pd_is_whole_list[pd_idx]
                                                # choose conversion factor
                                                harvest_facility_mean_gpps = harvest_facility_mean_loose_gpps 
                                                if is_whole == 1:
                                                    harvest_facility_mean_gpps = harvest_facility_mean_whole_gpps
                                                for a_customer_id in list(demand_dict[greenhouse_id][2][crop_id][a_product_id].keys()):
                                                    for a_demand_date in list(demand_dict[greenhouse_id][2][crop_id][a_product_id][a_customer_id].keys()):
                                                        a_short_demand_qty = demand_dict[greenhouse_id][2][crop_id][a_product_id][a_customer_id][a_demand_date]
                                                        # compute net_plant_sites for the short demand
                                                        net_plant_sites = int(np.ceil(float(a_short_demand_qty * net_weight_grams / harvest_facility_mean_gpps)))
                                                        short_demand_plant_sites += net_plant_sites


                                            # accumulate harvest_priority_plant_sites available for all products
                                            harvest_priority_plant_sites = harvest_facility_pre_plant_sites
                                            for check_idx in range(len(allocated_date_product_facility_key_list)):
                                                check_key = allocated_date_product_facility_key_list[check_idx]
                                                check_harvest_date = DT.datetime.strptime(str(check_key.split("_")[0]), '%Y-%m-%d').date()
                                                check_product_id = int(check_key.split("_")[1])
                                                check_facility_id = int(check_key.split("_")[2])
                                                check_crop_id = pd_crop_id_list[pd_product_id_list.index(check_product_id)]
                                                check_date_crop_facility_key = str(check_harvest_date) + '_' + str(check_crop_id) + '_' + str(check_facility_id)
                                                if check_harvest_date == harvest_date and check_facility_id == greenhouse_id and check_crop_id == crop_id:
                                                #if check_demand_allocation_date == demand_allocation_date and check_city_short_code == fd_city_short_code_list[fd_facility_id_list.index(harvest_facility_id)] and check_priority == production_priority and check_crop_id == crop_id:
                                                    check_allocated_product_plant_sites = allocated_product_plant_sites_list[check_idx]
                                                    # add the plant sites for the harvest_demand_ratio
                                                    harvest_priority_plant_sites += check_allocated_product_plant_sites
                                                    # subtract the plant sites from the overall allocation tracking list since the new value multiplied by harvest_demand_ratio will be added back
                                                    allocated_plant_sites_list[allocated_date_crop_facility_key_list.index(check_date_crop_facility_key)] -= check_allocated_product_plant_sites


                                            # compute harvest_demand_ratio (less than 1 in this case)
                                            harvest_demand_ratio = float(harvest_priority_plant_sites / short_demand_plant_sites)
                                            #print(list(demand_dict[2][crop_id].keys()))
                                            # allocate to every product ID where shorts exist
                                            for s_product_id in list(demand_dict[greenhouse_id][2][crop_id].keys()):
                                                s_pd_idx = pd_product_id_list.index(s_product_id)
                                                s_net_weight_grams = pd_net_weight_grams_list[s_pd_idx]
                                                s_is_whole = pd_is_whole_list[s_pd_idx]
                                                s_total_shelf_life = pd_total_shelf_life_list[s_pd_idx]
                                                # choose conversion factor
                                                s_harvest_facility_mean_gpps = harvest_facility_mean_loose_gpps 
                                                if s_is_whole == 1:
                                                    s_harvest_facility_mean_gpps = harvest_facility_mean_whole_gpps
                                                s_forecasted_gpps = round(float(harvest_facility_mean_gpps),2)
                                                s_allocated_date_product_facility_key = str(harvest_date) + '_' + str(s_product_id) + '_' + str(greenhouse_id)

                                                for s_customer_id in list(demand_dict[greenhouse_id][2][crop_id][s_product_id].keys()):
                                                    s_allocated_date_product_facility_customer_key = str(harvest_date) + '_' + str(s_product_id) + '_' + str(greenhouse_id)+ '_' + str(s_customer_id)
                                                    #print(list(demand_dict[2][crop_id][s_product_id][s_customer_id].keys()))
                                                    for s_demand_date in list(demand_dict[greenhouse_id][2][crop_id][s_product_id][s_customer_id].keys()):

                                                        s_short_demand_qty = demand_dict[greenhouse_id][2][crop_id][s_product_id][s_customer_id][s_demand_date]

                                                        # apply harvest_demand_ratio to allocated_qty
                                                        allocated_qty = int(np.floor(s_short_demand_qty * harvest_demand_ratio))
                                                        allocated_product_plant_sites = int(np.ceil(allocated_qty * s_net_weight_grams / s_forecasted_gpps))
                                                        allocated_grams = round(allocated_product_plant_sites * s_forecasted_gpps,2)
                                                        #print(s_short_demand_qty, allocated_qty)
                                                        if allocated_qty > 0:
                                                            # write to lists for HarvestAllocation_Facts
                                                            haf_demand_allocation_date_list += [harvest_date]
                                                            haf_demand_date_list += [s_demand_date]
                                                            haf_harvest_facility_id_list += [greenhouse_id]
                                                            haf_demand_facility_id_list += [greenhouse_id]
                                                            haf_crop_id_list += [crop_id]
                                                            haf_product_id_list += [s_product_id]
                                                            haf_customer_id_list += [s_customer_id]
                                                            haf_forecasted_gpps_list += [s_forecasted_gpps]
                                                            haf_allocated_plant_sites_list += [allocated_product_plant_sites]
                                                            haf_allocated_grams_list += [allocated_grams]
                                                            haf_allocated_qty_list += [allocated_qty]
                                                            haf_full_packout_list += [full_packout]

                                                            # update the allocation_lists
                                                            if allocated_date_crop_facility_key in allocated_date_crop_facility_key_list:        
                                                                allocated_plant_sites_list[allocated_date_crop_facility_key_list.index(allocated_date_crop_facility_key)] += allocated_product_plant_sites

                                                            if allocated_date_crop_facility_key not in allocated_date_crop_facility_key_list:
                                                                allocated_date_crop_facility_key_list += [allocated_date_crop_facility_key]
                                                                allocated_plant_sites_list += [allocated_product_plant_sites]
                                                                allocated_starting_ps_list += [harvest_facility_net_plant_sites]                          

                                                        if s_allocated_date_product_facility_key in allocated_date_product_facility_key_list:
                                                            del allocated_gpps_list[allocated_date_product_facility_key_list.index(s_allocated_date_product_facility_key)]
                                                            del allocated_qty_list[allocated_date_product_facility_key_list.index(s_allocated_date_product_facility_key)]
                                                            del allocated_product_plant_sites_list[allocated_date_product_facility_key_list.index(s_allocated_date_product_facility_key)]
                                                            del allocated_date_product_facility_key_list[allocated_date_product_facility_key_list.index(s_allocated_date_product_facility_key)]

                                                        # customer allocation
                                                        if s_allocated_date_product_facility_customer_key in allocated_date_product_facility_customer_key_list:
                                                            adpfc_key_idx = allocated_date_product_facility_customer_key_list.index(s_allocated_date_product_facility_customer_key)
                                                            del allocated_date_product_facility_customer_key_list[adpfc_key_idx]
                                                            del allocated_customer_gpps_list[adpfc_key_idx]
                                                            del allocated_customer_qty_list[adpfc_key_idx]
                                                            del allocated_customer_plant_sites_list[adpfc_key_idx]
                                                            del allocated_customer_demand_date_list[adpfc_key_idx]

                                                        # mark allocation as complete
                                                        if allocated_date_crop_facility_key not in complete_crop_allocation_key_list:
                                                            complete_crop_allocation_key_list += [allocated_date_crop_facility_key]
                                                        if s_allocated_date_product_facility_key not in complete_product_allocation_key_list:
                                                            complete_product_allocation_key_list += [s_allocated_date_product_facility_key]
                                                        if s_allocated_date_product_facility_customer_key not in complete_customer_allocation_key_list:
                                                            complete_customer_allocation_key_list += [s_allocated_date_product_facility_customer_key]

                                                        # new short demand
                                                        # continue looking from previous harvest day for the product/customer/demand_date
                                                        new_short_demand_qty = s_short_demand_qty - allocated_qty

                                                        #print(new_short_demand_qty)
                                                        # update dictionary
                                                        demand_dict[greenhouse_id][2][crop_id][s_product_id][s_customer_id][s_demand_date] = new_short_demand_qty



                                                        s_sdf_key = s_demand_date.strftime("%Y-%m-%d") + '_' + str(greenhouse_id) + '_' + str(s_product_id) + '_' + str(s_customer_id)

                                                        #print(og_key, s_sdf_key, new_short_demand_qty)

                                                        if s_sdf_key == og_key:
                                                            short_demand_qty = new_short_demand_qty

                                                        if new_short_demand_qty > 0 and s_sdf_key in ct_sdf_key_list:
                                                            ct_qty_list[ct_sdf_key_list.index(s_sdf_key)] = new_short_demand_qty                                                               
                                                        if new_short_demand_qty == 0 and s_sdf_key in ct_sdf_key_list:
                                                            del ct_qty_list[ct_sdf_key_list.index(s_sdf_key)]
                                                            del ct_sdf_key_list[ct_sdf_key_list.index(s_sdf_key)]


                                                        # skip main loop allocation for this product_customer_demanddate_harvestdate_shipgreenhouse
                                                        skip_key = s_sdf_key + '_' + harvest_date.strftime("%Y-%m-%d") + str(greenhouse_id)
                                                        skip_key_list += [skip_key]

                                        # next deal with what to do when we can successfully satisfy the demand
                                        if harvest_facility_post_plant_sites >= 0 and net_plant_sites > 0:
                                            # yay! we have enough plant sites to cover the demand (so far)
                                            # update the allocation lists

                                            if allocated_date_crop_facility_key in allocated_date_crop_facility_key_list:
                                                allocated_plant_sites_list[allocated_date_crop_facility_key_list.index(allocated_date_crop_facility_key)] += net_plant_sites

                                            # add to mid-allocation tracker if its a new key
                                            if allocated_date_crop_facility_key not in allocated_date_crop_facility_key_list:
                                                allocated_date_crop_facility_key_list += [allocated_date_crop_facility_key]
                                                allocated_plant_sites_list += [net_plant_sites]
                                                allocated_starting_ps_list += [harvest_facility_net_plant_sites]

                                            # create key for mid-allocation check
                                            allocated_date_product_facility_key = str(harvest_date) + '_' + str(product_id) + '_' + str(greenhouse_id)
                                            allocated_date_product_facility_customer_key = str(harvest_date) + '_' + str(product_id) + '_' + str(greenhouse_id) + '_' + str(customer_id)

                                            if allocated_date_product_facility_key in allocated_date_product_facility_key_list:
                                                allocated_qty_list[allocated_date_product_facility_key_list.index(allocated_date_product_facility_key)] += short_demand_qty
                                                allocated_product_plant_sites_list[allocated_date_product_facility_key_list.index(allocated_date_product_facility_key)] += net_plant_sites

                                            if allocated_date_product_facility_key not in allocated_date_product_facility_key_list:   
                                                allocated_date_product_facility_key_list += [allocated_date_product_facility_key]
                                                allocated_gpps_list += [round(float(harvest_facility_mean_gpps),2)]
                                                allocated_qty_list += [short_demand_qty]
                                                allocated_product_plant_sites_list += [net_plant_sites]

                                            if allocated_date_product_facility_customer_key in allocated_date_product_facility_customer_key_list:
                                                allocated_customer_qty_list[allocated_date_product_facility_customer_key_list.index(allocated_date_product_facility_customer_key)] += [short_demand_qty]
                                                allocated_customer_plant_sites_list[allocated_date_product_facility_customer_key_list.index(allocated_date_product_facility_customer_key)] += [net_plant_sites]
                                                allocated_customer_demand_date_list[allocated_date_product_facility_customer_key_list.index(allocated_date_product_facility_customer_key)] += [demand_date]
                                            if allocated_date_product_facility_customer_key not in allocated_date_product_facility_customer_key_list:   
                                                allocated_date_product_facility_customer_key_list += [allocated_date_product_facility_customer_key]
                                                allocated_customer_gpps_list += [round(float(harvest_facility_mean_gpps),2)]
                                                allocated_customer_qty_list += [[short_demand_qty]]
                                                allocated_customer_plant_sites_list += [[net_plant_sites]] 
                                                allocated_customer_demand_date_list += [[demand_date]]

                                            short_demand_qty = 0



                            # exit while loop through harvest dates

                            # short demand remaining
                            sdf_key = demand_date.strftime("%Y-%m-%d") + '_' + str(greenhouse_id) + '_' + str(product_id) + '_' + str(customer_id)
                            if short_demand_qty > 0 and sdf_key in ct_sdf_key_list:                                        
                                ct_qty_list[ct_sdf_key_list.index(sdf_key)] = short_demand_qty
                            if short_demand_qty == 0 and sdf_key in ct_sdf_key_list:
                                del ct_qty_list[ct_sdf_key_list.index(sdf_key)]
                                del ct_sdf_key_list[ct_sdf_key_list.index(sdf_key)]


            # exit loop through all demand
            # after going through all short demand

            # set full_packout to false
            full_packout = 0
            # write all complete allocations to customerHarvestAllocation_Facts for the remaining products
            for allocated_idx in range(len(allocated_date_product_facility_customer_key_list)):
                allocated_date_product_facility_customer_key = allocated_date_product_facility_customer_key_list[allocated_idx]
                # only write the keys that are not yet completed
                if allocated_date_product_facility_customer_key not in complete_customer_allocation_key_list:

                    customer_id = int(allocated_date_product_facility_customer_key.split("_")[3])
                    greenhouse_id = int(allocated_date_product_facility_customer_key.split("_")[2])
                    product_id = int(allocated_date_product_facility_customer_key.split("_")[1])
                    harvest_date_str = allocated_date_product_facility_customer_key.split("_")[0]
                    harvest_date = DT.datetime.strptime(harvest_date_str, '%Y-%m-%d').date()
                    # get values from Products_Dim
                    pd_idx = pd_product_id_list.index(product_id)
                    crop_id = pd_crop_id_list[pd_idx]
                    net_weight_grams = pd_net_weight_grams_list[pd_idx]
                    total_shelf_life = pd_total_shelf_life_list[pd_idx]
                    case_equivalent_multiplier = pd_case_equivalent_multiplier_list[pd_idx]
                    cases_per_pallet = pd_cases_per_pallet_list[pd_idx]

                    allocated_customer_qty_sub_list = allocated_customer_qty_list[allocated_idx]
                    forecasted_gpps = allocated_customer_gpps_list[allocated_idx]
                    allocated_customer_plant_sites_sub_list = allocated_customer_plant_sites_list[allocated_idx]
                    demand_date_sub_list = allocated_customer_demand_date_list[allocated_idx]

                    #print('full fill: ',harvest_date, crop_id, product_id, ship_greenhouse_id)

                    for sub_idx in range(len(demand_date_sub_list)):
                        allocated_customer_qty = allocated_customer_qty_sub_list[sub_idx]
                        allocated_customer_plant_sites = allocated_customer_plant_sites_sub_list[sub_idx]
                        demand_date = demand_date_sub_list[sub_idx]
                        allocated_grams = round(allocated_customer_plant_sites * forecasted_gpps,2)

                        # write to lists for CustomerHarvestAllocation_Facts
                        haf_demand_allocation_date_list += [harvest_date]
                        haf_demand_date_list += [demand_date]
                        haf_harvest_facility_id_list += [greenhouse_id]
                        haf_demand_facility_id_list += [greenhouse_id]
                        haf_crop_id_list += [crop_id]
                        haf_product_id_list += [product_id]
                        haf_customer_id_list += [customer_id]
                        haf_forecasted_gpps_list += [forecasted_gpps]
                        haf_allocated_plant_sites_list += [allocated_customer_plant_sites]
                        haf_allocated_grams_list += [allocated_grams]
                        haf_allocated_qty_list += [allocated_customer_qty]
                        haf_full_packout_list += [full_packout]


                    complete_customer_allocation_key_list += [allocated_date_product_facility_customer_key]

                  
    # harvest allocation output
    harvest_allocation_prior_LoL = [haf_demand_allocation_date_list,
        haf_demand_date_list,
        haf_harvest_facility_id_list,
        haf_demand_facility_id_list,
        haf_crop_id_list,
        haf_product_id_list,
        haf_customer_id_list,
        haf_forecasted_gpps_list,
        haf_allocated_plant_sites_list,
        haf_allocated_grams_list,
        haf_allocated_qty_list,
        haf_full_packout_list]  
    
    # allocated crops output
    allocated_crops_out_LoL = [allocated_date_crop_facility_key_list,
        allocated_starting_ps_list,
        allocated_plant_sites_list,
        complete_crop_allocation_key_list]
    
    # short demand
    ct_sdf_demand_date_list = list()
    ct_sdf_demand_allocation_date_list= list()
    ct_sdf_demand_facility_id_list= list()
    ct_sdf_product_id_list= list()
    ct_sdf_customer_id_list= list()
    ct_sdf_short_demand_qty_list= list()
    for ct_idx in range(len(ct_sdf_key_list)):
        ct_key = ct_sdf_key_list[ct_idx]
        ct_sdf_demand_date_list += [DT.datetime.strptime(ct_key.split('_')[0],"%Y-%m-%d").date()]
        ct_sdf_demand_allocation_date_list += [demand_allocation_date]
        ct_sdf_demand_facility_id_list += [int(ct_key.split('_')[1])]
        ct_sdf_product_id_list += [int(ct_key.split('_')[2])]
        ct_sdf_customer_id_list += [int(ct_key.split('_')[3])]
        ct_sdf_short_demand_qty_list += [ct_qty_list[ct_idx]]
    
    short_demand_out_LoL = [
        ct_sdf_demand_date_list,
        ct_sdf_demand_allocation_date_list,
        ct_sdf_demand_facility_id_list,
        ct_sdf_product_id_list,
        ct_sdf_customer_id_list,
        ct_sdf_short_demand_qty_list] 

    
    prior_harvest_tuple = (harvest_allocation_prior_LoL, allocated_crops_out_LoL, short_demand_out_LoL)
    
   # print('sl:', len(ct_sdf_short_demand_qty_list))
    return prior_harvest_tuple    

print('functions loaded')


# In[2]:


#start timer for execution
start_time = time.time()


if debug_status == 0:
    ## Check inventory for an actual count today

    HOSTNAME = socket.gethostname()

    if HOSTNAME == 'hostname':
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
                                Server=127.0.0.1,1443;
                                Database=databasename;
                                UID=%s;
                                PWD=%s;""" % (uid, pwd) # use config.yml on local machine

    cnxn = pyodbc.connect(CONNECTIONSTRING)   
    cnxn_cursor = cnxn.cursor()


    # check if data has been refreshed with actual inventory in the past

    sql = "SELECT InventoryLoadDate FROM InventoryStatus_Lov WHERE CONVERT(Date,InventoryLoadDate) = CONVERT(Date,GETDATE())"
    cnxn_cursor.execute(sql) 
    row = cnxn_cursor.fetchone()
    run_status = 0
    if row is not None:
        # run_status = 1 if the inventory has already been loaded and allocations were done already 
        run_status = 1

    cnxn.commit()
    cnxn_cursor.close()
    cnxn.close()
    ###########################################################################################

    # check inventory date

    if HOSTNAME == 'hostname':
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
    sql = "SELECT DISTINCT CoolerInventoryDate FROM Inventory_Facts WHERE CurrentRecord = 1"
    cnxn_cursor.execute(sql) 
    row = cnxn_cursor.fetchone()
    inventory_date = row[0]

    cnxn.commit()
    cnxn_cursor.close()
    cnxn.close()

    ###################
    check_today_datetime = DT.datetime.now()
    check_today_date = check_today_datetime.date()

    check_for_new_inventory = 0
    if check_today_date == inventory_date:
        check_for_new_inventory = 1
        print('actual inventory loaded')
        if run_status == 0:
            # this is the first run with actual inventory
            if HOSTNAME == 'hostname':
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
                                        Server=127.0.0.1,1443;
                                        Database=databasename;
                                        UID=%s;
                                        PWD=%s;""" % (uid, pwd) # use config.yml on local machine
            cnxn = pyodbc.connect(CONNECTIONSTRING) 
            cnxn_cursor = cnxn.cursor()
            sql = "SELECT MAX(InventoryStatusID) FROM InventoryStatus_Lov;"
            cnxn_cursor.execute(sql) 
            row = cnxn_cursor.fetchone()
            cnxn.commit()
            cnxn_cursor.close()
            cnxn.close()
            inventory_status_id = 0
            if row is not None:
                if row[0] is not None:
                    inventory_status_id = row[0]
            inventory_status_id += 1
            # write datetime to InventoryStatus_Lov
            if HOSTNAME == 'hostname':
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
                                        Server=127.0.0.1,1443;
                                        Database=databasename;
                                        UID=%s;
                                        PWD=%s;""" % (uid, pwd) # use config.yml on local machine
            cnxn = pyodbc.connect(CONNECTIONSTRING) 
            cnxn_cursor = cnxn.cursor()
            sql = """
            INSERT INTO InventoryStatus_Lov
            VALUES (?,?);
            """ 
            # write to database
            tuple_to_write = (inventory_status_id, check_today_datetime)
            cnxn_cursor.execute(sql, tuple_to_write)
            cnxn.commit()
            cnxn_cursor.close()
            cnxn.close()


    else:
        print('no new inventory')


# In[5]:



if debug_status == 1 or (check_for_new_inventory == 1 and run_status == 0):

    # load data
    HOSTNAME = socket.gethostname()

    if HOSTNAME == 'hostname':
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

    # pull from Crop_Dim
    crd_crop_id_list = list()
    crd_sage_crop_code_list = list()
    sql = "SELECT CropID, SageCropCode FROM Crop_Dim ORDER BY CropID"
    cnxn_cursor.execute(sql) 
    row = cnxn_cursor.fetchone()

    crd_crop_id_list += [row[0]]
    crd_sage_crop_code_list += [row[1]]


    while row is not None:
        row = cnxn_cursor.fetchone()
        if row is not None:
            crd_crop_id_list += [row[0]]
            crd_sage_crop_code_list += [row[1]]



    # pull from Greenhouses_Dim
    fd_facility_id_list = list()
    fd_location_name_list = list()
    fd_city_short_code_list = list()
    fd_latitude_list = list()
    fd_longitude_list = list()

    sql = """
    SELECT GreenhouseID,
    GreenhouseName,
    CityAbbreviation,
    Latitude,
    Longitude
    FROM
    Greenhouses_Dim
    """
    cnxn_cursor.execute(sql) 
    row = cnxn_cursor.fetchone()

    fd_facility_id_list += [row[0]]
    fd_location_name_list += [row[1].rstrip()]
    fd_city_short_code_list += [row[2]]
    fd_latitude_list += [row[3]]
    fd_longitude_list += [row[4]]

    while row is not None:
        row = cnxn_cursor.fetchone()
        if row is not None:
            fd_facility_id_list += [row[0]]
            fd_location_name_list += [row[1].rstrip()]
            fd_city_short_code_list += [row[2]]
            fd_latitude_list += [row[3]]
            fd_longitude_list += [row[4]]

    # for each location find the timezone based on latitude and longitude
    def get_offset(*, lat, lng):
        """
        returns a location's time zone offset from UTC in hours.
        """

        today = datetime.now()
        tz_target = timezone(tf.certain_timezone_at(lng=lng, lat=lat))
        # ATTENTION: tz_target could be None! handle error case
        today_target = tz_target.localize(today)
        today_utc = utc.localize(today)
        return (today_utc - today_target).total_seconds() / 3600

    tf = TimezoneFinder()

    # build list of last order call hours relative to east coast time
    fd_last_call_time_list = list()

    last_order_utc_hour = 8 # last order call is 12 PM local

    for fd_idx in range(len(fd_latitude_list)):
        fd_lat = fd_latitude_list[fd_idx]
        fd_lng = fd_longitude_list[fd_idx]

        hour_offset = get_offset(**{"lat": fd_lat, "lng": fd_lng})
        fd_last_call_time_list += [int(last_order_utc_hour - hour_offset)]

    # pull from Greenhouseline_Lov
    fld_facility_line_id_list = list()
    fld_facility_line_list = list()
    sql = "SELECT GreenhouseLineID, GreenhouseLine FROM GreenhouseLine_LOV"
    cnxn_cursor.execute(sql) 
    row = cnxn_cursor.fetchone()

    fld_facility_line_id_list += [row[0]]
    fld_facility_line_list += [row[1]]

    while row is not None:
        row = cnxn_cursor.fetchone()
        if row is not None:
            fld_facility_line_id_list += [row[0]]
            fld_facility_line_list += [row[1]]  


    # pull Customers_Dim
    cud_customers_id_list = list()
    cud_sage_customer_id_list = list()
    cud_service_factor_list = list()

    sql = "SELECT CustomersID, SageCustomerID,ServiceFactor FROM Customers_Dim"
    cnxn_cursor.execute(sql) 
    row = cnxn_cursor.fetchone()

    cud_customers_id_list += [row[0]]
    cud_sage_customer_id_list += [row[1].rstrip()]

    # defult service factor of 2.17
    cud_service_factor = 2.17
    if row[2] is not None:
        cud_service_factor = row[2]
    cud_service_factor_list += [cud_service_factor]

    while row is not None:
        row = cnxn_cursor.fetchone()
        if row is not None:
            cud_customers_id_list += [row[0]]
            cud_sage_customer_id_list += [row[1].rstrip()]
            cud_service_factor_list += [row[2]]

            # defult service factor of 2.17
            cud_service_factor = 2.17
            if row[2] is not None:
                cud_service_factor = row[2]
            cud_service_factor_list += [cud_service_factor]


    # pull HarvestForecastSeasonality_Facts

    hfsf_harvest_date_list = list()
    hfsf_facility_id_list = list()
    hfsf_facility_line_id_list = list()
    hfsf_crop_id_list = list()
    hfsf_expected_plant_sites_list = list()
    hfsf_avg_headweight_list = list()
    hfsf_loose_grams_per_plant_site_list = list()

    sql = """
    SELECT HarvestDate,
        GreenhouseID,
        GreenhouselineID,
        CropID,
        ExpectedPlantSites,
        AvgHeadweight,
        LooseGramsPerPlantSite
        FROM HarvestForecastSeasonality_Facts 
        WHERE IsActive = 1 
        ORDER BY HarvestDate, GreenhouselineID, CropID
    """
    cnxn_cursor.execute(sql) 
    row = cnxn_cursor.fetchone()

    hfsf_harvest_date_list += [row[0]]
    hfsf_facility_id_list+= [row[1]]
    hfsf_facility_line_id_list += [row[2]]
    hfsf_crop_id_list += [row[3]]
    hfsf_expected_plant_sites_list += [row[4]]
    hfsf_avg_headweight_list += [row[5]]
    hfsf_loose_grams_per_plant_site_list += [row[6]]

    while row is not None:
        row = cnxn_cursor.fetchone()
        if row is not None:
            hfsf_harvest_date_list += [row[0]]
            hfsf_facility_id_list+= [row[1]]
            hfsf_facility_line_id_list += [row[2]]
            hfsf_crop_id_list += [row[3]]
            hfsf_expected_plant_sites_list += [row[4]]
            hfsf_avg_headweight_list += [row[5]]
            hfsf_loose_grams_per_plant_site_list += [row[6]]


    #print('HarvestForecastSeasonality_Facts loaded')


    #  pull Products_Dim



    pd_product_id_list = list()
    pd_crop_id_list = list()
    pd_net_weight_grams_list = list()

    pd_is_whole_list = list()
    pd_lead_time_in_days_list = list()
    pd_production_priority_list = list()
    pd_shelf_life_guarantee_list = list()
    pd_total_shelf_life_list = list()
    pd_generic_item_number_list = list()
    pd_cases_per_pallet_list = list()
    pd_case_equivalent_multiplier_list = list()
    

    sql = """
    SELECT ProductID,
        CropID,
        NetWeight_Grams,
        IsWhole,
        LeadTimeInDays,
        ProductionPriority,
        ShelfLifeGuarantee,
        TotalShelfLife,
        GenericItemNumber,
        CasesPerPallet,
        CaseEquivalentMultiplier
        FROM Products_Dim
        WHERE GenericItemNumber IS NOT NULL
        AND ProductID NOT IN (121,134,139) --IDs allocated to errorneous entries: sub-assembly basil, temperature misc/freight/tempsensor, trial crop
        ORDER BY ProductID
    """

    cnxn_cursor.execute(sql)
    row = cnxn_cursor.fetchone()

    pd_product_id_list += [row[0]]

    pd_crop_id = 20
    if row[1] is not None:
        pd_crop_id = row[1]
    pd_crop_id_list += [pd_crop_id]

    pd_net_weight_grams = 127.57275 # defult to retail case
    if row[1] is not None and row[2] is not None:
        pd_net_weight_grams = row[2]
    pd_net_weight_grams_list += [pd_net_weight_grams]

    pd_is_whole = 0 # defult to loose leaf
    if row[1] is not None and row[3] is not None:
        pd_is_whole = row[3]
    pd_is_whole_list += [pd_is_whole]

    pd_lead_time_in_days = 1 # defult lead time to 1 day
    if row[1] is not None and row[4] is not None:
        pd_lead_time_in_days = row[4]
    pd_lead_time_in_days_list += [pd_lead_time_in_days]

    pd_production_priority = 6 # defult to out of scope for the allocations
    if row[1] is not None and row[5] is not None:
        pd_production_priority = row[5]
    pd_production_priority_list += [pd_production_priority]

    pd_shelf_life_guarantee = 30 # defult to thirty days
    if row[1] is not None and row[6] is not None:
        pd_shelf_life_guarantee = row[6]
    pd_shelf_life_guarantee_list += [pd_shelf_life_guarantee]

    pd_total_shelf_life = 365 # defult to one year
    if row[1] is not None and row[7] is not None:
        pd_total_shelf_life = row[7]
    pd_total_shelf_life_list += [pd_total_shelf_life]

    pd_generic_item_number = 'MISSING' # defult to out of scope for the allocations
    if row[1] is not None and row[8] is not None:
        pd_generic_item_number = row[8]
    pd_generic_item_number_list += [pd_generic_item_number]
    
    pd_cases_per_pallet = 160
    if row[9] is not None:
        pd_cases_per_pallet = row[9]
    if row[9] is None and row[0] == 143:
        pd_cases_per_pallet = 280
    pd_cases_per_pallet_list += [pd_cases_per_pallet]
    pd_case_equivalent_multiplier_list += [row[10]]



    while row is not None:  
        row = cnxn_cursor.fetchone()
        if row is not None:
            pd_product_id_list += [row[0]]

            pd_crop_id = 20
            if row[1] is not None:
                pd_crop_id = row[1]
            pd_crop_id_list += [pd_crop_id]

            pd_net_weight_grams = 127.57275 # defult to retail case
            if row[1] is not None and row[2] is not None:
                pd_net_weight_grams = row[2]
            pd_net_weight_grams_list += [pd_net_weight_grams]

            pd_is_whole = 0 # defult to loose leaf
            if row[1] is not None and row[3] is not None:
                pd_is_whole = row[3]
            pd_is_whole_list += [pd_is_whole]

            pd_lead_time_in_days = 1 # defult lead time to 1 day
            if row[1] is not None and row[4] is not None:
                pd_lead_time_in_days = row[4]
            pd_lead_time_in_days_list += [pd_lead_time_in_days]

            pd_production_priority = 6 # defult to out of scope for the allocations
            if row[1] is not None and row[5] is not None:
                pd_production_priority = row[5]
            pd_production_priority_list += [pd_production_priority]

            pd_shelf_life_guarantee = 1 # defult to out of scope for the allocations
            if row[1] is not None and row[6] is not None:
                pd_shelf_life_guarantee = row[6]
            pd_shelf_life_guarantee_list += [pd_shelf_life_guarantee]

            pd_total_shelf_life = 365 # defult to out of scope for the allocations
            if row[1] is not None and row[7] is not None:
                pd_total_shelf_life = row[7]
            pd_total_shelf_life_list += [pd_total_shelf_life]

            pd_generic_item_number = 'MISSING' # defult to out of scope for the allocations
            if row[1] is not None and row[8] is not None:
                pd_generic_item_number = row[8]
            pd_generic_item_number_list += [pd_generic_item_number]
    
            pd_cases_per_pallet = 160
            if row[9] is not None:
                pd_cases_per_pallet = row[9]
            if row[9] is None and row[0] == 143:
                pd_cases_per_pallet = 280
            pd_cases_per_pallet_list += [pd_cases_per_pallet]
            pd_case_equivalent_multiplier_list += [row[10]]




    # Inventory_Facts

    if_inventory_facility_name_list = list()
    if_product_id_list = list()
    if_enjoy_by_date_list = list()
    if_quantity_list = list()

    if_facility_id_list = list()
    if_facility_product_date_key_list = list()



    sql = """
    IF CONVERT(DATE,GETDATE()) = (SELECT DISTINCT CoolerInventoryDate FROM Inventory_Facts WHERE CurrentRecord = 1)
        -- if we can get the actual inventory count
        SELECT c.GreenhouseName, a.ProductID, a.EnjoyByDate, a.Quantity, a.CoolerInventoryDate
        FROM Inventory_Facts a,
        InventoryLocations_Dim b,
        Greenhouses_Dim c,
        Products_Dim d
        WHERE a.CurrentRecord = 1
        AND a.InventoryLocationID = b.InventoryLocationID
        AND b.GreenhouseID = c.GreenhouseID
        AND a.EnjoyByDate IS NOT NULL
        AND InventorySellOrHold = 1
        AND a.ProductID = d.ProductID
        AND (
            (a.CoolerInventoryDate > DATEADD(DAY, - d.TotalShelfLife, a.EnjoyByDate)
            AND d.ProductionPriority != 5)
            OR
            d.ProductionPriority = 5
            )
        ORDER BY b.InventoryFacilityName, a.ProductID, a.EnjoyByDate;
    ELSE
        -- if no count yet, use projected inventory count
        SELECT b.GreenhouseName, a.ProductID, a.EnjoybyDate, a.StartOfDayQty, a.InventoryDate
            FROM [hostname\DEV].[databasename].[dbo].[InventoryAllocation_Facts] a,
            Greenhouses_Dim b,
			Products_Dim c
            WHERE 
            a.IsActive = 1
            AND a.InventoryDate = CONVERT(DATE,GETDATE())
            AND a.InventoryGreenhouseID = b.GreenhouseID
			AND a.ProductID = c.ProductID
			AND (
            (a.InventoryDate > DATEADD(DAY, - c.TotalShelfLife, a.EnjoyByDate)
            AND c.ProductionPriority != 5)
            OR
            c.ProductionPriority = 5
            )
            ORDER BY a.InventoryAllocationID;
    """


    cnxn_cursor.execute(sql) 
    row = cnxn_cursor.fetchone()

    if row is not None:
        inv_facility_name = row[0].rstrip()    
        inv_facility_id = fd_facility_id_list[fd_location_name_list.index(str(row[0].rstrip()))]
        # consider one inventory per city
        if inv_facility_id in [1,2,9]:
            inv_facility_id = 3 # set NYC1, NYC2, and NYC4 to NYC3
            inv_facility_name = ['NYC3']
        if inv_facility_id == 4:
            inv_facility_id = 7 # set CHI1 to CHI2
            inv_facility_name = ['CHI2']

        if_inventory_facility_name_list += [inv_facility_name]
        if_product_id_list += [row[1]]
        if_enjoy_by_date_list += [row[2]]
        if_quantity_list += [row[3]]
        if_facility_id_list += [inv_facility_id]
        if_facility_product_date_key_list += [str(inv_facility_name) + '_' + str(row[1]) + '_' + str(row[2])]



    while row is not None:
        row = cnxn_cursor.fetchone()
        if row is not None:
            inv_facility_name = row[0].rstrip()    
            inv_facility_id = fd_facility_id_list[fd_location_name_list.index(str(row[0].rstrip()))]
            # consider one inventory per city
            if inv_facility_id in [1,2,9]:
                inv_facility_id = 3 # set NYC1, NYC2, and NYC4 to NYC3
                inv_facility_name = ['NYC3']
            if inv_facility_id == 4:
                inv_facility_id = 7 # set CHI1 to CHI2
                inv_facility_name = ['CHI2']

            if_inventory_facility_name_list += [inv_facility_name]
            if_product_id_list += [row[1]]
            if_enjoy_by_date_list += [row[2]]
            if_quantity_list += [row[3]]
            if_facility_id_list += [inv_facility_id]
            if_facility_product_date_key_list += [str(inv_facility_name) + '_' + str(row[1]) + '_' + str(row[2])]





    #print('Inventory_Facts loaded')



    #Calendars_Dim
    cald_date_day_list = list()
    cald_year_number_list = list()
    cald_week_of_year_list = list()
    cald_year_week_list = list()
    cald_year_week_dow_list = list()

    sql = """
    SELECT DateDay, YearNumber, WeekOfYear FROM Calendars_Dim
    """
    cnxn_cursor.execute(sql) 
    row = cnxn_cursor.fetchone()

    cald_date_day_list  += [row[0]]
    cald_year_number_list  += [row[1]]
    cald_week_of_year_list += [row[2]]
    cald_year_week_list  += [str(row[1]) + '_' + str(row[2])]
    cald_year_week_dow_list += [str(row[1]) + '_' + str(row[2]) + '_' + str(row[0].weekday())]


    while row is not None:
        row = cnxn_cursor.fetchone()
        if row is not None:
            cald_date_day_list  += [row[0]]
            cald_year_number_list  += [row[1]]
            cald_week_of_year_list += [row[2]]
            cald_year_week_list  += [str(row[1]) + '_' + str(row[2])]
            cald_year_week_dow_list += [str(row[1]) + '_' + str(row[2]) + '_' + str(row[0].weekday())]


    #print('Calendars_Dim loaded')


    cnxn.commit()
    cnxn_cursor.close()
    cnxn.close()



    # connect to database
    HOSTNAME = socket.gethostname()

    if HOSTNAME == 'hostname':
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server}; 
                                Server=127.0.0.1,1443;
                                Database=databasename;
                                trusted_connection=yes""" # use windows auth on DB01
    else:
        with open(os.path.join(sys.path[0], "config.yml"), 'r') as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.SafeLoader)

#         uid = 'sa'
#         pwd = cfg['databasename']['pwd'][:-3]
#         CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server};
#                                 Server=hostname\MSSQLSERVER1;
#                                 Database=databasename;
#                                 UID=%s;
#                                 PWD=%s;""" % (uid, pwd) # use config.yml on local machine
        uid = cfg['databasename']['uid']
        pwd = cfg['databasename']['pwd']
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server};
                                Server=127.0.0.1,1443;
                                Database=databasename;
                                UID=%s;
                                PWD=%s;""" % (uid, pwd) # use config.yml on local machine

    cnxn = pyodbc.connect(CONNECTIONSTRING)   
    cnxn_cursor = cnxn.cursor()


    #CustomerFillGoal_Dim
    cfgd_product_id_list = list()
    cfgd_customers_id_list = list()
    cfgd_year_number_list = list()
    cfgd_week_of_year_list = list()
    cfgd_customer_fill_goal_list = list()

    cfgd_year_week_list = list()
    cfgd_key_list = list()
    sql = """
    SELECT ProductID, CustomerID, YearNumber, WeekOfYear, CustomerFillGoal
    FROM CustomerFillGoal_Dim
    WHERE IsActive = 1
    """
    cnxn_cursor.execute(sql) 
    row = cnxn_cursor.fetchone()

    cfgd_product_id_list += [row[0]]
    cfgd_customers_id_list += [row[1]]
    cfgd_year_number_list += [row[2]]
    cfgd_week_of_year_list += [row[3]]
    cfgd_customer_fill_goal_list += [row[4]]
    cfgd_year_week_list += [str(row[2]) + '_' + str(row[3])]
    cfgd_key_list += [str(row[0]) + '_' + str(row[1]) + '_' + str(row[2]) + '_' + str(row[3])]

    while row is not None:
        row = cnxn_cursor.fetchone()
        if row is not None:
            cfgd_product_id_list += [row[0]]
            cfgd_customers_id_list += [row[1]]
            cfgd_year_number_list += [row[2]]
            cfgd_week_of_year_list += [row[3]]
            cfgd_customer_fill_goal_list += [row[4]]
            cfgd_year_week_list += [str(row[2]) + '_' + str(row[3])]
            cfgd_key_list += [str(row[0]) + '_' + str(row[1]) + '_' + str(row[2]) + '_' + str(row[3])]


    #print('CustomerFillGoal_Dim loaded')


    # TransferConstraints_Facts
    tcf_ship_greenhouse_id_list = list()
    tcf_arrival_greenhouse_id_list = list()
    tcf_ship_day_of_week_list = list()
    tcf_pack_lead_time_days_list = list()
    tcf_ship_duration_days_list = list()
    tcf_max_pallet_capacity_list = list()
    tcf_gfoods_transfer_list = list()
    
    sql = """
    SELECT ShipGreenhouseID, ArrivalGreenhouseID, ShipDayOfWeek, PackLeadTimeDays, ShipDurationDays, MaxPalletCapacity, GfoodsTransfer
    FROM TransferConstraints_Facts
    WHERE IsActive = 1
    """
    cnxn_cursor.execute(sql) 
    row = cnxn_cursor.fetchone()

    tcf_ship_greenhouse_id_list += [row[0]]
    tcf_arrival_greenhouse_id_list += [row[1]]
    tcf_ship_day_of_week_list += [row[2]]
    tcf_pack_lead_time_days_list += [row[3]]
    tcf_ship_duration_days_list += [row[4]]
    tcf_max_pallet_capacity_list += [row[5]]
    tcf_gfoods_transfer_list += [row[6]]
    while row is not None:
        row = cnxn_cursor.fetchone()
        if row is not None:
            tcf_ship_greenhouse_id_list += [row[0]]
            tcf_arrival_greenhouse_id_list += [row[1]]
            tcf_ship_day_of_week_list += [row[2]]
            tcf_pack_lead_time_days_list += [row[3]]
            tcf_ship_duration_days_list += [row[4]]
            tcf_max_pallet_capacity_list += [row[5]]
            tcf_gfoods_transfer_list += [row[6]]

            
    # RoutineTransfers_Facts
    rtf_ship_greenhouse_id_list = list()
    rtf_arrival_greenhouse_id_list = list()
    rtf_ship_day_of_week_list = list()
    rtf_pack_lead_time_days_list = list()
    rtf_ship_duration_days_list = list()
    rtf_max_pallet_capacity_list = list()
    rtf_gfoods_transfer_list = list()
    rtf_start_ship_period_dow_list = list()
    rtf_start_arrival_period_dow_list = list()
    rtf_end_arrival_period_dow_list = list()
    sql = """
    SELECT ShipGreenhouseID,
	ArrivalGreenhouseID,
	EndShipPeriodDayOfWeek,
	PackLeadTimeDays,
	TransitDurationDays,
	MaxPalletCapacity,
	GfoodsTransfer,
	StartShipPeriodDayOfWeek,
	StartArrivalPeriodDayOfWeek,
	EndArrivalPeriodDayOfWeek
    FROM RoutineTransfers_Facts
    WHERE IsActive = 1
    """
    cnxn_cursor.execute(sql) 
    row = cnxn_cursor.fetchone()

    rtf_ship_greenhouse_id_list += [row[0]]
    rtf_arrival_greenhouse_id_list += [row[1]]
    rtf_ship_day_of_week_list += [row[2]]
    rtf_pack_lead_time_days_list += [row[3]]
    rtf_ship_duration_days_list += [row[4]]
    rtf_max_pallet_capacity_list += [row[5]]
    rtf_gfoods_transfer_list += [row[6]]
    rtf_start_ship_period_dow_list += [row[7]]
    rtf_start_arrival_period_dow_list += [row[8]]
    rtf_end_arrival_period_dow_list += [row[9]]
    
    while row is not None:
        row = cnxn_cursor.fetchone()
        if row is not None:
            rtf_ship_greenhouse_id_list += [row[0]]
            rtf_arrival_greenhouse_id_list += [row[1]]
            rtf_ship_day_of_week_list += [row[2]]
            rtf_pack_lead_time_days_list += [row[3]]
            rtf_ship_duration_days_list += [row[4]]
            rtf_max_pallet_capacity_list += [row[5]]
            rtf_gfoods_transfer_list += [row[6]]
            rtf_start_ship_period_dow_list += [row[7]]
            rtf_start_arrival_period_dow_list += [row[8]]
            rtf_end_arrival_period_dow_list += [row[9]]


            
    # TransferSchedule_Facts
    # all transfers for inbound inventory forecast


    tsf_ship_date_list = list()
    tsf_arrival_date_list = list()
    tsf_ship_facility_id_list = list()
    tsf_arrival_facility_id_list = list()
    tsf_product_id_list = list()
    tsf_enjoy_by_date_list = list()
    tsf_transfer_qty_list = list()
    sql = """
    SELECT 
    ShipDate,
    ArrivalDate,
    ShipGreenhouseID,
    ArrivalGreenhouseID,
    ProductID,
    EnjoyByDate,
    TransferQty
    FROM PlannedTransfers_Facts WHERE IsActive = 1
    """
    cnxn_cursor.execute(sql) 
    row = cnxn_cursor.fetchone()
    if row is not None:
        tsf_ship_date_list += [row[0]]
        tsf_arrival_date_list += [row[1]]
        tsf_ship_facility_id_list += [row[2]]
        tsf_arrival_facility_id_list += [row[3]]
        tsf_product_id_list += [row[4]]
        tsf_enjoy_by_date_list += [row[5]]
        tsf_transfer_qty_list += [row[6]]
    while row is not None:
        row = cnxn_cursor.fetchone()
        if row is not None:
            tsf_ship_date_list += [row[0]]
            tsf_arrival_date_list += [row[1]]
            tsf_ship_facility_id_list += [row[2]]
            tsf_arrival_facility_id_list += [row[3]]
            tsf_product_id_list += [row[4]]
            tsf_enjoy_by_date_list += [row[5]]
            tsf_transfer_qty_list += [row[6]]
            
    
    # TransferSchedule_Facts
    # transfers for inventory alocation
    # all non-retail and retail that has pack date before the current date


    inv_tsf_ship_date_list = list()
    inv_tsf_arrival_date_list = list()
    inv_tsf_ship_facility_id_list = list()
    inv_tsf_arrival_facility_id_list = list()
    inv_tsf_product_id_list = list()
    inv_tsf_enjoy_by_date_list = list()
    inv_tsf_transfer_qty_list = list()
    sql = """
    SELECT 
    ShipDate,
    ArrivalDate,
    ShipGreenhouseID,
    ArrivalGreenhouseID,
    a.ProductID,
    EnjoyByDate,
    TransferQty
    FROM PlannedTransfers_Facts a,
	Products_Dim b
	WHERE a.IsActive = 1
	AND a.ProductID = b.ProductID
	AND
	((DATEADD(DAY, -b.TotalShelfLife,a.EnjoyByDate) < GETDATE()
		AND b.ProductionPriority = 2)
	OR
	(b.ProductionPriority != 2))
    """
    cnxn_cursor.execute(sql) 
    row = cnxn_cursor.fetchone()
    if row is not None:
        inv_tsf_ship_date_list += [row[0]]
        inv_tsf_arrival_date_list += [row[1]]
        inv_tsf_ship_facility_id_list += [row[2]]
        inv_tsf_arrival_facility_id_list += [row[3]]
        inv_tsf_product_id_list += [row[4]]
        inv_tsf_enjoy_by_date_list += [row[5]]
        inv_tsf_transfer_qty_list += [row[6]]
    while row is not None:
        row = cnxn_cursor.fetchone()
        if row is not None:
            inv_tsf_ship_date_list += [row[0]]
            inv_tsf_arrival_date_list += [row[1]]
            inv_tsf_ship_facility_id_list += [row[2]]
            inv_tsf_arrival_facility_id_list += [row[3]]
            inv_tsf_product_id_list += [row[4]]
            inv_tsf_enjoy_by_date_list += [row[5]]
            inv_tsf_transfer_qty_list += [row[6]]    
            
            
    # TransferSchedule_Facts
    # transfers for harvest allocation
    # all retail that has pack date after or matching the current date

    har_tsf_ship_date_list = list()
    har_tsf_arrival_date_list = list()
    har_tsf_ship_facility_id_list = list()
    har_tsf_arrival_facility_id_list = list()
    har_tsf_product_id_list = list()
    har_tsf_enjoy_by_date_list = list()
    har_tsf_transfer_qty_list = list()
    sql = """
	SELECT 
    ShipDate,
    ArrivalDate,
    ShipGreenhouseID,
    ArrivalGreenhouseID,
    a.ProductID,
    EnjoyByDate,
    TransferQty
    FROM PlannedTransfers_Facts a,
	Products_Dim b
	WHERE a.IsActive = 1
	AND a.ProductID = b.ProductID
	AND DATEADD(DAY, -b.TotalShelfLife,a.EnjoyByDate) >= GETDATE()
	AND b.ProductionPriority = 2
    """
    cnxn_cursor.execute(sql) 
    row = cnxn_cursor.fetchone()
    if row is not None:
        har_tsf_ship_date_list += [row[0]]
        har_tsf_arrival_date_list += [row[1]]
        har_tsf_ship_facility_id_list += [row[2]]
        har_tsf_arrival_facility_id_list += [row[3]]
        har_tsf_product_id_list += [row[4]]
        har_tsf_enjoy_by_date_list += [row[5]]
        har_tsf_transfer_qty_list += [row[6]]
    while row is not None:
        row = cnxn_cursor.fetchone()
        if row is not None:
            har_tsf_ship_date_list += [row[0]]
            har_tsf_arrival_date_list += [row[1]]
            har_tsf_ship_facility_id_list += [row[2]]
            har_tsf_arrival_facility_id_list += [row[3]]
            har_tsf_product_id_list += [row[4]]
            har_tsf_enjoy_by_date_list += [row[5]]
            har_tsf_transfer_qty_list += [row[6]]
            
    
    cnxn.commit()
    cnxn_cursor.close()
    cnxn.close()



    # connect to database
    HOSTNAME = socket.gethostname()

    if HOSTNAME == 'hostname':
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server}; 
                                Server=127.0.0.1,1443;
                                Database=databasename;
                                trusted_connection=yes""" # use windows auth on DB01
    else:
        with open(os.path.join(sys.path[0], "config.yml"), 'r') as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.SafeLoader)

        uid = 'sa'
        pwd = cfg['databasename']['pwd'][:-3]
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server};
                                Server=hostname\MSSQLSERVER1;
                                Database=databasename;
                                UID=%s;
                                PWD=%s;""" % (uid, pwd) # use config.yml on local machine
#         uid = cfg['databasename']['uid']
#         pwd = cfg['databasename']['pwd']
#         CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server};
#                                 Server=127.0.0.1,1443;
#                                 Database=databasename;
#                                 UID=%s;
#                                 PWD=%s;""" % (uid, pwd) # use config.yml on local machine

    cnxn = pyodbc.connect(CONNECTIONSTRING)   
    cnxn_cursor = cnxn.cursor()
    
    
    
    df_demand_date_list = list()
    df_demand_allocation_date_list  = list()
    df_facility_id_list  = list()
    df_product_id_list  = list()
    df_customer_id_list  = list()
    df_fill_goal_list  = list()
    df_safety_stock_qty_list  = list()
    df_rollover_qty_list  = list()
    df_demand_qty_list  = list()

    sql = """
    SELECT DemandDate,
    DemandAllocationDate,
    DemandGreenhouseID,
    ProductID,
    CustomerID,
    CustomerFillGoal,
    SafetyStockQty,
    RolloverQty,
    DemandQty
    FROM CustomerDemandForecast_Facts
    WHERE IsActive = 1
    ORDER BY DemandDate, DemandGreenhouseID, ProductID, CustomerFillGoal DESC, DemandQty DESC
    """
    cnxn_cursor.execute(sql) 
    row = cnxn_cursor.fetchone()

    df_demand_date_list += [row[0]]
    df_demand_allocation_date_list += [row[1]]
    df_facility_id_list += [row[2]]
    df_product_id_list += [row[3]]
    df_customer_id_list += [row[4]]
    df_fill_goal_list += [row[5]]
    df_safety_stock_qty_list += [row[6]]
    df_rollover_qty_list += [row[7]]
    df_demand_qty_list += [row[8]]

    while row is not None:
        row = cnxn_cursor.fetchone()
        if row is not None:
            df_demand_date_list += [row[0]]
            df_demand_allocation_date_list += [row[1]]
            df_facility_id_list += [row[2]]
            df_product_id_list += [row[3]]
            df_customer_id_list += [row[4]]
            df_fill_goal_list += [row[5]]
            df_safety_stock_qty_list += [row[6]]
            df_rollover_qty_list += [row[7]]
            df_demand_qty_list += [row[8]]
            
    print('Data loaded and ready')
    print("--- %s seconds---" % (time.time() - start_time))

    # Initialize inventory




    ################################################
    # clean inventory

    # date today
    date_today = DT.datetime.now().date()

    # Clean inventory
    cif_facility_id_list = list()
    cif_enjoy_by_date_list = list()
    cif_product_id_list = list()
    cif_quantity_list = list()
    cif_facility_product_date_key_list = list()

    for if_idx in range(len(if_facility_id_list)):
        if if_idx in range(len(if_facility_id_list)):

            check_facility_id = if_facility_id_list[if_idx]
            check_enjoy_by_date = if_enjoy_by_date_list[if_idx]
            check_product_id = if_product_id_list[if_idx]
            check_quantity = if_quantity_list[if_idx]
            check_facility_product_date_key = if_facility_product_date_key_list[if_idx]


            if check_quantity > 0:
                # the entry is valid if Quantity is greater than 0
                cif_facility_id_list += [check_facility_id]
                cif_enjoy_by_date_list += [check_enjoy_by_date]
                cif_product_id_list += [check_product_id]
                cif_quantity_list += [check_quantity]

                cif_facility_product_date_key_list += [check_facility_product_date_key]

            else:
                # handle negative entries
                outstanding_quantity = check_quantity

                while outstanding_quantity < 0:

                    # check before the entry
                    if check_facility_product_date_key in cif_facility_product_date_key_list:
                        previous_idx = cif_facility_product_date_key_list.index(check_facility_product_date_key)
                        old_val = cif_quantity_list[previous_idx]
                        new_val = old_val + outstanding_quantity # subtract outstanding quantity from existing entry
                        if new_val == 0:
                            # remove the record from the clean inventory
                            del cif_facility_id_list[previous_idx]
                            del cif_enjoy_by_date_list[previous_idx]
                            del cif_product_id_list[previous_idx]
                            del cif_quantity_list[previous_idx]
                            del cif_facility_product_date_key_list[previous_idx]
                            outstanding_quantity = 0
                        if new_val > 0:
                            # update the record in cif
                            cif_quantity_list[previous_idx] = new_val
                            outstanding_quantity = 0
                        if new_val < 0:   
                            # remove the record from the clean inventory continue searching
                            del cif_facility_id_list[previous_idx]
                            del cif_enjoy_by_date_list[previous_idx]
                            del cif_product_id_list[previous_idx]
                            del cif_quantity_list[previous_idx]
                            del cif_facility_product_date_key_list[previous_idx]                               
                            outstanding_quantity = new_val

                    # check after the entry
                    post_facility_product_date_key_list = if_facility_product_date_key_list[if_idx+1:]
                    if check_facility_product_date_key in post_facility_product_date_key_list:
                        post_idx = post_facility_product_date_key_list.index(check_facility_product_date_key)
                        post_idx_if = post_idx + if_idx + 1
                        old_val = if_quantity_list[post_idx_if]
                        new_val = old_val + outstanding_quantity
                        if new_val == 0:
                            # remove the record from the inventory facts lists
                            del if_facility_id_list[post_idx_if]
                            del if_product_id_list[post_idx_if]
                            del if_enjoy_by_date_list[post_idx_if]
                            del if_quantity_list[post_idx_if]
                            del if_facility_product_date_key_list[post_idx_if]   
                            outstanding_quantity = 0
                        if new_val > 0:
                            # update the record in inventory facts qty list
                            if_quantity_list[post_idx_if] = new_val
                            outstanding_quantity = 0
                        if new_val < 0:             
                            # remove the record from the inventory facts lists and continue searching
                            del if_facility_id_list[post_idx_if]
                            del if_product_id_list[post_idx_if]
                            del if_enjoy_by_date_list[post_idx_if]
                            del if_quantity_list[post_idx_if]
                            del if_facility_product_date_key_list[post_idx_if] 
                            outstanding_quantity = new_val
                    else:
                        print('Outstanding negative inventory: ' + str(-outstanding_quantity) + ' for ' + check_facility_product_date_key)
                        outstanding_quantity = 0


    ##########


    # connect to database
    HOSTNAME = socket.gethostname()

    if HOSTNAME == 'hostname':
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server}; 
                                Server=127.0.0.1,1443;
                                Database=databasename;
                                trusted_connection=yes""" # use windows auth on DB01
    else:
        with open(os.path.join(sys.path[0], "config.yml"), 'r') as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.SafeLoader)
    #    uid = cfg['databasename']['uid']
        uid = 'sa'
        pwd = cfg['databasename']['pwd'][:-3]
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server};
                                Server=hostname\MSSQLSERVER1;
                                Database=databasename;
                                UID=%s;
                                PWD=%s;""" % (uid, pwd) # use config.yml on local machine

    #     CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server};
    #                             Server=127.0.0.1,1443;
    #                             Database=databasename;
    #                             UID=%s;
    #                             PWD=%s;""" % (uid, pwd) # use config.yml on local machine

    cnxn = pyodbc.connect(CONNECTIONSTRING)   
    cnxn_cursor = cnxn.cursor()

    # change data capture

    sql = """
    UPDATE StopSell_Facts
    SET ToDate = GETDATE(), IsActive = 0
    WHERE IsActive = 1;
    """
    cnxn_cursor.execute(sql)



    sql = """
    SELECT MAX(StopSellID) FROM StopSell_Facts
    """
    cnxn_cursor.execute(sql)
    row = cnxn_cursor.fetchone()
    max_old_id = row[0]
    if max_old_id is None:
        max_old_id = 0
    shelf_life_guarantee_id = max_old_id + 1

    # new entries


    load_date = DT.datetime.now()
    to_date = DT.datetime.strptime('2099-12-31 00:00:00.000000', '%Y-%m-%d %H:%M:%S.%f')
    is_active = 1



    sql = """
    INSERT INTO StopSell_Facts
    VALUES (?,?,?,?,?,?,?,?,?,?);
    """ 


    # stop sell
    ssf_facility_id_list = list()
    ssf_enjoy_by_date_list = list()
    ssf_product_id_list = list()
    ssf_quantity_list = list()

    # Clean clean inventory (ccif) after removing stop sell products
    ccif_facility_id_list = list()
    ccif_enjoy_by_date_list = list()
    ccif_product_id_list = list()
    ccif_quantity_list = list()
    ccif_facility_product_date_key_list = list()


    for cif_idx in range(len(cif_facility_id_list)):

        check_facility_id = cif_facility_id_list[cif_idx]
        check_enjoy_by_date = cif_enjoy_by_date_list[cif_idx]
        check_product_id = cif_product_id_list[cif_idx]
        check_quantity = cif_quantity_list[cif_idx]
        check_facility_product_date_key = cif_facility_product_date_key_list[cif_idx]

        check_shelf_life_guarantee_days = pd_shelf_life_guarantee_list[pd_product_id_list.index(check_product_id)]
        shelf_life_guarantee_date = check_enjoy_by_date - DT.timedelta(days = check_shelf_life_guarantee_days)
        if shelf_life_guarantee_date >= date_today:
            ccif_facility_id_list += [check_facility_id]
            ccif_enjoy_by_date_list += [check_enjoy_by_date]
            ccif_product_id_list += [check_product_id]
            ccif_quantity_list += [check_quantity]
            ccif_facility_product_date_key_list += [check_facility_product_date_key]
        else:
            # write to StopSell_Facts if we can no longer sell the inventory item
            tuple_to_write = (shelf_life_guarantee_id, date_today, check_facility_id, check_product_id, check_enjoy_by_date, check_quantity,load_date,to_date,is_active,check_facility_id)
            cnxn_cursor.execute(sql, tuple_to_write)
            shelf_life_guarantee_id += 1

    cnxn.commit()
    cnxn_cursor.close()
    cnxn.close()

    #print(date_today, 'StopSell_Facts done')


    # aggregate inventory lists by facility_product_date key
    # Compressed clean clean inventory facts (cccif) after aggregating 
    cccif_facility_id_list = list()
    cccif_enjoy_by_date_list = list()
    cccif_product_id_list = list()
    cccif_quantity_list = list()
    cccif_facility_product_date_key_list = list()

    for ccif_idx in range(len(ccif_facility_id_list)):
        # check entry in clean clean inventory facts
        check_facility_id = ccif_facility_id_list[ccif_idx]
        check_enjoy_by_date = ccif_enjoy_by_date_list[ccif_idx]
        check_product_id = ccif_product_id_list[ccif_idx]
        check_quantity = ccif_quantity_list[ccif_idx]
        check_facility_product_date_key = ccif_facility_product_date_key_list[ccif_idx]


        if check_facility_product_date_key in cccif_facility_product_date_key_list:
            # add check entry to the existing entry in compressed inventory lists
            # index in new list
            cccif_idx = cccif_facility_product_date_key_list.index(check_facility_product_date_key)
            cccif_quantity_list[cccif_idx] += check_quantity

        if check_facility_product_date_key not in cccif_facility_product_date_key_list:
            # add check entry to new entry in compressed inventory lists
            cccif_facility_id_list += [check_facility_id]
            cccif_enjoy_by_date_list += [check_enjoy_by_date]
            cccif_product_id_list+= [check_product_id]
            cccif_quantity_list += [check_quantity]
            cccif_facility_product_date_key_list += [check_facility_product_date_key]



    # inbound transfers
    for tsf_idx in range(len(tsf_ship_date_list)):
        tsf_arrival_date = tsf_arrival_date_list[tsf_idx]
        if tsf_arrival_date == date_today:
            # add inbound transfer to inventory of arrival facility
            tsf_arrival_facility_id = tsf_arrival_facility_id_list[tsf_idx]
            tsf_arrival_location_name = fd_location_name_list[fd_facility_id_list.index(tsf_arrival_facility_id)]
            tsf_product_id = tsf_product_id_list[tsf_idx]
            tsf_enjoy_by_date = tsf_enjoy_by_date_list[tsf_idx]
            tsf_transfer_qty = tsf_transfer_qty_list[tsf_idx]

            tsf_facility_product_date_key = tsf_arrival_location_name + '_' + str(tsf_product_id) + '_' + str(tsf_enjoy_by_date)

            if tsf_facility_product_date_key in cccif_facility_product_date_key_list:
                cccif_idx = cccif_facility_product_date_key_list.index(tsf_facility_product_date_key)
                cccif_quantity_list[cccif_idx] += tsf_transfer_qty
            if tsf_facility_product_date_key not in cccif_facility_product_date_key_list:
                cccif_facility_id_list += [tsf_arrival_facility_id]
                cccif_enjoy_by_date_list += [tsf_enjoy_by_date]
                cccif_product_id_list+= [tsf_product_id]
                cccif_quantity_list += [tsf_transfer_qty]
                cccif_facility_product_date_key_list += [tsf_facility_product_date_key]

    #print(date_today, 'inbound transfers done')


    print('Inventory initialized')
    print("--- %s seconds---" % (time.time() - start_time))
    ##################################################################################

    # connect to database
    HOSTNAME = socket.gethostname()

    if HOSTNAME == 'hostname':
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server}; 
                                Server=127.0.0.1,1443;
                                Database=databasename;
                                trusted_connection=yes""" # use windows auth on DB01
    else:
        with open(os.path.join(sys.path[0], "config.yml"), 'r') as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.SafeLoader)
    #    uid = cfg['databasename']['uid']
        uid = 'sa'
        pwd = cfg['databasename']['pwd'][:-3]
        CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server};
                                Server=hostname\MSSQLSERVER1;
                                Database=databasename;
                                UID=%s;
                                PWD=%s;""" % (uid, pwd) # use config.yml on local machine

    #     CONNECTIONSTRING = """Driver={ODBC Driver 17 for SQL Server};
    #                             Server=127.0.0.1,1443;
    #                             Database=databasename;
    #                             UID=%s;
    #                             PWD=%s;""" % (uid, pwd) # use config.yml on local machine

    cnxn = pyodbc.connect(CONNECTIONSTRING)   
    cnxn_cursor = cnxn.cursor()



    #########################################################################
    #### CHANGE DATA CAPTURE

    base_name = 'CustomerInventoryAllocation' # define base name for SQL

    sql = """
    SELECT MAX("""+ base_name + """ID) FROM """+ base_name + """_Facts
    """
    cnxn_cursor.execute(sql)
    row = cnxn_cursor.fetchone()

    if row is not None:    

        sql = """
        UPDATE """+ base_name + """_Facts
        SET ToDate = GETDATE(), IsActive = 0
        WHERE IsActive = 1;
        """
        cnxn_cursor.execute(sql)

    #########################################################################
    #### CHANGE DATA CAPTURE

    base_name = 'CustomerHarvestAllocation' # define base name for SQL

    ###
    harvest_allocation_id = 1 # define variable name for Python

    sql = """
    SELECT MAX("""+ base_name + """ID) FROM """+ base_name + """_Facts
    """
    cnxn_cursor.execute(sql)
    row = cnxn_cursor.fetchone()

    if row is not None:    

        max_old_id = row[0]

        if max_old_id == None:
            max_old_id = 0
        harvest_allocation_id = max_old_id + 1 # define variable name for Python

        sql = """
        UPDATE """+ base_name + """_Facts
        SET ToDate = GETDATE(), IsActive = 0
        WHERE IsActive = 1;
        """
        cnxn_cursor.execute(sql)

    #########################################################################
    #### CHANGE DATA CAPTURE

    base_name = 'HarvestUnallocated' # define base name for SQL

    harvest_unallocated_id = 1 # define variable name for Python

    sql = """
    SELECT MAX("""+ base_name + """ID) FROM """+ base_name + """_Facts
    """
    cnxn_cursor.execute(sql)
    row = cnxn_cursor.fetchone()

    if row is not None:    

        max_old_id = row[0]

        if max_old_id == None:
            max_old_id = 0
        harvest_unallocated_id = max_old_id + 1 # define variable name for Python

        sql = """
        UPDATE """+ base_name + """_Facts
        SET ToDate = GETDATE(), IsActive = 0
        WHERE IsActive = 1;
        """
        cnxn_cursor.execute(sql)


    #########################################################################
    #### CHANGE DATA CAPTURE

    #####
    base_name = 'CustomerShortDemand'
    ###
    short_demand_id = 1 # define variable name for Python

    sql = """
    SELECT MAX("""+ base_name + """ID) FROM """+ base_name + """_Facts
    """
    cnxn_cursor.execute(sql)
    row = cnxn_cursor.fetchone()

    if row is not None:    

        max_old_id = row[0]
        if max_old_id is None:
            max_old_id = 0
        short_demand_id = max_old_id + 1 # define variable name for Python

        sql = """
        UPDATE """+ base_name + """_Facts
        SET ToDate = GETDATE(), IsActive = 0
        WHERE IsActive = 1;
        """
        cnxn_cursor.execute(sql)

    #########################################################################
    #### CHANGE DATA CAPTURE

    base_name = 'CustomerInventoryAllocationPending' # define base name for SQL

    sql = """
    SELECT MAX("""+ base_name + """ID) FROM """+ base_name + """_Facts
    """
    cnxn_cursor.execute(sql)
    row = cnxn_cursor.fetchone()

    if row is not None:    

        sql = """
        UPDATE """+ base_name + """_Facts
        SET ToDate = GETDATE(), IsActive = 0
        WHERE IsActive = 1;
        """
        cnxn_cursor.execute(sql)

    #########################################################################
    #### CHANGE DATA CAPTURE

    base_name = 'CustomerHarvestAllocationPending' # define base name for SQL

    ###
    harvest_allocation_id = 1 # define variable name for Python

    sql = """
    SELECT MAX("""+ base_name + """ID) FROM """+ base_name + """_Facts
    """
    cnxn_cursor.execute(sql)
    row = cnxn_cursor.fetchone()

    if row is not None:    

        max_old_id = row[0]

        if max_old_id == None:
            max_old_id = 0
        harvest_allocation_id = max_old_id + 1 # define variable name for Python

        sql = """
        UPDATE """+ base_name + """_Facts
        SET ToDate = GETDATE(), IsActive = 0
        WHERE IsActive = 1;
        """
        cnxn_cursor.execute(sql)

    #########################################################################
    #### CHANGE DATA CAPTURE

    base_name = 'HarvestUnallocatedPending' # define base name for SQL

    harvest_unallocated_id = 1 # define variable name for Python

    sql = """
    SELECT MAX("""+ base_name + """ID) FROM """+ base_name + """_Facts
    """
    cnxn_cursor.execute(sql)
    row = cnxn_cursor.fetchone()

    if row is not None:    

        max_old_id = row[0]

        if max_old_id == None:
            max_old_id = 0
        harvest_unallocated_id = max_old_id + 1 # define variable name for Python

        sql = """
        UPDATE """+ base_name + """_Facts
        SET ToDate = GETDATE(), IsActive = 0
        WHERE IsActive = 1;
        """
        cnxn_cursor.execute(sql)


    #########################################################################
    #### CHANGE DATA CAPTURE

    #####
    base_name = 'CustomerShortDemandPending'
    ###
    short_demand_id = 1 # define variable name for Python

    sql = """
    SELECT MAX("""+ base_name + """ID) FROM """+ base_name + """_Facts
    """
    cnxn_cursor.execute(sql)
    row = cnxn_cursor.fetchone()

    if row is not None:    

        max_old_id = row[0]
        if max_old_id is None:
            max_old_id = 0
        short_demand_id = max_old_id + 1 # define variable name for Python

        sql = """
        UPDATE """+ base_name + """_Facts
        SET ToDate = GETDATE(), IsActive = 0
        WHERE IsActive = 1;
        """
        cnxn_cursor.execute(sql)
        
    #########################################################################
    #### CHANGE DATA CAPTURE

    #####
    base_name = 'CalculatedTransfers'
    ###
    short_demand_id = 1 # define variable name for Python

    sql = """
    SELECT MAX("""+ base_name + """ID) FROM """+ base_name + """_Facts
    """
    cnxn_cursor.execute(sql)
    row = cnxn_cursor.fetchone()

    if row is not None:    

        max_old_id = row[0]
        if max_old_id is None:
            max_old_id = 0
        short_demand_id = max_old_id + 1 # define variable name for Python

        sql = """
        UPDATE """+ base_name + """_Facts
        SET ToDate = GETDATE(), IsActive = 0
        WHERE IsActive = 1;
        """
        cnxn_cursor.execute(sql)


    cnxn.commit()
    cnxn_cursor.close()
    cnxn.close()

    ######################################################################################
    # these lists will track the delta of harvest lists through the allocation process

    # allocation tracking: crop level across all time and tier
    allocated_date_crop_facility_key_list = list()
    allocated_plant_sites_list = list()
    complete_crop_allocation_key_list = list()
    allocated_starting_ps_list = list()

    # allocation_tracking: product level
    allocated_date_product_facility_key_list = list()
    allocated_gpps_list = list()
    allocated_qty_list = list()
    allocated_product_plant_sites_list = list()
    complete_product_allocation_key_list = list()

    # allocation tracking: customer level
    complete_customer_allocation_key_list = list()

    # initialize harvest
    harvest_in_LoL = [hfsf_harvest_date_list,
                      hfsf_facility_id_list,
                      hfsf_facility_line_id_list,
                      hfsf_crop_id_list,
                      hfsf_expected_plant_sites_list,
                      hfsf_avg_headweight_list,
                      hfsf_loose_grams_per_plant_site_list]


    # initialize demand
    demand_in_LoL = [df_demand_date_list,
                     df_demand_allocation_date_list,
                     df_facility_id_list,
                     df_product_id_list,
                     df_customer_id_list,
                     df_demand_qty_list,
                     df_rollover_qty_list,
                     df_safety_stock_qty_list]

    # initialize inventory
    starting_inventory_in = [cccif_facility_id_list,
                            cccif_product_id_list,
                            cccif_enjoy_by_date_list,
                            cccif_quantity_list]

    products_LoL = [pd_product_id_list,
                    pd_shelf_life_guarantee_list,
                    pd_crop_id_list,
                    pd_net_weight_grams_list,
                    pd_is_whole_list,
                    pd_total_shelf_life_list,
                    pd_production_priority_list,
                    pd_case_equivalent_multiplier_list,
                    pd_cases_per_pallet_list]

    # initialize allocation tracking

    #initialize lists for HarvestUnallocated_Facts


    allocated_crops_in_LoL =  [allocated_date_crop_facility_key_list, allocated_starting_ps_list, allocated_plant_sites_list, complete_crop_allocation_key_list]

    # initialize transfers
    transfers_LoL = [tsf_ship_date_list,
                     tsf_arrival_date_list,
                     tsf_ship_facility_id_list,
                     tsf_arrival_facility_id_list,
                     tsf_product_id_list,
                     tsf_enjoy_by_date_list,
                     tsf_transfer_qty_list]
 
    inv_transfers_LoL = [inv_tsf_ship_date_list,
                     inv_tsf_arrival_date_list,
                     inv_tsf_ship_facility_id_list,
                     inv_tsf_arrival_facility_id_list,
                     inv_tsf_product_id_list,
                     inv_tsf_enjoy_by_date_list,
                     inv_tsf_transfer_qty_list]

    har_transfers_LoL = [har_tsf_ship_date_list,
                     har_tsf_arrival_date_list,
                     har_tsf_ship_facility_id_list,
                     har_tsf_arrival_facility_id_list,
                     har_tsf_product_id_list,
                     har_tsf_enjoy_by_date_list,
                     har_tsf_transfer_qty_list]



    # initialize facilities
    facilities_LoL = [fd_facility_id_list, fd_city_short_code_list]

    # initialize lists for short demand
    new_sdf_demand_date_list = list()
    new_sdf_demand_allocation_date_list = list()
    new_sdf_demand_facility_id_list = list()
    new_sdf_product_id_list = list()
    new_sdf_customer_id_list = list()
    new_sdf_short_demand_qty_list = list()
    new_sdf_production_priority_list = list()

    sdf_idx_to_skip_list = list()


    # initialize distinct customer tiers (fill goal %)
    distinct_fill_goal_list = list()
    for fg_idx in range(len(cfgd_customer_fill_goal_list)):
        check_fill_goal =  cfgd_customer_fill_goal_list[fg_idx]
        if check_fill_goal not in distinct_fill_goal_list:
            distinct_fill_goal_list.append(check_fill_goal)

    sorted_distinct_fill_goal_list = list(np.sort(np.array(distinct_fill_goal_list))[::-1])


    # initialize distinct demand allocation dates from df_demand_date_list
    distinct_demand_allocation_date_list = list()
    for demand_allocation_date_idx in range(len(df_demand_allocation_date_list)):
        check_demand_allocation_date =  df_demand_allocation_date_list[demand_allocation_date_idx]
        # check if exists in distinct_demand_allocation_date_list
        if check_demand_allocation_date not in distinct_demand_allocation_date_list:
            distinct_demand_allocation_date_list.append(check_demand_allocation_date)

    distinct_demand_allocation_date_list = list(np.sort(distinct_demand_allocation_date_list))

#     if debug_status == 1:
#         distinct_demand_allocation_date_list = distinct_demand_allocation_date_list[0:5]

    # initialize first date 
    demand_allocation_date = distinct_demand_allocation_date_list[0] 


    # main loop
    # 1. customer tier
    # 2. time
    # order for each tier-timestep combination
    #    a. Inventory Rollover
    #    b. Inventory to Customer Allocation
    #    c. Harvest to Customer Allocation
    #    d. Prior Day Harvest to Customer Allocation (to do)





    tier_count = 0
    final_tier = (len(sorted_distinct_fill_goal_list) * 2) - 1
    # first pass allocations up to fill goal %
    # 1. customer tier
    for fill_goal in sorted_distinct_fill_goal_list:
        # customer tier indices for demand
        tier_indices = [i for i, x in enumerate(df_fill_goal_list) if x == fill_goal]


        ####
        tier_count += 1
        print('Tier ', tier_count, 'fill goal:', fill_goal, "- %s seconds-" % (time.time() - start_time))
        # 2. time - loop dates starting from the next demand allocation date
        for demand_allocation_date_idx in range(len(distinct_demand_allocation_date_list)):

            # set last allocation date
            last_allocation_date = demand_allocation_date
            # set demand allocation date
            demand_allocation_date = distinct_demand_allocation_date_list[demand_allocation_date_idx]

            time_indices = [i for i, x in enumerate(df_demand_allocation_date_list) if x == demand_allocation_date]
            tier_time_indices = list(set(tier_indices) & set(time_indices))


            # create list of list for customer tier and time demand

            if demand_allocation_date != distinct_demand_allocation_date_list[0]:
                last_tier_time_demand_in_LoL = tier_time_demand_in_LoL


            tier_time_demand_in_LoL = [[demand_in_LoL[0][idx] for idx in tier_time_indices],
                                        [demand_in_LoL[1][idx] for idx in tier_time_indices],
                                        [demand_in_LoL[2][idx] for idx in tier_time_indices],
                                        [demand_in_LoL[3][idx] for idx in tier_time_indices],
                                        [demand_in_LoL[4][idx] for idx in tier_time_indices],
                                        [int(round((demand_in_LoL[5][idx] - demand_in_LoL[6][idx] - demand_in_LoL[7][idx]) * fill_goal)) + int(round(demand_in_LoL[6][idx]*fill_goal)) + int(round(demand_in_LoL[7][idx]*fill_goal)) for idx in tier_time_indices],
                                        [int(round(demand_in_LoL[6][idx]*fill_goal)) for idx in tier_time_indices],
                                        [int(round(demand_in_LoL[7][idx]*fill_goal)) for idx in tier_time_indices]
                                      ]

            # inventory for Day 1 Tier 1
            if demand_allocation_date == distinct_demand_allocation_date_list[0] and tier_count == 1:
                # Compressed clean clean inventory facts (cccif) after aggregating
                inventory_out_LoL = starting_inventory_in


            # inventory rollover Tier 2+ Day 1 or Day 2+
            if (demand_allocation_date == distinct_demand_allocation_date_list[0] and tier_count > 1) or demand_allocation_date != distinct_demand_allocation_date_list[0]:

                inventory_in_date = last_allocation_date    
                inventory_in_LoL = inventoryRollover(inventory_in_date,products_LoL, demand_allocation_date)

                # inventory rollover from smooth quantities from last allocation date

                inventory_in_LoL = smoothRollover(last_allocation_date, inventory_in_LoL, roll_harvest_LoL, products_LoL, demand_allocation_date)

                # stop sell and add transfers in
                (inventory_out_LoL, shelf_life_guarantee_out_LoL) = inventoryForecast(demand_allocation_date, inventory_in_LoL, products_LoL, transfers_LoL, tier_count)

                allocated_crops_in_LoL = readAllocated()



            # customerInventoryAllocation
            (inventory_allocation_out_LoL, inventory_demand_out_LoL) = customerInventoryAllocation(demand_allocation_date, inventory_out_LoL, tier_time_demand_in_LoL, facilities_LoL, inv_transfers_LoL,tier_count)

            # writeCustomerInventoryAllocation
            inventory_allocation_str = writeCustomerInventoryAllocation(demand_allocation_date,inventory_allocation_out_LoL, tier_count)
            #print(inventory_allocation_str)

            #customerHarvestAllocation
            (harvest_allocation_out_LoL,allocated_crops_out_LoL, short_demand_out_LoL) = customerHarvestAllocation(demand_allocation_date, harvest_in_LoL, inventory_demand_out_LoL, facilities_LoL, allocated_crops_in_LoL, har_transfers_LoL, products_LoL, inventory_allocation_out_LoL, tier_count)
            #print(len(short_demand_out_LoL[0]))
            

            # create list of list for roll harvest
            haf_customer_id_list = harvest_allocation_out_LoL[6]

            roll_indices = [i for i, x in enumerate(haf_customer_id_list) if x == 0]
            roll_harvest_LoL = [[harvest_allocation_out_LoL[2][idx] for idx in roll_indices],
                                [harvest_allocation_out_LoL[5][idx] for idx in roll_indices],
                                [harvest_allocation_out_LoL[10][idx] for idx in roll_indices]]

            # writeCustomerHarvestAllocation
            harvest_allocation_str = writeCustomerHarvestAllocation(demand_allocation_date,harvest_allocation_out_LoL, tier_count)
            #print(harvest_allocation_str) 
            
            # prior day harvest allocation
            (harvest_allocation_prior_LoL,allocated_crops_out2_LoL, short_demand_out2_LoL) = priorHarvestAllocation(demand_allocation_date, harvest_in_LoL, short_demand_out_LoL, facilities_LoL, allocated_crops_out_LoL, products_LoL)
            harvest_allocation_str = writeCustomerHarvestAllocation(demand_allocation_date,harvest_allocation_prior_LoL, tier_count)

            # track mid-allocation harvest
            write_allocated_str = writeAllocated(allocated_crops_out2_LoL, tier_count)

            #writecustomerShortDemand
            short_demand_str = writeCustomerShortDemand(demand_allocation_date, short_demand_out2_LoL)
            #print(short_demand_str)

            ###

    # second pass allocations of remaining demand (100% - fill goal %)
    for fill_goal in sorted_distinct_fill_goal_list[1:]:

        tier_indices = [i for i, x in enumerate(df_fill_goal_list) if x == fill_goal]
        ####
        tier_count += 1
        print('Tier ', tier_count, 'fill goal:', fill_goal, 'second pass fill goal:', round(float(1- fill_goal),2), "- %s seconds-" % (time.time() - start_time))

        # 2. time - loop dates starting from the next demand allocation date
        for demand_allocation_date_idx in range(len(distinct_demand_allocation_date_list)):

            # set last allocation date
            last_allocation_date = demand_allocation_date
            # set demand allocation date
            demand_allocation_date = distinct_demand_allocation_date_list[demand_allocation_date_idx]

            time_indices = [i for i, x in enumerate(df_demand_allocation_date_list) if x == demand_allocation_date]
            tier_time_indices = list(set(tier_indices) & set(time_indices))

            # create list of list for customer tier and time demand

            if demand_allocation_date != distinct_demand_allocation_date_list[0]:
                last_tier_time_demand_in_LoL = tier_time_demand_in_LoL

            tier_time_demand_in_LoL = [[demand_in_LoL[0][idx] for idx in tier_time_indices],
                                        [demand_in_LoL[1][idx] for idx in tier_time_indices],
                                        [demand_in_LoL[2][idx] for idx in tier_time_indices],
                                        [demand_in_LoL[3][idx] for idx in tier_time_indices],
                                        [demand_in_LoL[4][idx] for idx in tier_time_indices],
                                        [demand_in_LoL[5][idx] - int(round((demand_in_LoL[5][idx] - demand_in_LoL[6][idx] - demand_in_LoL[7][idx]) * fill_goal)) - int(round(demand_in_LoL[6][idx]*fill_goal)) - int(round(demand_in_LoL[7][idx]*fill_goal)) for idx in tier_time_indices],
                                        [demand_in_LoL[6][idx] - int(round(demand_in_LoL[6][idx]*fill_goal)) for idx in tier_time_indices],
                                        [demand_in_LoL[7][idx] - int(round(demand_in_LoL[7][idx]*fill_goal)) for idx in tier_time_indices]
                                      ]

            inventory_in_date = last_allocation_date    

            inventory_in_LoL = inventoryRollover(inventory_in_date,products_LoL, demand_allocation_date)

            # inventory rollover from smooth quantities from last allocation date
            inventory_in_LoL = smoothRollover(last_allocation_date, inventory_in_LoL, roll_harvest_LoL, products_LoL, demand_allocation_date)

            # stop sell and add transfers in
            (inventory_out_LoL, shelf_life_guarantee_out_LoL) = inventoryForecast(demand_allocation_date, inventory_in_LoL, products_LoL, transfers_LoL, tier_count)

            # write stop sell on final tier Day 2+
            if tier_count == final_tier and demand_allocation_date != distinct_demand_allocation_date_list[0]: 
                shelf_life_guarantee_str = writeStopSell(demand_allocation_date,shelf_life_guarantee_out_LoL)
            #print(shelf_life_guarantee_str)

            allocated_crops_in_LoL = readAllocated()

            # customerInventoryAllocation
            (inventory_allocation_out_LoL, inventory_demand_out_LoL) = customerInventoryAllocation(demand_allocation_date, inventory_out_LoL, tier_time_demand_in_LoL, facilities_LoL,inv_transfers_LoL,tier_count)

            # writeCustomerInventoryAllocation
            inventory_allocation_str = writeCustomerInventoryAllocation(demand_allocation_date,inventory_allocation_out_LoL, tier_count)
            #print(inventory_allocation_str)

            #customerHarvestAllocation
            (harvest_allocation_out_LoL,allocated_crops_out_LoL, short_demand_out_LoL) = customerHarvestAllocation(demand_allocation_date, harvest_in_LoL, inventory_demand_out_LoL, facilities_LoL, allocated_crops_in_LoL, har_transfers_LoL, products_LoL, inventory_allocation_out_LoL, tier_count)

            # create list of list for roll harvest
            haf_customer_id_list = harvest_allocation_out_LoL[6]

            roll_indices = [i for i, x in enumerate(haf_customer_id_list) if x == 0]
            roll_harvest_LoL = [[harvest_allocation_out_LoL[2][idx] for idx in roll_indices],
                                [harvest_allocation_out_LoL[5][idx] for idx in roll_indices],
                                [harvest_allocation_out_LoL[10][idx] for idx in roll_indices]]

            # writeCustomerHarvestAllocation
            harvest_allocation_str = writeCustomerHarvestAllocation(demand_allocation_date,harvest_allocation_out_LoL,tier_count)
            #print(harvest_allocation_str)
            
            # prior day harvest allocation
            (harvest_allocation_prior_LoL,allocated_crops_out2_LoL, short_demand_out2_LoL) = priorHarvestAllocation(demand_allocation_date, harvest_in_LoL, short_demand_out_LoL, facilities_LoL, allocated_crops_out_LoL, products_LoL)
            harvest_allocation_str = writeCustomerHarvestAllocation(demand_allocation_date,harvest_allocation_prior_LoL, tier_count)

            # track mid-allocation harvest
            write_allocated_str = writeAllocated(allocated_crops_out2_LoL, tier_count)


            #writecustomerShortDemand
            short_demand_str = writeCustomerShortDemand(demand_allocation_date, short_demand_out2_LoL)
            #print(short_demand_str)

#             ###


    #HarvestUnallocated_Facts


    allocated_crops_in_LoL = readAllocated()
    harvest_unallocated_str = writeHarvestUnallocated(harvest_in_LoL, allocated_crops_in_LoL, facilities_LoL)
    #print(harvest_unallocated_str)



    ###### Calculated Transfers
    
    
        ######################################################################################
 

    # allocation tracking: crop level across all time and tier
    allocated_date_crop_facility_key_list = list()
    allocated_plant_sites_list = list()
    complete_crop_allocation_key_list = list()
    allocated_starting_ps_list = list()

    # allocation_tracking: product level
    allocated_date_product_facility_key_list = list()
    allocated_gpps_list = list()
    allocated_qty_list = list()
    allocated_product_plant_sites_list = list()
    complete_product_allocation_key_list = list()

    # allocation tracking: customer level
    complete_customer_allocation_key_list = list()

    # initialize allocation tracking

    #initialize lists for HarvestUnallocatedPending_Facts

    allocated_crops_in_LoL =  [allocated_date_crop_facility_key_list, allocated_starting_ps_list, allocated_plant_sites_list, complete_crop_allocation_key_list]


    # initialize lists for short demand
    new_sdf_demand_date_list = list()
    new_sdf_demand_allocation_date_list = list()
    new_sdf_demand_facility_id_list = list()
    new_sdf_product_id_list = list()
    new_sdf_customer_id_list = list()
    new_sdf_short_demand_qty_list = list()
    new_sdf_production_priority_list = list()

    sdf_idx_to_skip_list = list()


    calendar_LoL = [cald_date_day_list,
            cald_year_number_list,
            cald_week_of_year_list,
            cald_year_week_list,
            cald_year_week_dow_list]
    
    transfer_constraints_LoL = [
            tcf_ship_greenhouse_id_list,
            tcf_arrival_greenhouse_id_list,
            tcf_ship_day_of_week_list,
            tcf_pack_lead_time_days_list,
            tcf_ship_duration_days_list,
            tcf_max_pallet_capacity_list,
            tcf_gfoods_transfer_list]
    
    # initialize lists for calculated transfers
    calc_ship_date_list = list()
    calc_arrival_date_list = list()
    calc_ship_facility_id_list = list()
    calc_arrival_facility_id_list = list()
    calc_transfer_constraints_id_list = list()
    calc_product_id_list = list()
    calc_enjoy_by_date_list = list()
    calc_customer_id_list = list()
    calc_transfer_qty_list = list()
    calc_transfer_pallets_list = list()
    calc_truck_count_list = list()
    
    calc_transfers_LoL = [
        calc_ship_date_list,
        calc_arrival_date_list,
        calc_ship_facility_id_list,
        calc_arrival_facility_id_list,
        calc_transfer_constraints_id_list,
        calc_product_id_list,
        calc_enjoy_by_date_list,
        calc_customer_id_list,
        calc_transfer_qty_list,
        calc_transfer_pallets_list,
        calc_truck_count_list
        ]
    
    # initialize first date 
    demand_allocation_date = distinct_demand_allocation_date_list[0] 

    # write output to pending tables
    is_pending = 1

    # main loop for calculated transfers
    # 1. customer tier
    # 2. time
    # order for each tier-timestep combination
    #    a. Inventory Rollover
    #    b. Inventory to Customer Allocation
    #    c. Harvest to Customer Allocation
    #    d. Prior Day Harvest to Customer Allocation
    #    e. Harvest to Customer Calculated Transfers




    tier_count = 0
    final_tier = (len(sorted_distinct_fill_goal_list) * 2) - 1
    # first pass allocations up to fill goal %
    # 1. customer tier
    for fill_goal in sorted_distinct_fill_goal_list:
        # customer tier indices for demand
        tier_indices = [i for i, x in enumerate(df_fill_goal_list) if x == fill_goal]


        ####
        tier_count += 1
        print('Tier ', tier_count, 'fill goal:', fill_goal, "- %s seconds-" % (time.time() - start_time))
        # 2. time - loop dates starting from the next demand allocation date
        for demand_allocation_date_idx in range(len(distinct_demand_allocation_date_list)):

            # set last allocation date
            last_allocation_date = demand_allocation_date
            # set demand allocation date
            demand_allocation_date = distinct_demand_allocation_date_list[demand_allocation_date_idx]

            time_indices = [i for i, x in enumerate(df_demand_allocation_date_list) if x == demand_allocation_date]
            tier_time_indices = list(set(tier_indices) & set(time_indices))


            # create list of list for customer tier and time demand

            if demand_allocation_date != distinct_demand_allocation_date_list[0]:
                last_tier_time_demand_in_LoL = tier_time_demand_in_LoL


            tier_time_demand_in_LoL = [[demand_in_LoL[0][idx] for idx in tier_time_indices],
                                        [demand_in_LoL[1][idx] for idx in tier_time_indices],
                                        [demand_in_LoL[2][idx] for idx in tier_time_indices],
                                        [demand_in_LoL[3][idx] for idx in tier_time_indices],
                                        [demand_in_LoL[4][idx] for idx in tier_time_indices],
                                        [int(round((demand_in_LoL[5][idx] - demand_in_LoL[6][idx] - demand_in_LoL[7][idx]) * fill_goal)) + int(round(demand_in_LoL[6][idx]*fill_goal)) + int(round(demand_in_LoL[7][idx]*fill_goal)) for idx in tier_time_indices],
                                        [int(round(demand_in_LoL[6][idx]*fill_goal)) for idx in tier_time_indices],
                                        [int(round(demand_in_LoL[7][idx]*fill_goal)) for idx in tier_time_indices]
                                      ]

            # inventory for Day 1 Tier 1
            if demand_allocation_date == distinct_demand_allocation_date_list[0] and tier_count == 1:
                # Compressed clean clean inventory facts (cccif) after aggregating
                inventory_out_LoL = starting_inventory_in


            # inventory rollover Tier 2+ Day 1 or Day 2+
            if (demand_allocation_date == distinct_demand_allocation_date_list[0] and tier_count > 1) or demand_allocation_date != distinct_demand_allocation_date_list[0]:

                inventory_in_date = last_allocation_date    
                inventory_in_LoL = inventoryRollover(inventory_in_date,products_LoL, demand_allocation_date, is_pending)
                #inventory_in_LoL = inventoryRolloverPending(inventory_in_date,products_LoL, demand_allocation_date)

                inventory_in_LoL = smoothRollover(last_allocation_date, inventory_in_LoL, roll_harvest_LoL, products_LoL, demand_allocation_date)

                # stop sell and add transfers in
                (inventory_out_LoL, shelf_life_guarantee_out_LoL) = inventoryForecast(demand_allocation_date, inventory_in_LoL, products_LoL, transfers_LoL, tier_count)

                allocated_crops_in_LoL = readAllocated()

            # customerInventoryAllocation
            (inventory_allocation_out_LoL, inventory_demand_out_LoL) = customerInventoryAllocation(demand_allocation_date, inventory_out_LoL, tier_time_demand_in_LoL, facilities_LoL,inv_transfers_LoL,tier_count)

            # writeCustomerInventoryAllocation
            inventory_allocation_str = writeCustomerInventoryAllocation(demand_allocation_date,inventory_allocation_out_LoL, tier_count, is_pending)
            #print(inventory_allocation_str)

            #customerHarvestAllocation
            (harvest_allocation_out_LoL,allocated_crops_out_LoL, short_demand_out_LoL) = customerHarvestAllocation(demand_allocation_date, harvest_in_LoL, inventory_demand_out_LoL, facilities_LoL, allocated_crops_in_LoL, har_transfers_LoL, products_LoL, inventory_allocation_out_LoL, tier_count)

            # create list of list for roll harvest
            haf_customer_id_list = harvest_allocation_out_LoL[6]

            roll_indices = [i for i, x in enumerate(haf_customer_id_list) if x == 0]
            roll_harvest_LoL = [[harvest_allocation_out_LoL[2][idx] for idx in roll_indices],
                                [harvest_allocation_out_LoL[5][idx] for idx in roll_indices],
                                [harvest_allocation_out_LoL[10][idx] for idx in roll_indices]]

            # writeCustomerHarvestAllocation
            harvest_allocation_str = writeCustomerHarvestAllocation(demand_allocation_date,harvest_allocation_out_LoL, tier_count, is_pending)
            #print(harvest_allocation_str)    
            #print(len(short_demand_out_LoL[0]))
            
            # prior day harvest allocation
            (harvest_allocation_prior_LoL,allocated_crops_out2_LoL, short_demand_out2_LoL) = priorHarvestAllocation(demand_allocation_date, harvest_in_LoL, short_demand_out_LoL, facilities_LoL, allocated_crops_out_LoL, products_LoL)
            harvest_allocation_str = writeCustomerHarvestAllocation(demand_allocation_date,harvest_allocation_prior_LoL, tier_count, is_pending)


            # calculated transfers
            (inventory_allocation_transfers_LoL,harvest_allocation_transfers_LoL, allocated_crops_out3_LoL, short_demand_out3_LoL,calc_transfers_LoL) = calculateTransfers(demand_allocation_date, harvest_in_LoL, short_demand_out2_LoL, facilities_LoL, allocated_crops_out2_LoL, products_LoL, transfer_constraints_LoL, calendar_LoL, calc_transfers_LoL, inventory_allocation_out_LoL)
            inventory_allocation_str = writeCustomerInventoryAllocation(demand_allocation_date,inventory_allocation_transfers_LoL, tier_count, is_pending)
            harvest_allocation_str = writeCustomerHarvestAllocation(demand_allocation_date,harvest_allocation_transfers_LoL, tier_count, is_pending)
            

            # track mid-allocation harvest
            write_allocated_str = writeAllocated(allocated_crops_out3_LoL, tier_count)
            
            #writecustomerShortDemand
            short_demand_str = writeCustomerShortDemand(demand_allocation_date, short_demand_out3_LoL, is_pending)
            #print(short_demand_str)

            ###

    # second pass allocations of remaining demand (100% - fill goal %)
    for fill_goal in sorted_distinct_fill_goal_list[1:]:

        tier_indices = [i for i, x in enumerate(df_fill_goal_list) if x == fill_goal]
        ####
        tier_count += 1
        print('Tier ', tier_count, 'fill goal:', fill_goal, 'second pass fill goal:', round(float(1- fill_goal),2), "- %s seconds-" % (time.time() - start_time))

        # 2. time - loop dates starting from the next demand allocation date
        for demand_allocation_date_idx in range(len(distinct_demand_allocation_date_list)):

            # set last allocation date
            last_allocation_date = demand_allocation_date
            # set demand allocation date
            demand_allocation_date = distinct_demand_allocation_date_list[demand_allocation_date_idx]

            time_indices = [i for i, x in enumerate(df_demand_allocation_date_list) if x == demand_allocation_date]
            tier_time_indices = list(set(tier_indices) & set(time_indices))

            # create list of list for customer tier and time demand

            if demand_allocation_date != distinct_demand_allocation_date_list[0]:
                last_tier_time_demand_in_LoL = tier_time_demand_in_LoL

            tier_time_demand_in_LoL = [[demand_in_LoL[0][idx] for idx in tier_time_indices],
                                        [demand_in_LoL[1][idx] for idx in tier_time_indices],
                                        [demand_in_LoL[2][idx] for idx in tier_time_indices],
                                        [demand_in_LoL[3][idx] for idx in tier_time_indices],
                                        [demand_in_LoL[4][idx] for idx in tier_time_indices],
                                        [demand_in_LoL[5][idx] - int(round((demand_in_LoL[5][idx] - demand_in_LoL[6][idx] - demand_in_LoL[7][idx]) * fill_goal)) - int(round(demand_in_LoL[6][idx]*fill_goal)) - int(round(demand_in_LoL[7][idx]*fill_goal)) for idx in tier_time_indices],
                                        [demand_in_LoL[6][idx] - int(round(demand_in_LoL[6][idx]*fill_goal)) for idx in tier_time_indices],
                                        [demand_in_LoL[7][idx] - int(round(demand_in_LoL[7][idx]*fill_goal)) for idx in tier_time_indices]
                                      ]

            inventory_in_date = last_allocation_date    

            inventory_in_LoL = inventoryRollover(inventory_in_date,products_LoL, demand_allocation_date, is_pending)

            # inventory rollover from smooth quantities from last allocation date
            inventory_in_LoL = smoothRollover(last_allocation_date, inventory_in_LoL, roll_harvest_LoL, products_LoL, demand_allocation_date)

            # stop sell and add transfers in
            (inventory_out_LoL, shelf_life_guarantee_out_LoL) = inventoryForecast(demand_allocation_date, inventory_in_LoL, products_LoL, transfers_LoL, tier_count)

            # write stop sell on final tier Day 2+
            if tier_count == final_tier and demand_allocation_date != distinct_demand_allocation_date_list[0]: 
                shelf_life_guarantee_str = writeStopSell(demand_allocation_date,shelf_life_guarantee_out_LoL, is_pending)
            #print(shelf_life_guarantee_str)

            allocated_crops_in_LoL = readAllocated()

            # customerInventoryAllocation
            (inventory_allocation_out_LoL, inventory_demand_out_LoL) = customerInventoryAllocation(demand_allocation_date, inventory_out_LoL, tier_time_demand_in_LoL, facilities_LoL,inv_transfers_LoL,tier_count)

            # writeCustomerInventoryAllocation
            inventory_allocation_str = writeCustomerInventoryAllocation(demand_allocation_date,inventory_allocation_out_LoL, tier_count, is_pending)
            #print(inventory_allocation_str)

            #customerHarvestAllocation
            (harvest_allocation_out_LoL,allocated_crops_out_LoL, short_demand_out_LoL) = customerHarvestAllocation(demand_allocation_date, harvest_in_LoL, inventory_demand_out_LoL, facilities_LoL, allocated_crops_in_LoL, har_transfers_LoL, products_LoL, inventory_allocation_out_LoL, tier_count)

            # create list of list for roll harvest
            haf_customer_id_list = harvest_allocation_out_LoL[6]

            roll_indices = [i for i, x in enumerate(haf_customer_id_list) if x == 0]
            roll_harvest_LoL = [[harvest_allocation_out_LoL[2][idx] for idx in roll_indices],
                                [harvest_allocation_out_LoL[5][idx] for idx in roll_indices],
                                [harvest_allocation_out_LoL[10][idx] for idx in roll_indices]]

            # writeCustomerHarvestAllocation
            harvest_allocation_str = writeCustomerHarvestAllocation(demand_allocation_date,harvest_allocation_out_LoL,tier_count, is_pending)
            #print(harvest_allocation_str)

            # prior day harvest allocation
            (harvest_allocation_prior_LoL,allocated_crops_out2_LoL, short_demand_out2_LoL) = priorHarvestAllocation(demand_allocation_date, harvest_in_LoL, short_demand_out_LoL, facilities_LoL, allocated_crops_out_LoL, products_LoL)
            harvest_allocation_str = writeCustomerHarvestAllocation(demand_allocation_date,harvest_allocation_prior_LoL, tier_count, is_pending)


            # calculated transfers
            (inventory_allocation_transfers_LoL,harvest_allocation_transfers_LoL, allocated_crops_out3_LoL, short_demand_out3_LoL,calc_transfers_LoL) = calculateTransfers(demand_allocation_date, harvest_in_LoL, short_demand_out2_LoL, facilities_LoL, allocated_crops_out2_LoL, products_LoL, transfer_constraints_LoL, calendar_LoL, calc_transfers_LoL, inventory_allocation_out_LoL)
            inventory_allocation_str = writeCustomerInventoryAllocation(demand_allocation_date,inventory_allocation_transfers_LoL, tier_count, is_pending)
            harvest_allocation_str = writeCustomerHarvestAllocation(demand_allocation_date,harvest_allocation_transfers_LoL, tier_count, is_pending)
            
            # track mid-allocation harvest
            write_allocated_str = writeAllocated(allocated_crops_out3_LoL, tier_count)

            short_demand_str = writeCustomerShortDemand(demand_allocation_date, short_demand_out3_LoL, is_pending)
            #print(short_demand_str)

            ###


    #HarvestUnallocated_Facts

    allocated_crops_in_LoL = readAllocated()
    harvest_unallocated_str = writeHarvestUnallocated(harvest_in_LoL, allocated_crops_in_LoL, facilities_LoL, is_pending)
    #print(harvest_unallocated_str)
    
    # CalculatedTransfers_Facts
    
    calc_transfer_str = writeCalculatedTransfers(calc_transfers_LoL)

    
    
    print('pau')


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




