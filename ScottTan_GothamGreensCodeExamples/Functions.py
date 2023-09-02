#!/usr/bin/env python
# coding: utf-8

# In[3]:


# Gotham Greens Function Library

# Forecasting + Production Planning
# Written by Scott Tan
# Last Updated 4/7/2020
#
# Load all Gotham custom functions

import numpy as np

def cropAverages(source_dict,target_facility_line,target_crop_id):
    
#     goal: compute facility, city, and national yield metrics for a target facility line and crop id
    
#     input: yield metric dictionary, target facility line, and target crop
#     source_dict: dictionary containing avg plant sites per clam or average headweights (pspc_dict or avg_headweight_dict)
#     target_facility_line: string containing 3 or 4 length facility and line number separated by an underscore ('NYC2_4')
#     target_crop_id: integer corresponding to crop from CropDim_T

#     output: list of three average yield metrics (floats)
#         1. facility average headweight/plant sites per clam (float)
#         2. city average headweight/plant sites per clam (float)
#         3. national average headweight/plant sites per clam (float)
    
    target_city = target_facility_line[0:3]
    facility_match_facility_line_list = []
    facility_match_year_week_list = []
    facility_match_avg_val_for_year_week_list = []

    city_match_facility_line_list = []
    city_match_year_week_list = []
    city_match_avg_val_for_year_week_list = []

    nation_avg_val_for_year_week_list = []

    for test_facility_line in list(source_dict.keys()):

        test_facility = test_facility_line.split('_')[0]

        test_city = test_facility_line[0:3]
        
        crops = list(source_dict[test_facility_line].keys())
        if target_crop_id in crops:
            nation_avg_val_for_year_week_list += [np.mean(list(source_dict[test_facility_line][target_crop_id].values())[-1])]
            if test_facility == target_facility_line.split('_')[0]:
                facility_match_facility_line_list += [test_facility_line]
                facility_match_year_week_list += [list(source_dict[test_facility_line][target_crop_id].keys())[-1]]
                #print(list(source_dict[test_facility_line][target_crop_id].values())[-1])
                facility_match_avg_val_for_year_week_list += [np.mean(list(source_dict[test_facility_line][target_crop_id].values())[-1])]

            if test_city == target_city:
                city_match_facility_line_list += [test_facility_line]
                city_match_year_week_list += [list(source_dict[test_facility_line][target_crop_id].keys())[-1]]
                city_match_avg_val_for_year_week_list += [np.mean(list(source_dict[test_facility_line][target_crop_id].values())[-1])]

    # print(matching_facility_line_list)
    # print(year_week_list)
    # print(facility_line_avg_pspc_for_year_week_list)

    facility_avg_val = np.mean(facility_match_avg_val_for_year_week_list)
    #print(facility_avg_pspc)

    city_avg_val = np.mean(city_match_avg_val_for_year_week_list)
    #print(city_avg_pspc)

    nation_avg_val = np.mean(nation_avg_val_for_year_week_list)
    #print(nation_avg_pspc)

    return [facility_avg_val,city_avg_val,nation_avg_val]

##########################################


def orderForecast(allocation_class, lsd_list_of_lists, fs_list_of_lists, order_forecast_date_list):
    
#     goal: compute dictionaries for real and expected orders for the specified allocation class
    
#     inputs:
#     1) allocation_class- integer 1, 2, 3, 4, 5, or 6 cooresponding to the allocation class
#         1. OrderType = 'Food Service’ AND ItemNo NOT LIKE '%LOS%’
#         2. OrderType = 'Retail' AND SkuTypeShortName = 'Baby Butterhead’
#         3. OrderType = 'Food Service’ AND ItemNo LIKE '%LOS%’
#         4. OrderType = 'Retail' AND ProductTypeDesc = 'Leafy Greens' AND SkuTypeShortName != 'Gourmet Medley' AND SkuTypeShortName != 'Ugly Greens' AND SkuTypeShortName != 'Baby Butterhead’
#         5. OrderType = 'Retail'AND SkuTypeShortName = 'Gourmet Medley’
#         6. OrderType = 'Retail' AND ProductTypeDesc = 'Herbs’
#     2) lsd_list_of_lists- list of lists for actual orders = [lsd_date_list, lsd_item_no_list, lsd_original_qty_list, lsd_sage_customer_id_list, lsd_location_code_list, lsd_order_number_list, lsd_allocation_class_list]
#         1. order date (datetime)
#         2. item number (string)
#         3. original quantity (int)
#         4. sage customer id (string)
#         5. location code (string)
#         6. order number (string)
#         7. allocation class (int)
#     3) fs_list_of_list- list of lists for invoiced orders = [fs_order_date_list, fs_item_no_list, fs_original_qty_list, fs_sage_customer_id_list, fs_gg_location_name_list, fs_allocation_class_list]
#         1. harvest date (datetime)
#         2. item number (string)
#         3. original quantiy (int)
#         4. sage customer id (string)
#         5. location name (string)
#         6. allocation class (int)
#     4) order_forecast_date_list- list of datetimes to forecast orders

#     output: tuple containing 1) list of six lists cooresponding to actual orders and 2) dictionary of expected orders
#     1) lsdfs_list_of_lists = [lsdfs_date_list, lsdfs_item_no_list, lsdfs_original_qty_list, lsdfs_sage_customer_id_list, lsdfs_location_code_list, lsdfs_order_number_list]
#         1. order date (datetime)
#         2. item number (string)
#         3. original quantity (int)
#         4. sage customer id (string)
#         5. location code (string)
#         6. order number (string)
#     2) expected_dictionary[harvest_forecast_date][city_key][dc_key][crop_id_key][item_key] = expected_order
#         1. harvest date (datetime)
#         2. location name (string)
#         3. sage customer id (string)
#         4. crop id (int)
#         5. item number (string)
    fs_order_date_list = fs_list_of_lists[0]
    fs_item_no_list = fs_list_of_lists[1]
    fs_original_qty_list = fs_list_of_lists[2]
    fs_sage_customer_id_list = fs_list_of_lists[3]
    fs_gg_location_name_list = fs_list_of_lists[4]
    fs_allocation_class_list = fs_list_of_lists[5]
    
    lsd_date_list =  lsd_list_of_lists[0]
    lsd_item_no_list = lsd_list_of_lists[1]
    lsd_original_qty_list = lsd_list_of_lists[2]
    lsd_sage_customer_id_list = lsd_list_of_lists[3]
    lsd_location_code_list = lsd_list_of_lists[4]
    lsd_order_number_list = lsd_list_of_lists[5]
    lsd_allocation_class_list = lsd_list_of_lists[6]
    
    # fact sales data indices cooresponding to the allocation class
    fs_allocation_indices = [i for i, x in enumerate(fs_allocation_class_list) if x == allocation_class]
    # live sales data indices cooresponding to the allocation class
    lsd_allocation_indices = [i for i, x in enumerate(lsd_allocation_class_list) if x == allocation_class]     
    
    # fsfs: fact sales food service (example allocation_class = 1)
    fsfs_order_date_list = list()
    fsfs_item_no_list = list()
    fsfs_original_qty_list = list()
    fsfs_sage_customer_id_list = list()
    fsfs_location_name_list = list()
    
    for i in fs_allocation_indices:
        # make lists
        fsfs_order_date_list += [fs_order_date_list[i]]
        fsfs_item_no_list += [fs_item_no_list[i]]
        fsfs_original_qty_list += [fs_original_qty_list[i]]
        fsfs_sage_customer_id_list += [fs_sage_customer_id_list[i]]
        fsfs_location_name_list += [fs_location_name_list[i]]
   
    # lsdfs: live sales data food service (example allocation_class = 1)
    lsdfs_date_list = list()
    lsdfs_item_no_list = list()
    lsdfs_original_qty_list = list()
    lsdfs_sage_customer_id_list = list()
    lsdfs_location_code_list = list()
    lsdfs_order_number_list = list()

    for i in lsd_allocation_indices:
        
        lsdfs_date_list += [lsd_date_list[i]]
        lsdfs_item_no_list  += [lsd_item_no_list[i]]
        lsdfs_original_qty_list  += [lsd_original_qty_list[i]]
        lsdfs_sage_customer_id_list  += [lsd_sage_customer_id_list[i]]
        lsdfs_location_code_list  += [lsd_location_code_list[i]]
        lsdfs_order_number_list  += [lsd_order_number_list[i]]
    
    # dc_list: SageCustomerID of distribution centers identified by Chris Thompson
    dc_list = ['JWL001', 'WFM101', 'WFM201', 'WFM202', 'SHW001','WFM203', 'WFM204', 'WFM205', 'WFM206', 'SAF001', 'SAF101','KSP001','WAK001']


    # fsfs_dictionary[city][dc][crop_description][item_no][weekday][year_week] = total_quantity_ordered
    fsfs_dictionary = {}

    for fsfs_idx in range(len(fsfs_order_date_list)):
        fsfs_order_date = fsfs_order_date_list[fsfs_idx]
        fsfs_item_no = fsfs_item_no_list[fsfs_idx]
        fsfs_original_qty = fsfs_original_qty_list[fsfs_idx]
        fsfs_sage_customer_id = fsfs_sage_customer_id_list[fsfs_idx]
        fsfs_location_name = fsfs_location_name_list[fsfs_idx]

        fsfs_year = fsfs_order_date.year
        fsfs_week = fsfs_order_date.isocalendar()[1]
        fsfs_weekday = fsfs_order_date.weekday() # Monday is 0, Sunday is 6
        fsfs_city = fsfs_location_name

        fsfs_crop_description = fsfs_item_no[3:7]
        
#         if fsfs_item_no in spd_item_no_list:
#             fsfs_spd_idx = spd_item_no_list.index(fsfs_item_no)
#             fsfs_packed_weight_conversion_grams = spd_packed_weight_conversion_grams_list[fsfs_spd_idx]
#         else:
#             fsfs_packed_weight_conversion_grams = 0

        fsfs_dc = fsfs_sage_customer_id
#         if fsfs_dc not in dc_list:
#             fsfs_dc = 'OTHERS'

        fsfs_key_str = str(fsfs_year) + '_' + str(fsfs_week)   
        #fsfs_value_to_add = fsfs_qty_grams
        fsfs_value_to_add = fsfs_original_qty

        if fsfs_city in fsfs_dictionary.keys():
            fsfs_city_dictionary_value = fsfs_dictionary[fsfs_city]
            if fsfs_dc in fsfs_city_dictionary_value.keys():
                fsfs_dc_dictionary_value = fsfs_dictionary[fsfs_city][fsfs_dc]
                if fsfs_crop_description in fsfs_dc_dictionary_value.keys():
                    fsfs_crop_description_dictionary_value = fsfs_dictionary[fsfs_city][fsfs_dc][fsfs_crop_description]
                    if fsfs_item_no in fsfs_crop_description_dictionary_value.keys():
                        fsfs_item_no_dictionary_value = fsfs_dictionary[fsfs_city][fsfs_dc][fsfs_crop_description][fsfs_item_no]
                        if fsfs_weekday in fsfs_item_no_dictionary_value.keys():
                            fsfs_weekday_dictionary_value = fsfs_dictionary[fsfs_city][fsfs_dc][fsfs_crop_description][fsfs_item_no][fsfs_weekday]                     
                            if fsfs_key_str in fsfs_weekday_dictionary_value.keys():
                                fsfs_dictionary[fsfs_city][fsfs_dc][fsfs_crop_description][fsfs_item_no][fsfs_weekday][fsfs_key_str] += fsfs_value_to_add
                            else:
                                fsfs_dictionary[fsfs_city][fsfs_dc][fsfs_crop_description][fsfs_item_no][fsfs_weekday][fsfs_key_str] = fsfs_value_to_add
                        else:
                            fsfs_dictionary[fsfs_city][fsfs_dc][fsfs_crop_description][fsfs_item_no][fsfs_weekday] = {fsfs_key_str:fsfs_value_to_add}            
                    else:
                        fsfs_dictionary[fsfs_city][fsfs_dc][fsfs_crop_description][fsfs_item_no] = {fsfs_weekday:{fsfs_key_str:fsfs_value_to_add}}
                else:
                    fsfs_dictionary[fsfs_city][fsfs_dc][fsfs_crop_description] = {fsfs_item_no:{fsfs_weekday:{fsfs_key_str:fsfs_value_to_add}}}    
            else:
                fsfs_dictionary[fsfs_city][fsfs_dc] = {fsfs_crop_description:{fsfs_item_no:{fsfs_weekday:{fsfs_key_str:fsfs_value_to_add}}}}
        else:
            fsfs_dictionary[fsfs_city] = {fsfs_dc:{fsfs_crop_description:{fsfs_item_no:{fsfs_weekday:{fsfs_key_str:fsfs_value_to_add}}}}}

    # live sales data for whole plant food service cases

    # lsdfs_dictionary[city][dc][crop_description][item_no][weekday][year_week] = original quantity ordered (g)
    lsdfs_dictionary = {}

    #live_order_date_list = list()
    
    for lsdfs_idx in range(len(lsdfs_date_list)):
        lsdfs_order_date = lsdfs_date_list[lsdfs_idx]
        lsdfs_item_no = lsdfs_item_no_list[lsdfs_idx]
        lsdfs_original_qty = lsdfs_original_qty_list[lsdfs_idx]
        lsdfs_sage_customer_id = lsdfs_sage_customer_id_list[lsdfs_idx]
        lsdfs_location_name = lsdfs_location_code_list[lsdfs_idx]

        lsdfs_year = lsdfs_order_date.year
        lsdfs_week = lsdfs_order_date.isocalendar()[1]
        lsdfs_weekday = lsdfs_order_date.weekday() # Monday is 0, Sunday is 6
        
        lsdfs_city = lsdfs_location_name
        
        lsdfs_crop_description = lsdfs_item_no[3:7]
        
#         if lsdfs_order_date not in live_order_date_list:
#             live_order_date_list += [lsdfs_order_date]
        
#         if lsdfs_item_no in spd_item_no_list:
#             lsdfs_spd_idx = spd_item_no_list.index(lsdfs_item_no)
#             lsdfs_packed_weight_conversion_grams = spd_packed_weight_conversion_grams_list[lsdfs_spd_idx]
#         else:
#             lsdfs_packed_weight_conversion_grams = 0

        lsdfs_dc = lsdfs_sage_customer_id
#         if lsdfs_dc not in dc_list:
#             lsdfs_dc = 'OTHERS'

        lsdfs_key_str = str(lsdfs_year) + '_' + str(lsdfs_week)   
        #lsdfs_value_to_add = lsdfs_qty_grams
        lsdfs_value_to_add = lsdfs_original_qty

        if lsdfs_city in lsdfs_dictionary.keys():
            lsdfs_city_dictionary_value = lsdfs_dictionary[lsdfs_city]
            if lsdfs_dc in lsdfs_city_dictionary_value.keys():
                lsdfs_dc_dictionary_value = lsdfs_dictionary[lsdfs_city][lsdfs_dc]
                if lsdfs_crop_description in lsdfs_dc_dictionary_value.keys():
                    lsdfs_crop_description_dictionary_value = lsdfs_dictionary[lsdfs_city][lsdfs_dc][lsdfs_crop_description]
                    if lsdfs_item_no in lsdfs_crop_description_dictionary_value.keys():
                        lsdfs_item_no_dictionary_value = lsdfs_dictionary[lsdfs_city][lsdfs_dc][lsdfs_crop_description][lsdfs_item_no]
                        if lsdfs_weekday in lsdfs_item_no_dictionary_value.keys():
                            lsdfs_weekday_dictionary_value = lsdfs_dictionary[lsdfs_city][lsdfs_dc][lsdfs_crop_description][lsdfs_item_no][lsdfs_weekday]                     
                            if lsdfs_key_str in lsdfs_weekday_dictionary_value.keys():
                                lsdfs_dictionary[lsdfs_city][lsdfs_dc][lsdfs_crop_description][lsdfs_item_no][lsdfs_weekday][lsdfs_key_str] += lsdfs_value_to_add
                            else:
                                lsdfs_dictionary[lsdfs_city][lsdfs_dc][lsdfs_crop_description][lsdfs_item_no][lsdfs_weekday][lsdfs_key_str] = lsdfs_value_to_add
                        else:
                            lsdfs_dictionary[lsdfs_city][lsdfs_dc][lsdfs_crop_description][lsdfs_item_no][lsdfs_weekday] = {lsdfs_key_str:lsdfs_value_to_add}            
                    else:
                        lsdfs_dictionary[lsdfs_city][lsdfs_dc][lsdfs_crop_description][lsdfs_item_no] = {lsdfs_weekday:{lsdfs_key_str:lsdfs_value_to_add}}
                else:
                    lsdfs_dictionary[lsdfs_city][lsdfs_dc][lsdfs_crop_description] = {lsdfs_item_no:{lsdfs_weekday:{lsdfs_key_str:lsdfs_value_to_add}}}    
            else:
                lsdfs_dictionary[lsdfs_city][lsdfs_dc] = {lsdfs_crop_description:{lsdfs_item_no:{lsdfs_weekday:{lsdfs_key_str:lsdfs_value_to_add}}}}
        else:
            lsdfs_dictionary[lsdfs_city] = {lsdfs_dc:{lsdfs_crop_description:{lsdfs_item_no:{lsdfs_weekday:{lsdfs_key_str:lsdfs_value_to_add}}}}}


    expected_dictionary = {}
    std_expected_dictionary = {}
    live_dictionary = {}

    for harvest_forecast_date in order_forecast_date_list:
        harvest_forecast_year_week = str(harvest_forecast_date.year) + '_' + str(harvest_forecast_date.isocalendar()[1])
        #print(harvest_forecast_year_week)
        harvest_forecast_weekday = harvest_forecast_date.weekday() # Monday is 0, Sunday is 6
        for city_key in fsfs_dictionary.keys():
            for dc_key in fsfs_dictionary[city_key].keys():
                for crop_key in fsfs_dictionary[city_key][dc_key].keys():
                    for item_key in fsfs_dictionary[city_key][dc_key][crop_key].keys():
                        sage_crop_code = item_key[3:7]
                        #sage_crop_code_idx = sage_crop_code_list.index(sage_crop_code)
                        #crop_id = crop_id_list[sage_crop_code_idx]
                        #crop_id_key = crop_id
                        crop_id_key = sage_crop_code
                        if harvest_forecast_weekday in fsfs_dictionary[city_key][dc_key][crop_key][item_key]:
                            list_of_vals_6wk = list(fsfs_dictionary[city_key][dc_key][crop_key][item_key][harvest_forecast_weekday].values())[-6:]
                            if len(list_of_vals_6wk) < 6:
                                zeros_to_add = 6 - len(list_of_vals_6wk)
                                zeros_list = list(np.zeros(zeros_to_add))
                                list_of_vals_6wk = zeros_list + list_of_vals_6wk
                            list_of_vals_4wk = list_of_vals_6wk[-4:]
                            list_of_vals_5wk = list_of_vals_6wk[-5:]
                            # compute the expected order as the max of the 4-6 week trailing average from fsfs_dictionaryyup
                            avg4wk = np.mean(list_of_vals_4wk)
                            avg5wk = np.mean(list_of_vals_5wk)                
                            avg6wk = np.mean(list_of_vals_6wk)
                            #avg4wk = np.sum(list(fsfs_dictionary[city_key][dc_key][crop_key][item_key][harvest_forecast_weekday].values())[-4:])/4
                            #avg5wk = np.sum(list(fsfs_dictionary[city_key][dc_key][crop_key][item_key][harvest_forecast_weekday].values())[-5:])/5               
                            #avg6wk = np.sum(list(fsfs_dictionary[city_key][dc_key][crop_key][item_key][harvest_forecast_weekday].values())[-6:])/6
                            std4wk = np.std(list_of_vals_4wk)
                            std5wk = np.std(list_of_vals_5wk)                
                            std6wk = np.std(list_of_vals_6wk)
                            
                            max_of_4_5_6 = max(avg4wk,avg5wk,avg6wk) # project order quantity as the max of 4-6 most recent orders
                            max_idx = np.argmax([avg4wk,avg5wk,avg6wk])
                            std_of_max = [std4wk,std5wk,std6wk][max_idx] # standard deviation of the average
                            

                            # check live sales data for an actual order
                            live_order_check = 0
                            if city_key in lsdfs_dictionary.keys():
                                if dc_key in lsdfs_dictionary[city_key].keys():
                                    if crop_key in lsdfs_dictionary[city_key][dc_key]:
                                        if item_key in lsdfs_dictionary[city_key][dc_key][crop_key]:
                                            if harvest_forecast_weekday in lsdfs_dictionary[city_key][dc_key][crop_key][item_key]:
                                                if harvest_forecast_year_week in lsdfs_dictionary[city_key][dc_key][crop_key][item_key][harvest_forecast_weekday]:          
                                                    live_order_check = lsdfs_dictionary[city_key][dc_key][crop_key][item_key][harvest_forecast_weekday][harvest_forecast_year_week]

                            # if there is an actual order from a DC, write the actual order for all items
                            # if there is an actual order from 'OTHERS' then use the max of the live order and expected order for all items
                            # if there is no actual order, write the expected order
                            live_order_tag = 0
                            #if dc_key in dc_list and live_order_check != 0:
                            if live_order_check != 0:
                                #expected_or_actual = live_order_check
                                #expected_or_actual = 0
                                #live_order_tag = 1
                                expected_order = max_of_4_5_6
                            #if dc_key in dc_list and live_order_check == 0:
                            if live_order_check == 0:
                                #expected_or_actual = max_of_4_5_6
                                expected_order = max_of_4_5_6
                            if dc_key == 'OTHERS':
                                #expected_or_actual = max(max_of_4_5_6,live_order_check)
                                #expected_or_actual = max(max_of_4_5_6 - live_order_check,0) # additional expected order from OTHERS
                                #live_order_tag = np.argmax([max_of_4_5_6,live_order_check])
                                expected_order = max(max_of_4_5_6 - float(live_order_check),0)

                            if harvest_forecast_date in expected_dictionary.keys():
                                harvest_forecast_date_dictionary_value = expected_dictionary[harvest_forecast_date]
                                if city_key in harvest_forecast_date_dictionary_value.keys():
                                    city_dictionary_value = expected_dictionary[harvest_forecast_date][city_key]
                                    if dc_key in city_dictionary_value.keys():
                                        dc_dictionary_value = expected_dictionary[harvest_forecast_date][city_key][dc_key]
                                        if crop_id_key in dc_dictionary_value.keys():
                                            crop_id_dictionary_value = expected_dictionary[harvest_forecast_date][city_key][dc_key][crop_id_key]
                                            if item_key in crop_id_dictionary_value.keys():
                                                expected_dictionary[harvest_forecast_date][city_key][dc_key][crop_id_key][item_key] += expected_order
                                                std_expected_dictionary[harvest_forecast_date][city_key][dc_key][crop_id_key][item_key]= ((std_expected_dictionary[harvest_forecast_date][city_key][dc_key][crop_id_key][item_key])**2 + (std_of_max/expected_order)**2)**0.5
                                                live_dictionary[harvest_forecast_date][city_key][dc_key][crop_id_key][item_key] += live_order_check
                                            else:
                                                expected_dictionary[harvest_forecast_date][city_key][dc_key][crop_id_key][item_key] = expected_order
                                                std_expected_dictionary[harvest_forecast_date][city_key][dc_key][crop_id_key][item_key] = std_of_max
                                                live_dictionary[harvest_forecast_date][city_key][dc_key][crop_id_key][item_key] = live_order_check
                                        else:
                                            expected_dictionary[harvest_forecast_date][city_key][dc_key][crop_id_key]= {item_key:expected_order}
                                            std_expected_dictionary[harvest_forecast_date][city_key][dc_key][crop_id_key]= {item_key:std_of_max}
                                            live_dictionary[harvest_forecast_date][city_key][dc_key][crop_id_key]= {item_key:live_order_check}
                                    else:
                                        expected_dictionary[harvest_forecast_date][city_key][dc_key]= {crop_id_key:{item_key:expected_order}}
                                        std_expected_dictionary[harvest_forecast_date][city_key][dc_key]= {crop_id_key:{item_key:std_of_max}}
                                        live_dictionary[harvest_forecast_date][city_key][dc_key]= {crop_id_key:{item_key:live_order_check}}
                                else:
                                    expected_dictionary[harvest_forecast_date][city_key]= {dc_key:{crop_id_key:{item_key:expected_order}}}
                                    std_expected_dictionary[harvest_forecast_date][city_key]= {dc_key:{crop_id_key:{item_key:std_of_max}}} 
                                    live_dictionary[harvest_forecast_date][city_key]= {dc_key:{crop_id_key:{item_key:live_order_check}}}
                            else:
                                expected_dictionary[harvest_forecast_date] = {city_key:{dc_key:{crop_id_key:{item_key:expected_order}}}}    
                                std_expected_dictionary[harvest_forecast_date] = {city_key:{dc_key:{crop_id_key:{item_key:std_of_max}}}}
                                live_dictionary[harvest_forecast_date] = {city_key:{dc_key:{crop_id_key:{item_key:live_order_check}}}}
    
    lsdfs_list_of_lists = [lsdfs_date_list, lsdfs_item_no_list, lsdfs_original_qty_list, lsdfs_sage_customer_id_list, lsdfs_location_code_list, lsdfs_order_number_list]
    
    tuple_to_return = (lsdfs_list_of_lists,expected_dictionary,std_expected_dictionary, live_dictionary)
    
    
    return tuple_to_return


def actualOrderInventoryAllocation(inventory_lists, actual_orders_lists, allocation_date):
    
    # goal: allocate items in the inventory to live orders
    
    # inputs: list of two lists and a datetime: 1) inventory, 2) actual orders, and 3) the date to allocate orders
    # 1) inventory lists: lists with information from the inventory
        # 1. lot code (string)
        # 2. item number (string)
        # 3. quantity (int)
        # 4. original quantity (int)
        # 5. city (sting)
    # 2) actual orders lists: lists with information from the actual sales orders
        # 1. date (datetime)
        # 2. item number (string)
        # 3. original quantity (int)
        # 4. customer ID (string)
        # 5. location name (string)
        # 6. order number (string)
    # 3) allocation date: datetime of the date to allocate the order from inventory
    
    # outputs: list of three lists: 1) allocations to write to InventoryAllocation_V, 2) remainining inventory, and 3) remaining unallocated orders
    # 1) all_tuple_to_insert_list: list of tuples containing seven data entries
        # 1. order date (datetime)
        # 2. item number (string)
        # 3. sage customer id (string)
        # 4. original quantity (int)
        # 5. allocation quantity (int)
        # 6. allocation lot code (sting)
        # 7. order number (string)
    # 2) new2_ccmci_lists: lists of the updated inventory
        # 1. lot code (string)
        # 2. item number (string)
        # 3. quantity (int)
        # 4. original quantity (int)
        # 5. city (sting)
    # 3) new2_order_lists: lists of the updated orders
        # 1. date (datetime)
        # 2. item number (string)
        # 3. original quantity (int)
        # 4. customer ID (string)
        # 5. location name (string)
        # 6. order number (string)
    
    all_tuple_to_insert_list = list()
    
    c_lsdfs_lists  = actual_orders_lists

    lsdfs_date_list = c_lsdfs_lists[0]
    lsdfs_item_no_list = c_lsdfs_lists[1]
    lsdfs_original_qty_list = c_lsdfs_lists[2]
    lsdfs_sage_customer_id_list = c_lsdfs_lists[3]
    lsdfs_location_name_list = c_lsdfs_lists[4]
    lsdfs_order_number_list = c_lsdfs_lists[5]
    
    new_ccmci_lot_code_list= inventory_lists[0]
    new_ccmci_item_number_list = inventory_lists[1]
    new_ccmci_quantity_list = inventory_lists[2]
    new_ccmci_original_quantity_list = inventory_lists[3]
    new_ccmci_city_list = inventory_lists[4]

    new_order_date_list = list()
    new_item_no_list  = list()
    new_original_qty_list  = list()
    new_sage_customer_id_list  = list()
    new_order_number_list = list()

    lsdfs_tomorrow_indices = [i for i, x in enumerate(lsdfs_date_list) if x == allocation_date.date()]

    for lsdfs_idx in lsdfs_tomorrow_indices:

        lsdfs_order_date = lsdfs_date_list[lsdfs_idx]
        lsdfs_item_no = lsdfs_item_no_list[lsdfs_idx]
        lsdfs_original_qty = lsdfs_original_qty_list[lsdfs_idx]
        lsdfs_sage_customer_id = lsdfs_sage_customer_id_list[lsdfs_idx]
        lsdfs_location_name = lsdfs_location_name_list[lsdfs_idx]
        lsdfs_order_number = lsdfs_order_number_list[lsdfs_idx]
        #print(lsdfs_order_number)
        lsdfs_year = lsdfs_order_date.year
        lsdfs_week = lsdfs_order_date.isocalendar()[1]
        lsdfs_weekday = lsdfs_order_date.weekday() # Monday is 0, Sunday is 6
        lsdfs_city = lsdfs_item_no[10:13]
#         print(lsdfs_item_no)
#         print(new_ccmci_item_number_list)
        
        # amount left to allocate
        remaining_allocation = int(lsdfs_original_qty)
        
        if lsdfs_item_no in new_ccmci_item_number_list:
            
            
            # indicies of the items in the inventory (new_ccmci) that match the live sales data order
            indices = [i for i, x in enumerate(new_ccmci_item_number_list) if x == lsdfs_item_no]

#             lccmci_location_name_list = []
            lccmci_lot_code_list = []
            lccmci_item_number_list = []
            lccmci_quantity_list = []
            lccmci_original_quantity_list = []
            lccmci_city_list = []

            lccmci_lot_code_date_list = []

            for idx in indices:
#                 lccmci_location_name_list += [new_ccmci_location_name_list[idx]]
                lccmci_lot_code_list += [new_ccmci_lot_code_list[idx]]
                lccmci_item_number_list += [new_ccmci_item_number_list[idx]]
                lccmci_quantity_list += [new_ccmci_quantity_list[idx]]
                lccmci_original_quantity_list += [new_ccmci_original_quantity_list[idx]]
                lccmci_city_list += [new_ccmci_city_list[idx]]        

                lccmci_lot_code_date_list += [int(new_ccmci_lot_code_list[idx][0:6])]

            # allocate from inventory in order of min to max lot code date
            sorted_idx_lccmci_lot_code_date_list = np.argsort(lccmci_lot_code_date_list)

            sorted_idx_idx = 0
            lccmci_idx = sorted_idx_lccmci_lot_code_date_list[sorted_idx_idx] #index in lccmci of the entry to allocate from
#             next_lccmci_location_name = lccmci_location_name_list[lccmci_idx]
            next_lccmci_lot_code = lccmci_lot_code_list[lccmci_idx]
            next_lccmci_item_number = lccmci_item_number_list[lccmci_idx]
            next_lccmci_quantity = int(lccmci_quantity_list[lccmci_idx])    
            next_lccmci_original_quantity = lccmci_original_quantity_list[lccmci_idx]
            next_lccmci_city = lccmci_city_list[lccmci_idx]

            # index in new_ccmci of the entry to allocate from
            new_ccmci_idx = indices[lccmci_idx]

            updated_lccmci_idx_list = []
            updated_lccmci_quantity_list = []
            lsdfs_allocation_lot_code_list = []
            lsdfs_allocation_quantity_list = []

            while remaining_allocation > 0:
                old_remaining_allocation = remaining_allocation
                # allocate inventory item to LiveSalesData order
                remaining_allocation = old_remaining_allocation - next_lccmci_quantity
                if remaining_allocation == 0:
                    inventory_after_allocation = 0
                    #print('Remaining inventory: ' + str(inventory_after_allocation))                
#                     del new_ccmci_location_name_list[new_ccmci_idx]
#                     del new_ccmci_lot_code_list[new_ccmci_idx]
#                     del new_ccmci_item_number_list[new_ccmci_idx]
#                     del new_ccmci_quantity_list[new_ccmci_idx]
#                     del new_ccmci_original_quantity_list[new_ccmci_idx]
#                     del new_ccmci_city_list[new_ccmci_idx]
                    new_ccmci_quantity_list[new_ccmci_idx] = 0
                    updated_lccmci_idx_list += [lccmci_idx]
                    updated_lccmci_quantity_list += [inventory_after_allocation]
                    lsdfs_allocation_lot_code_list += [next_lccmci_lot_code]
                    lsdfs_allocation_quantity_list += [next_lccmci_quantity]   
                if remaining_allocation < 0: # if there is more in the inventory than what is needed to allocate
                    inventory_after_allocation = -remaining_allocation
                    #print('Remaining inventory: ' + str(inventory_after_allocation))
                    new_ccmci_quantity_list[new_ccmci_idx] = inventory_after_allocation
                    updated_lccmci_idx_list += [lccmci_idx]
                    updated_lccmci_quantity_list += [inventory_after_allocation]
                    lsdfs_allocation_lot_code_list += [next_lccmci_lot_code]
                    lsdfs_allocation_quantity_list += [old_remaining_allocation]
                if remaining_allocation > 0: # if we need to allocate more from inventory or harvest to fulfill the order
                    inventory_after_allocation = 0
                    #print('Remaining inventory: ' + str(inventory_after_allocation))
                    #new_ccmci_quantity_list[new_ccmci_idx] = inventory_after_allocation
#                     del new_ccmci_location_name_list[new_ccmci_idx]
#                     del new_ccmci_lot_code_list[new_ccmci_idx]
#                     del new_ccmci_item_number_list[new_ccmci_idx]
#                     del new_ccmci_quantity_list[new_ccmci_idx]
#                     del new_ccmci_original_quantity_list[new_ccmci_idx]
#                     del new_ccmci_city_list[new_ccmci_idx]
                    new_ccmci_quantity_list[new_ccmci_idx] = 0
                    sorted_idx_idx += 1
                    updated_lccmci_idx_list += [lccmci_idx]
                    updated_lccmci_quantity_list += [inventory_after_allocation]
                    lsdfs_allocation_lot_code_list += [next_lccmci_lot_code]
                    lsdfs_allocation_quantity_list += [next_lccmci_quantity]                
                    # if there are other inventory items with different lot codes, allocate those in the next while loop iteration
                    if sorted_idx_idx < len(sorted_idx_lccmci_lot_code_date_list):
                        #print('Remaining to allocate from inventory: ' + str(remaining_allocation))
                        lccmci_idx = sorted_idx_lccmci_lot_code_date_list[sorted_idx_idx]
                        new_ccmci_idx = indices[lccmci_idx]
#                         next_lccmci_location_name = lccmci_location_name_list[lccmci_idx]
                        next_lccmci_lot_code = lccmci_lot_code_list[lccmci_idx]
                        next_lccmci_item_number = lccmci_item_number_list[lccmci_idx]
                        next_lccmci_quantity = int(lccmci_quantity_list[lccmci_idx]) 
                        next_lccmci_original_quantity = lccmci_original_quantity_list[lccmci_idx]
                        next_lccmci_city = lccmci_city_list[lccmci_idx]                    
                    # else when there is nothing in inventory left to allocate, allocate from greenhouse lines
                    else:
                        #print('Remaining to allocate from greenhouse: ' + str(remaining_allocation))

                        new_order_date_list += [lsdfs_order_date]
                        new_item_no_list  += [lsdfs_item_no]
                        new_original_qty_list += [remaining_allocation]
                        new_sage_customer_id_list  += [lsdfs_sage_customer_id]
                        new_order_number_list += [lsdfs_order_number]

                        remaining_allocation = 0

            for idx in range(len(lsdfs_allocation_lot_code_list)):
                lsdfs_allocation_lot_code = lsdfs_allocation_lot_code_list[idx]
                lsdfs_allocation_quantity = lsdfs_allocation_quantity_list[idx]
                if lsdfs_allocation_quantity != 0 and 'LETT' not in lsdfs_item_no:
                    tuple_to_insert = (lsdfs_order_date,lsdfs_item_no,lsdfs_sage_customer_id,lsdfs_original_qty, lsdfs_allocation_quantity,lsdfs_allocation_lot_code,lsdfs_order_number)
                    all_tuple_to_insert_list += [tuple_to_insert]
        else:
            #print('Remaining to allocate from greenhouse: ' + str(remaining_allocation))

            new_order_date_list += [lsdfs_order_date]
            new_item_no_list  += [lsdfs_item_no]
            new_original_qty_list += [remaining_allocation]
            new_sage_customer_id_list  += [lsdfs_sage_customer_id]
            new_order_number_list += [lsdfs_order_number]

            remaining_allocation = 0
            
    new2_ccmci_lists = [new_ccmci_lot_code_list, new_ccmci_item_number_list, new_ccmci_quantity_list, new_ccmci_original_quantity_list, new_ccmci_city_list]

    new2_order_lists = [new_order_date_list, new_item_no_list, new_original_qty_list, new_sage_customer_id_list, new_order_number_list]
    
    return [all_tuple_to_insert_list,new2_ccmci_lists,new2_order_lists]


def expectedOrderInventoryAllocation(inventory_lists, expected_orders_dict, order_lists, allocation_date):
        
    # goal: allocate items in the inventory to predicted orders
    
    # inputs: list of three lists and a datetime: 1) inventory, 2) expected orders, 3) actual orders, and 4) the date to allocate orders
    # 1) inventory lists: lists with information from the inventory
        # 1. lot code (string)
        # 2. item number (string)
        # 3. quantity (int)
        # 4. original quantity (int)
        # 5. city (sting)
    # 2) expected_dictionary[harvest_forecast_date][city_key][dc_key][crop_id_key][item_key] = expected_order
        # 1. harvest date (datetime)
        # 2. city (string)
        # 3. distribution center (string)
        # 4. crop id (int)
        # 5. item number (string)
    # 3) actual orders lists: lists with information from the actual sales orders
        # 1. date (datetime)
        # 2. item number (string)
        # 3. original quantity (int)
        # 4. customer ID (string)
        # 5. location name (string)
        # 6. order number (string)
    # 4) allocation date: datetime of the date to allocate the order from inventory
    
    # outputs: list of three lists: 1) allocations to write to InventoryAllocation_V, 2) remainining inventory, and 3) remaining unallocated orders
    # 1) all_tuple_to_insert_list: list of tuples containing seven data entries
        # 1. order date (datetime)
        # 2. item number (string)
        # 3. sage customer id (string)
        # 4. original quantity (int)
        # 5. allocation quantity (int)
        # 6. allocation lot code (sting)
        # 7. order number (string 'prediction')
    # 2) new2_ccmci_lists: lists of the updated inventory
        # 1. lot code (string)
        # 2. item number (string)
        # 3. quantity (int)
        # 4. original quantity (int)
        # 5. city (sting)
    # 3) new2_order_lists: lists of the updated orders
        # 1. date (datetime)
        # 2. item number (string)
        # 3. original quantity (int)
        # 4. customer ID (string)
        # 5. location name (string)
        # 6. order number (string 'prediction')
    

    
    all_tuple_to_insert_list = list()
    
    #ew_ccmci_location_name_list = inventory_lists[0]
    new_ccmci_lot_code_list= inventory_lists[0]
    new_ccmci_item_number_list = inventory_lists[1]
    new_ccmci_quantity_list = inventory_lists[2]
    new_ccmci_original_quantity_list = inventory_lists[3]
    new_ccmci_city_list = inventory_lists[4]
    
    new_expected_orders_dict = expected_orders_dict
    
    new_order_date_list = order_lists[0]
    new_item_no_list  = order_lists[1]
    new_original_qty_list  = order_lists[2]
    new_sage_customer_id_list  = order_lists[3]
    new_order_number_list = order_lists[4]
    
    # allocate additional expected orders
    expected_order_number = 'prediction'

  
    first_key = allocation_date
    expected_order_date = first_key
    if first_key in expected_orders_dict.keys():
        for second_key in expected_orders_dict[first_key].keys():
            for third_key in expected_orders_dict[first_key][second_key].keys():
                expected_sage_customer_id = third_key
                for fourth_key in expected_orders_dict[first_key][second_key][third_key].keys():
                    expected_crop_id = fourth_key
                    for fifth_key in expected_orders_dict[first_key][second_key][third_key][fourth_key].keys():
                        expected_item_number = fifth_key
                        expected_original_qty = int(expected_orders_dict[first_key][second_key][third_key][fourth_key][fifth_key])
                        if expected_original_qty != 0:
                            # amount left to allocate
                            remaining_allocation = int(expected_original_qty)
                            if expected_item_number in new_ccmci_item_number_list:
                                # indicies of the items in the inventory (new_ccmci) that match the expected order
                                indices = [i for i, x in enumerate(new_ccmci_item_number_list) if x == expected_item_number]

        #                         lccmci_location_name_list = []
                                lccmci_lot_code_list = []
                                lccmci_item_number_list = []
                                lccmci_quantity_list = []
                                lccmci_original_quantity_list = []
                                lccmci_city_list = []

                                lccmci_lot_code_date_list = []

                                for idx in indices:
        #                             lccmci_location_name_list += [new_ccmci_location_name_list[idx]]
                                    lccmci_lot_code_list += [new_ccmci_lot_code_list[idx]]
                                    lccmci_item_number_list += [new_ccmci_item_number_list[idx]]
                                    lccmci_quantity_list += [new_ccmci_quantity_list[idx]]
                                    lccmci_original_quantity_list += [new_ccmci_original_quantity_list[idx]]
                                    lccmci_city_list += [new_ccmci_city_list[idx]]        

                                    lccmci_lot_code_date_list += [int(new_ccmci_lot_code_list[idx][0:6])]

                                # allocate from inventory in order of min to max lot code date
                                sorted_idx_lccmci_lot_code_date_list = np.argsort(lccmci_lot_code_date_list)



                                sorted_idx_idx = 0
                                lccmci_idx = sorted_idx_lccmci_lot_code_date_list[sorted_idx_idx] #index in lccmci of the entry to allocate from
        #                         next_lccmci_location_name = lccmci_location_name_list[lccmci_idx]
                                next_lccmci_lot_code = lccmci_lot_code_list[lccmci_idx]
                                next_lccmci_item_number = lccmci_item_number_list[lccmci_idx]
                                next_lccmci_quantity = int(lccmci_quantity_list[lccmci_idx])    
                                next_lccmci_original_quantity = lccmci_original_quantity_list[lccmci_idx]
                                next_lccmci_city = lccmci_city_list[lccmci_idx]

                                # index in new_ccmci of the entry to allocate from
                                new_ccmci_idx = indices[lccmci_idx]
                                updated_lccmci_idx_list = []
                                updated_lccmci_quantity_list = []
                                expected_allocation_lot_code_list = []
                                expected_allocation_quantity_list = []

                                while remaining_allocation > 0:
                                    old_remaining_allocation = remaining_allocation
                                    # allocate inventory item to LiveSalesData order
                                    remaining_allocation = remaining_allocation - next_lccmci_quantity
                                    if remaining_allocation == 0:
                                        inventory_after_allocation = 0
                                        #print('Remaining inventory: ' + str(inventory_after_allocation))                
        #                                 del new_ccmci_location_name_list[new_ccmci_idx]
    #                                     del new_ccmci_lot_code_list[new_ccmci_idx]
    #                                     del new_ccmci_item_number_list[new_ccmci_idx]
    #                                     del new_ccmci_quantity_list[new_ccmci_idx]
    #                                     del new_ccmci_original_quantity_list[new_ccmci_idx]
    #                                     del new_ccmci_city_list[new_ccmci_idx]
                                        new_ccmci_quantity_list[new_ccmci_idx] = 0
                                        updated_lccmci_idx_list += [lccmci_idx]
                                        updated_lccmci_quantity_list += [inventory_after_allocation]
                                        expected_allocation_lot_code_list += [next_lccmci_lot_code]
                                        expected_allocation_quantity_list += [next_lccmci_quantity]   
                                    if remaining_allocation < 0: # if there is more in the inventory than what is needed to allocate
                                        inventory_after_allocation = -remaining_allocation
                                        #print('Remaining inventory: ' + str(inventory_after_allocation))
    #                                    new_ccmci_quantity_list[new_ccmci_idx] = inventory_after_allocation
                                        updated_lccmci_idx_list += [lccmci_idx]
                                        updated_lccmci_quantity_list += [inventory_after_allocation]
                                        expected_allocation_lot_code_list += [next_lccmci_lot_code]
                                        expected_allocation_quantity_list += [old_remaining_allocation]
                                    if remaining_allocation > 0: # if we need to allocate more from inventory or harvest to fulfill the order
                                        inventory_after_allocation = 0
                                        #print('Remaining inventory: ' + str(inventory_after_allocation))
                                        #new_ccmci_quantity_list[new_ccmci_idx] = inventory_after_allocation
        #                                 del new_ccmci_location_name_list[new_ccmci_idx]
    #                                     del new_ccmci_lot_code_list[new_ccmci_idx]
    #                                     del new_ccmci_item_number_list[new_ccmci_idx]
    #                                     del new_ccmci_quantity_list[new_ccmci_idx]
    #                                     del new_ccmci_original_quantity_list[new_ccmci_idx]
    #                                     del new_ccmci_city_list[new_ccmci_idx]
                                        new_ccmci_quantity_list[new_ccmci_idx] = 0
                                        sorted_idx_idx += 1
                                        updated_lccmci_idx_list += [lccmci_idx]
                                        updated_lccmci_quantity_list += [inventory_after_allocation]
                                        expected_allocation_lot_code_list += [next_lccmci_lot_code]
                                        expected_allocation_quantity_list += [next_lccmci_quantity]                
                                        # if there are other inventory items with different lot codes, allocate those in the next while loop iteration
                                        if sorted_idx_idx < len(sorted_idx_lccmci_lot_code_date_list):
                                            #print('Remaining to allocate from inventory: ' + str(remaining_allocation))
                                            lccmci_idx = sorted_idx_lccmci_lot_code_date_list[sorted_idx_idx]
                                            new_ccmci_idx = indices[lccmci_idx]
                                            #next_lccmci_location_name = lccmci_location_name_list[lccmci_idx]
                                            next_lccmci_lot_code = lccmci_lot_code_list[lccmci_idx]
                                            next_lccmci_item_number = lccmci_item_number_list[lccmci_idx]
                                            next_lccmci_quantity = int(lccmci_quantity_list[lccmci_idx]) 
                                            next_lccmci_original_quantity = lccmci_original_quantity_list[lccmci_idx]
                                            next_lccmci_city = lccmci_city_list[lccmci_idx]                    
                                        # else when there is nothing in inventory left to allocate, allocate from greenhouse lines
                                        else:
    #                                        print('Remaining to allocate from greenhouse: ' + str(remaining_allocation))

                                            new_order_date_list += [expected_order_date]
                                            new_item_no_list  += [expected_item_number]
                                            new_original_qty_list += [remaining_allocation]
                                            new_sage_customer_id_list  += [expected_sage_customer_id]
                                            new_order_number_list += [expected_order_number]

                                            remaining_allocation = 0

                                for idx in range(len(expected_allocation_lot_code_list)):
                                    expected_allocation_lot_code = expected_allocation_lot_code_list[idx]
                                    expected_allocation_quantity = expected_allocation_quantity_list[idx]
                                    if expected_allocation_quantity != 0 and 'LETT' not in expected_item_number:
                                        tuple_to_insert = (expected_order_date,expected_item_number,expected_sage_customer_id,expected_original_qty, expected_allocation_quantity,expected_allocation_lot_code,expected_order_number)
                                        all_tuple_to_insert_list += [tuple_to_insert]
                            else:
    #                            print('Remaining to allocate from greenhouse: ' + str(remaining_allocation))

                                new_order_date_list += [expected_order_date]
                                new_item_no_list  += [expected_item_number]
                                new_original_qty_list += [remaining_allocation]
                                new_sage_customer_id_list  += [expected_sage_customer_id]
                                new_order_number_list += [expected_order_number]

                                remaining_allocation = 0

    new_ccmci_lists2 = [new_ccmci_lot_code_list, new_ccmci_item_number_list, new_ccmci_quantity_list, new_ccmci_original_quantity_list, new_ccmci_city_list]
    new_order_lists2 = [new_order_date_list, new_item_no_list, new_original_qty_list, new_sage_customer_id_list, new_order_number_list]
    
    return [all_tuple_to_insert_list,new_ccmci_lists2,new_order_lists2]


def lastMatchingValue(a,b):
    # goal: compute the last matching value for two lists
    
    # input: two lists of integers
    # a: list of integers
    # b: list of integers
    
    # output: integer value of the last match that exists in both lists (or -1 if there is no match)

    a.reverse()
    b.reverse()

    match_idx = -1

    a_idx = 0
    while a_idx < len(a):
        av = a[a_idx]
        b_idx = 0
        while b_idx < len(b):
            bv = b[b_idx]
            if av == bv:
                match_idx = av
                a_idx = len(a)
                b_idx = len(b)
            else:
                b_idx += 1
        a_idx += 1
    
    return match_idx
                                    
def harvestAllocation(new_order_lists,expected_harvest_dict_list, sort_metric_dict_list, allocation_class, allocation_date):
    
    # goal: compute crop allocations from each line to fulfill orders
    
    # input: order lists, harvest dictionary, yield metric dictionaries, allocation class, and allocation date
        # 1. new_order_lists: list of 5 lists for new orders
            # 1. order date (datetime)
            # 2. item no (string)
            # 3. original qty (int)
            # 4. sage customer id (string)
            # 5. order number(string)
        # 2. expected_harvest_dict_list: list of 3 dictionaries for the expected harvest to allocate from
            # 1. expected_ps_dict[harvest_date][facility_line][crop_id] = total_plant_sites
            # 2. expected_whole_plant_biomass_dict[harvest_date][facility_line][crop_id] = expected_whole_plant_biomass (g)
            # 3. expected_loose_plant_biomass_dict[harvest_date][facility_line][crop_id] = expected_loose_plant_biomass (g)
        # 3. sort_metric_dict_list: list of 2 dictionaries
            # 1. avg_headweight_dict[facility_line][crop_id][year_week] = [avg_headweight, avg_headweight2,...]
            # 2. pspc_dict[facility_line][crop_id][year_week] = [pspc1, pspc2,...]
        # 4. allocation_class: int (1-6) cooresponding to allocation class        
        # 5. allocation_date: datetime cooresponding to allocation date
    
    # output: list of three lists: 1) harvest allocation for harvestAllocation_V, 2) remaining plant sites, and 3) remaining orders
    # 1) all_tuple_to_insert_list: list of tuples of harvest allocations
        # 1. order date (datetime)
        # 2. item number (string)
        # 3. sage customer id (string)
        # 4. original qty (int)
        # 5. allocation qty (int)
        # 6. allocation lot code (string)
        # 7. order number (string)
    # 2) new_expected_ps_dict: dictionary of remaining expected plant sites that are left after allocation
        # expected_ps_dict[harvest_date][facility_line][crop_id] = remaining plant sites
    # 3) new_order_lists: lists cooresponding to outstanding orders
        # 1. new_order_date_list ( list of datetimes)
        # 2. new_item_no_list (list of strings)
        # 3. new_original_qty_list (list of ints)
        # 4. new_sage_customer_id_list (list of strings)
        # 5. new_order_number_list (list of strings)
    
    date_tomorrow = allocation_date
    
    all_tuple_to_insert_list = list()

    new_order_date_list = new_order_lists[0]
    new_item_no_list  = new_order_lists[1]
    new_original_qty_list  = new_order_lists[2]
    new_sage_customer_id_list  = new_order_lists[3]
    new_order_number_list = new_order_lists[4]

    expected_ps_dict = expected_harvest_dict_list[0]
    expected_whole_plant_biomass_dict = expected_harvest_dict_list[1]
    expected_loose_plant_biomass_dict = expected_harvest_dict_list[2]

    avg_headweight_dict = sort_metric_dict_list[0]
    pspc_dict = sort_metric_dict_list[1]


    if allocation_class == 1:    
        expected_harvest_dict = expected_whole_plant_biomass_dict
        sort_metric_dict = avg_headweight_dict
        unit_str = 'whole grams'
    if allocation_class == 2:
        expected_harvest_dict = expected_ps_dict
        sort_metric_dict = avg_headweight_dict
        unit_str = 'plant sites'
    if allocation_class > 2:
        expected_harvest_dict = expected_loose_plant_biomass_dict
        sort_metric_dict = pspc_dict
        unit_str = 'loose grams'


    new_expected_ps_dict = expected_ps_dict
    new_expected_harvest_dict = expected_harvest_dict
    
    # list of orders remaining to allocate if we run out of crops at the facility
    new2_order_date_list = list()
    new2_item_no_list  = list()
    new2_original_qty_list  = list()
    new2_sage_customer_id_list  = list()
    new2_order_number_list = list()

    updated_facility_lines_list = list()
    updated_crop_id_list = list()
    updated_total_harvest_list = list()

    # loop through order list after inventory allocation
    # allocate starting from the smallest line with average headweight > 90 g
        
    for order_idx in range(len(new_order_date_list)):
        order_date = new_order_date_list[order_idx]
        item_no = new_item_no_list[order_idx]
        original_qty = new_original_qty_list[order_idx]
        sage_customer_id = new_sage_customer_id_list[order_idx]
        order_number = new_order_number_list[order_idx]

        order_tuple = (order_date, item_no, sage_customer_id, original_qty, order_number)
        
        city = item_no[10:13]

        city_sort_metrics = []
        city_facility_lines = []
        if item_no not in spd_item_no_list:
            print(item_no + 'not in SageProductsDim')
        conversion_factor = 1 # let expected_order_packed_weight = original_qty if no conversion_factor exists
        if item_no in spd_item_no_list:
            conversion_factor = float(spd_packed_weight_conversion_grams_list[spd_item_no_list.index(item_no)])
        
        expected_order_packed_weight = float(original_qty * conversion_factor)

        sage_crop_code = item_no[3:7]
        crop_id = crop_id_list[sage_crop_code_list.index(sage_crop_code)]

        print('Order for ' + item_no + ': '+ str(original_qty) + ' qty (' + str(int(expected_order_packed_weight)) + ' ' + unit_str + ')')

        ##### Here we define in what order to select lines for a city
        # currently set to pull starting from the smallest average headweight or pspc
        # whole plant food service allocated from smallest to largest average headweight
        # whole headcount allocated from smallest to largest average headweight starting above 90g (min size req. for baby butterhead)
        # loose plant food service allocated from smallest to largest plant sites per clam (biggest plants first)
        # loop through each facility_line
        for facility_line_key in sort_metric_dict:
            key_city = facility_line_key[0:3]
            # if the facility_line is in the city under evaluation
            if key_city == city:
                selected_facility_line_crop_id_list = list(sort_metric_dict[facility_line_key].keys())
                for idx in range(len(selected_facility_line_crop_id_list)):
                    if selected_facility_line_crop_id_list[idx] == crop_id:
                        #last_sort_metric = np.mean(list(sort_metric_dict[facility_line_key][selected_facility_line_crop_id_list[idx]].values())[-1])
                        last_sort_metric = list(sort_metric_dict[facility_line_key][selected_facility_line_crop_id_list[idx]].values())[-1][-1]
                        if last_sort_metric != 0:
                            city_sort_metrics += [last_sort_metric]
                            city_facility_lines += [facility_line_key]
                        else:
                            last_sort_metric = list(sort_metric_dict[facility_line_key][selected_facility_line_crop_id_list[idx]].values())[-1][-2]
                            city_sort_metrics += [last_sort_metric]
                            city_facility_lines += [facility_line_key]                            
                            
                        #print(city_sort_metrics)
        sort_index = np.argsort(city_sort_metrics)

        sorted_city_facility_lines = []
        sorted_city_sort_metrics = []
        for idx in sort_index:
            facility_line = city_facility_lines[idx]
            sort_metric = city_sort_metrics[idx]
            sorted_city_facility_lines += [facility_line]
            sorted_city_sort_metrics += [sort_metric]
            #print(facility_line + ' has average headweight of ' + str(sort_metric))
        #print(sorted_city_facility_lines)

        idx_next = 0

        if item_no[3:10] == 'BTHDBBY':
            # average headweight >= 90 g to use for baby butterhead 
            idx_next = np.nonzero(np.array(sorted_city_sort_metrics) >= 90)[0][0]

        facility_line_idx = idx_next-1

        remaining_allocation = expected_order_packed_weight
        
        while remaining_allocation > 0:    
            facility_line_idx += 1

            if facility_line_idx < len(sorted_city_facility_lines):
                facility_line_next = sorted_city_facility_lines[facility_line_idx]
                sort_metric_next = float(sorted_city_sort_metrics[facility_line_idx]) # whole g/PS
                if allocation_class > 2: # loose g/PS
                    g_per_clam = 128
                    if crop_id == 1:
                        g_per_clam = 114 # arugula
                    if crop_id == 3:
                        g_per_clam = 35.4 # basil
                    sort_metric_next = 1/sort_metric_next * g_per_clam
                if item_no[3:10] == 'BTHDBBY':
                    sort_metric_next = 1
                
                if date_tomorrow in new_expected_harvest_dict.keys():

                    if facility_line_next in new_expected_harvest_dict[date_tomorrow]:
                        crop_id_keys = new_expected_harvest_dict[date_tomorrow][facility_line_next].keys()
                        if crop_id in crop_id_keys:

                            expected_harvest_from_next = new_expected_harvest_dict[date_tomorrow][facility_line_next][crop_id]
                            if facility_line_next in updated_facility_lines_list and crop_id in updated_crop_id_list:
                                matching_facility_line_indices = [i for i, x in enumerate(updated_facility_lines_list) if x == facility_line_next]
                                matching_crop_id_indices = [i for i, x in enumerate(updated_crop_id_list) if x == crop_id]
                                last_facility_line_crop_id_match_idx = lastMatchingValue(matching_facility_line_indices,matching_crop_id_indices)
                                expected_harvest_from_next = updated_total_harvest_list[last_facility_line_crop_id_match_idx]

                            expected_ps_from_next = int(expected_harvest_from_next/sort_metric_next)

                            if expected_ps_from_next != 0:
                                print(facility_line_next + ' total starting '+ sage_crop_code + ' plant sites: ' + str(expected_ps_from_next) + '(' + str(expected_harvest_from_next)+ ' ' + unit_str + ')')
                                remaining_harvest = float(expected_harvest_from_next) - remaining_allocation

                                if remaining_harvest >= 0: # we expect more remaining harvest than we need to allocate for the order
                                    remaining_allocation_ps = int(remaining_allocation/sort_metric_next)
                                    remaining_harvest_ps = int(remaining_harvest/sort_metric_next)

                                    print("Pack " + sage_crop_code + " from " + facility_line_next + " to " + sage_customer_id + "(" + order_number + "): " + str(remaining_allocation_ps) + ' PS '+ '(' + str(int(remaining_allocation)) +' '+ unit_str + ')')

        #                            print("Remaining plant sites to allocate from " + facility_line_next + ": " + str(remaining_harvest_ps))

                                    tuple_to_insert = order_tuple + (remaining_allocation_ps, facility_line_next.split('_')[0], facility_line_next.split('_')[1],facility_line_next, crop_id)
                                    all_tuple_to_insert_list += [tuple_to_insert]

                                    new_expected_harvest_dict[date_tomorrow][facility_line_next][crop_id] = remaining_harvest                                                    
                                    new_expected_ps_dict[date_tomorrow][facility_line_next][crop_id] = remaining_harvest_ps

                                    updated_facility_lines_list += [facility_line_next]
                                    updated_crop_id_list += [crop_id]
                                    updated_total_harvest_list += [remaining_harvest]
                                    remaining_allocation = 0

                                else:
                                    expected_harvest_from_next_ps = int(expected_harvest_from_next/sort_metric_next)
                                    #print("Pack " + str(expected_harvest_from_next_ps) + '(' + str(expected_harvest_from_next) + ' ' + unit_str + ') plant sites of ' + sage_crop_code + " from " + facility_line_next)    
                                    print("Pack " + sage_crop_code + " from " + facility_line_next + " to " + sage_customer_id + "(" + order_number + "): " + str(expected_ps_from_next) + ' PS '+ '(' + str(int(expected_harvest_from_next)) +' '+ unit_str + ')')
        #                            print("Remaining plant sites to allocate from " + facility_line_next + ": " + str(0))

                                    tuple_to_insert = order_tuple + (expected_harvest_from_next_ps, facility_line_next.split('_')[0], facility_line_next.split('_')[1],facility_line_next, crop_id)
                                    all_tuple_to_insert_list += [tuple_to_insert]

                                    new_expected_harvest_dict[date_tomorrow][facility_line_next][crop_id] = 0                                                                                                        
                                    new_expected_ps_dict[date_tomorrow][facility_line_next][crop_id] = 0 

                                    updated_facility_lines_list += [facility_line_next]
                                    updated_crop_id_list += [crop_id]
                                    updated_total_harvest_list += [0]

                                    remaining_allocation = -remaining_harvest
            else:
                # if we run out of facility_lines, we need to allocate crop from elsewhere
                if sage_crop_code != 'LETT':
                    print('Outstanding order for ' + sage_crop_code + ' in ' + city + ': ' + str(remaining_allocation) + ' ' + unit_str)
                    new2_order_date_list += [order_date]
                    new2_item_no_list += [item_no]
                    #new2_original_qty_list  += [original_qty]
                    new2_original_qty_list  += [int(np.ceil(remaining_allocation/ conversion_factor))]
                    new2_sage_customer_id_list += [sage_customer_id]
                    new2_order_number_list += [order_number]

                remaining_allocation = 0
                
    new_order_lists = [new2_order_date_list, new2_item_no_list, new2_original_qty_list, new2_sage_customer_id_list, new2_order_number_list]
    
    return [all_tuple_to_insert_list,new_expected_ps_dict,new_order_lists] 



def harvestAllocationFromGMED(new_order_lists,expected_harvest_dict_list, sort_metric_dict_list, allocation_class, allocation_date):
    
    
    # goal: compute crop allocations from GMED lines to fulfill orders for allocation class 1-4
    
    # input: order lists, harvest dictionary, yield metric dictionaries, allocation class, and allocation date
        # 1. new_order_lists: list of 5 lists for new orders
            # 1. order date (datetime)
            # 2. item no (string)
            # 3. original qty (int)
            # 4. sage customer id (string)
            # 5. order number(string)
        # 2. expected_harvest_dict_list: list of 3 dictionaries for the expected harvest to allocate from
            # 1. expected_ps_dict[harvest_date][facility_line][crop_id] = total_plant_sites
            # 2. expected_whole_plant_biomass_dict[harvest_date][facility_line][crop_id] = expected_whole_plant_biomass (g)
            # 3. expected_loose_plant_biomass_dict[harvest_date][facility_line][crop_id] = expected_loose_plant_biomass (g)
        # 3. sort_metric_dict_list: list of 2 dictionaries
            # 1. avg_headweight_dict[facility_line][crop_id][year_week] = [avg_headweight, avg_headweight2,...]
            # 2. pspc_dict[facility_line][crop_id][year_week] = [pspc1, pspc2,...]
        # 4. allocation_class: int (1-6) cooresponding to allocation class        
        # 5. allocation_date: datetime cooresponding to allocation date
    
    # output: list of three lists: 1) harvest allocation for harvestAllocation_V, 2) remaining plant sites, and 3) remaining orders
    # 1) all_tuple_to_insert_list: list of tuples of harvest allocations
        # 1. order date (datetime)
        # 2. item number (string)
        # 3. sage customer id (string)
        # 4. original qty (int)
        # 5. allocation qty (int)
        # 6. allocation lot code (string)
        # 7. order number (string)
    # 2) new_expected_ps_dict: dictionary of remaining expected plant sites that are left after allocation
        # expected_ps_dict[harvest_date][facility_line][crop_id] = remaining plant sites
    # 3) new_order_lists: lists cooresponding to outstanding orders
        # 1. new_order_date_list ( list of datetimes)
        # 2. new_item_no_list (list of strings)
        # 3. new_original_qty_list (list of ints)
        # 4. new_sage_customer_id_list (list of strings)
        # 5. new_order_number_list (list of strings)
    
    all_tuple_to_insert_list = list()

    new_order_date_list = new_order_lists[0]
    new_item_no_list  = new_order_lists[1]
    new_original_qty_list  = new_order_lists[2]
    new_sage_customer_id_list  = new_order_lists[3]
    new_order_number_list = new_order_lists[4]

    expected_ps_dict = expected_harvest_dict_list[0]
    expected_whole_plant_biomass_dict = expected_harvest_dict_list[1]
    expected_loose_plant_biomass_dict = expected_harvest_dict_list[2]

    avg_headweight_dict = sort_metric_dict_list[0]
    pspc_dict = sort_metric_dict_list[1]

    date_tomorrow = allocation_date

    if allocation_class == 1:    
        expected_harvest_dict = expected_whole_plant_biomass_dict
        sort_metric_dict = avg_headweight_dict
        unit_str = 'whole grams'
    if allocation_class == 2:
        expected_harvest_dict = expected_ps_dict
        sort_metric_dict = avg_headweight_dict
        unit_str = 'plant sites'
    if allocation_class > 2:
        expected_harvest_dict = expected_loose_plant_biomass_dict
        sort_metric_dict = pspc_dict
        unit_str = 'loose grams'


    new_expected_ps_dict = expected_ps_dict
    new_expected_harvest_dict = expected_harvest_dict
    
    # list of orders remaining to allocate if we run out of crops at the facility
    new2_order_date_list = list()
    new2_item_no_list  = list()
    new2_original_qty_list  = list()
    new2_sage_customer_id_list  = list()
    new2_order_number_list = list()

    updated_facility_lines_list = list()
    updated_crop_id_list = list()
    updated_total_harvest_list = list()

    # loop through order list

    for order_idx in range(len(new_order_date_list)):
        order_date = new_order_date_list[order_idx]
        item_no = new_item_no_list[order_idx]
        original_qty = new_original_qty_list[order_idx]
        sage_customer_id = new_sage_customer_id_list[order_idx]
        order_number = new_order_number_list[order_idx]

        order_tuple = (order_date, item_no, sage_customer_id, original_qty, order_number)
        
        city = item_no[10:13]

        city_sort_metrics = []
        city_facility_lines = []
        
        if item_no not in spd_item_no_list:
            print(item_no + 'not in SageProductsDim')
        conversion_factor = 1 # let expected_order_packed_weight = original_qty if no conversion_factor exists
        if item_no in spd_item_no_list:
            conversion_factor = float(spd_packed_weight_conversion_grams_list[spd_item_no_list.index(item_no)])
        
        expected_order_packed_weight = float(original_qty * conversion_factor)

        sage_crop_code = item_no[3:7]
        crop_id = crop_id_list[sage_crop_code_list.index(sage_crop_code)]

        print('Order for ' + item_no + ': '+ str(original_qty) + ' qty (' + str(int(expected_order_packed_weight)) + ' ' + unit_str + ')')

        ##### Here we define how in what order to select lines for a city


        # For GMED pull, we want to select from the gmed lines in the city
        gmed_crop_id = 9

        # whole plant food service allocated from smallest to largest average headweight
        # whole headcount allocated from smallest to largest average headweight starting above 90g (min size req. for baby butterhead)
        # loose plant food service allocated from smallest to largest plant sites per clam (biggest plants first)
        # loop through each facility_line
        for facility_line_key in sort_metric_dict:
            key_city = facility_line_key[0:3]
            # if the facility_line is in the city under evaluation
            if key_city == city:
                selected_facility_line_crop_id_list = list(sort_metric_dict[facility_line_key].keys())
                for idx in range(len(selected_facility_line_crop_id_list)):
                    if selected_facility_line_crop_id_list[idx] == gmed_crop_id:
                        #last_sort_metric = np.mean(list(sort_metric_dict[facility_line_key][selected_facility_line_crop_id_list[idx]].values())[-1])
                        last_sort_metric = list(sort_metric_dict[facility_line_key][selected_facility_line_crop_id_list[idx]].values())[-1][-1]
                        if last_sort_metric != 0:
                            city_sort_metrics += [last_sort_metric]
                            city_facility_lines += [facility_line_key]
                            #print(city_sort_metrics)
                        else:
                            last_sort_metric = list(sort_metric_dict[facility_line_key][selected_facility_line_crop_id_list[idx]].values())[-1][-2]
                            city_sort_metrics += [last_sort_metric]
                            city_facility_lines += [facility_line_key]    
        sort_index = np.argsort(city_sort_metrics)

        # sorted lists of facility lines and sort metrics to pull from
        sorted_city_facility_lines = []
        sorted_city_sort_metrics = []
        for idx in sort_index:
            facility_line = city_facility_lines[idx]
            sort_metric = city_sort_metrics[idx]
            sorted_city_facility_lines += [facility_line]
            sorted_city_sort_metrics += [sort_metric]
            #print(facility_line + ' has average headweight of ' + str(sort_metric))
        #print(sorted_city_facility_lines)


        #####################
        
        
        idx_next = 0

        facility_line_idx = idx_next-1

        remaining_allocation = expected_order_packed_weight
        
        while remaining_allocation > 0:    
            facility_line_idx += 1

            if facility_line_idx < len(sorted_city_facility_lines):
                facility_line_next = sorted_city_facility_lines[facility_line_idx]
                sort_metric_next = float(sorted_city_sort_metrics[facility_line_idx]) # whole g/PS
                if allocation_class > 2: # loose g/PS
                    g_per_clam = 128
                    if crop_id == 1:
                        g_per_clam = 114 # arugula
                    if crop_id == 3:
                        g_per_clam = 35.4 # basil
                    sort_metric_next = 1/sort_metric_next * g_per_clam
                if item_no[3:10] == 'BTHDBBY':
                    sort_metric_next = 1
                if date_tomorrow in new_expected_harvest_dict.keys():
                    if facility_line_next in new_expected_harvest_dict[date_tomorrow]:
                        crop_id_keys = new_expected_harvest_dict[date_tomorrow][facility_line_next].keys()
                        if gmed_crop_id in crop_id_keys:
                            expected_harvest_from_next = new_expected_harvest_dict[date_tomorrow][facility_line_next][gmed_crop_id]                       
                            expected_ps_from_next = int(expected_harvest_from_next/sort_metric_next)

                            gmed_harvest_percentage_cap = 0.05
                            expected_harvest_from_next_gmed = expected_harvest_from_next * gmed_harvest_percentage_cap
                            expected_ps_from_next_gmed = int(expected_ps_from_next * gmed_harvest_percentage_cap)
                            gmed_harvest_reserve = expected_harvest_from_next - expected_harvest_from_next_gmed
                            gmed_ps_reserve = int(expected_ps_from_next - expected_ps_from_next_gmed)

                            if facility_line_next in updated_facility_lines_list and crop_id in updated_crop_id_list:
                                matching_facility_line_indices = [i for i, x in enumerate(updated_facility_lines_list) if x == facility_line_next]
                                matching_crop_id_indices = [i for i, x in enumerate(updated_crop_id_list) if x == crop_id]
                                last_facility_line_crop_id_match_idx = lastMatchingValue(matching_facility_line_indices,matching_crop_id_indices)
                                updated_harvest_val = updated_total_harvest_list[last_facility_line_crop_id_match_idx]

                                if gmed_harvest_reserve >= updated_harvest_val:
                                    # if the line already had the same crop pulled, the line cannot be used again
                                    expected_ps_from_next_gmed = 0
                                else:
                                    # some of the remaining 5% can still be allocated
                                    expected_harvest_from_next = updated_harvest_val
                                    expected_ps_from_next =int(expected_harvest_from_next/sort_metric_next)
                                    expected_harvest_from_next_gmed = updated_harvest_val - gmed_harvest_reserve
                                    expected_ps_from_next_gmed = int(expected_harvest_from_next_gmed/sort_metric_next)

                            #expected_ps_from_next = int(new_expected_ps_dict[date_tomorrow][facility_line_next][crop_id])
                            if expected_ps_from_next_gmed != 0:
                                print(facility_line_next + ' total starting GMED plant sites: ' + str(expected_ps_from_next) + '(' + str(expected_harvest_from_next)+ ' ' + unit_str + ')')
                                print(facility_line_next + ' total starting 5% of GMED plant sites: ' + str(expected_ps_from_next_gmed) + '(' + str(expected_harvest_from_next_gmed)+ ' ' + unit_str + ')')
                                remaining_harvest = float(expected_harvest_from_next_gmed) - remaining_allocation

                                if remaining_harvest >= 0: # we expect more remaining harvest than we need to allocate for the order
                                    remaining_allocation_ps = int(remaining_allocation/sort_metric_next)
                                    remaining_harvest_ps = int(remaining_harvest/sort_metric_next) + gmed_ps_reserve

                                    print("Pack " + sage_crop_code + " from GMED in " + facility_line_next + " to " + sage_customer_id + "(" + order_number + "): " + str(remaining_allocation_ps) + ' PS '+ '(' + str(int(remaining_allocation)) +' '+ unit_str + ')')

                                    print("Remaining plant sites to allocate from " + facility_line_next + ": " + str(remaining_harvest_ps))

                                    tuple_to_insert = order_tuple + (remaining_allocation_ps, facility_line_next.split('_')[0], facility_line_next.split('_')[1],facility_line_next, crop_id)
                                    all_tuple_to_insert_list += [tuple_to_insert]

                                    new_expected_harvest_dict[date_tomorrow][facility_line_next][crop_id] = remaining_harvest                                                    
                                    new_expected_ps_dict[date_tomorrow][facility_line_next][crop_id] = remaining_harvest_ps

                                    updated_facility_lines_list += [facility_line_next]
                                    updated_crop_id_list = [crop_id]
                                    updated_total_harvest_list += [remaining_harvest]
                                    remaining_allocation = 0

                                else:
                                    expected_harvest_from_next_ps_gmed = int(expected_harvest_from_next_gmed/sort_metric_next)
                                    #print("Pack " + str(expected_harvest_from_next_ps) + '(' + str(expected_harvest_from_next) + ' ' + unit_str + ') plant sites of ' + sage_crop_code + " from " + facility_line_next)    
                                    print("Pack " + sage_crop_code + " from " + facility_line_next + " to " + sage_customer_id + "(" + order_number + "): " + str(expected_ps_from_next_gmed) + ' PS '+ '(' + str(int(expected_harvest_from_next_gmed)) +' '+ unit_str + ')')
        #                            print("Remaining plant sites to allocate from " + facility_line_next + ": " + str(0))

                                    tuple_to_insert = order_tuple + (expected_harvest_from_next_ps_gmed, facility_line_next.split('_')[0], facility_line_next.split('_')[1],facility_line_next, crop_id)
                                    all_tuple_to_insert_list += [tuple_to_insert]

                                    new_expected_harvest_dict[date_tomorrow][facility_line_next][crop_id] = gmed_harvest_reserve                                                                                                        
                                    new_expected_ps_dict[date_tomorrow][facility_line_next][crop_id] = gmed_ps_reserve

                                    updated_facility_lines_list += [facility_line_next]
                                    updated_crop_id_list += [crop_id]
                                    updated_total_harvest_list += [gmed_harvest_reserve]

                                    remaining_allocation = -remaining_harvest
            else:
                # if we run out of facility_lines, we need to allocate crop from elsewhere
                if sage_crop_code != 'LETT':
                    print('Outstanding order for ' + sage_crop_code + ' in ' + city + ': ' + str(remaining_allocation) + ' ' + unit_str)
                    new2_order_date_list += [order_date]
                    new2_item_no_list += [item_no]
                    #new2_original_qty_list  += [original_qty]
                    new2_original_qty_list  += [int(np.ceil(remaining_allocation/ conversion_factor))]
                    new2_sage_customer_id_list += [sage_customer_id]
                    new2_order_number_list += [order_number]

                remaining_allocation = 0
                
    new_order_lists = [new2_order_date_list, new2_item_no_list, new2_original_qty_list, new2_sage_customer_id_list, new2_order_number_list]
    
    return [all_tuple_to_insert_list,new_expected_ps_dict,new_order_lists]



def harvestAllocationToGMED(new_order_lists,expected_harvest_dict_list, sort_metric_dict_list, allocation_class, allocation_date):
    
    # goal: compute crop allocations to GMED to fulfill orders for allocation class 5
    
    # input: order lists, harvest dictionary, yield metric dictionaries, allocation class, and allocation date
        # 1. new_order_lists: list of 5 lists for new orders
            # 1. order date (datetime)
            # 2. item no (string)
            # 3. original qty (int)
            # 4. sage customer id (string)
            # 5. order number(string)
        # 2. expected_harvest_dict_list: list of 3 dictionaries for the expected harvest to allocate from
            # 1. expected_ps_dict[harvest_date][facility_line][crop_id] = total_plant_sites
            # 2. expected_whole_plant_biomass_dict[harvest_date][facility_line][crop_id] = expected_whole_plant_biomass (g)
            # 3. expected_loose_plant_biomass_dict[harvest_date][facility_line][crop_id] = expected_loose_plant_biomass (g)
        # 3. sort_metric_dict_list: list of 2 dictionaries
            # 1. avg_headweight_dict[facility_line][crop_id][year_week] = [avg_headweight, avg_headweight2,...]
            # 2. pspc_dict[facility_line][crop_id][year_week] = [pspc1, pspc2,...]
        # 4. allocation_class: int (1-6) cooresponding to allocation class        
        # 5. allocation_date: datetime cooresponding to allocation date
    
    # output: list of three lists: 1) harvest allocation for harvestAllocation_V, 2) remaining plant sites, and 3) remaining orders
    # 1) all_tuple_to_insert_list: list of tuples of harvest allocations
        # 1. order date (datetime)
        # 2. item number (string)
        # 3. sage customer id (string)
        # 4. original qty (int)
        # 5. allocation qty (int)
        # 6. allocation lot code (string)
        # 7. order number (string)
    # 2) new_expected_ps_dict: dictionary of remaining expected plant sites that are left after allocation
        # expected_ps_dict[harvest_date][facility_line][crop_id] = remaining plant sites
    # 3) new_order_lists: lists cooresponding to outstanding orders
        # 1. new_order_date_list ( list of datetimes)
        # 2. new_item_no_list (list of strings)
        # 3. new_original_qty_list (list of ints)
        # 4. new_sage_customer_id_list (list of strings)
        # 5. new_order_number_list (list of strings)
    all_tuple_to_insert_list = list()

    new_order_date_list = new_order_lists[0]
    new_item_no_list  = new_order_lists[1]
    new_original_qty_list  = new_order_lists[2]
    new_sage_customer_id_list  = new_order_lists[3]
    new_order_number_list = new_order_lists[4]

    expected_ps_dict = expected_harvest_dict_list[0]
    expected_whole_plant_biomass_dict = expected_harvest_dict_list[1]
    expected_loose_plant_biomass_dict = expected_harvest_dict_list[2]

    avg_headweight_dict = sort_metric_dict_list[0]
    pspc_dict = sort_metric_dict_list[1]

    date_tomorrow = allocation_date

    if allocation_class == 1:    
        expected_harvest_dict = expected_whole_plant_biomass_dict
        sort_metric_dict = avg_headweight_dict
        unit_str = 'whole grams'
    if allocation_class == 2:
        expected_harvest_dict = expected_ps_dict
        sort_metric_dict = avg_headweight_dict
        unit_str = 'plant sites'
    if allocation_class > 2:
        expected_harvest_dict = expected_loose_plant_biomass_dict
        sort_metric_dict = pspc_dict
        unit_str = 'loose grams'


    new_expected_ps_dict = expected_ps_dict
    new_expected_harvest_dict = expected_harvest_dict
    
    # list of orders remaining to allocate if we run out of crops at the facility
    new2_order_date_list = list()
    new2_item_no_list  = list()
    new2_original_qty_list  = list()
    new2_sage_customer_id_list  = list()
    new2_order_number_list = list()

    updated_facility_lines_list = list()
    updated_crop_id_list = list()
    updated_total_harvest_list = list()

    # loop through order list

    for order_idx in range(len(new_order_date_list)):
        order_date = new_order_date_list[order_idx]
        item_no = new_item_no_list[order_idx]
        original_qty = new_original_qty_list[order_idx]
        sage_customer_id = new_sage_customer_id_list[order_idx]
        order_number = new_order_number_list[order_idx]

        order_tuple = (order_date, item_no, sage_customer_id, original_qty, order_number)
        
        city = item_no[10:13]

        city_sort_metrics = []
        city_facility_lines = []
        
        if item_no not in spd_item_no_list:
            print(item_no + 'not in SageProductsDim')
        conversion_factor = 1 # let expected_order_packed_weight = original_qty if no conversion_factor exists
        if item_no in spd_item_no_list:
            conversion_factor = float(spd_packed_weight_conversion_grams_list[spd_item_no_list.index(item_no)])
        
        expected_order_packed_weight = float(original_qty * conversion_factor)

        sage_crop_code = item_no[3:7]
        crop_id = crop_id_list[sage_crop_code_list.index(sage_crop_code)]

        print('Order for ' + item_no + ': '+ str(original_qty) + ' qty (' + str(int(expected_order_packed_weight)) + ' ' + unit_str + ')')

        ##### Here we define how in what order to select lines for a city


        # For GMED push, we want to select from the equally from different lettuce lines in the city
        lettuce_crop_id_list = [5, 7, 10, 12, 13, 15, 18]
        city_lettuce_crop_id_list = []
        for facility_line_key in sort_metric_dict:
            key_city = facility_line_key[0:3]
            # if the facility_line is in the city under evaluation
            if key_city == city:
                selected_facility_line_crop_id_list = list(sort_metric_dict[facility_line_key].keys())
                for idx in range(len(selected_facility_line_crop_id_list)):
                    if selected_facility_line_crop_id_list[idx] in lettuce_crop_id_list and selected_facility_line_crop_id_list[idx] not in city_lettuce_crop_id_list:        
                        city_lettuce_crop_id_list += [selected_facility_line_crop_id_list[idx]]
        # whole plant food service allocated from smallest to largest average headweight
        # whole headcount allocated from smallest to largest average headweight starting above 90g (min size req. for baby butterhead)
        # loose plant food service allocated from smallest to largest plant sites per clam (biggest plants first)
        
        
        variety_count = len(city_lettuce_crop_id_list)
        expected_order_packed_weight_fraction = expected_order_packed_weight/variety_count
        
        # loop through for each lettuce crop
        for target_crop_id in city_lettuce_crop_id_list:
            
            remaining_allocation = expected_order_packed_weight_fraction
            
            target_sage_crop_code = sage_crop_code_list[crop_id_list.index(target_crop_id)]
            # loop through each facility_line
            for facility_line_key in sort_metric_dict:
                key_city = facility_line_key[0:3]
                # if the facility_line is in the city under evaluation
                if key_city == city:
                    selected_facility_line_crop_id_list = list(sort_metric_dict[facility_line_key].keys())
                    for idx in range(len(selected_facility_line_crop_id_list)):
                        if selected_facility_line_crop_id_list[idx] == target_crop_id:
                            #last_sort_metric = np.mean(list(sort_metric_dict[facility_line_key][selected_facility_line_crop_id_list[idx]].values())[-1])
                            last_sort_metric = list(sort_metric_dict[facility_line_key][selected_facility_line_crop_id_list[idx]].values())[-1][-1]
                            if last_sort_metric != 0:
                                city_sort_metrics += [last_sort_metric]
                                city_facility_lines += [facility_line_key]
                                #print(city_sort_metrics)
                            else:
                                print(sort_metric_dict[facility_line_key][selected_facility_line_crop_id_list[idx]].values())
            sort_index = np.argsort(city_sort_metrics)

            # sorted lists of facility lines and sort metrics to pull from
            sorted_city_facility_lines = []
            sorted_city_sort_metrics = []
            for idx in sort_index:
                facility_line = city_facility_lines[idx]
                sort_metric = city_sort_metrics[idx]
                sorted_city_facility_lines += [facility_line]
                sorted_city_sort_metrics += [sort_metric]
                #print(facility_line + ' has average headweight of ' + str(sort_metric))
            #print(sorted_city_facility_lines)

            idx_next = 0
            facility_line_idx = idx_next-1

            while remaining_allocation > 0:    
                facility_line_idx += 1

                if facility_line_idx < len(sorted_city_facility_lines):
                    facility_line_next = sorted_city_facility_lines[facility_line_idx]
                    sort_metric_next = float(sorted_city_sort_metrics[facility_line_idx]) # whole g/PS
                    if allocation_class > 2: # loose g/PS
                        g_per_clam = 128
                        if crop_id == 1:
                            g_per_clam = 114 # arugula
                        if crop_id == 3:
                            g_per_clam = 35.4 # basil
                        sort_metric_next = 1/sort_metric_next * g_per_clam
#                     if item_no[3:10] == 'BTHDBBY':
#                         sort_metric_next = 1
                    if date_tomorrow in new_expected_harvest_dict.keys():
                        if facility_line_next in new_expected_harvest_dict[date_tomorrow]:
                            crop_id_keys = new_expected_harvest_dict[date_tomorrow][facility_line_next].keys()
                            if target_crop_id in crop_id_keys:

                                expected_harvest_from_next = new_expected_harvest_dict[date_tomorrow][facility_line_next][target_crop_id]
                                if facility_line_next in updated_facility_lines_list and target_crop_id in updated_crop_id_list:
                                    matching_facility_line_indices = [i for i, x in enumerate(updated_facility_lines_list) if x == facility_line_next]
                                    matching_crop_id_indices = [i for i, x in enumerate(updated_crop_id_list) if x == target_crop_id]
                                    last_facility_line_crop_id_match_idx = lastMatchingValue(matching_facility_line_indices,matching_crop_id_indices)
                                    expected_harvest_from_next = updated_total_harvest_list[last_facility_line_crop_id_match_idx]

                                expected_ps_from_next = int(expected_harvest_from_next/sort_metric_next)

                                if expected_ps_from_next != 0:
                                    print(facility_line_next + ' total starting ' + target_sage_crop_code + ' plant sites: ' + str(expected_ps_from_next) + '(' + str(expected_harvest_from_next)+ ' ' + unit_str + ')')
                                    remaining_harvest = float(expected_harvest_from_next) - remaining_allocation

                                    if remaining_harvest >= 0: # we expect more remaining harvest than we need to allocate for the order
                                        remaining_allocation_ps = int(remaining_allocation/sort_metric_next)
                                        remaining_harvest_ps = int(remaining_harvest/sort_metric_next)

                                        print("Pack " + target_sage_crop_code + " to GMED in " + facility_line_next + " to " + sage_customer_id + "(" + order_number + "): " + str(remaining_allocation_ps) + ' PS '+ '(' + str(int(remaining_allocation)) +' '+ unit_str + ')')

                                        print("Remaining plant sites to allocate from " + facility_line_next + ": " + str(remaining_harvest_ps))

                                        tuple_to_insert = order_tuple + (remaining_allocation_ps, facility_line_next.split('_')[0], facility_line_next.split('_')[1],facility_line_next, crop_id)
                                        all_tuple_to_insert_list += [tuple_to_insert]

                                        new_expected_harvest_dict[date_tomorrow][facility_line_next][crop_id] = remaining_harvest                                                    
                                        new_expected_ps_dict[date_tomorrow][facility_line_next][crop_id] = remaining_harvest_ps

                                        updated_facility_lines_list += [facility_line_next]
                                        updated_crop_id_list += [target_crop_id]
                                        updated_total_harvest_list += [remaining_harvest]
                                        remaining_allocation = 0

                                    else:
                                        expected_harvest_from_next_ps = int(expected_harvest_from_next/sort_metric_next)
                                        #print("Pack " + str(expected_harvest_from_next_ps) + '(' + str(expected_harvest_from_next) + ' ' + unit_str + ') plant sites of ' + sage_crop_code + " from " + facility_line_next)    
                                        print("Pack " + target_sage_crop_code + " to GMED in " + facility_line_next + " to " + sage_customer_id + "(" + order_number + "): " + str(expected_ps_from_next) + ' PS '+ '(' + str(int(expected_harvest_from_next)) +' '+ unit_str + ')')
                                        print("Remaining plant sites to allocate from " + facility_line_next + ": " + str(0))

                                        tuple_to_insert = order_tuple + (expected_harvest_from_next_ps, facility_line_next.split('_')[0], facility_line_next.split('_')[1],facility_line_next, crop_id)
                                        all_tuple_to_insert_list += [tuple_to_insert]

                                        new_expected_harvest_dict[date_tomorrow][facility_line_next][crop_id] = 0                                                                                                   
                                        new_expected_ps_dict[date_tomorrow][facility_line_next][crop_id] = 0

                                        updated_facility_lines_list += [facility_line_next]
                                        updated_crop_id_list += [target_crop_id]
                                        updated_total_harvest_list += [0]

                                        remaining_allocation = -remaining_harvest
                else:
                    # if we run out of facility_lines, we need to allocate crop from elsewhere
                    if sage_crop_code != 'LETT':
                        print('Outstanding order for ' + target_sage_crop_code + ' in ' + city + ': ' + str(remaining_allocation) + ' ' + unit_str)
                        new2_order_date_list += [order_date]
                        new2_item_no_list += [item_no]
                        #new2_original_qty_list  += [original_qty]
                        new2_original_qty_list  += [int(np.ceil(remaining_allocation/ conversion_factor))]
                        new2_sage_customer_id_list += [sage_customer_id]
                        new2_order_number_list += [order_number]

                    remaining_allocation = 0
                
    new_order_lists = [new2_order_date_list, new2_item_no_list, new2_original_qty_list, new2_sage_customer_id_list, new2_order_number_list]
    
    return [all_tuple_to_insert_list,new_expected_ps_dict,new_order_lists]



def plantSiteToMass(plant_site_dict, avg_headweight_dict, pspc_dict):

    # goal: compute expected whole plant biomass and expected loose plant biomass using trailing 5-day averages avg_headweight_dict and pspc_dict
    
    # input: three dictionaries of plant sites, average headweight, and plant sites per clam
        # 1. plant_site_dict[harvest_date][facility_line][crop_id] = remaining_plant_sites
        # 2. avg_headweight_dict[facility_line][crop_id][year_week] = [avg_headweight, avg_headweight2,...]
        # 3. pspc_dict[facility_line][crop_id][year_week] = [pspc1, pspc2,...]
    
    # output: list of three dictionaries for  1) plant sites, 2) whole plant biomass, and 3) loose plant biomass
        # 1. plant_site_dict[harvest_date][facility_line][crop_id] = remaining_plant_sites
        # 2. expected_whole_plant_biomass_trail_dict[harvest_date][facility_line][crop_id] = expected_whole_plant_biomass (g)
        # 3. expected_loose_plant_biomass_trail_dict[harvest_date][facility_line][crop_id] = expected_loose_plant_biomass (g)
        
    expected_whole_plant_biomass_dict = {}
    expected_loose_plant_biomass_dict = {}
    
    expected_whole_plant_biomass_trail_dict = {}
    expected_whole_plant_biomass_trail_std_dict = {}
    expected_loose_plant_biomass_trail_dict = {}
    expected_loose_plant_biomass_trail_std_dict = {}           
            
    trail_five_pspc_dict = {}
    trail_five_avg_headweight_dict = {}

    csf_harvest_date_list = list()
    csf_facility_list = list()
    csf_finishing_line_list = list()
    csf_crop_id_list = list()
    csf_total_plant_sites_list = list()
    
    for harvest_date in plant_site_dict.keys():
        if plant_site_dict[harvest_date] == {}:
            expected_loose_plant_biomass_dict[harvest_date] = {}
            expected_whole_plant_biomass_dict[harvest_date] = {}
        else:
            for facility_line in plant_site_dict[harvest_date].keys():
                facility = facility_line.split('_')[0]
                finishing_line = facility_line.split('_')[1]
                for crop_id in plant_site_dict[harvest_date][facility_line].keys():
                    total_plant_sites = plant_site_dict[harvest_date][facility_line][crop_id]
                    csf_harvest_date_list += [harvest_date]
                    csf_facility_list += [facility]
                    csf_finishing_line_list += [finishing_line]
                    csf_crop_id_list += [crop_id]
                    csf_total_plant_sites_list += [total_plant_sites]

    for csf_idx in range(len(csf_harvest_date_list)):
        csf_harvest_date = csf_harvest_date_list[csf_idx]
        csf_facility = csf_facility_list[csf_idx]
        csf_finishing_line = csf_finishing_line_list[csf_idx]
        csf_crop_id = csf_crop_id_list[csf_idx]
        csf_total_plant_sites = csf_total_plant_sites_list[csf_idx]
        csf_facility_line = csf_facility + '_' + csf_finishing_line
        


        if csf_facility_line in pspc_dict.keys():
            if csf_crop_id in pspc_dict[csf_facility_line]:
                last_year_week = list(pspc_dict[csf_facility_line][csf_crop_id].keys())[-1]
                last_pspc = list(pspc_dict[csf_facility_line][csf_crop_id][last_year_week])[-1]
                [trail_pspc,trail_std_pspc] = trailingAverage(pspc_dict, 5, csf_facility_line, csf_crop_id)

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



        if csf_facility_line in avg_headweight_dict.keys():
            if csf_crop_id in avg_headweight_dict[csf_facility_line]:
                last_year_week = list(avg_headweight_dict[csf_facility_line][csf_crop_id].keys())[-1]
                last_avg_headweight = list(avg_headweight_dict[csf_facility_line][csf_crop_id][last_year_week])[-1]
                [trail_five_avg_headweight,trail_five_std_headweight] = trailingAverage(avg_headweight_dict, 5, csf_facility_line, csf_crop_id)

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


    return [plant_site_dict,expected_whole_plant_biomass_trail_dict,expected_loose_plant_biomass_trail_dict, trail_five_pspc_dict, trail_five_avg_headweight_dict]      

def plantSiteToMassOptimized(plant_site_dict, avg_headweight_dict, pspc_dict):

    # goal: compute expected whole plant biomass and expected loose plant biomass using optimal trailing (or year-over-year) averages 
    
    # input: three dictionaries of plant sites, average headweight, and plant sites per clam
        # 1. plant_site_dict[harvest_date][facility_line][crop_id] = remaining_plant_sites
        # 2. avg_headweight_dict[facility_line][crop_id][year_week] = [avg_headweight, avg_headweight2,...]
        # 3. pspc_dict[facility_line][crop_id][year_week] = [pspc1, pspc2,...]
    
    # output: list of three dictionaries for  1) plant sites, 2) whole plant biomass, and 3) loose plant biomass
        # 1. plant_site_dict[harvest_date][facility_line][crop_id] = remaining_plant_sites
        # 2. expected_whole_plant_biomass_trail_dict[harvest_date][facility_line][crop_id] = expected_whole_plant_biomass (g)
        # 3. expected_loose_plant_biomass_trail_dict[harvest_date][facility_line][crop_id] = expected_loose_plant_biomass (g)
        
    expected_whole_plant_biomass_dict = {}
    expected_loose_plant_biomass_dict = {}
    
    expected_whole_plant_biomass_trail_dict = {}
    expected_whole_plant_biomass_trail_std_dict = {}
    expected_loose_plant_biomass_trail_dict = {}
    expected_loose_plant_biomass_trail_std_dict = {}           
            
    trail_five_pspc_dict = {}
    trail_five_avg_headweight_dict = {}

    csf_harvest_date_list = list()
    csf_facility_list = list()
    csf_finishing_line_list = list()
    csf_crop_id_list = list()
    csf_total_plant_sites_list = list()
    
    for harvest_date in plant_site_dict.keys():
        if plant_site_dict[harvest_date] == {}:
            expected_loose_plant_biomass_dict[harvest_date] = {}
            expected_whole_plant_biomass_dict[harvest_date] = {}
        else:
            for facility_line in plant_site_dict[harvest_date].keys():
                facility = facility_line.split('_')[0]
                finishing_line = facility_line.split('_')[1]
                for crop_id in plant_site_dict[harvest_date][facility_line].keys():
                    total_plant_sites = plant_site_dict[harvest_date][facility_line][crop_id]
                    csf_harvest_date_list += [harvest_date]
                    csf_facility_list += [facility]
                    csf_finishing_line_list += [finishing_line]
                    csf_crop_id_list += [crop_id]
                    csf_total_plant_sites_list += [total_plant_sites]

    for csf_idx in range(len(csf_harvest_date_list)):
        csf_harvest_date = csf_harvest_date_list[csf_idx]
        csf_facility = csf_facility_list[csf_idx]
        csf_finishing_line = csf_finishing_line_list[csf_idx]
        csf_crop_id = csf_crop_id_list[csf_idx]
        csf_total_plant_sites = csf_total_plant_sites_list[csf_idx]
        csf_facility_line = csf_facility + '_' + csf_finishing_line
        


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
                    [trail_pspc,trail_std_pspc] = trailingAverage(pspc_dict, crop_line_optimal_trail, csf_facility_line, csf_crop_id)

                if crop_line_optimal_trail == 0:
                    [trail_pspc,trail_std_pspc] = yearOverYearAverage(pspc_dict,csf_facility_line,csf_crop_id,csf_harvest_date)


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
                    [trail_five_avg_headweight,trail_five_std_headweight] = trailingAverage(avg_headweight_dict, crop_line_optimal_trail, csf_facility_line, csf_crop_id)

                if crop_line_optimal_trail == 0:
                    [trail_five_avg_headweight,trail_five_std_headweight] = yearOverYearAverage(avg_headweight_dict,csf_facility_line,csf_crop_id,csf_harvest_date)

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
                

    return [plant_site_dict,expected_whole_plant_biomass_trail_dict,expected_loose_plant_biomass_trail_dict, trail_five_pspc_dict, trail_five_avg_headweight_dict]      




def isListEmpty(inList):
    # goal: check if list is empty
    
    # input: list inlist
    
    # output: True if the list is empty, False if the list is not empty
    
    if isinstance(inList, list): # Is a list
        return all( map(isListEmpty, inList) )
    return False # Not a list

def remainingHarvest(new_expected_harvest_dict_list, allocation_date):

    # goal: compute remaining harvest
    
    # input: list of three dictionaries corresponding to the expected harvest (plant sites dictionary, whole plant biomass dictionary, loose plant biomass dictionary) and the allocation date
        # 1. new_expected_harvest_dict_list = [plant_site_dict,expected_whole_plant_biomass_trail_dict,expected_loose_plant_biomass_trail_dict]
            # 1. plant_site_dict[harvest_date][facility_line][crop_id] = remaining_plant_sites
            # 2. expected_whole_plant_biomass_trail_dict[harvest_date][facility_line][crop_id] = expected_whole_plant_biomass (g)
            # 3. expected_loose_plant_biomass_trail_dict[harvest_date][facility_line][crop_id] = expected_loose_plant_biomass (g)
        # 2. allocation_date (datetime)
    # output: all_tuple_to_insert_list- list of tuples of remaining harvest
        # 1. harvest date (datetime)
        # 2. facility id (int)
        # 3. line (int)
        # 4. crop id (int)
        # 5. expected plant sites (int)
        # 6. expected whole grams (float)
        # 7. expected loose grams (float)
        
    all_tuple_to_insert_list = []
    
    expected_ps_dict = new_expected_harvest_dict_list[0]
    expected_whole_plant_biomass_dict = new_expected_harvest_dict_list[1]
    expected_loose_plant_biomass_dict = new_expected_harvest_dict_list[2]
    
    date_tomorrow = allocation_date

    harvest_date_to_write = date_tomorrow
    ps_active_lines = list(expected_ps_dict[date_tomorrow].keys())
    for active_line in ps_active_lines:
        facility_line_to_write = active_line
        location_name_to_write = active_line.split('_')[0]
        facility_id_to_write = facility_list[location_name_list.index(active_line.split('_')[0])]
        city_to_write = facility_line_to_write[0:3]
        line_to_write = int(active_line.split('_')[1])
        ps_active_line_crop_id_list = list(expected_ps_dict[date_tomorrow][active_line].keys())
        active_line_expected_ps_list = list(expected_ps_dict[date_tomorrow][active_line].values())
        region_to_write = region_list[location_name_list.index(active_line.split('_')[0])]
        if date_tomorrow in list(expected_whole_plant_biomass_dict.keys()):
            if active_line in list(expected_whole_plant_biomass_dict[date_tomorrow].keys()):
                whole_active_line_crop_id_list = list(expected_whole_plant_biomass_dict[date_tomorrow][active_line].keys())
                active_line_expected_whole_biomass_list = list(expected_whole_plant_biomass_dict[date_tomorrow][active_line].values())
            else:
                whole_active_line_crop_id_list = list(expected_ps_dict[date_tomorrow][active_line].keys())
                active_line_expected_whole_biomass_list = [0] * len(list(expected_ps_dict[date_tomorrow][active_line].values()))
        else:
            whole_active_line_crop_id_list = list(expected_ps_dict[date_tomorrow][active_line].keys())
            active_line_expected_whole_biomass_list = [0] * len(list(expected_ps_dict[date_tomorrow][active_line].values()))

        if date_tomorrow in list(expected_loose_plant_biomass_dict.keys()):
            if active_line in list(expected_loose_plant_biomass_dict[date_tomorrow].keys()):
                loose_active_line_crop_id_list = list(expected_loose_plant_biomass_dict[date_tomorrow][active_line].keys())                
                active_line_expected_loose_biomass_list = list(expected_loose_plant_biomass_dict[date_tomorrow][active_line].values())
            else:
                loose_active_line_crop_id_list = [0] * len(list(expected_ps_dict[date_tomorrow][active_line].keys()))  
                active_line_expected_loose_biomass_list = [0] * len(list(expected_ps_dict[date_tomorrow][active_line].values()))
        else:
            loose_active_line_crop_id_list = [0] * len(list(expected_ps_dict[date_tomorrow][active_line].keys()))
            active_line_expected_loose_biomass_list = [0] * len(list(expected_ps_dict[date_tomorrow][active_line].values()))

        line_tuple = (harvest_date_to_write, facility_id_to_write, line_to_write)
        for idx in range(len(ps_active_line_crop_id_list)):
            crop_id_to_write = ps_active_line_crop_id_list[idx]
            sage_crop_code_to_write = sage_crop_code_list[crop_id_list.index(crop_id_to_write)]
            expected_plant_sites_to_write = active_line_expected_ps_list[idx]
            expected_whole_grams_to_write = 0
            expected_loose_grams_to_write = 0
            whole_spatial_precision_to_write = 4
            loose_spatial_precision_to_write = 4
            if crop_id_to_write in whole_active_line_crop_id_list:
                whole_idx = whole_active_line_crop_id_list.index(crop_id_to_write)
                expected_whole_grams_to_write = active_line_expected_whole_biomass_list[whole_idx]
                whole_spatial_precision_to_write = 0
            if crop_id_to_write in loose_active_line_crop_id_list:            
                loose_idx = loose_active_line_crop_id_list.index(crop_id_to_write)
                expected_loose_grams_to_write = active_line_expected_loose_biomass_list[loose_idx]
                loose_spatial_precision_to_write = 0
            # compute at lower spatial precision if expected biomass is zero
            if expected_whole_grams_to_write == 0:
                crop_averages_list = cropAverages(avg_headweight_dict,active_line,ps_active_line_crop_id_list[idx])
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
                        #print(expected_whole_grams_to_write)
                        whole_spatial_precision_to_write = idx_to_try+1
                        # add entry to avg_headweight_dict
                        #new_avg_headweight_dict[facility_line_to_write] = {crop_id_to_write:{'0000_00':conversion_factor}}

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
                crop_averages_list = cropAverages(pspc_dict,active_line,ps_active_line_crop_id_list[idx])
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

            tuple_to_insert = line_tuple + (crop_id_to_write, expected_plant_sites_to_write, expected_whole_grams_to_write, expected_loose_grams_to_write,facility_line_to_write)
            if expected_plant_sites_to_write != 0:
                all_tuple_to_insert_list += [tuple_to_insert]
    return all_tuple_to_insert_list


def allocateToNextDay(new_expected_ps_dict):
    
    # goal: allocate remaining plant sites in the first key to the second key of new_expected_ps_dict
    
    # input: dictionary of expected plant sites
    # new_expected_ps_dict[harvest_date][facility_line][crop_id] = total_plant_sites
    
    # output: output_dict = input dictionary with all remaining to be harvested tomorrow moved to the following harvest day
    # output_dict[harvest_date][facility_line][crop_id] = total_plant_sites
    
    output_dict = {}
    date_tomorrow = list(new_expected_ps_dict.keys())[0]
    date_after_tomorrow = list(new_expected_ps_dict.keys())[1]
    output_dict[date_after_tomorrow] = {}
    for facility_line_dt in new_expected_ps_dict[date_tomorrow].keys():
        facility_line_value_dictionary = new_expected_ps_dict[date_tomorrow][facility_line_dt]
        # if it is already a facility_line scheduled for tomorrow
        if facility_line_dt in new_expected_ps_dict[date_after_tomorrow].keys():
            for crop_id_dt in new_expected_ps_dict[date_tomorrow][facility_line_dt].keys():
                ps_dt = new_expected_ps_dict[date_tomorrow][facility_line_dt][crop_id_dt]
                # if is alrady a crop scheduled at the facility_line
                if crop_id_dt in new_expected_ps_dict[date_after_tomorrow][facility_line_dt].keys():
                    # add the ps values for tomorrow and the day after tomorrow
                    ps_dat = new_expected_ps_dict[date_after_tomorrow][facility_line_dt][crop_id_dt]
                    ps_dt_dat_sum = ps_dt + ps_dat
                    output_dict[date_after_tomorrow][facility_line_dt] = {crop_id_dt:ps_dt_dat_sum}
                else:
                    # only crop for tomorrow
                    output_dict[date_after_tomorrow][facility_line_dt] = {crop_id_dt:ps_dt}
        else:
            # only facility_line for tomorrow
            output_dict[date_after_tomorrow][facility_line_dt] = facility_line_value_dictionary
    
    # add other entries for date_after_tomorrow
    for facility_line_dat in new_expected_ps_dict[date_after_tomorrow].keys():
        facility_line_value_dictionary = new_expected_ps_dict[date_after_tomorrow][facility_line_dat]
        if facility_line_dat in new_expected_ps_dict[date_tomorrow].keys():
            for crop_id_dat in new_expected_ps_dict[date_after_tomorrow][facility_line_dat].keys():
                if crop_id_dat not in new_expected_ps_dict[date_tomorrow][facility_line_dat].keys():
                    output_dict[date_after_tomorrow][facility_line_dat][crop_id_dat] = new_expected_ps_dict[date_after_tomorrow][facility_line_dat][crop_id_dat]
        if facility_line_dat not in new_expected_ps_dict[date_tomorrow].keys():
            output_dict[date_after_tomorrow][facility_line_dat] = facility_line_value_dictionary
            
    # add other entries for the rest of the dictionary
    next_date_idx = 2
    last_date_idx = len(list(new_expected_ps_dict.keys()))-1
    
    while next_date_idx <= last_date_idx:
        date_next = list(new_expected_ps_dict.keys())[next_date_idx]
        date_next_value_dictionary = new_expected_ps_dict[date_next]
        output_dict[date_next] = date_next_value_dictionary
        next_date_idx += 1
        
    return output_dict


def liveOrderCheck(expected_ps_dict, lsd_date_list):
    
    # goal: add dictionary keys to expected_ps_dict if there are live orders to allocate
    
    # input: dictionary of expected plant sites and list of live sales data dates
    # expected_ps_dict[harvest_date][facility_line][crop_id] = total_plant_sites
    # lsd_date_list: list of datetimes
    
    # output: dictionary with additional date keys (usually Saturdays)
    # new2_expected_ps_dict[harvest_date][facility_line][crop_id] = total_plant_sites
    

    # clean dictionary keys to contain only dates and not datetimes
    expected_ps_datetime_list = list(expected_ps_dict.keys())
    #expected_ps_val_list = list(expected_ps_dict.values())
    expected_ps_date_list = []
    #new_expected_ps_dict = {}
    new_expected_ps_dict = expected_ps_dict
    for d in range(len(expected_ps_datetime_list)):
        expected_ps_datetime = expected_ps_datetime_list[d]
        #expected_ps_value = expected_ps_val_list[d]
        expected_ps_date = expected_ps_datetime.date()

        expected_ps_date_list += [expected_ps_datetime]
        #new_expected_ps_dict[expected_ps_datetime] = expected_ps_value
    #print(new_expected_ps_dict.keys())

    # create list of missing dates in the harvest where there are live orders

    missing_date_list = []

    for lsd_order_date in lsd_date_list:
        lsd_order_date_dt = datetime.combine(lsd_order_date, datetime.min.time()) 
        if lsd_order_date_dt not in expected_ps_date_list and lsd_order_date_dt not in missing_date_list: 
            missing_date_list += [lsd_order_date_dt]

    # create new dictionary that includes missing dates as keys with empty values
    new2_expected_ps_dict = {}
    new_expected_ps_date_list = expected_ps_date_list

    for missing_date in missing_date_list:

        if len(new_expected_ps_date_list) > 0:
            i = 0
            while i < len(new_expected_ps_date_list):
                date_key = new_expected_ps_date_list[i]
                i += 1
                date_key_val = new_expected_ps_dict[date_key]
                if date_key < missing_date:
                    new2_expected_ps_dict[date_key] = date_key_val
                    new_expected_ps_date_list = [d for d in new_expected_ps_date_list if d != date_key]
                    i = 0
                else:
                    new2_expected_ps_dict[missing_date] = {}
        else:
            new2_expected_ps_dict[missing_date] = {}

    # add any remaining dates to the new dictionary
    for i in range(len(new_expected_ps_date_list)):
        key = new_expected_ps_date_list[i]
        value = new_expected_ps_dict[new_expected_ps_date_list[i]]
        new2_expected_ps_dict[new_expected_ps_date_list[i]] = new_expected_ps_dict[new_expected_ps_date_list[i]]

    return new2_expected_ps_dict



def trailingAverage(source_dict,trail_length,target_facility_line,target_crop_id, start_idx = 0):
    
    # goal: compute the mean and standard deviation for a specific facility, line, and crop id

    # input: conversion factors, trailing average length, target facility line, and target crop id
    # source_dict: dictionary of conversion factors (avg_headweight_dict or pspc_dict)
    # trail_length: integer number of days to look back (max length of the trailing average vector to account for)
    # target_facility_line: string of the facility and line number
    # target_crop_id: integer cooresponding to the crop id in CropDim_T
    # start_idx: integer number of days to look back

    # output: list of two floats: [avg_trail_val, std_trail_val]
        # 1. avg_trail_val: float which is the mean a.k.a. trailing average
        # 2. std_trail_val: float which is the standard deviation of the list (and if there is only one value, the std is 0)

    [avg_trail_val, std_trail_val] = [0,0]
    if target_facility_line in source_dict.keys():
        if target_crop_id in source_dict[target_facility_line].keys():
            val_list = list()

            for year_week in source_dict[target_facility_line][target_crop_id]:
                val = source_dict[target_facility_line][target_crop_id][year_week]
                for v in val:
                    val_list += [v]

            val_list.reverse()
            val_list_len = len(val_list)
            if val_list_len >= trail_length:
                trail_val_list = val_list[start_idx:trail_length+start_idx]
            else:
                trail_val_list = val_list

            avg_trail_val = np.mean(trail_val_list)
            std_trail_val = np.std(trail_val_list)
            #print(target_facility_line + ' ' + str(target_crop_id) + ' ' + str(avg_trail_val)+ ' +/- ' + str(std_trail_val))
    
    return [avg_trail_val, std_trail_val]


def trailingAverageSkip(source_dict,trail_length,target_facility_line,target_crop_id, start_idx = 0):
    
    # goal: compute the mean and standard deviation for a specific facility, line, and crop id

    # input: conversion factors, trailing average length, target facility line, and target crop id
    # source_dict: dictionary of conversion factors (avg_headweight_dict or pspc_dict)
    # trail_length: integer number of days to look back (max length of the trailing average vector to account for)
    # target_facility_line: string of the facility and line number
    # target_crop_id: integer cooresponding to the crop id in CropDim_T
    # start_idx: integer number of days to look back

    # output: list of two floats: [avg_trail_val, std_trail_val]
        # 1. avg_trail_val: float which is the mean a.k.a. trailing average
        # 2. std_trail_val: float which is the standard deviation of the list (and if there is only one value, the std is 0)

    [avg_trail_val, std_trail_val] = [0,0]
    if target_facility_line in source_dict.keys():
        if target_crop_id in source_dict[target_facility_line].keys():
            val_list = list()

            for year_week in source_dict[target_facility_line][target_crop_id]:
                val = source_dict[target_facility_line][target_crop_id][year_week]
                for v in val:
                    val_list += [v]

            val_list.reverse()
            val_list_len = len(val_list)
            if val_list_len >= trail_length+1:
                trail_val_list = val_list[start_idx+1:trail_length+1+start_idx]
            else:
                trail_val_list = val_list[1:]

            avg_trail_val = np.mean(trail_val_list)
            std_trail_val = np.std(trail_val_list)
            #print(target_facility_line + ' ' + str(target_crop_id) + ' ' + str(avg_trail_val)+ ' +/- ' + str(std_trail_val))
    
    return [avg_trail_val, std_trail_val]



def optimalTrailingLength(trail_lengths_list,facility_line_crop_id_list, source_dict):
    
    # goal: compute the optimal trailing length

    # input: list of trail lengths
    # trail_lengths_list: list of two integer trailing average lengths to compare
    # facility_line_crop_id_list: list of strings for distinct facility line and crop id's in the crop schedule
    # source_dict: dictionary of plant sites per clam or average headweight (pspc_dict or avg_headweight_dict)

    # output: list of five objects: [favor_trail_length, favor_total, favor_max_percent, use_list, remaining_list]
        # 1. favor_trail_length: integer optimal trail length
        # 2. favor_total: integer total number of crop lines considered
        # 3. favor_max_percent: percentage of crop lines with optimal trail length 
        # 4. use_list: list of strings facility_line_crop_id that are more accurate using the first trailing length in trail_lengths_list
        # 5. remaining_list: list of strings facility_line_crop_id that are more accurate using the second trailing length in trail_lengths_list
        
    val_list_length_min = max(trail_lengths_list) + 1
    favor_total = 0
    favor_trail_list = len(trail_lengths_list) * [0]
    use_list = list()
    remaining_list = list()
    for facility_line in source_dict.keys():
        for crop_id in source_dict[facility_line].keys():

            facility_line_crop_id = facility_line + '_' + str(crop_id)

            #print(facility_line_crop_id)
            if facility_line_crop_id in facility_line_crop_id_list:
                #print('Facility: ' + facility_line.split('_')[0] + ', Line: ' + facility_line.split('_')[1] + ', Crop: ' + crop_description_list[crop_id_list.index(crop_id)])

                val_list = list()
                for year_week in source_dict[facility_line][crop_id]:
                    val = source_dict[facility_line][crop_id][year_week]
                    for v in val:
                        val_list += [v]

                val_list.reverse()
                val_list_len = len(val_list)
                if val_list_len >= val_list_length_min:
                    number_of_days_to_evaluate = val_list_len - val_list_length_min + 1
                    #print('Number of days to evaluate: ' + str(number_of_days_to_evaluate))
                    closest_trail_length_count_list = len(trail_lengths_list) * [0]
                    all_idx_count = 0
                    for eval_idx in range(number_of_days_to_evaluate):
                        actual_val = val_list[eval_idx]
                        #print(actual_val)
                        abs_diff_list = list()
                        for trail_length in trail_lengths_list:
                            trail_val_list = val_list[eval_idx + 1 : eval_idx + trail_length + 1]
                            #print(trail_val_list)

                            avg_trail_val = np.mean(trail_val_list)
                            std_trail_val = np.std(trail_val_list)    
                            #print(avg_trail_val)

                            actual_prediction_abs_diff = abs(actual_val-avg_trail_val)
                            abs_diff_list += [actual_prediction_abs_diff]

                        # evaluate all trail lengths
                        #print(abs_diff_list)
                        closest_trail = min(abs_diff_list)
                        closest_trail_idx = abs_diff_list.index(closest_trail)
                        #print(closest_trail_idx)
                        closest_trail_length_count_list[closest_trail_idx] += 1
                        all_idx_count += 1


                    #print(closest_trail_length_count_list)
                    optimal_trail_length = trail_lengths_list[closest_trail_length_count_list.index(max(closest_trail_length_count_list))]
                    #print('Facility: ' + facility_line.split('_')[0] + ', Line: ' + facility_line.split('_')[1] + ', CropID: ' + str(crop_id))
                    #print('Optimal trail length out of '+ str(number_of_days_to_evaluate) +' days: ' + str(optimal_trail_length) + ' (' + str(round(max(closest_trail_length_count_list)/all_idx_count,2)*100) + '%)' )
                    #print()
                    if optimal_trail_length == trail_lengths_list[0]:
                        use_list += [facility_line_crop_id]
                    
                    if optimal_trail_length != trail_lengths_list[0]:
                        remaining_list += [facility_line_crop_id]  
                        
                    favor_total += 1
                    favor_trail_list[closest_trail_length_count_list.index(max(closest_trail_length_count_list))] += 1
                else:
                    #print('Facility: ' + facility_line.split('_')[0] + ', Line: ' + facility_line.split('_')[1] + ', CropID: ' + str(crop_id))
                    #print('Crop line has only ' + str(val_list_len) + ' days')
                    #print()
                    use_list += [facility_line_crop_id]

    favor_max_percent = round(max(favor_trail_list)/favor_total,2) * 100
    favor_trail_length = trail_lengths_list[favor_trail_list.index(max(favor_trail_list))]
    #print('Out of ' + str(favor_total) + ' crop lines, ' + str(favor_max_percent) + '% have better accuracy with '+ str(favor_trail_length) +'-day trailing average')                
    #print()
    
    return [favor_trail_length, favor_total, favor_max_percent, use_list, remaining_list]


def optimalYearOverYear(source_dict,facility_line_crop_id_list,use_five_list,use_six_list):
    # goal: compute which facility lines are more accurate using year over year average than optimal trailing average

    # input:
    # source_dict: dictionary of plant sites per clam or average headweight (pspc_dict or avg_headweight_dict)
    # facility_line_crop_id_list: list of strings for distinct facility line and crop id's in the crop schedule
    # use_five_list: list of facility_line_crop_id strings where 5-day trail is optimal
    # use_six_list: list of facility_line_cropp_id strings where 6-day trail is optimal

    # output: 
    # use_yoy_list: list of facility_line_crop_id strings where the weekly average from a year ago is more accurate than the optimal trailing average
    
    use_yoy_list = list()
    for facility_line in source_dict.keys():
        for crop_id in source_dict[facility_line].keys():

            facility_line_crop_id = facility_line + '_' + str(crop_id)

            #print(facility_line_crop_id)
            if facility_line_crop_id in facility_line_crop_id_list:
                #print('Facility: ' + facility_line.split('_')[0] + ', Line: ' + facility_line.split('_')[1] + ', Crop: ' + crop_description_list[crop_id_list.index(crop_id)])
                yoy_count = 0
                all_count = 0

                val_list = list()
                year_week_list = list()
                for year_week in source_dict[facility_line][crop_id]:
                    val = source_dict[facility_line][crop_id][year_week]
                    for v in val:
                        val_list += [v]
                        year_week_list += [year_week]
                val_list.reverse()
                year_week_list.reverse()

                for year_week_idx in range(len(year_week_list)):
                    year_week = year_week_list[year_week_idx]
                    this_year_week = year_week
                    previous_year = int(this_year_week.split('_')[0])-1
                    week = this_year_week.split('_')[1]
                    last_year_week = str(previous_year) + '_' + week

                    test_val = val_list[year_week_idx]

                    if last_year_week in source_dict[facility_line][crop_id].keys():
                        last_year_avg_val = np.mean(source_dict[facility_line][crop_id][last_year_week])

                        all_count += 1
                        #print('Facility: ' + facility_line.split('_')[0] + ', Line: ' + facility_line.split('_')[1] + ', Crop: ' + crop_description_list[crop_id_list.index(crop_id)])
                        #print(test_val)
                        #print(last_year_avg_val)
                        crop_line_optimal_trail = 1
                        csf_facility_line = facility_line_crop_id.split('_')[0]+"_" + facility_line_crop_id.split('_')[1]
                        
                        if csf_facility_line in use_five_list:
                            crop_line_optimal_trail = 5
                        if csf_facility_line in use_six_list:
                            crop_line_optimal_trail = 6

                        start_idx = year_week_idx
                        #print(start_idx)
                        [trail_val,trail_std_val] = trailingAverageSkip(source_dict, crop_line_optimal_trail, facility_line, crop_id,start_idx)
                        #print(trail_val)
                        yoy_diff = abs(test_val-last_year_avg_val)
                        trail_diff = abs(test_val-trail_val)

                        if yoy_diff < trail_diff:
                            #print(yoy_diff)
                            #print(trail_diff)
                            yoy_count += 1

                if all_count != 0:
                    yoy_ratio = yoy_count/all_count
                    if yoy_ratio > 0.5:        
                        #print('Facility: ' + facility_line.split('_')[0] + ', Line: ' + facility_line.split('_')[1] + ', CropID: ' + str(crop_id))
                        #print('Year over year is optimal out of '+ str(all_count) +' days (' + str(round(yoy_ratio,2)*100) + '%)' )
                        #print()
                        use_yoy_list += [facility_line_crop_id]


    return use_yoy_list

def yearOverYearAverage(source_dict,facility_line,crop_id,harvest_date):
    # goal: compute year over year average plant sites per clam or average headweight for a given facility_line and crop_id

    # input:
    # source_dict: dictionary of plant sites per clam or average headweight (pspc_dict or avg_headweight_dict)
    # facility_line: string corresponding to facility line
    # crop_id: int cooresponding to crop id
    # harvest_date: datetime cooresponding to the date of harvest

    # output: list of two floats
    # yoy_avg_val: plant sites per clam or average headweight week average from one year ago
    # yoy_std: plant sites per clam or average headweight week standard deviation from one year ago
    harvest_year = harvest_date.year
    harvest_week = harvest_date.isocalendar()[1]
    last_year = harvest_year-1
    last_year_week = str(last_year) + '_' + str(harvest_week)

    if last_year_week in source_dict[facility_line][crop_id].keys():
        last_year_avg_val = np.mean(source_dict[facility_line][crop_id][last_year_week])
        last_year_std = np.std(source_dict[facility_line][crop_id][last_year_week])
    else:
        print(last_year_week + 'not in source dictionary')
        [last_year_avg_val, last_year_std] = trailingAverage(source_dict,1,facility_line,crop_id)
            
    return [last_year_avg_val, last_year_std]




#print('functions loaded')


# In[ ]:




