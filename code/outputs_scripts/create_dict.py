#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 12 11:18:52 2025

@author: lirhoxhaj
"""

## PURPOSE OF FILE: Processing OUTPUT_0, OUTPUT_1, OUTPUT_1_corp and OUTPUT_1_ind and merging with other relevant data to get: 
# - OUTPUT_1_final_collapsed
# - OUTPUT_1_final_collapsed_dict
# - OUTPUT_2
# - OUTPUT_3
# - OUTPUT_4
# - OUTPUT_5
# - OUTPUT_6
# - OUTPUT_7
# - OUTPUT_8
# - OUTPUT_9


### LIBRARIES

# pip install polars

import os
import pandas as pd
import numpy as np
    
#%%

# Manually creating dictionary of variables
OUTPUT_1_final_collapsed_dict = {
    
    # Entity-time variables
    'district': {
            'description': 'District code: two-letter state code followed by congressional district number. Used to merge various datasets',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_1 (Entity-time variables)'
            },       
    
    'cycle': {
            'description': 'Year of election cycle. Used to merge various datasets',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_1 (Entity-time variables)'
            },       
    
    'real_data': {
            'description': "Because we balance the panel data (i.e., have every cycle for each district), this dummy is used to indicate whether the district-cycle existed (value: 1) or not (value: 0). For the sake of this research, we consider a district to exist in cycles when elections are held. We create the dummy when merging the grouped-by district and cycle contribution-level dataset (depending on the variables, either OUTPUT_2, OUTPUT_3, OUTPUT_4, or OUTPUT_8) with a dataset that contains all district and cycle combinations that were either created or discontinued after 1980 (new_districts_filtered.csv), since every DIME contribution comes after this election year. If the district-cycle was created before 1980 and has no discontinuation year, it implies the district's persistent existence in our period of interest (i.e., 1980 to 2024), meaning that the district will always receive 1 and we don't have to deal with this. If a district-cycle was either created or discontinued or both after 1980, then this district-cycle receives 1 if the cycle is between the district's creation and discontinuation year, while it receives 0 if the cycle is before the district's creation year or later than the district's discontinuation year. Missing values coming from 'fake districts' (real_data == 0) were replaced with zeros.",
            'source': 'Wikipedia',
            'origin_dataset': 'new_districts.html; new_districts.csv; new_districts_filtered_universe.csv; new_districts_filtered_universe_party.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_United_States_congressional_districts',
            'output_relation': 'OUTPUT_2, OUTPUT_3, OUTPUT_4, OUTPUT_8 (Variables measured in or related to dollar amounts)'
            },       
    
    'territorial': {
            'description': "Dummy indicating whether district is a non-voting delegation (i.e. does not have a vote in the US House of Representative).",
            'source': 'Wikipedia',
            'origin_dataset': 'new_districts.html; new_districts.csv; new_districts_filtered_universe.csv; new_districts_filtered_universe_party.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_United_States_congressional_districts',
            'output_relation': 'OUTPUT_2, OUTPUT_3, OUTPUT_4, OUTPUT_8 (Variables measured in or related to dollar amounts)'
            },    
    
    
    # OUTPUT_2
    'total_amount': {
            'description': 'Total amount of all contribution types going to all election types in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_2'
            },       
    
    'tran_count': {
            'description': 'Number of transactions for total_amount',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_2'
            },       
    
    'total_amount_without_LTS1': {
            'description': 'Total amount of all contribution types going to all election types before the date of the special election in that district-cycle, if applicable (measured in dollars). We get special elections date from our webscraping the Wikipedia page with this information (see variables in OUTPUT_7 for more details).',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },       
    
    'tran_count_without_LTS1': {
            'description': 'Number of transactions for total_amount_without_LTS1',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },       
    
    'total_amount_no_primary': {
            'description': 'Total amount of all contribution types going to all election types (except primary elections) in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_2'
            },       
    
    'tran_count_no_primary': {
            'description': 'Number of transactions for total_amount_no_primary',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_2'
            },       
    
    'total_amount_no_primary_without_LTS1': {
            'description': 'Total amount of all contribution types going to all election types (except primary elections) before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },       
    
    'tran_count_no_primary_without_LTS1': {
            'description': 'Number of transactions for total_amount_no_primary_without_LTS1',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },       
    
    'total_amount_primary': {
            'description': 'Total amount of all contribution types going to primary elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_2'
            },       
    
    'tran_count_primary': {
            'description': 'Number of transactions for total_amount_primary',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_2'
            },       
    
    'total_amount_primary_without_LTS1': {
            'description': 'Total amount of all contribution types going to primary elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },       
    
    'tran_count_primary_without_LTS1': {
            'description': 'Number of transactions for total_amount_primary_without_LTS1',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },       
    
    'total_amount_gen': {
            'description': 'Total amount of all contribution types going to general elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_2'
            },       
    
    'tran_count_gen': {
            'description': 'Number of transactions for total_amount_gen',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_2'
            },       
    
    'total_amount_gen_without_LTS1': {
            'description': 'Total amount of all contribution types going to general elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },       
    
    'tran_count_gen_without_LTS1': {
            'description': 'Number of transactions for total_amount_gen_without_LTS1',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },       
    
    'total_amount_special': {
            'description': 'Total amount of all contribution types going to special elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_2'
            },       
    
    'tran_count_special': {
            'description': 'Number of transactions for total_amount_special',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_2'
            },       
    
    'total_amount_special_without_LTS1': {
            'description': 'Total amount of all contribution types going to special elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },       
    
    'tran_count_special_without_LTS1': {
            'description': 'Number of transactions for total_amount_special_without_LTS1',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },       
    
    'total_amount_dem_gen': {
            'description': 'Total amount of all contribution types going to Democratic candidate in general elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_2'
            },       
    
    'tran_count_dem_gen': {
            'description': 'Number of transactions for total_amount_dem_gen',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_2'
            },       
    
    'total_amount_dem_gen_without_LTS1': {
            'description': 'Total amount of all contribution types going to Democratic candidate in general elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },       
    
    'tran_count_dem_gen_without_LTS1': {
            'description': 'Number of transactions for total_amount_dem_gen_without_LTS1',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },       
    
    'total_amount_dem_primary': {
            'description': 'Total amount of all contribution types going to Democratic candidates in primary elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_2'
            },       
    
    'tran_count_dem_primary': {
            'description': 'Number of transactions for total_amount_dem_primary',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_2'
            },       
    
    'total_amount_dem_primary_without_LTS1': {
            'description': 'Total amount of all contribution types going to Democratic candidates in primary elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },       
    
    'tran_count_dem_primary_without_LTS1': {
            'description': 'Number of transactions for total_amount_dem_primary_without_LTS1',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },       
    
    'total_amount_dem_special': {
            'description': 'Total amount of all contribution types going to Democratic candidates in special elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_2'
            },       
    
    'tran_count_dem_special': {
            'description': 'Number of transactions for total_amount_dem_special',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_2'
            },       
    
    'total_amount_dem_special_without_LTS1': {
            'description': 'Total amount of all contribution types going to Democratic candidates in special elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },       
    
    'tran_count_dem_special_without_LTS1': {
            'description': 'Number of transactions for total_amount_dem_special_without_LTS1',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },       
    
    'total_amount_rep_gen': {
            'description': 'Total amount of all contribution types going to Republican candidate in general elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_2'
            },       
    
    'tran_count_rep_gen': {
            'description': 'Number of transactions for total_amount_rep_gen',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_2'
            },       
    
    'total_amount_rep_gen_without_LTS1': {
            'description': 'Total amount of all contribution types going to Republican candidate in general elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },       
    
    'tran_count_rep_gen_without_LTS1': {
            'description': 'Number of transactions for total_amount_rep_gen_without_LTS1',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },       
    
    'total_amount_rep_primary': {
            'description': 'Total amount of all contribution types going to Republican candidates in primary elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_2'
            },       
    
    'tran_count_rep_primary': {
            'description': 'Number of transactions for total_amount_rep_primary',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_2'
            },       
    
    'total_amount_rep_primary_without_LTS1': {
            'description': 'Total amount of all contribution types going to Republican candidates in primary elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },       
    
    'tran_count_rep_primary_without_LTS1': {
            'description': 'Number of transactions for total_amount_rep_primary_without_LTS1',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },       
    
    'total_amount_rep_special': {
            'description': 'Total amount of all contribution types going to Republican candidates in special elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_2'
            },       
    
    'tran_count_rep_special': {
            'description': 'Number of transactions for total_amount_rep_special',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_2'
            },       
    
    'total_amount_rep_special_without_LTS1': {
            'description': 'Total amount of all contribution types going to Democratic candidates in special elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },       
    
    'tran_count_rep_special_without_LTS1': {
            'description': 'Number of transactions for total_amount_rep_special_without_LTS1',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },   

    # OUTPUT_3    
    'total_amount_corp': {
            'description': 'Total amount of all corporate contributions going to all election types in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_3'
            },       
    
    'tran_count_corp': {
            'description': 'Number of transactions for total_amount_corp',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_3'
            },       
    
    'total_amount_without_LTS1_corp': {
            'description': 'Total amount of all corporate contributions going to all election types before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_3'
            },       
    
    'tran_count_without_LTS1_corp': {
            'description': 'Number of transactions for total_amount_without_LTS1_corp',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_3'
            },       
    
    'total_amount_no_primary_corp': {
            'description': 'Total amount of all corporate contributions going to all election types (except primary elections) in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_3'
            },       
    
    'tran_count_no_primary_corp': {
            'description': 'Number of transactions for total_amount_no_primary_corp',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_3'
            },       
    
    'total_amount_no_primary_without_LTS1_corp': {
            'description': 'Total amount of all corporate contributions going to all election types (except primary elections) before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_3'
            },       
    
    'tran_count_no_primary_without_LTS1_corp': {
            'description': 'Number of transactions for total_amount_no_primary_without_LTS1_corp',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_3'
            },       
    
    'total_amount_primary_corp': {
            'description': 'Total amount of all corporate contributions going to primary elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_3'
            },       
    
    'tran_count_primary_corp': {
            'description': 'Number of transactions for total_amount_primary_corp',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_3'
            },       
    
    'total_amount_primary_without_LTS1_corp': {
            'description': 'Total amount of all corporate contributions going to primary elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_3'
            },       
    
    'tran_count_primary_without_LTS1_corp': {
            'description': 'Number of transactions for total_amount_primary_without_LTS1_corp',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_3'
            },       
    
    'total_amount_gen_corp': {
            'description': 'Total amount of all corporate contributions going to general elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_3'
            },       
    
    'tran_count_gen_corp': {
            'description': 'Number of transactions for total_amount_gen_corp',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_3'
            },       
    
    'total_amount_gen_without_LTS1_corp': {
            'description': 'Total amount of all corporate contributions going to general elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_3'
            },       
    
    'tran_count_gen_without_LTS1_corp': {
            'description': 'Number of transactions for total_amount_gen_without_LTS1_corp',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_3'
            },       
    
    'total_amount_special_corp': {
            'description': 'Total amount of all corporate contributions going to special elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_3'
            },       
    
    'tran_count_special_corp': {
            'description': 'Number of transactions for total_amount_special_corp',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_3'
            },       
    
    'total_amount_special_without_LTS1_corp': {
            'description': 'Total amount of all corporate contributions going to special elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_3'
            },       
    
    'tran_count_special_without_LTS1_corp': {
            'description': 'Number of transactions for total_amount_special_without_LTS1_corp',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_3'
            },       
    
    'total_amount_dem_gen_corp': {
            'description': 'Total amount of all corporate contributions going to Democratic candidate in general elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_3'
            },       
    
    'tran_count_dem_gen_corp': {
            'description': 'Number of transactions for total_amount_dem_gen_corp',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_3'
            },       
    
    'total_amount_dem_gen_without_LTS1_corp': {
            'description': 'Total amount of all corporate contributions going to Democratic candidate in general elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_3'
            },       
    
    'tran_count_dem_gen_without_LTS1_corp': {
            'description': 'Number of transactions for total_amount_dem_gen_without_LTS1_corp',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_3'
            },       
    
    'total_amount_dem_primary_corp': {
            'description': 'Total amount of all corporate contributions going to Democratic candidates in primary elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_3'
            },       
    
    'tran_count_dem_primary_corp': {
            'description': 'Number of transactions for total_amount_dem_primary_corp',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_3'
            },       
    
    'total_amount_dem_primary_without_LTS1_corp': {
            'description': 'Total amount of all corporate contributions going to Democratic candidates in primary elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_3'
            },       
    
    'tran_count_dem_primary_without_LTS1_corp': {
            'description': 'Number of transactions for total_amount_dem_primary_without_LTS1_corp',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_3'
            },       
    
    'total_amount_dem_special_corp': {
            'description': 'Total amount of all corporate contributions going to Democratic candidates in special elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_3'
            },       
    
    'tran_count_dem_special_corp': {
            'description': 'Number of transactions for total_amount_dem_special_corp',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_3'
            },       
    
    'total_amount_dem_special_without_LTS1_corp': {
            'description': 'Total amount of all corporate contributions going to Democratic candidates in special elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_3'
            },       
    
    'tran_count_dem_special_without_LTS1_corp': {
            'description': 'Number of transactions for total_amount_dem_special_without_LTS1_corp',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_3'
            },       
    
    'total_amount_rep_gen_corp': {
            'description': 'Total amount of all corporate contributions going to Republican candidate in general elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_3'
            },       
    
    'tran_count_rep_gen_corp': {
            'description': 'Number of transactions for total_amount_rep_gen_corp',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_3'
            },       
    
    'total_amount_rep_gen_without_LTS1_corp': {
            'description': 'Total amount of all corporate contributions going to Republican candidate in general elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_3'
            },       
    
    'tran_count_rep_gen_without_LTS1_corp': {
            'description': 'Number of transactions for total_amount_rep_gen_without_LTS1_corp',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_3'
            },       
    
    'total_amount_rep_primary_corp': {
            'description': 'Total amount of all corporate contributions going to Republican candidates in primary elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_3'
            },       
    
    'tran_count_rep_primary_corp': {
            'description': 'Number of transactions for total_amount_rep_primary_corp',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_3'
            },       
    
    'total_amount_rep_primary_without_LTS1_corp': {
            'description': 'Total amount of all corporate contributions going to Republican candidates in primary elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_3'
            },       
    
    'tran_count_rep_primary_without_LTS1_corp': {
            'description': 'Number of transactions for total_amount_rep_primary_without_LTS1_corp',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_3'
            },       
    
    'total_amount_rep_special_corp': {
            'description': 'Total amount of all corporate contributions going to Republican candidates in special elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_3'
            },       
    
    'tran_count_rep_special_corp': {
            'description': 'Number of transactions for total_amount_rep_special_corp',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_3'
            },       
    
    'total_amount_rep_special_without_LTS1_corp': {
            'description': 'Total amount of all corporate contributions going to Democratic candidates in special elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_3'
            },       
    
    'tran_count_rep_special_without_LTS1_corp': {
            'description': 'Number of transactions for total_amount_rep_special_without_LTS1_corp',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_3'
            },   
    
    # OUTPUT_4 (OUTPUT_4_1 and OUTPUT_4_2)
    'total_amount_ind': {
            'description': 'Total amount of all individual contributions going to all election types in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'tran_count_ind': {
            'description': 'Number of transactions for total_amount_ind',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'total_amount_without_LTS1_ind': {
            'description': 'Total amount of all individual contributions going to all election types before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'tran_count_without_LTS1_ind': {
            'description': 'Number of transactions for total_amount_without_LTS1_ind',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'total_amount_no_primary_ind': {
            'description': 'Total amount of all individual contributions going to all election types (except primary elections) in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'tran_count_no_primary_ind': {
            'description': 'Number of transactions for total_amount_no_primary_ind',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'total_amount_no_primary_without_LTS1_ind': {
            'description': 'Total amount of all individual contributions going to all election types (except primary elections) before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'tran_count_no_primary_without_LTS1_ind': {
            'description': 'Number of transactions for total_amount_no_primary_without_LTS1_ind',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'total_amount_primary_ind': {
            'description': 'Total amount of all individual contributions going to primary elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'tran_count_primary_ind': {
            'description': 'Number of transactions for total_amount_primary_ind',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'total_amount_primary_without_LTS1_ind': {
            'description': 'Total amount of all individual contributions going to primary elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'tran_count_primary_without_LTS1_ind': {
            'description': 'Number of transactions for total_amount_primary_without_LTS1_ind',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'total_amount_gen_ind': {
            'description': 'Total amount of all individual contributions going to general elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'tran_count_gen_ind': {
            'description': 'Number of transactions for total_amount_gen_ind',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'total_amount_gen_without_LTS1_ind': {
            'description': 'Total amount of all individual contributions going to general elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'tran_count_gen_without_LTS1_ind': {
            'description': 'Number of transactions for total_amount_gen_without_LTS1_ind',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'total_amount_special_ind': {
            'description': 'Total amount of all individual contributions going to special elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'tran_count_special_ind': {
            'description': 'Number of transactions for total_amount_special_ind',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'total_amount_special_without_LTS1_ind': {
            'description': 'Total amount of all individual contributions going to special elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'tran_count_special_without_LTS1_ind': {
            'description': 'Number of transactions for total_amount_special_without_LTS1_ind',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'total_amount_dem_gen_ind': {
            'description': 'Total amount of all individual contributions going to Democratic candidate in general elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'tran_count_dem_gen_ind': {
            'description': 'Number of transactions for total_amount_dem_gen_ind',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'total_amount_dem_gen_without_LTS1_ind': {
            'description': 'Total amount of all individual contributions going to Democratic candidate in general elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'tran_count_dem_gen_without_LTS1_ind': {
            'description': 'Number of transactions for total_amount_dem_gen_without_LTS1_ind',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'total_amount_dem_primary_ind': {
            'description': 'Total amount of all individual contributions going to Democratic candidates in primary elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'tran_count_dem_primary_ind': {
            'description': 'Number of transactions for total_amount_dem_primary_ind',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'total_amount_dem_primary_without_LTS1_ind': {
            'description': 'Total amount of all individual contributions going to Democratic candidates in primary elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'tran_count_dem_primary_without_LTS1_ind': {
            'description': 'Number of transactions for total_amount_dem_primary_without_LTS1_ind',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'total_amount_dem_special_ind': {
            'description': 'Total amount of all individual contributions going to Democratic candidates in special elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'tran_count_dem_special_ind': {
            'description': 'Number of transactions for total_amount_dem_special_ind',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'total_amount_dem_special_without_LTS1_ind': {
            'description': 'Total amount of all individual contributions going to Democratic candidates in special elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'tran_count_dem_special_without_LTS1_ind': {
            'description': 'Number of transactions for total_amount_dem_special_without_LTS1_ind',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'total_amount_rep_gen_ind': {
            'description': 'Total amount of all individual contributions going to Republican candidate in general elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'tran_count_rep_gen_ind': {
            'description': 'Number of transactions for total_amount_rep_gen_ind',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'total_amount_rep_gen_without_LTS1_ind': {
            'description': 'Total amount of all individual contributions going to Republican candidate in general elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'tran_count_rep_gen_without_LTS1_ind': {
            'description': 'Number of transactions for total_amount_rep_gen_without_LTS1_ind',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'total_amount_rep_primary_ind': {
            'description': 'Total amount of all individual contributions going to Republican candidates in primary elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'tran_count_rep_primary_ind': {
            'description': 'Number of transactions for total_amount_rep_primary_ind',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'total_amount_rep_primary_without_LTS1_ind': {
            'description': 'Total amount of all individual contributions going to Republican candidates in primary elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'tran_count_rep_primary_without_LTS1_ind': {
            'description': 'Number of transactions for total_amount_rep_primary_without_LTS1_ind',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'total_amount_rep_special_ind': {
            'description': 'Total amount of all individual contributions going to Republican candidates in special elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'tran_count_rep_special_ind': {
            'description': 'Number of transactions for total_amount_rep_special_ind',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'total_amount_rep_special_without_LTS1_ind': {
            'description': 'Total amount of all corporate contributions going to Democratic candidates in special elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },       
    
    'tran_count_rep_special_without_LTS1_ind': {
            'description': 'Number of transactions for total_amount_rep_special_without_LTS1_ind',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_1)'
            },   
    
    'total_amount_smallind': {
                'description': 'Total amount of all individual contributions with small donations (less than 200) going to all election types in that district-cycle (measured in dollars)',
                'source': 'DIME data',
                'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
                'relevant_URLs': 'https://data.stanford.edu/dime',
                'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
                },       
    
    'tran_count_smallind': {
            'description': 'Number of transactions for total_amount_smallind',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'total_amount_without_LTS1_smallind': {
            'description': 'Total amount of all individual contributions with small donations (less than 200) going to all election types before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'tran_count_without_LTS1_smallind': {
            'description': 'Number of transactions for total_amount_without_LTS1_smallind',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'total_amount_no_primary_smallind': {
            'description': 'Total amount of all individual contributions with small donations (less than 200) going to all election types (except primary elections) in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'tran_count_no_primary_smallind': {
            'description': 'Number of transactions for total_amount_no_primary_smallind',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'total_amount_no_primary_without_LTS1_smallind': {
            'description': 'Total amount of all individual contributions with small donations (less than 200) going to all election types (except primary elections) before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'tran_count_no_primary_without_LTS1_smallind': {
            'description': 'Number of transactions for total_amount_no_primary_without_LTS1_smallind',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'total_amount_primary_smallind': {
            'description': 'Total amount of all individual contributions with small donations (less than 200) going to primary elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'tran_count_primary_smallind': {
            'description': 'Number of transactions for total_amount_primary_smallind',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'total_amount_primary_without_LTS1_smallind': {
            'description': 'Total amount of all individual contributions with small donations (less than 200) going to primary elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'tran_count_primary_without_LTS1_smallind': {
            'description': 'Number of transactions for total_amount_primary_without_LTS1_smallind',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'total_amount_gen_smallind': {
            'description': 'Total amount of all individual contributions with small donations (less than 200) going to general elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'tran_count_gen_smallind': {
            'description': 'Number of transactions for total_amount_gen_smallind',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'total_amount_gen_without_LTS1_smallind': {
            'description': 'Total amount of all individual contributions with small donations (less than 200) going to general elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'tran_count_gen_without_LTS1_smallind': {
            'description': 'Number of transactions for total_amount_gen_without_LTS1_smallind',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'total_amount_special_smallind': {
            'description': 'Total amount of all individual contributions with small donations (less than 200) going to special elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'tran_count_special_smallind': {
            'description': 'Number of transactions for total_amount_special_smallind',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'total_amount_special_without_LTS1_smallind': {
            'description': 'Total amount of all individual contributions with small donations (less than 200) going to special elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'tran_count_special_without_LTS1_smallind': {
            'description': 'Number of transactions for total_amount_special_without_LTS1_smallind',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'total_amount_dem_gen_smallind': {
            'description': 'Total amount of all individual contributions with small donations (less than 200) going to Democratic candidate in general elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'tran_count_dem_gen_smallind': {
            'description': 'Number of transactions for total_amount_dem_gen_smallind',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'total_amount_dem_gen_without_LTS1_smallind': {
            'description': 'Total amount of all individual contributions with small donations (less than 200) going to Democratic candidate in general elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'tran_count_dem_gen_without_LTS1_smallind': {
            'description': 'Number of transactions for total_amount_dem_gen_without_LTS1_smallind',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'total_amount_dem_primary_smallind': {
            'description': 'Total amount of all individual contributions with small donations (less than 200) going to Democratic candidates in primary elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'tran_count_dem_primary_smallind': {
            'description': 'Number of transactions for total_amount_dem_primary_smallind',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'total_amount_dem_primary_without_LTS1_smallind': {
            'description': 'Total amount of all individual contributions with small donations (less than 200) going to Democratic candidates in primary elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'tran_count_dem_primary_without_LTS1_smallind': {
            'description': 'Number of transactions for total_amount_dem_primary_without_LTS1_smallind',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'total_amount_dem_special_smallind': {
            'description': 'Total amount of all individual contributions with small donations (less than 200) going to Democratic candidates in special elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'tran_count_dem_special_smallind': {
            'description': 'Number of transactions for total_amount_dem_special_smallind',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'total_amount_dem_special_without_LTS1_smallind': {
            'description': 'Total amount of all individual contributions with small donations (less than 200) going to Democratic candidates in special elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'tran_count_dem_special_without_LTS1_smallind': {
            'description': 'Number of transactions for total_amount_dem_special_without_LTS1_smallind',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'total_amount_rep_gen_smallind': {
            'description': 'Total amount of all individual contributions with small donations (less than 200) going to Republican candidate in general elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'tran_count_rep_gen_smallind': {
            'description': 'Number of transactions for total_amount_rep_gen_smallind',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'total_amount_rep_gen_without_LTS1_smallind': {
            'description': 'Total amount of all individual contributions with small donations (less than 200) going to Republican candidate in general elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'tran_count_rep_gen_without_LTS1_smallind': {
            'description': 'Number of transactions for total_amount_rep_gen_without_LTS1_smallind',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'total_amount_rep_primary_smallind': {
            'description': 'Total amount of all individual contributions with small donations (less than 200) going to Republican candidates in primary elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'tran_count_rep_primary_smallind': {
            'description': 'Number of transactions for total_amount_rep_primary_smallind',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'total_amount_rep_primary_without_LTS1_smallind': {
            'description': 'Total amount of all individual contributions with small donations (less than 200) going to Republican candidates in primary elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'tran_count_rep_primary_without_LTS1_smallind': {
            'description': 'Number of transactions for total_amount_rep_primary_without_LTS1_smallind',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'total_amount_rep_special_smallind': {
            'description': 'Total amount of all individual contributions with small donations (less than 200) going to Republican candidates in special elections only in that district-cycle (measured in dollars)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'tran_count_rep_special_smallind': {
            'description': 'Number of transactions for total_amount_rep_special_smallind',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'total_amount_rep_special_without_LTS1_smallind': {
            'description': 'Total amount of all individual contributions with small donations (less than 200) going to Republican candidates in special elections only before the date of the special election in that district-cycle, if applicable (measured in dollars)',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },       
    
    'tran_count_rep_special_without_LTS1_smallind': {
            'description': 'Number of transactions for total_amount_rep_special_without_LTS1_smallind',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_4 (or OUTPUT_4_2)'
            },
    
    
    # OUTPUT_5
    'district_color_35_65': {
            'description': "First, we compute average vote share of Democratic Presidential nominee in the district in the closest Presidential election (var: district.pres.vs) across all cycles. Second, if this is between 35 and 65, we assign value 'C'. We assign value 'D' if this average is above 65, and value 'R' otherwise.",
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_5'
            },       
    
    'district_color_30_70': {
            'description': 'Similar to district_color_35_65, but we use 30 and 70 as thresholds.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_5'
            },       
    
    'district_color_40_60': {
            'description': 'Similar to district_color_35_65, but we use 40 and 60 as thresholds.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_5'
            },       
    
    # OUTPUT_6
    'totalvotes': {
            'description': 'Total number of votes in general elections in that district-cycle',
            'source': 'MIT_eMIT elections data',
            'origin_dataset': '1976-2022-house.csv',
            'relevant_URLs': 'https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/IG0UN2',
            'output_relation': 'OUTPUT_6'
            },    
    
    'G_dem': {
            'description': 'Vote share of Democratic candidate in general elections',
            'source': 'MIT_eMIT elections data',
            'origin_dataset': '1976-2022-house.csv',
            'relevant_URLs': 'https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/IG0UN2',
            'output_relation': 'OUTPUT_6'
            },       

    'G_rep': {
            'description': 'Vote share of Republican candidate in general elections',
            'source': 'MIT_eMIT elections data',
            'origin_dataset': '1976-2022-house.csv',
            'relevant_URLs': 'https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/IG0UN2',
            'output_relation': 'OUTPUT_6'
            },       

    'G_dispersion': {
            'description': 'Absolute difference between G_dem and G_rep',
            'source': 'MIT_eMIT elections data',
            'origin_dataset': '1976-2022-house.csv',
            'relevant_URLs': 'https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/IG0UN2',
            'output_relation': 'OUTPUT_6'
            },       
    
    'num_candidates': {
            'description': 'Number of candidates running in general elections',
            'source': 'MIT_eMIT elections data',
            'origin_dataset': '1976-2022-house.csv',
            'relevant_URLs': 'https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/IG0UN2',
            'output_relation': 'OUTPUT_6'
            },       
    
    'G_dispersion_lag': {
            'description': 'Lag of G_dispersion',
            'source': 'MIT_eMIT elections data',
            'origin_dataset': '1976-2022-house.csv',
            'relevant_URLs': 'https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/IG0UN2',
            'output_relation': 'OUTPUT_6'
            },       

    'P_max_dem': {
            'description': 'Vote share of Democratic candidates with highest vote share in primary elections',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_6'
            },       
    
    'P_min_dem': {
            'description': 'Vote share of Democratic candidates with second highest vote share in primary elections',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_6'
            },       
    
    'P_dispersion_dem': {
            'description': 'Absolute difference between P_max_dem and P_min_dem',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_6'
            },       
    
    'num_candidates_dem': {
            'description': 'Number of candidates running in Democratic primary elections',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_6'
            },       
    
    'P_dispersion_dem_lag': {
            'description': 'Lag of P_dispersion_dem',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_6'
            },       
    
    'P_max_rep': {
            'description': 'Vote share of Republican candidates with highest vote share in primary elections',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_6'
            },       

    'P_min_rep': {
            'description': 'Vote share of Republican candidates with second highest vote share in primary elections',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_6'
            },       

    'P_dispersion_rep': {
            'description': 'Absolute difference between P_max_rep and P_min_rep',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_6'
            },       

    'num_candidates_rep': {
            'description': 'Number of candidates running in Republican primary elections',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_6'
            },       

    'P_dispersion_rep_lag': {
            'description': 'Lag of P_dispersion_rep',
            'source': 'DIME data',
            'origin_dataset': 'election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_6'
            },       

    'Election_day': {
            'description': 'Date of the general election held at that election cycle.',
            'source': 'ChatGPT',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': '',
            'output_relation': 'OUTPUT_6'
            },       

    
    # OUTPUT_7
    
    'spec_cycle': {
            'description': "Year of election cycle when special elections took place. Death cycle is assumed known when treatment values appear. If empty, then death_cycle variable not equal to cycle variable.",
            'source': 'Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7'
            },       

    'death_date': {
            'description': "Date when incumbent passed away. In terms of time, this is when we consider a district-cycle observation to be treated (for the first time at least).",
            'source': 'Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7'
            },               
    
    'special_elections_date': {
            'description': "Date when special elections took place, following death of incumbent.",
            'source': 'Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7'        
            },
    
    'dead_member_margin': {
            'description': "Margin of votes (in percent) by how much deceased incumbent won in the last election before they passed away.",
            'source': 'Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7'        
            },

    'spec_winner_margin': {
            'description': "Margin of votes (in percent) by how new district representative won in the special elections following the incumbent's death.",
            'source': 'Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7'        
            },
    
    'runoff': {
            'description': "Dummy indicating whether there was a runoff in special elections",
            'source': 'Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7'        
            },    
    
    'treat_1': {
            'description': "First dummy we use to measure the impact of the death of an incumbent on contributions. For single death districts, we assign values of 0 before death of the incumbent, 1 at the district-cycle when the first (and only) death occurs and all following cycles. For multiple death districts, we assume the first death is the only death, hence the same logic is repeated. Otherwise, variable will always have zero values if districts are not part of the treatment (i.e., has never experienced the death of an incumbent).",
            'source': 'Author (from merged datasets)',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7'
            },       
    
    'treat_2': {
            'description': "Second dummy we use to measure the impact of the death of an incumbent on contributions. For single death districts, we repeat the logic we used for these districts in treat_1. For multiple death districts, we assign 0 to all cycles coming before the first death of an incumbent in that district's history, 1 at the district-cycle when the first death occurs. In the new election cycle the values are set back to 0, and only assigned 1 again at the following death. This is repeated for the second death, and all other deaths. Otherwise, variable will always have zero values if districts are not part of the treatment (i.e., has never experienced the death of an incumbent).",
            'source': 'Author (from merged datasets)',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7'
            },      
    
    'treat_3': {
            'description': "Third dummy we use to measure the impact of the death of an incumbent on contributions. For single death districts, we assign 0 to all cycles coming before the first (only) death of an incumbent in that district's history, 1 at the district-cycle when the first (only) death occurs. In the new election cycle the values are set back to 0. For multiple death districts, we repeat the logic we used for these districts in treat_2, so 1 for all treated district-cycles, else 0 for all other untreated rows.",
            'source': 'Author (from merged datasets)',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7'
            },      
    
    'death_unexpected_1': {
            'description': "Dummy variable tied with death_age_1 and death_party_1, which all have non-missing values if there was a death in the district's history (based on whether treat_1 is 1). Indicates if death of candidate was unexpected or not (1 or 0), based on the cause of the death. This value is constant for entire district, if applicable, otherwise it will have missing values if districts are not part of the treatment (i.e., has never experienced the death of an incumbent).",
            'source': 'ChatGPT ; Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7'
            },       
    
    'death_age_1': {
            'description': "Numerical variable tied with death_unexpected_1 and death_party_1, which all have non-missing values if there was a death in the district's history (based on whether treat_1 is 1). Indicates age at which candidate passed away. This value is constant for entire district, if applicable, otherwise it will have missing values if districts are not part of the treatment (i.e., has never experienced the death of an incumbent).",
            'source': 'ChatGPT ; Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7'
            },       
    
    'death_party_1': {
            'description': "Categorical variable tied with death_unexpected_1 and death_age_1, which all have non-missing values if there was a death in the district's history (based on whether treat_1 is 1). Indicates party of candidate ('R' - Republican, 'D' - Democrat). This value is constant for entire district, if applicable, otherwise it will have missing values if districts are not part of the treatment (i.e., has never experienced the death of an incumbent).",
            'source': 'ChatGPT ; Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7'
            },       
    
    'death_unexpected_2': {
            'description': "Dummy variable tied with death_age_2 and death_party_2, which all have non-missing values if there was a death in the district's history (based on whether treat_2 is 1). Indicates if death of candidate was unexpected or not (1 or 0), based on the cause of the death. This value is constant for all cycles coming before the first death, value switches accordingly at district-cycle of second death and thereafter, and again if other deaths are present after the second one. If a district-cycle is between two district-cycle rows that have experienced deaths, we assign the previous death's values of the variable. Otherwise, the variable will have missing values if districts are not part of the treatment (i.e., has never experienced the death of an incumbent).",
            'source': 'ChatGPT ; Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7'
            },       
    
    'death_age_2': {
            'description': "Numerical variable tied with death_unexpected_2 and death_party_2, which all have non-missing values if there was a death in the district's history (based on whether treat_2 is 1). Indicates age at which candidate passed away. This value is constant for all cycles coming before the first death, value switches accordingly at district-cycle of second death and thereafter, and again if other deaths are present after the second one. If a district-cycle is between two district-cycle rows that have experienced deaths, we assign the previous death's values of the variable. Otherwise, the variable will have missing values if districts are not part of the treatment (i.e., has never experienced the death of an incumbent).",
            'source': 'ChatGPT ; Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7'
            },       
    
    'death_party_2': {
            'description': "Categorical variable tied with death_unexpected_2 and death_age_2, which all have non-missing values if there was a death in the district's history (based on whether treat_2 is 1). Indicates party of candidate ('R' - Republican, 'D' - Democrat). This value is constant for all cycles coming before the first death, value switches accordingly at district-cycle of second death and thereafter, and again if other deaths are present after the second one. If a district-cycle is between two district-cycle rows that have experienced deaths, we assign the previous death's values of the variable. Otherwise, the variable will have missing values if districts are not part of the treatment (i.e., has never experienced the death of an incumbent).",
            'source': 'ChatGPT ; Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7'
            },    
    
    'death_unexpected_3': {
            'description': "Dummy variable tied with death_age_3 and death_party_3, which all have non-missing values if there was a death in the district's history (based on whether treat_3 is 1). Indicates if death of candidate was unexpected or not (1 or 0), based on the cause of the death.",
            'source': 'ChatGPT ; Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7'
            },       
    
    'death_age_3': {
            'description': "Numerical variable tied with death_unexpected_3 and death_party_3, which all have non-missing values if there was a death in the district's history (based on whether treat_3 is 1). Indicates age at which candidate passed away.",
            'source': 'ChatGPT ; Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7'
            },       
    
    'death_party_3': {
            'description': "Categorical variable tied with death_unexpected_3 and death_age_3, which all have non-missing values if there was a death in the district's history (based on whether treat_3 is 1). Indicates party of candidate ('R' - Republican, 'D' - Democrat). ",
            'source': 'ChatGPT ; Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7'
            },   
    
    'spec_member': {
            'description': 'String variable indicating the full name of incumbent (candidate) that either resigned or passed away at that specific district-cycle. Missing values if no candidate passed away or resigned at that district-cycle.',
            'source': 'ChatGPT ; Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7'
            },       
    
    'special_elections_cause': {
            'description': "String variable indicating the cause of vacancy and, therefore, special elections (either 'Resigned' or 'Death').",
            'source': 'ChatGPT ; Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7'
            },       
    
    'special_elections': {
            'description': 'Dummy variable indicating if special elections occurred on that district-cycle (1, yes, 0, not).',
            'source': 'ChatGPT ; Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7'
            },       
    
    # OUTPUT_8    
    
    'avg_counting_hedging_corp': {
            'description': 'Average number of candidates (in general election only, and before the special election) funded by corporations in the district/cycle',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8'
            },       
    
    'avg_counting_hedging_corp_dem_primary': {
            'description': 'Average number of Democratic candidates (in primary election only, and before the special election) funded by corporations in the district/cycle',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8'
            },       
    
    'avg_counting_hedging_corp_rep_primary': {
            'description': 'Average number of Republican candidates (in primary election only, and before the special election) funded by corporations in the district/cycle',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8'
            },       
    
    'hedging_money_general_corp': {
            'description': 'The index of extensive-margin hedging is computed as the absolute difference between a corporation’s contributions to Republican and Democratic candidates in a given district and election cycle (only for general election, before the special election). This captures the extent to which a firm biases its contributions toward one party over the other. The index is constructed taking the average of this difference across corporations in the same district and cycle. ',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8'
            },       
    
    'hedging_money_dem_primary_corp': {
            'description': "Similar to hedging_money_general, but look at corporate contributions going to Democratic primaries.",
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8'
            },       
    
    'hedging_money_rep_primary_corp': {
            'description': "Similar to hedging_money_general, but look at corporate contributions going to Republican primaries.",
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8'
            },       
    
    'avg_counting_hedging_ind': {
            'description': 'Average number of candidates (in general election only, and before the special election) funded by individuals in the district/cycle',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8'
            },       
    
    'avg_counting_hedging_ind_dem_primary': {
            'description': 'Average number of Democratic candidates (in primary election only, and before the special election) funded by individuals in the district/cycle',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8'
            },       
    
    'avg_counting_hedging_ind_rep_primary': {
            'description': 'Average number of Republican candidates (in primary election only, and before the special election) funded by individuals in the district/cycle',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8'
            },       
    
    'hedging_money_general_ind': {
            'description': 'Same as hedging_money_general_corp, but for contributions coming from inidviduals.',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8'
            },       
    
    'hedging_money_dem_primary_ind': {
            'description': "Similar to hedging_money_general_ind, but look at individual contributions going to Democratic primaries.",
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8'
            },       
    
    'hedging_money_rep_primary_ind': {
            'description': "Similar to hedging_money_general, but look at individual contributions going to Republican primaries.",
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8'
            },         
    
    'avg_counting_hedging_smallind': {
            'description': 'Average number of candidates (in general election only, and before the special election) funded by individuals with small donations (less than 200) in the district/cycle',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8'
            },       
    
    'avg_counting_hedging_smallind_dem_primary': {
            'description': 'Average number of Democratic candidates (in primary election only, and before the special election) funded by individuals with small donations (less than 200) in the district/cycle',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8'
            },       
    
    'avg_counting_hedging_smallind_rep_primary': {
            'description': 'Average number of Republican candidates (in primary election only, and before the special election) funded by individuals with small donations (less than 200) in the district/cycle',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8'
            },       
    
    'hedging_money_general_smallind': {
            'description': 'Same as hedging_money_general_corp, but for contributions coming from individuals with small donations (less than 200).',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8'
            },       
    
    'hedging_money_dem_primary_smallind': {
            'description': "Similar to hedging_money_general_smallind, but look at individual contributions with small donations (less than 200) going to Democratic primaries.",
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8'
            },       
    
    'hedging_money_rep_primary_smallind': {
            'description': "Similar to hedging_money_general_smallind, but look at individual contributions with small donations (less than 200) going to Republican primaries.",
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8'
            },    
    
    # OUTPUT_9
    'cfscore_mean_prim_dem': {
            'description': 'Average CFscore of candidates in the Democratic primary (computed using recipient.cfscores.dyn, and recipient.cfscores when former not available)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_9'
            },       
    
    'cfscore_mean_prim_rep': {
            'description': 'Average CFscore of candidates in the Republican primary (computed using recipient.cfscores.dyn, and recipient.cfscores when former not available)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_9'
            },       
    
    'cfscore_prim_abs_diff': {
            'description': 'Absolute value of difference between cfscore_mean_prim_dem and cfscore_mean_prim_rep, capturing the difference in ideology between the two parties in the primaries in the district-cycle.',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_9'
            },       
    
    'cfscore_gen_dem': {
            'description': 'CFscore of Democratic candidate in general',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_9'
            },       
    
    'cfscore_gen_rep': {
            'description': 'CFscore of Republican candidate in general',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_9'
            },       
    
    'cfscore_gen_abs_diff': {
            'description': 'Absolute value of difference between cfscore_mean_prim_dem and cfscore_mean_prim_rep, capturing the difference in ideology between the two parties in the general election in the district-cycle.',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_9'
            },       
    
    'cfscore_mean_contrib': {
            'description': 'Average CFscore of all contributors (inidividual and corporate)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_9'
            },       
    
    'cfscore_mean_contrib_dem': {
            'description': 'Average CFscore of all contributors (inidividual and corporate) donating to Democrats',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_9'
            },       
    
    'cfscore_mean_contrib_rep': {
            'description': 'Average CFscore of all contributors (inidividual and corporate) donating to Republicans',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime',
            'output_relation': 'OUTPUT_9'
            },          
    
    # OUTPUT_2_ext
    
    'total_amount_gen_np': {
        'description': 'Total amount of all contribution types going to general elections only in untreated district-cycle; going to general and special elections in treated district-cycle. Treatment of district-cycle is defined by treat_3 variable.',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_2_ext'
        },       

    'tran_count_gen_np': {
        'description': 'Number of transactions for total_amount_gen_np.',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_2_ext'
        },       
    

    'total_amount_gen_np_without_LTS1': {
            'description': 'Total amount of all contribution types going to general elections only before the date of the special election in untreated district-cycle; going to general and special elections in treated district-cycle. Treatment of district-cycle is defined by treat_3 variable.',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },       
    
    'tran_count_gen_np_without_LTS1': {
            'description': 'Number of transactions for total_amount_gen_np_without_LTS1',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_2'
            },       
    
    
    'total_amount_dem_gen_np': {
        'description': 'Total amount of all contribution types going to Democratic candidate in general elections only in untreated district-cycle; to general and special elections in treated district-cycle (NP construction; treatment defined by treat_3).',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_2_ext'
    },
    
    'tran_count_dem_gen_np': {
        'description': 'Number of transactions for total_amount_dem_gen_np.',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_2_ext'
    },
    
    'total_amount_dem_gen_np_without_LTS1': {
        'description': 'Total amount to Democratic candidate before the special-election date in untreated district-cycles (general only); to general and special elections before the special-election date in treated district-cycles (NP construction; treatment defined by treat_3).',
        'source': 'DIME data ; ChatGPT ; Wikipedia',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
        'output_relation': 'OUTPUT_2_ext'
    },
    
    'tran_count_dem_gen_np_without_LTS1': {
        'description': 'Number of transactions for total_amount_dem_gen_np_without_LTS1.',
        'source': 'DIME data ; ChatGPT ; Wikipedia',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
        'output_relation': 'OUTPUT_2_ext'
    },
    
    'total_amount_rep_gen_np': {
        'description': 'Total amount of all contribution types going to Republican candidate in general elections only in untreated district-cycle; to general and special elections in treated district-cycle (NP construction; treatment defined by treat_3).',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_2_ext'
    },
    'tran_count_rep_gen_np': {
        'description': 'Number of transactions for total_amount_rep_gen_np.',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_2_ext'
    },
    
    'total_amount_rep_gen_np_without_LTS1': {
        'description': 'Total amount to Republican candidate before the special-election date in untreated district-cycles (general only); to general and special elections before the special-election date in treated district-cycles (NP construction; treatment defined by treat_3).',
        'source': 'DIME data ; ChatGPT ; Wikipedia',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
        'output_relation': 'OUTPUT_2_ext'
    },
    
    'tran_count_rep_gen_np_without_LTS1': {
        'description': 'Number of transactions for total_amount_rep_gen_np_without_LTS1.',
        'source': 'DIME data ; ChatGPT ; Wikipedia',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
        'output_relation': 'OUTPUT_2_ext'
    },
    
    # OUTPUT_3_ext
    'total_amount_gen_np_corp': {
        'description': 'Total corporate contributions to general elections only in untreated district-cycles; to general and special elections in treated district-cycles (NP construction; treatment defined by treat_3). Measured in dollars.',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_3_ext'
    },
    
    'tran_count_gen_np_corp': {
        'description': 'Number of transactions for total_amount_gen_np_corp.',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_3_ext'
    },
    
    'total_amount_gen_np_without_LTS1_corp': {
        'description': 'Corporate contributions before the special-election date under NP construction (general-only if untreated; general+special if treated).',
        'source': 'DIME data ; ChatGPT ; Wikipedia',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
        'output_relation': 'OUTPUT_3_ext'
    },
    
    'tran_count_gen_np_without_LTS1_corp': {
        'description': 'Number of transactions for total_amount_gen_np_without_LTS1_corp.',
        'source': 'DIME data ; ChatGPT ; Wikipedia',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
        'output_relation': 'OUTPUT_3_ext'
    },
    
    'total_amount_dem_gen_np_corp': {
        'description': 'Total corporate contributions to Democratic candidate under NP construction (general-only if untreated; general+special if treated).',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_3_ext'
    },
    
    'tran_count_dem_gen_np_corp': {
        'description': 'Number of transactions for total_amount_dem_gen_np_corp.',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_3_ext'
    },
    
    'total_amount_dem_gen_np_without_LTS1_corp': {
        'description': 'Corporate contributions to Democratic candidate before the special-election date (NP construction).',
        'source': 'DIME data ; ChatGPT ; Wikipedia',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
        'output_relation': 'OUTPUT_3_ext'
    },
    
    'tran_count_dem_gen_np_without_LTS1_corp': {
        'description': 'Number of transactions for total_amount_dem_gen_np_without_LTS1_corp.',
        'source': 'DIME data ; ChatGPT ; Wikipedia',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
        'output_relation': 'OUTPUT_3_ext'
    },
    
    'total_amount_rep_gen_np_corp': {
        'description': 'Total corporate contributions to Republican candidate under NP construction (general-only if untreated; general+special if treated).',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_3_ext'
    },
    
    'tran_count_rep_gen_np_corp': {
        'description': 'Number of transactions for total_amount_rep_gen_np_corp.',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_3_ext'
    },
    
    'total_amount_rep_gen_np_without_LTS1_corp': {
        'description': 'Corporate contributions to Republican candidate before the special-election date (NP construction).',
        'source': 'DIME data ; ChatGPT ; Wikipedia',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
        'output_relation': 'OUTPUT_3_ext'
    },
    
    'tran_count_rep_gen_np_without_LTS1_corp': {
        'description': 'Number of transactions for total_amount_rep_gen_np_without_LTS1_corp.',
        'source': 'DIME data ; ChatGPT ; Wikipedia',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
        'output_relation': 'OUTPUT_3_ext'
    },
    
    # OUTPUT_4_1_ext
    'total_amount_gen_np_ind': {
        'description': 'Total individual contributions to general elections only in untreated district-cycles; to general and special elections in treated district-cycles (NP construction).',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_4_ext'
    },
    
    'tran_count_gen_np_ind': {
        'description': 'Number of transactions for total_amount_gen_np_ind.',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_4_ext'
    },
    
    'total_amount_gen_np_without_LTS1_ind': {
        'description': 'Individual contributions before the special-election date under NP construction (general-only if untreated; general+special if treated).',
        'source': 'DIME data ; ChatGPT ; Wikipedia',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
        'output_relation': 'OUTPUT_4_ext'
    },
    
    'tran_count_gen_np_without_LTS1_ind': {
        'description': 'Number of transactions for total_amount_gen_np_without_LTS1_ind.',
        'source': 'DIME data ; ChatGPT ; Wikipedia',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
        'output_relation': 'OUTPUT_4_ext'
    },
    
    'total_amount_dem_gen_np_ind': {
        'description': 'Total individual contributions to Democratic candidate under NP construction.',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_4_ext'
    },
    
    'tran_count_dem_gen_np_ind': {
        'description': 'Number of transactions for total_amount_dem_gen_np_ind.',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_4_ext'
    },
    
    'total_amount_dem_gen_np_without_LTS1_ind': {
        'description': 'Individual contributions to Democratic candidate before the special-election date (NP construction).',
        'source': 'DIME data ; ChatGPT ; Wikipedia',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
        'output_relation': 'OUTPUT_4_ext'
    },
    
    'tran_count_dem_gen_np_without_LTS1_ind': {
        'description': 'Number of transactions for total_amount_dem_gen_np_without_LTS1_ind.',
        'source': 'DIME data ; ChatGPT ; Wikipedia',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
        'output_relation': 'OUTPUT_4_ext'
    },
    
    'total_amount_rep_gen_np_ind': {
        'description': 'Total individual contributions to Republican candidate under NP construction.',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_4_ext'
    },
    
    'tran_count_rep_gen_np_ind': {
        'description': 'Number of transactions for total_amount_rep_gen_np_ind.',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_4_ext'
    },
    
    'total_amount_rep_gen_np_without_LTS1_ind': {
        'description': 'Individual contributions to Republican candidate before the special-election date (NP construction).',
        'source': 'DIME data ; ChatGPT ; Wikipedia',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
        'output_relation': 'OUTPUT_4_ext'
    },
    
    'tran_count_rep_gen_np_without_LTS1_ind': {
        'description': 'Number of transactions for total_amount_rep_gen_np_without_LTS1_ind.',
        'source': 'DIME data ; ChatGPT ; Wikipedia',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
        'output_relation': 'OUTPUT_4_ext'
    },
    
    # OUTPUT_4_2_ext
    'total_amount_gen_np_smallind': {
        'description': 'Total small-dollar individual contributions (<$200) to general elections only in untreated district-cycles; to general and special elections in treated district-cycles (NP construction).',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_4_ext (or OUTPUT_4_2_ext)'
    },
    
    'tran_count_gen_np_smallind': {
        'description': 'Number of transactions for total_amount_gen_np_smallind.',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_4_ext (or OUTPUT_4_2_ext)'
    },
    
    'total_amount_gen_np_without_LTS1_smallind': {
        'description': 'Small-dollar individual contributions (<$200) before the special-election date under NP construction (general-only if untreated; general+special if treated).',
        'source': 'DIME data ; ChatGPT ; Wikipedia',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
        'output_relation': 'OUTPUT_4_ext (or OUTPUT_4_2_ext)'
    },
    
    'tran_count_gen_np_without_LTS1_smallind': {
        'description': 'Number of transactions for total_amount_gen_np_without_LTS1_smallind.',
        'source': 'DIME data ; ChatGPT ; Wikipedia',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
        'output_relation': 'OUTPUT_4_ext (or OUTPUT_4_2_ext)'
    },
    
    'total_amount_dem_gen_np_smallind': {
        'description': 'Total small-dollar individual contributions (<$200) to Democratic candidate under NP construction.',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_4_ext (or OUTPUT_4_2_ext)'
    },
    
    'tran_count_dem_gen_np_smallind': {
        'description': 'Number of transactions for total_amount_dem_gen_np_smallind.',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_4_ext (or OUTPUT_4_2_ext)'
    },
    
    'total_amount_dem_gen_np_without_LTS1_smallind': {
        'description': 'Small-dollar individual contributions (<$200) to Democratic candidate before the special-election date (NP construction).',
        'source': 'DIME data ; ChatGPT ; Wikipedia',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
        'output_relation': 'OUTPUT_4_ext (or OUTPUT_4_2_ext)'
    },
    
    'tran_count_dem_gen_np_without_LTS1_smallind': {
        'description': 'Number of transactions for total_amount_dem_gen_np_without_LTS1_smallind.',
        'source': 'DIME data ; ChatGPT ; Wikipedia',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
        'output_relation': 'OUTPUT_4_ext (or OUTPUT_4_2_ext)'
    },
    
    'total_amount_rep_gen_np_smallind': {
        'description': 'Total small-dollar individual contributions (<$200) to Republican candidate under NP construction.',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_4_ext (or OUTPUT_4_2_ext)'
    },
    
    'tran_count_rep_gen_np_smallind': {
        'description': 'Number of transactions for total_amount_rep_gen_np_smallind.',
        'source': 'DIME data',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime',
        'output_relation': 'OUTPUT_4_ext (or OUTPUT_4_2_ext)'
    },
    
    'total_amount_rep_gen_np_without_LTS1_smallind': {
        'description': 'Small-dollar individual contributions (<$200) to Republican candidate before the special-election date (NP construction).',
        'source': 'DIME data ; ChatGPT ; Wikipedia',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
        'output_relation': 'OUTPUT_4_ext (or OUTPUT_4_2_ext)'
    },
    
    'tran_count_rep_gen_np_without_LTS1_smallind': {
        'description': 'Number of transactions for total_amount_rep_gen_np_without_LTS1_smallind.',
        'source': 'DIME data ; ChatGPT ; Wikipedia',
        'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
        'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
        'output_relation': 'OUTPUT_4_ext (or OUTPUT_4_2_ext)'
    },

    # OUTPUT_8_ext

    'hedging_money_general_np_corp': {
            'description': 'The index of extensive-margin hedging is computed as the absolute difference between a corporation’s contributions to Republican and Democratic candidates in a given district and election cycle (contributions filtered before the special election; if district-cycle treated, then for general election and special election contributions; else, general elections only). This captures the extent to which a firm biases its contributions toward one party over the other. The index is constructed taking the average of this difference across corporations in the same district and cycle. ',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8_ext'
            },       

    'avg_counting_hedging_np_corp': {
            'description': 'Average number of candidates funded by corporations in the district/cycle (if district-cycle treated, then for general election and special election contributions; else, general elections only) ',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8_ext'
            },       
    
    'hedging_money_general_np_ind': {
            'description': 'Same as hedging_money_general_np_corp but for individual contributions only.',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8_ext'
            },       

    'avg_counting_hedging_np_ind': {
            'description': 'Average number of candidates funded by individuals in the district/cycle (if district-cycle treated, then for general election and special election contributions; else, general elections only) ',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8_ext'
            },       

    'hedging_money_general_np_smallind': {
            'description': 'Same as hedging_money_general_np_corp but for small individual contributions only (less than 200 USD).',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8_ext'
            },       
    
    'avg_counting_hedging_np_smallind': {
            'description': 'Average number of candidates funded by small individuals donors (less than 200 USD) in the district/cycle (if district-cycle treated, then for general election and special election contributions; else, general elections only) ',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_8_ext'
            },       
    
}

def dict_to_df(dictionary):
    # Create a dictionary with each variable having four attributes
    rows = []
    for var_name, attributes in dictionary.items():
        rows.append({
            'variable_name': var_name,
            'description': attributes['description'],
            'source': attributes['source'],
            'origin_dataset': attributes['origin_dataset'],
            'relevant_URLs': attributes['relevant_URLs'],
            'output_relation': attributes['output_relation'],
        })
    df = pd.DataFrame(rows)
    return df

OUTPUT_1_final_collapsed_dict_df = dict_to_df(OUTPUT_1_final_collapsed_dict)


#%%
# Manually creating dictionary of variables
OUTPUT_1_final_collapsed_ext_dict = {
    
    # Entity-time variables
    'district': {
            'description': 'District code: two-letter state code followed by congressional district number. Used to merge various datasets',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_1 (Entity-time variables)'
            },       
    
    'cycle': {
            'description': 'Year of election cycle. Used to merge various datasets',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_1 (Entity-time variables)'
            },       
    
    'treat_1': {
            'description': "First dummy we use to measure the impact of the death of an incumbent on contributions. For single death districts, we assign values of 0 before death of the incumbent, 1 at the district-cycle when the first (and only) death occurs and all following cycles. For multiple death districts, we assume the first death is the only death, hence the same logic is repeated. Otherwise, variable will always have zero values if districts are not part of the treatment (i.e., has never experienced the death of an incumbent).",
            'source': 'Author (from merged datasets)',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7 (Entity-time variables)'
            },       
    
    'treat_2': {
            'description': "Second dummy we use to measure the impact of the death of an incumbent on contributions. For single death districts, we repeat the logic we used for these districts in treat_1. For multiple death districts, we assign 0 to all cycles coming before the first death of an incumbent in that district's history, 1 at the district-cycle when the first death occurs. In the new election cycle the values are set back to 0, and only assigned 1 again at the following death. This is repeated for the second death, and all other deaths. Otherwise, variable will always have zero values if districts are not part of the treatment (i.e., has never experienced the death of an incumbent).",
            'source': 'Author (from merged datasets)',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7 (Entity-time variables)'
            },       

    'treat_3': {
            'description': "Third dummy we use to measure the impact of the death of an incumbent on contributions. For single death districts, we assign 0 to all cycles coming before the first (only) death of an incumbent in that district's history, 1 at the district-cycle when the first (only) death occurs. In the new election cycle the values are set back to 0. For multiple death districts, we repeat the logic we used for these districts in treat_2, so 1 for all treated district-cycles, else 0 for all other untreated rows.",
            'source': 'Author (from merged datasets)',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7 (Entity-time variables)'
            },      

    'death_date': {
            'description': "Year of election cycle when incumbent passed away. In terms of time, this is when we consider a district-cycle observation to be treated (for the first time at least).",
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7 (Entity-time variables)'
            },               
    
    'special_elections_date': {
            'description': "Date when special elections took place, following death of incumbent.",
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives',
            'output_relation': 'OUTPUT_7 (Entity-time variables)'
            },       

    
    'real_data': {
            'description': "Because we balance the panel data (i.e., have every cycle for each district), this dummy is used to indicate whether the district-cycle existed (value: 1) or not (value: 0). For the sake of this research, we consider a district to exist in cycles when elections are held. We create the dummy when merging the grouped-by district and cycle contribution-level dataset (depending on the variables, either OUTPUT_2, OUTPUT_3, OUTPUT_4, or OUTPUT_8) with a dataset that contains all district and cycle combinations that were either created or discontinued after 1980 (new_districts_filtered.csv), since every DIME contribution comes after this election year. If the district-cycle was created before 1980 and has no discontinuation year, it implies the district's persistent existence in our period of interest (i.e., 1980 to 2024), meaning that the district will always receive 1 and we don't have to deal with this. If a district-cycle was either created or discontinued or both after 1980, then this district-cycle receives 1 if the cycle is between the district's creation and discontinuation year, while it receives 0 if the cycle is before the district's creation year or later than the district's discontinuation year. Missing values coming from 'fake districts' (real_data == 0) were replaced with zeros.",
            'source': 'Wikipedia',
            'origin_dataset': 'new_districts.html; new_districts.csv; new_districts_filtered_universe.csv; new_districts_filtered_universe_party.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_United_States_congressional_districts',
            'output_relation': 'OUTPUT_2, OUTPUT_3, OUTPUT_4, OUTPUT_8 (Variables measured in or related to dollar amounts)'
            },       
    
    'territorial': {
            'description': "Dummy indicating whether district is a non-voting delegation (i.e. does not have a vote in the US House of Representative).",
            'source': 'Wikipedia',
            'origin_dataset': 'new_districts.html; new_districts.csv; new_districts_filtered_universe.csv; new_districts_filtered_universe_party.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_United_States_congressional_districts',
            'output_relation': 'OUTPUT_2, OUTPUT_3, OUTPUT_4, OUTPUT_8 (Variables measured in or related to dollar amounts)'
            },    
    
    # OUTPUT_{i}_ext
    '_suffix': {
            'description': 'For treated district-cycle observations, we apply condition that keeps only contributions coming between death date and date of special elections. Suffix determines what treatment we used to apply this condition (either treat_1, treat_2, or treat_3); No suffix indicates original variable (i.e. filtering condition not applied). For single-death district, condition applied similarly for all variables (_1, _2, _3) when first treatment occurs. In multiple-death districts, we apply condition for _1 variables only when first death occurs, for _2 and _3 variables we apply the condition when the death(s) occur(s) again.',
            'source': '(see original var)',
            'origin_dataset': '(see original var)',
            'relevant_URLs': '(see original var)',
            'output_relation': '(see original var)'
            },       
    
    '_gen_np': {
            'description': "For all variables that contain '_gen_' (meaning 'general elections'), in treated district-cycle observations, we apply condition that keeps contributions of general and special elections coming between death date and date of special elections; else, in untreated district-cycle we only keep general elections contributions. Treatment is defined by respective treatment variable: treat_1, treat_2, treat_3. For _1 variables, we consider a district-cycle row treated only for the first death (for both single-death and multiple-death districts). For _2 and _3, we treat all deaths. Therefore, in case of single-death districts, the first and only treated row will be identical for all _1, _2, _3 variable types. For multiple-death districts, they are only identical in the first death, otherwise they differ.",
            'source': '(see original var)',
            'origin_dataset': '(see original var)',
            'relevant_URLs': '(see original var)',
            'output_relation': '(see original var)'
            },       

    '_gen_np_spec': {
            'description': "Similar to _gen_np, for all variables that contain '_gen_', in treated district-cycle observations, we apply condition that keeps contributions of (only) special elections coming between death date and date of special elections; else, in untreated district-cycle we only keep general elections contributions. Treatment is defined the same way as for _gen_np variables.",
            'source': '(see original var)',
            'origin_dataset': '(see original var)',
            'relevant_URLs': '(see original var)',
            'output_relation': '(see original var)'
            },       
    
    
    }

# # Create a dictionary with each variable having four attributes
# rows = []
# for var_name, attributes in OUTPUT_1_final_collapsed_ext_dict.items():
#     rows.append({
#         'variable_name': var_name,
#         'description': attributes['description'],
#         'source': attributes['source'],
#         'origin_dataset': attributes['origin_dataset'],
#         'relevant_URLs': attributes['relevant_URLs'],
#         'output_relation': attributes['output_relation'],
#     })

# OUTPUT_1_final_collapsed_ext_dict_df = pd.DataFrame(rows)

# def dict_to_df(dictionary):
#     # Create a dictionary with each variable having four attributes
#     rows = []
#     for var_name, attributes in dictionary.items():
#         rows.append({
#             'variable_name': var_name,
#             'description': attributes['description'],
#             'source': attributes['source'],
#             'origin_dataset': attributes['origin_dataset'],
#             'relevant_URLs': attributes['relevant_URLs'],
#             'output_relation': attributes['output_relation'],
#         })
#     df = pd.DataFrame(rows)
#     return df

OUTPUT_1_final_collapsed_ext_dict_df = dict_to_df(OUTPUT_1_final_collapsed_ext_dict)


