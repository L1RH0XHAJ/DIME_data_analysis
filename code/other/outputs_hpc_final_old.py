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
# import inspect
import pandas as pd
import numpy as np
# from pathlib import Path

#%%

### SETUP

# These lines will get the location of this file '\code\main.py'. Please ensure file is saved in folder \code. 

# This line does not work in interactive environment (e.g., Jupyter Notebook or interpreters like IDLE)
# code_folder = os.path.dirname(os.path.abspath(__file__))

# Get the directory where the current script is located
# code_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

# If all fails define working folder manually and run the lines here
# code_folder = r"C:\Users\lhoxhaj\OneDrive - Imperial College London\Desktop\RA\Tommaso\Contributions_Paper\working_folder_lir\code"
code_folder = "/Users/lirhoxhaj/Library/CloudStorage/OneDrive-ImperialCollegeLondon/Desktop/RA/Tommaso/Contributions_Paper/working_folder_lir/code"

# This is your working folder where folders '\code' and '\data' are saved
code_folder = "/rds/general/user/lhoxhaj/home/code/Contributions_Paper/"
parent_folder = "/rds/general/user/lhoxhaj/home/"
data_folder = "/rds/general/user/lhoxhaj/home/data/"

print("Parent folder:", parent_folder, "\n")
print("Code folder:", code_folder, "\n")
print("Data folder:", data_folder, "\n")


#%%

### GENERAL PURPOSE FUNCTIONS

# See how many times each combination appears
def display_duplicates(df, varlist):
    df_filter = df.groupby(varlist).size()
    # Filter to show only combinations that appear more than once
    print(df_filter[df_filter > 1])


#%%

### READING DATASETS


## OUTPUT_0: a Cartesian product of unique values of district and cycles (useful for merging with other OUTPUT data)
## OUTPUT_0_2: similar to OUTPUT_0, but has an additional categorisation by party as well
print("Reading OUTPUT_0 and OUTPUT_0_2...")

OUTPUT_0 = pd.read_csv(
    data_folder + "/OUTPUTS/OUTPUT_0.csv", 
    encoding='latin-1'
    )

OUTPUT_0_2 = pd.read_csv(
    data_folder + "/OUTPUTS/OUTPUT_0_2.csv", 
    encoding='latin-1'
    )

## OUTPUT 1: contribution-day level dataset
print("Reading OUTPUT_1...")

OUTPUT_1 = pd.read_csv(
    data_folder + "/OUTPUTS/OUTPUT_1.csv", 
    encoding='latin-1'
    )

## Special elections data and death districts
print("Reading special elections data...")
special_elections = pd.read_csv(data_folder + "/special_elections_final.csv", encoding='latin-1')

special_elections_districts = special_elections.groupby('district')['spec_member'].nunique().reset_index()
death_counts = special_elections[special_elections['cause_vacancy'] == 'Death'].groupby('district')['spec_member'].nunique().reset_index()
death_counts_unique = special_elections[special_elections['cause_vacancy'] == 'Death']
single_death_districts = death_counts[death_counts['spec_member'] == 1]['district'].tolist()
multiple_death_districts = death_counts[death_counts['spec_member'] > 1]['district'].tolist()
no_death_districts = special_elections[special_elections['cause_vacancy'] == 'Resigned'].groupby('district')['spec_member'].nunique().reset_index()
no_death_districts_unique = special_elections[special_elections['cause_vacancy'] == 'Resigned']

print("Total number of districts with special elections", len(special_elections_districts))
print("Total number of districts with one death", len(single_death_districts))
print("Total number of districts with multiple deaths", len(multiple_death_districts))
print("Nr of districts with resignations", len(no_death_districts))
print()
print("Total number of deaths:", len(death_counts_unique))
print("Total number of resignations:", len(no_death_districts_unique))

## Manually created general elections dataset for each year (cycle) in our dataset
print("Reading elections dates data...")
election_dates_df = pd.read_csv(data_folder + "/election_dates.csv", encoding='latin-1')

#%%

### Reading and processing new data here
#     1. MIT_eMIT elections data
#     2. new_districts_df: newly created districts

## 1. MIT_eMIT US House 1976 - 2024 elections data (source: https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/IG0UN2)
# print("Reading MIT US House 1976 - 2022 elections data...")
# gen_elect_df = pd.read_csv(data_folder + "/1976-2022-house.csv", encoding='latin-1')
print("Reading MIT US House 1976 - 2024 elections data...")
gen_elect_df = pd.read_csv(data_folder + "/1976-2024-house.csv", encoding='utf-8')

# Renaming columns
gen_elect_df = gen_elect_df.rename(columns = {'year' : 'cycle'})

gen_elect_df['cycle'] = pd.to_numeric(gen_elect_df['cycle'], errors='coerce')  # will convert None values to NaN

# Dropping rows
gen_elect_df = gen_elect_df[gen_elect_df['cycle'].isin(OUTPUT_1['cycle'].unique())] # cycle in OUTPUT_1
gen_elect_df = gen_elect_df[gen_elect_df['stage']=='GEN'] # very few observations for primary ('PRI'), no point in using them
gen_elect_df = gen_elect_df[gen_elect_df['special']== False] # very few observations for special elections
gen_elect_df = gen_elect_df[gen_elect_df['runoff']== False] # very few observations for runoff elections
# gen_elect_df = gen_elect_df[(gen_elect_df['party']=='DEMOCRAT') | (gen_elect_df['party']=='REPUBLICAN')] # only dems and reps
# print(gen_elect_df['cycle'].value_counts())
# print(gen_elect_df['cycle'].unique())

# Replacing values
# print(gen_elect_df['party'].value_counts())

gen_elect_df['party_2'] = 300 # all others
gen_elect_df.loc[gen_elect_df['party'] == 'DEMOCRAT', 'party_2'] = 100
gen_elect_df.loc[gen_elect_df['party'] == 'REPUBLICAN', 'party_2'] = 200
# gen_elect_df.loc[~gen_elect_df['party'].isin([100, 200]), 'party'] = 300 # other
# print(gen_elect_df['party'].value_counts())
# print(gen_elect_df['party_2'].value_counts())

gen_elect_df = gen_elect_df.drop(columns = ['party'])
gen_elect_df = gen_elect_df.rename(columns = {'party_2':'party'})

gen_elect_df['party'] = pd.to_numeric(gen_elect_df['party'], errors='coerce') # will convert None values to NaN

# Authors code at-large districts with 0, in the other datasets they are saved as 01 (e.g., for Alaska 'AK01')
gen_elect_df.loc[gen_elect_df['district'] == 0, 'district'] = 1

# Creating district-specific code to merge with other datasets
gen_elect_df['district_number'] = np.nan
gen_elect_df['district_number'] = gen_elect_df['district'].apply(lambda x: f"{x:02d}") # two-digit string
gen_elect_df['district'] = gen_elect_df['state_po'] + gen_elect_df['district_number'].astype(str)

print("Number of unique district:", gen_elect_df['district'].nunique())

# Dropping unneccesary variables
gen_elect_df = gen_elect_df.drop(columns = ['state', 'state_po', 'state_fips', 'state_cen', 'state_ic',
                                            'office', # ALways US HOUSE
                                            'stage',  # Always general elections
                                            'special', 'runoff',
                                            'mode',   # Always TOTAL
                                            'unofficial',
                                            'version', 'district_number'])

# display_duplicates(gen_elect_df, ['district', 'cycle', 'party', 'candidate'])


# Checking how many unique values of totalvotes for each district-cycle there, should select highest (CHECK ONLINE)
unique_counts = gen_elect_df.groupby(['district', 'cycle'])['totalvotes'].nunique().reset_index()
unique_counts.columns = ['district', 'cycle', 'nunique_totalvotes']
multiple_values = unique_counts[unique_counts['nunique_totalvotes'] > 1]
print(f"\nDistrict-cycles with multiple totalvotes values:\n{multiple_values}")
# LA05 2002 is a problem

# Find the maximum totalvotes for each district-cycle, merge with main data and keep only rows that have max totalvotes for district-cycle
max_totalvotes = gen_elect_df.groupby(['district', 'cycle'])['totalvotes'].max().reset_index()
max_totalvotes.columns = ['district', 'cycle', 'max_totalvotes']
gen_elect_df = gen_elect_df.merge(max_totalvotes, on=['district', 'cycle'], how='left')
gen_elect_df = gen_elect_df[gen_elect_df['totalvotes'] == gen_elect_df['max_totalvotes']]
gen_elect_df = gen_elect_df.drop('max_totalvotes', axis=1)

# Manually fixing values (wrong in raw data)
gen_elect_df.loc[
    (gen_elect_df['district'] == 'IN02') & 
    (gen_elect_df['cycle'] == 2002) & 
    (gen_elect_df['candidate'] == 'WRITEIN'),
    'candidatevotes'
] = 12 # was originally 6

# Get the party associated with the highest candidatevotes for each candidate
main_party = gen_elect_df.loc[
    gen_elect_df.groupby(['district', 'cycle', 'candidate'])['candidatevotes'].idxmax()
][['district', 'cycle', 'candidate', 'party']]

# Sum candidatevotes
summed_df = gen_elect_df.groupby(['district', 'cycle', 'candidate']).agg({
    'candidatevotes': 'sum',
    # 'party': 'first',
    'writein': 'first',
    'totalvotes': 'first',
    'fusion_ticket': 'first'
}).reset_index()

# Merge to get the main party
gen_elect_df = summed_df.merge(main_party, on=['district', 'cycle', 'candidate'], how='left')


votes_check = gen_elect_df.groupby(['district', 'cycle']).agg({
    'candidatevotes': 'sum',
    'totalvotes': 'first',
}).reset_index()
print(np.corrcoef(votes_check['candidatevotes'], votes_check['totalvotes']))
print(votes_check[votes_check['candidatevotes'] != votes_check['totalvotes']])


# Creating variable for OUTPUT_6 (since every row is each candidate in each district-cycle, we don't need to pivot or do anything else with the data to generate this data)
gen_elect_df['gen_vote_pct'] = gen_elect_df['candidatevotes'] / gen_elect_df['totalvotes']

# We keep only dems and reps
gen_elect_df = gen_elect_df[gen_elect_df['party'] != 300]


## Checking if any duplicates
# display_duplicates(gen_elect_df, ['district', 'cycle', 'party', 'candidate'])
display_duplicates(gen_elect_df, ['district', 'cycle', 'candidate'])



#%%


## 2. Newly created districts data (from convert_html_to_csv_2.py)
print("Reading data of newly created districts...")
new_districts_df = pd.read_csv(data_folder + "/new_districts_filtered.csv", encoding='latin-1')

print("Processing new_districts_df_0...") # cartesian product of districts in new_districts_df and all cycles (all election years)

unique_districts = new_districts_df['district'].dropna().unique()
unique_cycles = list(range(1980, 2026, 2))
districts_list = []
cycles_list = []

for district in unique_districts:
    for cycle in unique_cycles:
        districts_list.append(district)
        cycles_list.append(cycle)

new_districts_df_0 = pd.DataFrame({
    'district': districts_list,
    'cycle': cycles_list
})

new_districts_df_0 = new_districts_df_0.sort_values(['district', 'cycle']).reset_index(drop=True)

# Merging cartesian product and new_districts_df
print("Merging new_districts_df...")
new_districts_df = pd.merge(
    new_districts_df_0,
    new_districts_df,
    on=['district'],
    how='outer'
    )


# Creating dummy real_data 
new_districts_df['real_data'] = np.nan

# 0, if cycle is before created_year and cycle is after discontinued_year
new_districts_df.loc[
    (new_districts_df['cycle'] < new_districts_df['created_year']) | 
    (new_districts_df['cycle'] > new_districts_df['discontinued_year']), 
    'real_data'] = 0

# 1, if cycle is between created_year and discontinued_year
new_districts_df.loc[
    (new_districts_df['cycle'] >= new_districts_df['created_year']) | 
    (new_districts_df['cycle'] <= new_districts_df['discontinued_year']), 
    'real_data'] = 1

# Special cases, which have alternate between 0 and 1 (have both created_year and discontinued_year)
df_filtered = new_districts_df[~new_districts_df['created_year'].isna() & 
                                     ~new_districts_df['discontinued_year'].isna()]

if 'real_data' not in new_districts_df.columns:
    new_districts_df['real_data'] = 0

for idx in df_filtered.index:
    row = new_districts_df.loc[idx]
    # Check if cycle is between created_year and discontinued_year (inclusive)
    if row['cycle'] >= row['created_year'] and row['cycle'] <= row['discontinued_year']:
        new_districts_df.loc[idx, 'real_data'] = 1
    else:
        new_districts_df.loc[idx, 'real_data'] = 0

# Filter data cycle in OUTPUT_2[cycle].unique() or less 
new_districts_df = new_districts_df[new_districts_df['cycle'].isin(OUTPUT_1['cycle'].unique())]
print("Earliest year", new_districts_df['cycle'].min())
print("Latest year", new_districts_df['cycle'].max())
 
new_districts_df.to_csv(os.path.join(data_folder, "new_districts_filtered_2.csv"), index = False)

new_districts_df = new_districts_df[['district', 'cycle', 'real_data']]

#%%

### OUTPUT_2: 
print("Processing OUTPUT_2...")

# NOTE: (100 = Dem, 200 = Rep, 328 = Ind)
#       All total_amount have tran_count variable to show number of transactions as well.

#     1 total_amount: Sum of contributions in the district/cycle.

#     2 total_amount_without_LTS1: For district/cycle with multiple elections, here we consider only those contributions that are earlier than the date of the special election. 

#     3 total_amount_no_primary: Sum of contributions in the district/cycle. Not considering primaries

#     4 total_amount_no_primary_without_LTS1: Same as above, but before special elections date.

#     5 total_amount_primary: Sum of contributions in the district/cycle. Only considering primaries

#     6 total_amount_primary_without_LTS1: Same as above, but before special elections date.

#     7 total_amount_gen: Sum of contributions in the district/cycle. Only considering primaries

#     8 total_amount_gen_without_LTS1: Same as above, but before special elections date.

#     9 total_amount_special: For district/cycle with multiple elections, here we consider only those contributions that are not going towards special elections, as per Bonica's definition. 

#     10 total_amount_special_without_LTS1: Excluding contributions before election date, but keeping only special ones

#     11 total_amount_dem_gen: Sum of contributions in the district/cycle to Democratic candidate in the general election

#     12 total_amount_dem_gen_without_LTS1: Same as total_amount_dem_gen, but we don't consider contributions later than special elections

#     13 total_amount_dem_primary: Sum of contributions to Democratic candidates in primary

#     14 total_amount_dem_primary_without_LTS1: Same as total_amount_dem_gen, but we don't consider contributions later than special elections

#     15 total_amount_dem_special: Sum of contributions to Democratic candidates for special elections

#     16 total_amount_dem_special_without_LTS1: Sum of contributions to Democratic candidates for special elections, before date of special election

#     17 total_amount_rep_gen: Same, but for Republicans

#     18 total_amount_rep_gen_without_LTS1: Same, but for Republicans

#     19 total_amount_rep_primary: Same, but for Republicans

#     20 total_amount_rep_primary_without_LTS1: Same, but for Republicans

#     21 total_amount_rep_special: Same, but for Republicans

#     22 total_amount_rep_special_without_LTS1: Same, but for Republicans


def create_aggregated_outputs(input_df, output_prefix, filter_type=None, amount_filter=None, suffix=''):
    # Apply filter if specified
    if filter_type:
        filtered_df = input_df[input_df['contributor.type'] == filter_type]
        print("Raw dataset:", input_df.shape)
        print("Used", filter_type, "for filtering")
        print("New dataset:", filtered_df.shape)
    else:
        print("Raw dataset:", input_df.shape)
        filtered_df = input_df.copy()
        print("No filtering used")
        
    if amount_filter:
        print("Filtering for rows with amounts less than:", amount_filter)
        filtered_df = filtered_df[filtered_df['amount'] < 200]
        print("New dataset:", filtered_df.shape)
    else:
        filtered_df = filtered_df.copy()
    
    print(f"Processing {output_prefix}...")
    
    # Create output dataframes with appropriate filters
    output_dfs = {}
    
    # 1: All contributions (all election types)
    print(f"Filtering {output_prefix}_1...")
    output_dfs[f"{output_prefix}_1"] = filtered_df.groupby(['district', 'cycle']).agg(
        **{f'total_amount{suffix}': ('amount', 'sum'),
           f'tran_count{suffix}': ('transaction.id', 'count')}
    ).reset_index()
    
    # 2: All contributions (all election types), before date of special election
    print(f"Filtering {output_prefix}_2...")
    output_dfs[f"{output_prefix}_2"] = filtered_df[
        filtered_df['later_than_special'] != 1
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_without_LTS1{suffix}': ('amount', 'sum'),
           f'tran_count_without_LTS1{suffix}': ('transaction.id', 'count'),
           # f'avg_amount_without_LTS1{suffix}': ('amount', 'mean')
           }
    ).reset_index()
    
    # 3: All contributions (all election types), excluding primary elections
    print(f"Filtering {output_prefix}_3...")
    output_dfs[f"{output_prefix}_3"] = filtered_df[
        filtered_df['election.type'] != 'P'
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_no_primary{suffix}': ('amount', 'sum'),
           f'tran_count_no_primary{suffix}': ('transaction.id', 'count'),
           # f'avg_amount_no_primary{suffix}': ('amount', 'mean')
           }
    ).reset_index()
        
    # 4: All contributions (all election types), excluding primary elections, before date of special election
    print(f"Filtering {output_prefix}_4...")
    output_dfs[f"{output_prefix}_4"] = filtered_df[
        (filtered_df['election.type'] != 'P') &
        (filtered_df['later_than_special'] != 1)
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_no_primary_without_LTS1{suffix}': ('amount', 'sum'),
           f'tran_count_no_primary_without_LTS1{suffix}': ('transaction.id', 'count'),
           # f'avg_amount_no_primary_without_LTS1{suffix}': ('amount', 'mean')
           }
    ).reset_index()
        
    # 5: All contributions for primary elections
    print(f"Filtering {output_prefix}_5...")
    output_dfs[f"{output_prefix}_5"] = filtered_df[
        filtered_df['election.type'] == 'P'
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_primary{suffix}': ('amount', 'sum'),
           f'tran_count_primary{suffix}': ('transaction.id', 'count'),
           # f'avg_amount_primary{suffix}': ('amount', 'mean')
           }
    ).reset_index()
        
    # 6: All contributions for primary elections, before date of special election
    print(f"Filtering {output_prefix}_6...")
    output_dfs[f"{output_prefix}_6"] = filtered_df[
        (filtered_df['election.type'] == 'P') &
        (filtered_df['later_than_special'] != 1)
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_primary_without_LTS1{suffix}': ('amount', 'sum'),
           f'tran_count_primary_without_LTS1{suffix}': ('transaction.id', 'count'),
           # f'avg_amount_primary_without_LTS1{suffix}': ('amount', 'mean')
           }
    ).reset_index()

    # 7: All contributions for general elections
    print(f"Filtering {output_prefix}_7...")
    output_dfs[f"{output_prefix}_7"] = filtered_df[
        filtered_df['election.type'] == 'G'
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_gen{suffix}': ('amount', 'sum'),
           f'tran_count_gen{suffix}': ('transaction.id', 'count'),
           # f'avg_amount_gen{suffix}': ('amount', 'mean')
           }
    ).reset_index()
        
    # 8: All contributions for general elections, before date of special election
    print(f"Filtering {output_prefix}_8...")
    output_dfs[f"{output_prefix}_8"] = filtered_df[
        (filtered_df['election.type'] == 'G') &
        (filtered_df['later_than_special'] != 1)
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_gen_without_LTS1{suffix}': ('amount', 'sum'),
           f'tran_count_gen_without_LTS1{suffix}': ('transaction.id', 'count'),
           # f'avg_amount_gen_without_LTS1{suffix}': ('amount', 'mean')
           }
    ).reset_index()
        
    # 9: All contributions for special elections
    print(f"Filtering {output_prefix}_9...")
    output_dfs[f"{output_prefix}_9"] = filtered_df[
        filtered_df['election.type'] == 'S'
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_special{suffix}': ('amount', 'sum'),
           f'tran_count_special{suffix}': ('transaction.id', 'count'),
           # f'avg_amount_special{suffix}': ('amount', 'mean')
           }
    ).reset_index()
        
    # 10: All contributions for special elections, before date of special election
    print(f"Filtering {output_prefix}_10...")
    output_dfs[f"{output_prefix}_10"] = filtered_df[
        (filtered_df['election.type'] == 'S') &
        (filtered_df['later_than_special'] != 1)
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_special_without_LTS1{suffix}': ('amount', 'sum'),
           f'tran_count_special_without_LTS1{suffix}': ('transaction.id', 'count'),
           # f'avg_amount_special_without_LTS1{suffix}': ('amount', 'mean')
           }
    ).reset_index()
        
    # 11: All contributions for general elections going to Democrats
    print(f"Filtering {output_prefix}_11...")
    output_dfs[f"{output_prefix}_11"] = filtered_df[
        (filtered_df['party'] == 100) &
        (filtered_df['election.type'] == 'G')
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_dem_gen{suffix}': ('amount', 'sum'),
           f'tran_count_dem_gen{suffix}': ('transaction.id', 'count'),
           # f'avg_amount_dem_gen{suffix}': ('amount', 'mean')
           }
    ).reset_index()
        
    # 12: All contributions for general elections going to Democrats, before date of special election
    print(f"Filtering {output_prefix}_12...")
    output_dfs[f"{output_prefix}_12"] = filtered_df[
        (filtered_df['party'] == 100) &
        (filtered_df['election.type'] == 'G') &
        (filtered_df['later_than_special'] != 1)
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_dem_gen_without_LTS1{suffix}': ('amount', 'sum'),
           f'tran_count_dem_gen_without_LTS1{suffix}': ('transaction.id', 'count'),
           # f'avg_amount_dem_gen_without_LTS1{suffix}': ('amount', 'mean')
           }
    ).reset_index()
        
    # 13: All contributions for primary elections going to Democrats
    print(f"Filtering {output_prefix}_13...")
    output_dfs[f"{output_prefix}_13"] = filtered_df[
        (filtered_df['party'] == 100) &
        (filtered_df['election.type'] == 'P')
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_dem_primary{suffix}': ('amount', 'sum'),
           f'tran_count_dem_primary{suffix}': ('transaction.id', 'count'),
           # f'avg_amount_dem_primary{suffix}': ('amount', 'mean')
           }
    ).reset_index()
        
    # 14: All contributions for primary elections going to Democrats, before date of special election
    print(f"Filtering {output_prefix}_14...")
    output_dfs[f"{output_prefix}_14"] = filtered_df[
        (filtered_df['party'] == 100) &
        (filtered_df['election.type'] == 'P') &
        (filtered_df['later_than_special'] != 1)
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_dem_primary_without_LTS1{suffix}': ('amount', 'sum'),
           f'tran_count_dem_primary_without_LTS1{suffix}': ('transaction.id', 'count'),
           # f'avg_amount_dem_primary_without_LTS1{suffix}': ('amount', 'mean')
           }
    ).reset_index()
        
    # 15: All contributions for special elections going to Democrats
    print(f"Filtering {output_prefix}_15...")
    output_dfs[f"{output_prefix}_15"] = filtered_df[
        (filtered_df['party'] == 100) &
        (filtered_df['election.type'] == 'S')
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_dem_special{suffix}': ('amount', 'sum'),
           f'tran_count_dem_special{suffix}': ('transaction.id', 'count'),
           # f'avg_amount_dem_special{suffix}': ('amount', 'mean')
           }
    ).reset_index()
        
    # 16: All contributions for special elections going to Democrats, before date of special election
    print(f"Filtering {output_prefix}_16...")
    output_dfs[f"{output_prefix}_16"] = filtered_df[
        (filtered_df['party'] == 100) &
        (filtered_df['election.type'] == 'S') &
        (filtered_df['later_than_special'] != 1)
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_dem_special_without_LTS1{suffix}': ('amount', 'sum'),
           f'tran_count_dem_special_without_LTS1{suffix}': ('transaction.id', 'count'),
           # f'avg_amount_dem_special_without_LTS1{suffix}': ('amount', 'mean')
           }
    ).reset_index()
        
        
    # 17: All contributions for general elections going to Republicans
    print(f"Filtering {output_prefix}_17...")
    output_dfs[f"{output_prefix}_17"] = filtered_df[
        (filtered_df['party'] == 200) &
        (filtered_df['election.type'] == 'G')
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_rep_gen{suffix}': ('amount', 'sum'),
           f'tran_count_rep_gen{suffix}': ('transaction.id', 'count'),
           # f'avg_amount_rep_gen{suffix}': ('amount', 'mean')
           }
    ).reset_index()
        
    # 18: All contributions for general elections going to Republicans, before date of special election
    print(f"Filtering {output_prefix}_18...")
    output_dfs[f"{output_prefix}_18"] = filtered_df[
        (filtered_df['party'] == 200) &
        (filtered_df['election.type'] == 'G') &
        (filtered_df['later_than_special'] != 1)
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_rep_gen_without_LTS1{suffix}': ('amount', 'sum'),
           f'tran_count_rep_gen_without_LTS1{suffix}': ('transaction.id', 'count'),
           # f'avg_amount_rep_gen_without_LTS1{suffix}': ('amount', 'mean')
           }
    ).reset_index()
        
    # 19: All contributions for primary elections going to Republicans
    print(f"Filtering {output_prefix}_19...")
    output_dfs[f"{output_prefix}_19"] = filtered_df[
        (filtered_df['party'] == 200) &
        (filtered_df['election.type'] == 'P')
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_rep_primary{suffix}': ('amount', 'sum'),
           f'tran_count_rep_primary{suffix}': ('transaction.id', 'count'),
           # f'avg_amount_rep_primary{suffix}': ('amount', 'mean')
           }
    ).reset_index()
        
    # 20: All contributions for primary elections going to Republicans, before date of special election
    print(f"Filtering {output_prefix}_20...")
    output_dfs[f"{output_prefix}_20"] = filtered_df[
        (filtered_df['party'] == 200) &
        (filtered_df['election.type'] == 'P') &
        (filtered_df['later_than_special'] != 1)
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_rep_primary_without_LTS1{suffix}': ('amount', 'sum'),
           f'tran_count_rep_primary_without_LTS1{suffix}': ('transaction.id', 'count'),
           # f'avg_amount_rep_primary_without_LTS1{suffix}': ('amount', 'mean')
           }
    ).reset_index()
        
    # 21: All contributions for special elections going to Republicans
    print(f"Filtering {output_prefix}_21...")
    output_dfs[f"{output_prefix}_21"] = filtered_df[
        (filtered_df['party'] == 200) &
        (filtered_df['election.type'] == 'S')
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_rep_special{suffix}': ('amount', 'sum'),
           f'tran_count_rep_special{suffix}': ('transaction.id', 'count'),
           # f'avg_amount_rep_special{suffix}': ('amount', 'mean')
           }
    ).reset_index()
        
    # 22: All contributions for special elections going to Republicans, before date of special election
    print(f"Filtering {output_prefix}_22...")
    output_dfs[f"{output_prefix}_22"] = filtered_df[
        (filtered_df['party'] == 200) &
        (filtered_df['election.type'] == 'S') &
        (filtered_df['later_than_special'] != 1)
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_rep_special_without_LTS1{suffix}': ('amount', 'sum'),
           f'tran_count_rep_special_without_LTS1{suffix}': ('transaction.id', 'count'),
           # f'avg_amount_rep_special_without_LTS1{suffix}': ('amount', 'mean')
           }
    ).reset_index()
        

    # Merge all outputs
    final_output = output_dfs[f"{output_prefix}_1"].copy()
    for i in range(2, 23):
        print(f"Merging {output_prefix} with {output_prefix}_{i}")
        df = output_dfs[f"{output_prefix}_{i}"]
        
        final_output = pd.merge(
            final_output,
            df,
            on=['cycle', 'district'],
            how='outer'
        )
    
    print("Merge complete!")
    return final_output

OUTPUT_2 = create_aggregated_outputs(OUTPUT_1, 'OUTPUT_2')    

# Merging with districts that have a creation year or discontinuation year or both after 1980
print("Merging OUTPUTS with new_districts_df")
OUTPUT_2 = pd.merge(
    OUTPUT_2,
    new_districts_df,
    on=['district', 'cycle'],
    how='left'
    )
# Missing values of real_data are replaced with 1, because they are real data (district has existed prior to 1980 and has existed until 2024)
OUTPUT_2.loc[OUTPUT_2['real_data'].isna(), 'real_data'] = 1
# Re-ordering data
OUTPUT_2 = OUTPUT_2.sort_values(by=['district', 'cycle'], ascending=[True, True])
columns_list = ['district', 'cycle', 'real_data']
cols = columns_list + [
    col for col in OUTPUT_2.columns if col not in columns_list]
OUTPUT_2 = OUTPUT_2[cols]

print("Merging OUTPUTS with OUTPUT_0")
OUTPUT_2 = pd.merge(
    OUTPUT_0,
    OUTPUT_2,
    on=['cycle', 'district'],
    how='outer'
    )

# Missing values of real_data are replaced with 0, these don't exist but are fake rows to balance our dataset
OUTPUT_2.loc[OUTPUT_2['real_data'].isna(), 'real_data'] = 0

# Checking...
print("Total length of dataset OUTPUT_2:", len(OUTPUT_2))
print("  - real_data == 1 and total_amount != Nan:", len(OUTPUT_2[(OUTPUT_2['real_data'] == 1) & (~OUTPUT_2['total_amount'].isna())]))
print("  - real_data == 1 and total_amount == Nan:", len(OUTPUT_2[(OUTPUT_2['real_data'] == 1) & (OUTPUT_2['total_amount'].isna())]), "(ISSUE if >0)")
print("  - real_data == 0 and total_amount != Nan:", len(OUTPUT_2[(OUTPUT_2['real_data'] == 0) & (~OUTPUT_2['total_amount'].isna())]), "(ISSUE if >0)")
print("  - real_data == 0 and total_amount == Nan:", len(OUTPUT_2[(OUTPUT_2['real_data'] == 0) & (OUTPUT_2['total_amount'].isna())]))

test = OUTPUT_2[(OUTPUT_2['real_data'] == 0) & (~OUTPUT_2['total_amount'].isna())]

# test.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_2_problem.csv"), index=False)

# Missing values of other columns are replaced with 0
# OUTPUT_2_processed = OUTPUT_2.fillna(0)
OUTPUT_2 = OUTPUT_2.fillna(0)
OUTPUT_2.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_2.csv"), index=False)

#%%

### OUTPUT_3: 
print("Processing OUTPUT_3...")
    
# all variables are similar to OUTPUT_2 but apply only to contributions coming from corporations

OUTPUT_3 = create_aggregated_outputs(OUTPUT_1, 'OUTPUT_3', filter_type='C', suffix='_corp')

# NOTE: We have already created real_data dummy, we get this information from OUTPUT_2 (the 'universe' of district-cycles) and avoid creating a new one for district-cycle individual contributions
OUTPUT_3 = pd.merge(
    OUTPUT_2[['district', 'cycle', 'real_data']], # we only need this information from OUTPUT_2
    OUTPUT_3,
    on=['cycle', 'district'],
    how='outer'
    )

# Checking...
print("Total length of dataset OUTPUT_3:", len(OUTPUT_3))
print("  - real_data == 1 and total_amount_corp != Nan:", len(OUTPUT_3[(OUTPUT_3['real_data'] == 1) & (~OUTPUT_3['total_amount_corp'].isna())]))
print("  - real_data == 1 and total_amount_corp == Nan:", len(OUTPUT_3[(OUTPUT_3['real_data'] == 1) & (OUTPUT_3['total_amount_corp'].isna())]), "(ISSUE if >0)")
print("  - real_data == 0 and total_amount_corp != Nan:", len(OUTPUT_3[(OUTPUT_3['real_data'] == 0) & (~OUTPUT_3['total_amount_corp'].isna())]), "(ISSUE if >0)")
print("  - real_data == 0 and total_amount_corp == Nan:", len(OUTPUT_3[(OUTPUT_3['real_data'] == 0) & (OUTPUT_3['total_amount_corp'].isna())]))

# test = OUTPUT_3[(OUTPUT_3['real_data'] == 0) & (~OUTPUT_3['total_amount_corp'].isna())]

# test.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_3_problem.csv"), index=False)

# Missing values of other columns are replaced with 0
# OUTPUT_3_processed = OUTPUT_3.fillna(0)
OUTPUT_3 = OUTPUT_3.fillna(0)
OUTPUT_3.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_3.csv"), index=False)
OUTPUT_3 = OUTPUT_3.drop(columns = 'real_data') # no duplicate columns when merging with OUTPUT_2


#%%

### OUTPUT_4: 
print("Processing OUTPUT_4...")

    
# all variables are similar to OUTPUT_2 but apply only to contributions coming from individuals

OUTPUT_4_1 = create_aggregated_outputs(OUTPUT_1, 'OUTPUT_4_1', filter_type='I', suffix='_ind')

OUTPUT_4_2 = create_aggregated_outputs(OUTPUT_1, 'OUTPUT_4_2', filter_type='I', amount_filter=200, suffix='_smallind')

# NOTE: We have already created real_data dummy, we get this information from OUTPUT_2 (the 'universe' of district-cycles) and avoid creating a new one for district-cycle individual contributions
OUTPUT_4_1 = pd.merge(
    OUTPUT_2[['district', 'cycle', 'real_data']], # we only need this information from OUTPUT_2
    OUTPUT_4_1,
    on=['cycle', 'district'],
    how='outer'
    )

OUTPUT_4_2 = pd.merge(
    OUTPUT_2[['district', 'cycle', 'real_data']], # we only need this information from OUTPUT_2
    OUTPUT_4_2,
    on=['cycle', 'district'],
    how='outer'
    )

# Checking...
print("Total length of dataset OUTPUT_4_1:", len(OUTPUT_4_1))
print("  - real_data == 1 and total_amount_ind != Nan:", len(OUTPUT_4_1[(OUTPUT_4_1['real_data'] == 1) & (~OUTPUT_4_1['total_amount_ind'].isna())]))
print("  - real_data == 1 and total_amount_ind == Nan:", len(OUTPUT_4_1[(OUTPUT_4_1['real_data'] == 1) & (OUTPUT_4_1['total_amount_ind'].isna())]), "(ISSUE if >0)")
print("  - real_data == 0 and total_amount_ind != Nan:", len(OUTPUT_4_1[(OUTPUT_4_1['real_data'] == 0) & (~OUTPUT_4_1['total_amount_ind'].isna())]), "(ISSUE if >0)")
print("  - real_data == 0 and total_amount_ind == Nan:", len(OUTPUT_4_1[(OUTPUT_4_1['real_data'] == 0) & (OUTPUT_4_1['total_amount_ind'].isna())]))

print("Total length of dataset OUTPUT_4_2:", len(OUTPUT_4_2))
print("  - real_data == 1 and total_amount_smallind != Nan:", len(OUTPUT_4_2[(OUTPUT_4_2['real_data'] == 1) & (~OUTPUT_4_2['total_amount_smallind'].isna())]))
print("  - real_data == 1 and total_amount_smallind == Nan:", len(OUTPUT_4_2[(OUTPUT_4_2['real_data'] == 1) & (OUTPUT_4_2['total_amount_smallind'].isna())]), "(ISSUE if >0)")
print("  - real_data == 0 and total_amount_smallind != Nan:", len(OUTPUT_4_2[(OUTPUT_4_2['real_data'] == 0) & (~OUTPUT_4_2['total_amount_smallind'].isna())]), "(ISSUE if >0)")
print("  - real_data == 0 and total_amount_smallind == Nan:", len(OUTPUT_4_2[(OUTPUT_4_2['real_data'] == 0) & (OUTPUT_4_2['total_amount_smallind'].isna())]))


# test = OUTPUT_4[(OUTPUT_4['real_data'] == 0) & (~OUTPUT_4['total_amount_ind'].isna())]

# test.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_4_problem.csv"), index=False)

# Missing values of other columns are replaced with 0
# OUTPUT_4_1_processed = OUTPUT_4_1.fillna(0)
OUTPUT_4_1 = OUTPUT_4_1.fillna(0)
OUTPUT_4_1.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_4_1.csv"), index=False)
OUTPUT_4_1 = OUTPUT_4_1.drop(columns = 'real_data') # no duplicate columns when merging with OUTPUT_2

# OUTPUT_4_2_processed = OUTPUT_4_2.fillna(0)
OUTPUT_4_2 = OUTPUT_4_2.fillna(0)
OUTPUT_4_2.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_4_2.csv"), index=False)
OUTPUT_4_2 = OUTPUT_4_2.drop(columns = 'real_data') # no duplicate columns when merging with OUTPUT_2

OUTPUT_4 = pd.merge(
    OUTPUT_4_1,
    OUTPUT_4_2,
    on=['cycle', 'district'],
    how='outer'
    )

OUTPUT_4.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_4.csv"), index=False)
# OUTPUT_4 = OUTPUT_4_2.drop(columns = 'real_data') # no duplicate columns when merging with OUTPUT_2


#%%

## OUTPUT_5: 
print("Processing OUTPUT_5...")

#     1 district_color_35_65: This variable is constructed as follows. First, we compute average vote share of Democratic Presidential nominee in the district in the closest Presidential election (var: district.pres.vs) across all cycles. Second, if this is between 35 and 65, we assign value "C". We assign value "D" if this average is above 65, and value "R" otherwise

#     2 district_color_30_70: Same as district_color_35_65 but using 30 and 70 as thresholds

#     3 district_color_40_60: Same as district_color_35_65 but using 40 and 60 as thresholds

print("Filtering OUTPUT_5...")
OUTPUT_5 = OUTPUT_1.groupby(
    ['district', 'cycle']).agg(
        district_pres_vs_avg = ('district.pres.vs', 'mean'),
        district_pres_vs_num_candidates = ('bonica.rid', 'nunique')
).reset_index()
        

labels = ['R', 'C', 'D']

# Assign categories
OUTPUT_5['district_color_35_65'] = pd.cut(
    OUTPUT_5['district_pres_vs_avg'], 
    bins = [0, 0.35, 0.65, 1], 
    labels = labels, 
    include_lowest = True  # Ensures 0 is included
)

OUTPUT_5['district_color_30_70'] = pd.cut(
    OUTPUT_5['district_pres_vs_avg'], 
    bins = [0, 0.30, 0.70, 1], 
    labels = labels, 
    include_lowest = True
)

OUTPUT_5['district_color_40_60'] = pd.cut(
    OUTPUT_5['district_pres_vs_avg'], 
    bins = [0, 0.40, 0.60, 1], 
    labels = labels, 
    include_lowest = True
)

print("Finished merging OUTPUT_5!")

OUTPUT_5 = pd.merge(
    OUTPUT_0,
    OUTPUT_5,
    on=['cycle', 'district'],
    how='outer'
    )

OUTPUT_5.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_5.csv"), index = False)

OUTPUT_5 = OUTPUT_5[['district', 'cycle', 'district_color_35_65', 'district_color_30_70', 'district_color_40_60']]

#%%

## OUTPUT_6: 
print("Processing OUTPUT_6...")

# THESE ARE OLD VAR NAMES

#     - dist_cycle_comp_35_65: We compute the average across candidates of the vote shares obtained in the general election. We consider only the two main candidates for this. Second, if this is between 35 and 65, we assign value 1, and 0 otherwise.

#     - dist_cycle_comp_30_70: Same as dist_cycle_comp_35_65 but using 30 and 70 as thresholds

#     - dist_cycle_comp_40_60: Same as dist_cycle_comp_35_65 but using 40 and 60 as thresholds

#     - avg_gen_vote_pct_dem: Average gen.vote.pct values for Democrats for each district in all cycles prior to the current one, excluding the present cycle

#     - avg_gen_vote_pct_rep: Average gen.vote.pct values for Republicans for each district in all cycles prior to the current one, excluding the present cycle

#     - dist_cycle_comp_35_65_lag: dist_cycle_comp_35_65 lagged by one election cycle

#     - dist_cycle_comp_30_70_lag: dist_cycle_comp_30_70 lagged by one election cycle

#     - dist_cycle_comp_40_60_lag: dist_cycle_comp_40_60 lagged by one election cycle

#     - dem_prim_cycle_comp_35_65: Same as dist_cycle_comp_35_65 but considering only (two main) candidates in Dem primary

#     - dem_prim_cycle_comp_30_70: Same as dist_cycle_comp_30_70 but considering only (two main) candidates in Dem primary

#     - dem_prim_cycle_comp_40_60: Same as dist_cycle_comp_40_60 but considering only (two main) candidates in Dem primary

#     - dem_prim_cycle_comp_35_65_lag: dem_prim_cycle_comp_35_65 lagged by one election cycle

#     - dem_prim_cycle_comp_30_70_lag: dem_prim_cycle_comp_30_70 lagged by one election cycle

#     - dem_prim_cycle_comp_40_60_lag: dem_prim_cycle_comp_40_60 lagged by one election cycle

#     - rep_prim_cycle_comp_35_65: Same as dist_cycle_comp_35_65 but considering only (two main) candidates in Rep primary

#     - rep_prim_cycle_comp_30_70: Same as dist_cycle_comp_30_70 but considering only (two main) candidates in Rep primary

#     - rep_prim_cycle_comp_40_60: Same as dist_cycle_comp_40_60 but considering only (two main) candidates in Rep primary

#     - rep_prim_cycle_comp_35_65_lag: rep_prim_cycle_comp_35_65 lagged by one election cycle

#     - rep_prim_cycle_comp_30_70_lag: rep_prim_cycle_comp_30_70 lagged by one election cycle

#     - rep_prim_cycle_comp_40_60_lag: rep_prim_cycle_comp_40_60 lagged by one election cycle

#     - Election_day: Date of the general election held at that election cycle.



# NEW VAR NAMES

#     - totalvotes: Total number of votes in district-cycle

#     - G_dem: Vote share of Democratic candidate in general elections

#     - G_rep: Vote share of Republican candidate in general elections

#     - G_dispersion: Absolute difference between G_dem and G_rep

#     - num_candidates: Number of candidates running in general elections

#     - G_dispersion_lag: Lag of G_dispersion

#     - P_max_dem: Vote share of Democratic candidates with highest vote share in primary elections

#     - P_min_dem: Vote share of Democratic candidates with second highest vote share in primary elections

#     - P_dispersion_dem: Absolute difference between P_max_dem and P_min_dem

#     - num_candidates_dem: Number of candidates running in Democratic primary elections

#     - P_dispersion_dem_lag: Lag of P_dispersion_dem

#     - P_max_rep: Vote share of Republican candidates with highest vote share in primary elections

#     - P_min_rep: Vote share of Republican candidates with second highest vote share in primary elections

#     - P_dispersion_rep: Absolute difference between P_max_rep and P_min_rep

#     - num_candidates_rep: Number of candidates running in Republican primary elections

#     - P_dispersion_rep_lag: Lag of P_dispersion_rep

#     - Election_day: Date of the general election held at that election cycle.


# Functions

# 1.
def calculate_metrics(data, measure_var):
    result = {}
    
    """
    Using either gen.vote.pct (G) and prim.vote.pct (P) variable
    Calculating _max, _min, and _dipsersion
    """
    
    # # Filter to only rows that have the measure variable
    # df_filtered = data[data[measure_var].notna()].copy()

    if measure_var == 'gen.vote.pct':
        
        # 1. Generate max, min, dispersion
        # Sort by general election vote percentage (descending)
        gen_sorted = data.sort_values(by='gen.vote.pct', ascending=False)
        
        # Get top 2 candidates (by unique bonica.rid)
        gen_top_candidates = gen_sorted.drop_duplicates(subset='bonica.rid').head(2)
        
        # Calulating max, min, and dispersion
        if len(gen_top_candidates) > 0:
            result['G_max'] = gen_top_candidates['gen.vote.pct'].iloc[0]
            if len(gen_top_candidates) > 1:
                result['G_min'] = gen_top_candidates['gen.vote.pct'].iloc[1]
                result['G_dispersion'] = result['G_max'] - result['G_min']
                result['G_average'] = (result['G_max'] + result['G_min']) / 2
                
            else:
                result['G_min'] = np.nan
                result['G_dispersion'] = np.nan
                result['G_average'] = np.nan
                
        # Count unique candidates
        result['num_candidates'] = data['bonica.rid'].nunique()
                    
    elif measure_var == 'prim.vote.pct':
        # Sort by primary election vote percentage (descending)
        prim_sorted = data.sort_values(by='prim.vote.pct', ascending=False)
        
        # Get top 2 candidates (by unique bonica.rid)
        prim_top_candidates = prim_sorted.drop_duplicates(subset='bonica.rid').head(2)
        
        # Calulating max, min, and dispersion
        if len(prim_top_candidates) > 0:
            result['P_max'] = prim_top_candidates['prim.vote.pct'].iloc[0]
            if len(prim_top_candidates) > 1:
                result['P_min'] = prim_top_candidates['prim.vote.pct'].iloc[1]
                result['P_dispersion'] = result['P_max'] - result['P_min']
                result['P_average'] = (result['P_max'] + result['P_min']) / 2
                
            else:
                result['P_min'] = np.nan
                result['P_dispersion'] = np.nan
                result['P_average'] = np.nan
    
        # Count unique candidates
        result['num_candidates'] = data['bonica.rid'].nunique()
    
    # Special condition: MIT data
    # elif measure_var == 'gen_vote_pct':
    #     # First, get the top candidate from each party
    #     dem_candidate = data[data['party'] == 100].sort_values(by='gen_vote_pct', ascending=False)
    #     if not dem_candidate.empty:
    #         dem_top = dem_candidate.iloc[0]
    #     else:
    #         dem_top = None
            
    #     rep_candidate = data[data['party'] == 200].sort_values(by='gen_vote_pct', ascending=False)
    #     if not rep_candidate.empty:
    #         rep_top = rep_candidate.iloc[0]
    #     else:
    #         rep_top = None
        
    #     # # DataFrame with the top two candidates (one from each party)
    #     gen_top_candidates = pd.DataFrame()
    #     if dem_top is not None:
    #         gen_top_candidates = pd.concat([gen_top_candidates, pd.DataFrame([dem_top])])
    #     if rep_top is not None:
    #         gen_top_candidates = pd.concat([gen_top_candidates, pd.DataFrame([rep_top])])
        
    #     # Calculate metrics
    #     if len(gen_top_candidates) > 0:
    #         result['G_dem'] = dem_top
    #         result['G_rep'] = rep_top
    #         if len(gen_top_candidates) > 1:
    #             # result['G_min'] = gen_top_candidates['gen_vote_pct'].min()
    #             result['G_dispersion'] = abs(result['G_dem'] - result['G_dem'])
    #             result['G_average'] = (result['G_dem'] + result['G_rep']) / 2
    #         else:
    #             # result['G_min'] = np.nan
    #             result['G_dem'] = np.nan
    #             result['G_dem'] = np.nan / 2
    #             result['G_dispersion'] = np.nan
    #             result['G_average'] = np.nan
       
    #     result['num_candidates'] = data['candidate'].nunique()    
    
    # Special condition: MIT data
    elif measure_var == 'gen_vote_pct':
        # First, get the top candidate from each party
        dem_candidate = data[data['party'] == 100].sort_values(by='gen_vote_pct', ascending=False)
        if not dem_candidate.empty:
            dem_top = dem_candidate['gen_vote_pct'].iloc[0]
        else:
            dem_top = None
            
        rep_candidate = data[data['party'] == 200].sort_values(by='gen_vote_pct', ascending=False)
        if not rep_candidate.empty:
            rep_top = rep_candidate['gen_vote_pct'].iloc[0]
        else:
            rep_top = None
        
        # Calculate metrics
        if dem_top is not None or rep_top is not None:
            result['G_dem'] = dem_top if dem_top is not None else np.nan
            result['G_rep'] = rep_top if rep_top is not None else np.nan
            
            if dem_top is not None and rep_top is not None:
                # Fix: Calculate dispersion as absolute difference between dem and rep percentages
                result['G_dispersion'] = abs(result['G_dem'] - result['G_rep'])
                result['G_average'] = (result['G_dem'] + result['G_rep']) / 2
            else:
                result['G_dispersion'] = np.nan
                result['G_average'] = np.nan
       
        result['num_candidates'] = data['candidate'].nunique()
        
    else:
        print(f"{measure_var} not found")
        
    return pd.Series(result)

# 2.
def calculate_avg_gen_vote_pct(data, value_column='G_average'):
    """
    Calculate average general vote percentages for Democrats and Republicans by district and cycle.
    For cycle == 1980, treat as missing data.
    For other cycles, calculate the average of G_average for each party using all previous cycles.
    
    Parameters:
    data: DataFrame containing district, cycle, party, and G_average columns
    value_column: Column name to use for calculations (default: 'G_average')
    
    Returns:
    DataFrame with district, cycle, avg_gen_vote_pct_dem, and avg_gen_vote_pct_rep columns
    """
    # Create a copy of the data to avoid modifying the original
    data = data.copy()
    
    # Create an empty DataFrame to store results
    result_df = pd.DataFrame()
    
    # Get unique district-party combinations
    district_party_combos = data[['district', 'party']].dropna().drop_duplicates()
    
    # For each district and party, calculate averages of previous cycles
    for _, row in district_party_combos.iterrows():
        district = row['district']
        party = row['party']
        
        # Get data for this district and party
        district_party_data = data[
            (data['district'] == district) & 
            (data['party'] == party)
        ].sort_values('cycle')
        
        # Get unique cycles for this district-party combination
        cycles = district_party_data['cycle'].unique()
        
        # For each cycle, calculate average of previous cycles
        for cycle in cycles:
            # For 1980, we'll set NaN
            if cycle == 1980:
                avg_vote_pct = np.nan
            else:
                # Get all data from previous cycles (strictly less than current cycle)
                prev_cycle_data = district_party_data[district_party_data['cycle'] < cycle]
                
                # Calculate average if there's previous data, otherwise NaN
                if len(prev_cycle_data) > 0 and value_column in prev_cycle_data.columns:
                    # Only consider non-null values
                    valid_prev_data = prev_cycle_data[prev_cycle_data[value_column].notna()]
                    if len(valid_prev_data) > 0:
                        avg_vote_pct = valid_prev_data[value_column].mean()
                    else:
                        avg_vote_pct = np.nan
                else:
                    avg_vote_pct = np.nan
            
            # Add to result DataFrame
            result_df = pd.concat([result_df, pd.DataFrame({
                'district': [district],
                'cycle': [cycle],
                'party': [party],
                'avg_value': [avg_vote_pct]
            })], ignore_index=True)
    
    # Pivot to get one row per district-cycle with columns for each party
    if not result_df.empty:
        pivoted = result_df.pivot(index=['district', 'cycle'], 
                                columns='party', 
                                values='avg_value').reset_index()
        
        # Rename columns (100 for Democrats, 200 for Republicans)
        pivoted.columns.name = None
        
        # Create column names based on the value column used
        dem_col = f'avg_{value_column}_dem'
        rep_col = f'avg_{value_column}_rep'
        
        # Ensure columns for both parties exist (they might not if data is missing)
        if 100 in pivoted.columns:
            pivoted = pivoted.rename(columns={100: dem_col})
        else:
            pivoted[dem_col] = np.nan
            
        if 200 in pivoted.columns:
            pivoted = pivoted.rename(columns={200: rep_col})
        else:
            pivoted[rep_col] = np.nan
        
        # Ensure 1980 values are NaN
        pivoted.loc[pivoted['cycle'] == 1980, [dem_col, rep_col]] = np.nan
        
        return pivoted
    else:
        # Return empty DataFrame with appropriate columns if no data
        dem_col = f'avg_{value_column}_dem'
        rep_col = f'avg_{value_column}_rep'
        return pd.DataFrame(columns=['district', 'cycle', dem_col, rep_col])    
    
# # 3.
# def create_cat_vars(data, dictionary, var, labels):
#     result_data = data.copy()
#     for key, value in dictionary.items():
#         print(key, value)
#         result_data[key] = pd.cut(
#             result_data[var], 
#             bins=value, 
#             labels=labels, 
#             ordered=False,
#             include_lowest=True
#         )
#     return result_data
    

    
# Creating datasets
print("Filtering OUTPUT_6_1_1...")
# OUTPUT_6_1_1 = OUTPUT_1.groupby(['district', 'cycle']).apply(
#     lambda x: calculate_metrics(x, 'gen.vote.pct')
# ).reset_index()

OUTPUT_6_1_1 = gen_elect_df.groupby(['district', 'cycle', 'totalvotes']).apply(
    lambda x: calculate_metrics(x, 'gen_vote_pct')
).reset_index()


OUTPUT_6_1_1 = pd.merge(
    OUTPUT_0,
    OUTPUT_6_1_1,
    on=['cycle', 'district'],
    how='outer'
    )

# display_duplicates(gen_elect_df, ['district', 'cycle', 'candidate'])
# display_duplicates(OUTPUT_6_1_1, ['district', 'cycle'])

# We groupby party as well, to get G_max for Dems and Reps (their vote share)
print("Filtering OUTPUT_6_1_2...")
# OUTPUT_6_1_2 = OUTPUT_1.groupby(['district', 'cycle', 'party']).apply(
#     lambda x: calculate_metrics(x, 'gen.vote.pct')
# ).reset_index()

OUTPUT_6_1_2 = gen_elect_df.groupby(['district', 'cycle', 'party']).apply(
    lambda x: calculate_metrics(x, 'gen_vote_pct')
).reset_index()

OUTPUT_6_1_2 = pd.merge(
    OUTPUT_0_2,
    OUTPUT_6_1_2,
    on=['cycle', 'district', 'party'],
    how='outer'
    )
# OUTPUT_6_1_2 = OUTPUT_6_1_2[OUTPUT_6_1_2['party'].isin(unique_parties)]

# Now, we use the output above to calculate the average values of gen.vote.pct for dems and reps
print("Filtering OUTPUT_6_1_3...")
OUTPUT_6_1_3 = calculate_avg_gen_vote_pct(OUTPUT_6_1_2, value_column='G_max')
OUTPUT_6_1_3 = OUTPUT_6_1_3.rename(columns = {
    'avg_G_max_dem': 'avg_gen_vote_pct_dem',
    'avg_G_max_rep': 'avg_gen_vote_pct_rep',
    }
)


# Then, we continue for primary elections
print("Filtering OUTPUT_6_2 and processing OUTPUT_6_2_dem and OUTPUT_6_2_rep...")
OUTPUT_6_2 = OUTPUT_1.groupby(['district', 'cycle', 'party']).apply(
    lambda x: calculate_metrics(x, 'prim.vote.pct')
).reset_index()


OUTPUT_6_2_dem = OUTPUT_6_2[OUTPUT_6_2['party'] == 100].drop(columns = 'party')
OUTPUT_6_2_rep = OUTPUT_6_2[OUTPUT_6_2['party'] == 200].drop(columns = 'party')


# Adding suffix _dem and _rep to all variable names (except district, cycle)
OUTPUT_6_2_dem_dict = {col: f"{col}_dem" for col in OUTPUT_6_2_dem.columns if col not in ["district", "cycle"]}
OUTPUT_6_2_dem = OUTPUT_6_2_dem.rename(columns=OUTPUT_6_2_dem_dict)
OUTPUT_6_2_rep_dict = {col: f"{col}_rep" for col in OUTPUT_6_2_rep.columns if col not in ["district", "cycle"]}
OUTPUT_6_2_rep = OUTPUT_6_2_rep.rename(columns=OUTPUT_6_2_rep_dict)


# Applying a function to create variables using a for loop for each dicitonary
# print("Creating categorical variables for district in general...")
# label_values = [0, 1, 0]

# # NOTE: gen.vote.pct and prim.vote.pct use different scales for measuring percentages, labels adjusted accordingly
# # OUTPUT_6_1_dict_G = {
# #     'dist_cycle_comp_35_65': [0, 35, 65, 100],
# #     'dist_cycle_comp_30_70': [0, 30, 70, 100],
# #     'dist_cycle_comp_40_60': [0, 40, 60, 100]
# #     }

# OUTPUT_6_1_dict_G = {
#     'dist_cycle_comp_35_65': [0, 0.35, 0.65, 1],
#     'dist_cycle_comp_30_70': [0, 0.30, 0.70, 1],
#     'dist_cycle_comp_40_60': [0, 0.40, 0.60, 1]
#     }


# OUTPUT_6_2_dict_dem_P = {
#     'dem_prim_cycle_comp_35_65': [0, 0.35, 0.65, 1],
#     'dem_prim_cycle_comp_30_70': [0, 0.30, 0.70, 1],
#     'dem_prim_cycle_comp_40_60': [0, 0.40, 0.60, 1]
#     }

# OUTPUT_6_2_dict_rep_P = {
#     'rep_prim_cycle_comp_35_65': [0, 0.35, 0.65, 1],
#     'rep_prim_cycle_comp_30_70': [0, 0.30, 0.70, 1],
#     'rep_prim_cycle_comp_40_60': [0, 0.40, 0.60, 1]
#     }


# OUTPUT_6_1_1 = create_cat_vars(OUTPUT_6_1_1, OUTPUT_6_1_dict_G, 'G_dispersion', label_values)
# OUTPUT_6_2_dem = create_cat_vars(OUTPUT_6_2_dem, OUTPUT_6_2_dict_dem_P, 'P_dispersion', label_values)
# OUTPUT_6_2_rep = create_cat_vars(OUTPUT_6_2_rep, OUTPUT_6_2_dict_rep_P, 'P_dispersion', label_values)

# Balancing datasets
OUTPUT_6_2_dem = pd.merge(
    OUTPUT_0,
    OUTPUT_6_2_dem,
    on=['cycle', 'district'],
    how='outer'
    )
OUTPUT_6_2_rep = pd.merge(
    OUTPUT_0,
    OUTPUT_6_2_rep,
    on=['cycle', 'district'],
    how='outer'
    )

# Adding lagged variables
print("Adding lagged variables")

# OUTPUT_6_1_1 = OUTPUT_6_1_1.sort_values(by=['district', 'cycle'])
# for var in ['dist_cycle_comp_35_65', 'dist_cycle_comp_30_70', 'dist_cycle_comp_40_60']:
#     OUTPUT_6_1_1[f'{var}_lag'] = OUTPUT_6_1_1.groupby('district')[var].shift(1)

# OUTPUT_6_2_dem = OUTPUT_6_2_dem.sort_values(by=['district', 'cycle'])
# for var in ['dem_prim_cycle_comp_35_65', 'dem_prim_cycle_comp_30_70', 'dem_prim_cycle_comp_40_60']:
#     OUTPUT_6_2_dem[f'{var}_lag'] = OUTPUT_6_2_dem.groupby('district')[var].shift(1)

# OUTPUT_6_2_rep = OUTPUT_6_2_rep.sort_values(by=['district', 'cycle'])
# for var in ['rep_prim_cycle_comp_35_65', 'rep_prim_cycle_comp_30_70', 'rep_prim_cycle_comp_40_60']:
#     OUTPUT_6_2_rep[f'{var}_lag'] = OUTPUT_6_2_rep.groupby('district')[var].shift(1)


OUTPUT_6_1_1 = OUTPUT_6_1_1.sort_values(by=['district', 'cycle'])
for var in ['G_dispersion']:
    OUTPUT_6_1_1[f'{var}_lag'] = OUTPUT_6_1_1.groupby('district')[var].shift(1)

OUTPUT_6_2_dem = OUTPUT_6_2_dem.sort_values(by=['district', 'cycle'])
for var in ['P_dispersion_dem']:
    OUTPUT_6_2_dem[f'{var}_lag'] = OUTPUT_6_2_dem.groupby('district')[var].shift(1)

OUTPUT_6_2_rep = OUTPUT_6_2_rep.sort_values(by=['district', 'cycle'])
for var in ['P_dispersion_rep']:
    OUTPUT_6_2_rep[f'{var}_lag'] = OUTPUT_6_2_rep.groupby('district')[var].shift(1)


# Saving datasets before dropping columns
# OUTPUT_6_1_1.to_csv(os.path.join(data_folder, 'OUTPUTS', 'OUTPUT_6_1_1.csv'), index = False)
# OUTPUT_6_1_3.to_csv(os.path.join(data_folder, 'OUTPUTS', 'OUTPUT_6_1_3.csv'), index = False)
# OUTPUT_6_2_dem.to_csv(os.path.join(data_folder, 'OUTPUTS', 'OUTPUT_6_2_dem.csv'), index = False)
# OUTPUT_6_2_rep.to_csv(os.path.join(data_folder, 'OUTPUTS', 'OUTPUT_6_2_rep.csv'), index = False)

# Dropping columns

dataframes = [OUTPUT_6_1_1, OUTPUT_6_1_3, OUTPUT_6_2_dem, OUTPUT_6_2_rep]
drop_columns = ['G_average','P_average_dem', 'P_average_rep',
                'avg_gen_vote_pct_dem', 'avg_gen_vote_pct_rep']
for i in range(len(dataframes)):
    for column in dataframes[i].columns:
        if column in drop_columns:
            dataframes[i].drop(
                columns=[column],
                inplace=True
            )
            
for i in range(len(dataframes)):
    for column in dataframes[i].columns:
        if column in drop_columns:
            dataframes[i].drop(
                columns=[column],
                inplace=True
            )


# Merging all datasets to one final dataset
print("Merging datasets to OUTPUT_6...")


OUTPUT_6 = pd.merge(
    OUTPUT_6_1_1,
    OUTPUT_6_1_3,
    on=['cycle', 'district'],
    how='outer'
    )

OUTPUT_6 = pd.merge(
    OUTPUT_6,
    OUTPUT_6_2_dem,
    on=['cycle', 'district'],
    how='outer'
    )

OUTPUT_6 = pd.merge(
    OUTPUT_6,
    OUTPUT_6_2_rep,
    on=['cycle', 'district'],
    how='outer'
    )

OUTPUT_6 = pd.merge(
    OUTPUT_6,
    election_dates_df, # merging to get election dates
    on='cycle',
    how='left'
)

OUTPUT_6 = OUTPUT_6.rename(columns = {
    'election_date_in_cycle':'Election_day'
    }
    )

print("Merge complete!")



# # Adding lags variables
# OUTPUT_6 = OUTPUT_6.sort_values(by=['district', 'cycle'])
# for var in ['dist_cycle_comp_35_65', 'dist_cycle_comp_30_70', 'dist_cycle_comp_40_60',
#             'dem_prim_cycle_comp_35_65', 'dem_prim_cycle_comp_30_70', 'dem_prim_cycle_comp_40_60', 
#             'rep_prim_cycle_comp_35_65', 'rep_prim_cycle_comp_30_70', 'rep_prim_cycle_comp_40_60']:
#     OUTPUT_6[f'{var}_lag'] = OUTPUT_6.groupby('district')[var].shift(1)

OUTPUT_6.to_csv(os.path.join(data_folder, 'OUTPUTS', 'OUTPUT_6.csv'), index = False)



#%%

## OUTPUT_7: 
print("Processing OUTPUT_7...")

#   - "treat_1": "For single districts, we assign 1 for district/cycles after death of an incumbent. We assign 0 otherwise (before death). For multiple death districts, we copy treat_1 logic and take into account only first death",

#   - "treat_2": "For single districts, we copy treat_1 logic. For multiple death districts, we assign 1 for district/cycles for the first cycle after death of an incumbent. We assign 0 afterwards until second death, where we assign 1 again in the first cycle after second death, and 0 again. We repeat for third death, if applicable.",

#   - "death_date": "Date of incumbent's death, if applicable"

#   - "death_unexpected_1": "For treat_1, we take this dummy from Deaths.xlsx dataset. 1, if if cause of death was an unexpected event.",

#   - "death_age_1": "For treat_1, age of dead incumbent",

#   - "death_party_1": "For treat_1, party of dead incumbent.",

#   - "death_unexpected_2": "For treat_2, we take this dummy from Deaths.xlsx dataset. 1, if if cause of death was an unexpected event.",

#   - "death_age_2": "For treat_2, age of dead incumbent",

#   - "death_party_2": "For treat_2, party of dead incumbent.",

#   - "spec_member": "District representative in special elections (includes deaths and resignations)",

#   - "special_elections_date": "Date of special elections, if applicable"

#   - "special_elections_cause": "Cause of special elections (either death or resignation of representative)",

#   - "special_elections": "Dummy to indicate district-cycle with special election",


OUTPUT_7 = OUTPUT_1[['district', 'cycle']].drop_duplicates()
OUTPUT_7 = OUTPUT_7[~OUTPUT_7['cycle'].isna()]
print("Balancing dataset...")
OUTPUT_7 = pd.merge(
    OUTPUT_0,
    OUTPUT_7,
    how = 'outer',
    on = ['district', 'cycle']
    )

# Adding information from special elections data
OUTPUT_7 = pd.merge(
    OUTPUT_7,
    special_elections[['district', 'death_cycle', 'spec_cycle', 'death_date', 'spec_election_date', 
                       'dead_member_margin', 'spec_winner_margin', 'runoff']].rename(
        columns = {
            'death_cycle': 'cycle',
            'spec_election_date': 'special_elections_date'
            }),
    how = 'left',
    on = ['district', 'cycle']
    )


# Creating treat_1
# print("Creating treat_1...")
# OUTPUT_7['treat_1'] = 0
# for district in single_death_districts + multiple_death_districts:
#     # Get death year for this district
#     district_death_dates = OUTPUT_1[
#         (OUTPUT_1['district'] == district) & 
#         (OUTPUT_1['death_date'].notna())
#     ]['death_date']
    
#     if not district_death_dates.empty:
#         death_date = district_death_dates.iloc[0]
#         death_year = pd.to_datetime(death_date).year
        
#         # Assign treatment to cycles after death year
#         OUTPUT_7.loc[
#             (OUTPUT_7['district'] == district) & 
#             (OUTPUT_7['cycle'] > death_year), 
#             'treat_1'] = 1
#     else:
#         print(f"Warning: No death date found for district {district}")
# print("Finished processing treat_1")

print("Creating treat_1...")
OUTPUT_7['treat_1'] = 0
for district in single_death_districts + multiple_death_districts:
    # Assign treat_1 = 1 where death_date is not NA and for all cycles after in that district
    district_mask = OUTPUT_7['district'] == district
    district_data = OUTPUT_7[district_mask].copy()
    
    # Find the first row where death_date is not NA
    first_death_idx = district_data[district_data['death_date'].notna()].index
    
    if not first_death_idx.empty:
        first_death_cycle = OUTPUT_7.loc[first_death_idx[0], 'cycle']
        
        # Assign treat_1 = 1 for cycles >= first death cycle
        OUTPUT_7.loc[
            (OUTPUT_7['district'] == district) & 
            (OUTPUT_7['cycle'] >= first_death_cycle), 
            'treat_1'] = 1
    else:
        print(f"Warning: No death date found for district {district} (likely because of merge btw special_elections and sample used")
        
print("Finished processing treat_1")

# Creating treat_2
# print("Creating treat_2...")
# OUTPUT_7['treat_2'] = 0
# for district in single_death_districts + multiple_death_districts:
#     # Get all deaths for this district with their years
#     district_deaths = OUTPUT_1[
#         (OUTPUT_1['district'] == district) & 
#         (OUTPUT_1['death_date'].notna())
#     ][['death_date', 'cycle']].drop_duplicates()
    
#     district_deaths['death_year'] = pd.to_datetime(district_deaths['death_date']).dt.year
    
#     # For each contribution in the district
#     district_mask = OUTPUT_7['district'] == district
    
#     if not district_death_dates.empty:
        
#         if district in single_death_districts:
            
#             # Copying treat_1 to treat_2
#             OUTPUT_7.loc[district_mask, 'treat_2'] = OUTPUT_7.loc[district_mask, 'treat_1']
        
#         # elif district in multiple_death_districts:
#         #     print(f"{district} is multiple death")
#         #     # Get all special election years for this district
#         #     special_elections_death_dates = special_elections[
#         #         (special_elections['district'] == district)
#         #     ]['spec_election_date'].unique()
            
#         #     special_elections_years = pd.to_datetime(special_elections_death_dates).year
            
#         #     print(f"Distict: {district}, Special elections years: {special_elections_years}")
#         #     print()
            
#         #     # For each cycle in this district (sorted)
#         #     district_cycles = sorted(OUTPUT_7[OUTPUT_7['district'] == district]['cycle'])       
            
#         #     for i, cycle in enumerate(district_cycles):
#         #         state = 0 # default state is 0
    
#         #         # Check each death
#         #         for _, death_row in district_deaths.iterrows():
#         #             death_year = death_row['death_year']
                    
#         #             # If current cycle is the first cycle after death
#         #             if cycle > death_year:
#         #                 # Check if there was a previous cycle after death but before current cycle
#         #                 if i > 0 and district_cycles[i-1] > death_year:
#         #                     # Not the first cycle after death, skip
#         #                     continue
                        
#         #                 # Check if there's a special election between death and this cycle
#         #                 intervening_special = any((special_year > death_year) & (special_year <= cycle) 
#         #                                          for special_year in special_elections_years)
                        
#         #                 # If no intervening special election and this is the first cycle after death
#         #                 if not intervening_special:
#         #                     state = 1
#         #                     break
                
#         #         OUTPUT_7.loc[
#         #             (OUTPUT_7['district'] == district) & 
#         #             (OUTPUT_7['cycle'] == cycle), 
#         #             'treat_2'] = state
                
#         #         print(f"Finished processing treat_2 for {district}")
                
            
#         elif district in multiple_death_districts:
            
#             # Get all special election years for this district
#             special_elections_death_dates = special_elections[
#                 (special_elections['district'] == district)
#             ]['spec_election_date'].unique()
            
#             special_elections_years = pd.to_datetime(special_elections_death_dates).year
            
#             # For each cycle in this district (sorted)
#             cycle_mask = OUTPUT_7['district'] == district
#             district_cycles = sorted(OUTPUT_7[cycle_mask]['cycle'])       
            
#             for cycle in district_cycles:
#                 state = 0  # default state is 0
#                 cycle_idx = district_cycles.index(cycle)
    
#                 # Check each death
#                 for _, death_row in district_deaths.iterrows():
#                     death_year = death_row['death_year']
                    
#                     # If current cycle is after death
#                     if cycle > death_year:
#                         # Check if there was a previous cycle after death but before current cycle
#                         if cycle_idx > 0 and district_cycles[cycle_idx-1] > death_year:
#                             # Not the first cycle after death, skip
#                             continue
                        
#                         # Check if there's a special election between death and this cycle
#                         intervening_special = any((special_year > death_year) & (special_year <= cycle) 
#                                                  for special_year in special_elections_years)
                        
#                         # If no intervening special election and this is the first cycle after death
#                         if not intervening_special:
#                             state = 1
#                             break
                
#                 # Use the combined mask for district and cycle
#                 combined_mask = district_mask & (OUTPUT_7['cycle'] == cycle)
#                 OUTPUT_7.loc[combined_mask, 'treat_2'] = state
                
#         else:
#             print("District not found in single_death_districts nor in multiple_death_districts")
        
#     else:         
#         print(f"Warning: No death date found for district {district}")        
# print("Finished processing treat_2")   
        
print("Creating treat_2...")
OUTPUT_7['treat_2'] = 0

for district in single_death_districts + multiple_death_districts:
    district_mask = OUTPUT_7['district'] == district
    
    if district in single_death_districts:
        # For single death districts, copy treat_1 logic
        OUTPUT_7.loc[district_mask, 'treat_2'] = OUTPUT_7.loc[district_mask, 'treat_1']
        
    elif district in multiple_death_districts:
        # For multiple death districts, treat_2 = 1 only when death_date is not NA
        OUTPUT_7.loc[
            (OUTPUT_7['district'] == district) & 
            (OUTPUT_7['death_date'].notna()), 
            'treat_2'
        ] = 1
    else:
        print(f"District {district} not found in single_death_districts nor in multiple_death_districts")

print("Finished processing treat_2")



# Creating treat_3
print("Creating treat_3...")
OUTPUT_7['treat_3'] = 0

for district in single_death_districts + multiple_death_districts:
    district_mask = OUTPUT_7['district'] == district
    
    if district in single_death_districts:
        # For single death districts, treat_3 = 1 only when death_date is not NA
        OUTPUT_7.loc[
            (OUTPUT_7['district'] == district) & 
            (OUTPUT_7['death_date'].notna()), 
            'treat_3'
        ] = 1
        
    elif district in multiple_death_districts:
        # For multiple death districts, replicate treat_2
        OUTPUT_7.loc[district_mask, 'treat_3'] = OUTPUT_7.loc[district_mask, 'treat_2']
    else:
        print(f"District {district} not found in single_death_districts nor in multiple_death_districts")

print("Finished processing treat_3")


# RENAMING CYCLE BACK TO 


# Creating death_unexpected, death_age, and death_party_member
print("Creating death_unexpected_1 (and _2), death_age_1 (and _2), and death_party_member_1 (and _2)...")
OUTPUT_7['death_unexpected_1'] = np.nan
OUTPUT_7['death_age_1'] = np.nan
OUTPUT_7['death_party_1'] = np.nan
OUTPUT_7['death_unexpected_2'] = np.nan
OUTPUT_7['death_age_2'] = np.nan
OUTPUT_7['death_party_2'] = np.nan
OUTPUT_7['death_unexpected_3'] = np.nan
OUTPUT_7['death_age_3'] = np.nan
OUTPUT_7['death_party_3'] = np.nan

# for district in single_death_districts + multiple_death_districts:
#     # Get all deaths for this district with their metadata
#     district_deaths = OUTPUT_1[
#         (OUTPUT_1['district'] == district) & 
#         (OUTPUT_1['death_date'].notna())
#     ][['death_date', 'cycle', 'death_unexpected', 'death_age', 'spec_party', 'treat_1', 'treat_2']].drop_duplicates()
    
#     if district_deaths.empty:
#         print(f"Warning: No death data found for district {district}")
#         continue
    
#     # Convert death_date to datetime and sort by date
#     district_deaths['death_date'] = pd.to_datetime(district_deaths['death_date'])
#     district_deaths = district_deaths.sort_values('death_date')
#     district_deaths['death_year'] = district_deaths['death_date'].dt.year
    
#     # Get all special election dates for this district if it's a multiple death district
#     if district in multiple_death_districts:
#         special_elections_dates = pd.to_datetime(special_elections[
#             (special_elections['district'] == district)
#         ]['spec_election_date'].unique())
#         special_elections_years = pd.Series([date.year for date in special_elections_dates if not pd.isna(date)])
    
#     # Get all cycles for this district (sorted)
#     district_cycles = sorted(OUTPUT_7[OUTPUT_7['district'] == district]['cycle'])
    
#     # For single death districts, process is simpler
#     if district in single_death_districts:
#         death_row = district_deaths.iloc[0]  # Get the only death row
        
#         # For ALL cycles in this district, assign the first death's attributes
#         for cycle in district_cycles:
#             OUTPUT_7.loc[
#                 (OUTPUT_7['district'] == district) & 
#                 (OUTPUT_7['cycle'] == cycle), 
#                 ['death_unexpected', 'death_age', 'spec_party']
#             ] = [
#                 death_row['death_unexpected'],
#                 death_row['death_age'],
#                 death_row['spec_party']
#             ]
    
#     # For multiple death districts
#     else:
#         # Get the first death's data
#         first_death = district_deaths.iloc[0]
#         first_death_year = first_death['death_year']
        
#         # For each cycle
#         for cycle in district_cycles:
#             # Default to using first death's data
#             relevant_death = first_death
            
#             # If we're after the first death, check if we should use a different death
#             if cycle > first_death_year:
#                 for idx, death_row in district_deaths.iterrows():
#                     death_year = death_row['death_year']
                    
#                     # If death is before this cycle
#                     if death_year < cycle:
#                         # Check if there's a special election between this death and the cycle
#                         intervening_special = any(
#                             (special_year > death_year) & (special_year <= cycle) 
#                             for special_year in special_elections_years
#                         )
                        
#                         # If no intervening special election, this death is relevant
#                         if not intervening_special:
#                             relevant_death = death_row
            
#             # Assign the relevant death's attributes
#             OUTPUT_7.loc[
#                 (OUTPUT_7['district'] == district) & 
#                 (OUTPUT_7['cycle'] == cycle), 
#                 ['death_unexpected', 'death_age', 'spec_party']
#             ] = [
#                 relevant_death['death_unexpected'],
#                 relevant_death['death_age'],
#                 relevant_death['spec_party']
#             ]


for district in single_death_districts + multiple_death_districts:
    # Get all deaths for this district with their metadata
    district_deaths = OUTPUT_1[
        (OUTPUT_1['district'] == district) & 
        (OUTPUT_1['death_date'].notna())
    ][['death_date', 'cycle', 'death_unexpected', 'death_age', 'spec_party']].drop_duplicates()
    
    if district_deaths.empty:
        print(f"Warning: No death data found for district {district}")
        continue
    
    # Convert death_date to datetime and sort by date
    district_deaths['death_date'] = pd.to_datetime(district_deaths['death_date'])
    district_deaths = district_deaths.sort_values('death_date')
    district_deaths['death_year'] = district_deaths['death_date'].dt.year
    
    # Get all special election dates for this district
    special_elections_dates_temp = pd.to_datetime(special_elections[
        (special_elections['district'] == district)
    ]['spec_election_date'].unique())
    special_elections_years = pd.Series([date.year for date in special_elections_dates_temp if not pd.isna(date)])
    
    # Get all cycles for this district (sorted)
    district_cycles = sorted(OUTPUT_7[OUTPUT_7['district'] == district]['cycle'])
    
    # Initialize columns for this district if not already present
    for col in ['death_unexpected_1', 'death_age_1', 'death_party_1', 
                'death_unexpected_2', 'death_age_2', 'death_party_2',
                'death_unexpected_3', 'death_age_3', 'death_party_3']:
        if col not in OUTPUT_7.columns:
            OUTPUT_7[col] = np.nan
    
    # For treat_1 logic (same for single and multiple death districts)
    # Always use the first death
    first_death = district_deaths.iloc[0]
    
    # Apply first death attributes to all cycles in the district for treat_1 variables
    OUTPUT_7.loc[
        OUTPUT_7['district'] == district,
        ['death_unexpected_1', 'death_age_1', 'death_party_1']
    ] = [
        first_death['death_unexpected'],
        first_death['death_age'],
        first_death['spec_party']
    ]
    
    # For treat_2 logic
    for cycle in district_cycles:
        # Find the closest death before (or at) this cycle
        deaths_before_cycle = district_deaths[district_deaths['death_year'] <= cycle]
        
        if deaths_before_cycle.empty:
            # No deaths before this cycle, use first death's attributes instead of null
            relevant_death = first_death
        else:
            # Get the most recent death before this cycle
            relevant_death = deaths_before_cycle.iloc[-1]
            
            # For multiple death districts with treat_2 logic
            if district in multiple_death_districts:
                death_year = relevant_death['death_year']
                
                # Check if there's a special election between this death and the cycle
                intervening_special = any(
                    (special_year > death_year) & (special_year <= cycle) 
                    for special_year in special_elections_years
                )
                
                # If there's an intervening special election, use first death instead of null
                if intervening_special:
                    relevant_death = first_death
        
        # Always assign death attributes (now we always have a relevant_death)
        OUTPUT_7.loc[
            (OUTPUT_7['district'] == district) & 
            (OUTPUT_7['cycle'] == cycle), 
            ['death_unexpected_2', 'death_age_2', 'death_party_2']
        ] = [
            relevant_death['death_unexpected'],
            relevant_death['death_age'],
            relevant_death['spec_party']
        ]
    
    # For treat_3 logic
    if district in single_death_districts:
        # For single death districts, assign attributes only when death_date is not NA
        OUTPUT_7.loc[
            (OUTPUT_7['district'] == district) & 
            (OUTPUT_7['death_date'].notna()),
            ['death_unexpected_3', 'death_age_3', 'death_party_3']
        ] = [
            first_death['death_unexpected'],
            first_death['death_age'],
            first_death['spec_party']
        ]
    elif district in multiple_death_districts:
        # For multiple death districts, replicate treat_2 logic
        # Assign attributes only when death_date is not NA
        for cycle in district_cycles:
            # Find deaths that occurred in this cycle
            cycle_mask = (OUTPUT_7['district'] == district) & (OUTPUT_7['cycle'] == cycle)
            deaths_in_this_cycle = district_deaths[district_deaths['cycle'] == cycle]
            
            # Check if there's a death_date in this cycle
            if OUTPUT_7.loc[cycle_mask, 'death_date'].notna().any() and not deaths_in_this_cycle.empty:
                relevant_death = deaths_in_this_cycle.iloc[0]
                
                OUTPUT_7.loc[
                    cycle_mask,
                    ['death_unexpected_3', 'death_age_3', 'death_party_3']
                ] = [
                    relevant_death['death_unexpected'],
                    relevant_death['death_age'],
                    relevant_death['spec_party']
                ]
                
        
    # IF DEATH ATTRIBUTES NEED TO BE MISSING FOR ZERO VALUES OF TREAT_2
    # # For treat_2 logic
    # for cycle in district_cycles:
    #     # Find the closest death before (or at) this cycle
    #     deaths_before_cycle = district_deaths[district_deaths['death_year'] <= cycle]
        
    #     if deaths_before_cycle.empty:
    #         # No deaths before this cycle, set null values
    #         relevant_death = None
    #     else:
    #         # Get the most recent death before this cycle
    #         relevant_death = deaths_before_cycle.iloc[-1]
            
    #         # For multiple death districts with treat_2 logic
    #         if district in multiple_death_districts:
    #             death_year = relevant_death['death_year']
                
    #             # Check if there's a special election between this death and the cycle
    #             intervening_special = any(
    #                 (special_year > death_year) & (special_year <= cycle) 
    #                 for special_year in special_elections_years
    #             )
                
    #             # If there's an intervening special election, this death is no longer relevant
    #             if intervening_special:
    #                 relevant_death = None
        
    #     # Assign the relevant death's attributes for treat_2 variables
    #     if relevant_death is not None:
    #         OUTPUT_7.loc[
    #             (OUTPUT_7['district'] == district) & 
    #             (OUTPUT_7['cycle'] == cycle), 
    #             ['death_unexpected_2', 'death_age_2', 'death_party_2']
    #         ] = [
    #             relevant_death['death_unexpected'],
    #             relevant_death['death_age'],
    #             relevant_death['spec_party']
    #         ]
    #     else:
    #         # If no relevant death for treat_2, set to null
    #         OUTPUT_7.loc[
    #             (OUTPUT_7['district'] == district) & 
    #             (OUTPUT_7['cycle'] == cycle), 
    #             ['death_unexpected_2', 'death_age_2', 'death_party_2']
    #         ] = [np.nan, np.nan, np.nan]
    
print("Finished processing death attributes")


# Creating special_elections and special_elections_cause data
print("Creating special_elections and special_elections_cause variables")

# Subset of vars from special_elections data, latter only to show what has been merged
special_elections_2 = special_elections[['district', 'spec_cycle', 'spec_member', 'cause_vacancy']].rename( 
    columns = {'spec_cycle': 'cycle',
               'cause_vacancy': 'special_elections_cause'}) # rename for merging

OUTPUT_7 = pd.merge(
    OUTPUT_7,
    special_elections_2,
    how = 'left',
    on = ['district', 'cycle']
    )

OUTPUT_7['special_elections'] = np.where(OUTPUT_7['spec_member'].notna(), 1, 0)

OUTPUT_7['death_date'] = pd.to_datetime(OUTPUT_7['death_date'])
OUTPUT_7['special_elections_date'] = pd.to_datetime(OUTPUT_7['special_elections_date'])


OUTPUT_7.to_csv(os.path.join(data_folder, 'OUTPUTS', 'OUTPUT_7.csv'), index = False)



#%%

## OUTPUT_8: 
print("Processing OUTPUT_8...")

#     1 avg_counting_hedging_corp: Average number of candidates (in general election only, and before the special election) funded by corporations in the district/cycle

#     2 avg_counting_hedging_corp_dem_primary: Same as avg_counting_hedging_corp but for candidates to Dem primary only

#     3 avg_counting_hedging_corp_rep_primary: Same as avg_counting_hedging_corp but for candidates to Rep primary only

#     4 avg_counting_hedging_ind: Average number of candidates (in general election only, and before the special election) funded by individuals in the district/cycle

#     5 avg_counting_hedging_ind_dem_primary: Same as avg_counting_hedging_ind but for candidates to Dem primary only

#     6 avg_counting_hedging_ind_rep_primary: Same as avg_counting_hedging_ind but for candidates to Rep primary only

#     7 hedging_money_general_corp: The index of extensive-margin hedging is computed as the absolute difference between a corporation’s contributions to Republican and Democratic candidates in a given district and election cycle (only for general election, before the special election). This captures the extent to which a firm biases its contributions toward one party over the other. The index is constructed taking the average of this difference across corporations in the same district and cycle. See the keynote file strategies_hedging

#     8 hedging_money_dem_primary_corp: Same as hedging_money_general_corp but computed using contributions to two main candidates in Dem primary only

#     9 hedging_money_rep_primary_corp: Same as hedging_money_general_corp but computed using contributions to two main candidates in Rep primary only

#     10 hedging_money_general_ind: For contributions coming from individuals

#     11 hedging_money_dem_primary_ind: Same as hedging_money_general_ind but computed using contributions to two main candidates in Dem primary only

#     12 hedging_money_rep_primary_ind: Same as hedging_money_general_ind but computed using contributions to two main candidates in Rep primary only



# Creating avg_counting_hedging_corp and avg_counting_hedging_ind
print("Creating avg_counting_hedging_corp and avg_counting_hedging_ind...")
OUTPUT_8_1_corp = OUTPUT_1[
    (OUTPUT_1['election.type'] == 'G') & 
    (OUTPUT_1['contributor.type'] == 'C') & 
    (OUTPUT_1['later_than_special'] != 1)
    ].groupby(['district', 'cycle', 'party']).agg(
    counting_hedging_corp = ('bonica.rid', 'nunique'),
).reset_index()

OUTPUT_8_1_corp = OUTPUT_8_1_corp.groupby(['district', 'cycle']).agg(
    avg_counting_hedging_corp = ('counting_hedging_corp', 'mean'),
).reset_index()

OUTPUT_8_1_ind = OUTPUT_1[
    (OUTPUT_1['election.type'] == 'G') & 
    (OUTPUT_1['contributor.type'] == 'I') & 
    (OUTPUT_1['later_than_special'] != 1)
    ].groupby(['district', 'cycle', 'party']).agg(
    counting_hedging_ind = ('bonica.rid', 'nunique'),
).reset_index()

OUTPUT_8_1_ind = OUTPUT_8_1_ind.groupby(['district', 'cycle']).agg(
    avg_counting_hedging_ind = ('counting_hedging_ind', 'mean'),
).reset_index()

OUTPUT_8_1_smallind = OUTPUT_1[
    (OUTPUT_1['election.type'] == 'G') & 
    (OUTPUT_1['contributor.type'] == 'I') & 
    (OUTPUT_1['later_than_special'] != 1) &
    (OUTPUT_1['amount'] < 200)
    ].groupby(['district', 'cycle', 'party']).agg(
    counting_hedging_smallind = ('bonica.rid', 'nunique'),
).reset_index()

OUTPUT_8_1_smallind = OUTPUT_8_1_smallind.groupby(['district', 'cycle']).agg(
    avg_counting_hedging_smallind = ('counting_hedging_smallind', 'mean'),
).reset_index()

        
# Creating avg_counting_hedging_corp_dem_primary and avg_counting_hedging_corp_rep_primary
print("Creating avg_counting_hedging_corp_dem_primary and avg_counting_hedging_corp_rep_primary...")
OUTPUT_8_2_corp = OUTPUT_1[
    (OUTPUT_1['election.type'] == 'P') & 
    (OUTPUT_1['contributor.type'] == 'C') &
    (OUTPUT_1['later_than_special'] != 1)
    ].groupby(['district', 'cycle', 'party']).agg(
    counting_hedging_corp_primary = ('bonica.rid', 'nunique'),
).reset_index()

OUTPUT_8_2_corp = OUTPUT_8_2_corp.pivot_table(
    index=['district', 'cycle'],
    columns='party',
    values='counting_hedging_corp_primary',
    fill_value=0
).reset_index().rename(columns = {100.0 : "counting_hedging_corp_dem_primary", 
                                  200.0 : "counting_hedging_corp_rep_primary"})
OUTPUT_8_2_corp = OUTPUT_8_2_corp[['district', 'cycle', 'counting_hedging_corp_dem_primary', 'counting_hedging_corp_rep_primary']] # keeping only Dems and Reps
                                  
OUTPUT_8_2_corp = OUTPUT_8_2_corp.groupby(['district', 'cycle']).agg(
    avg_counting_hedging_corp_dem_primary = ('counting_hedging_corp_dem_primary', 'mean'),
    avg_counting_hedging_corp_rep_primary = ('counting_hedging_corp_rep_primary', 'mean'),
).reset_index()

# Creating avg_counting_hedging_ind_dem_primary and avg_counting_hedging_ind_rep_primary (Individual)
print("Creating avg_counting_hedging_ind_dem_primary and avg_counting_hedging_ind_rep_primary...")
OUTPUT_8_2_ind = OUTPUT_1[
    (OUTPUT_1['election.type'] == 'P') & 
    (OUTPUT_1['contributor.type'] == 'I') &
    (OUTPUT_1['later_than_special'] != 1) 
    ].groupby(['district', 'cycle', 'party']).agg(
    counting_hedging_ind_primary = ('bonica.rid', 'nunique'),
).reset_index()

OUTPUT_8_2_ind = OUTPUT_8_2_ind.pivot_table(
    index=['district', 'cycle'],
    columns='party',
    values='counting_hedging_ind_primary',
    fill_value=0
).reset_index().rename(columns = {100.0 : "counting_hedging_ind_dem_primary", 
                                  200.0 : "counting_hedging_ind_rep_primary"})
OUTPUT_8_2_ind = OUTPUT_8_2_ind[['district', 'cycle', 'counting_hedging_ind_dem_primary', 'counting_hedging_ind_rep_primary']] # keeping only Dems and Reps
                                  
OUTPUT_8_2_ind = OUTPUT_8_2_ind.groupby(['district', 'cycle']).agg(
    avg_counting_hedging_ind_dem_primary = ('counting_hedging_ind_dem_primary', 'mean'),
    avg_counting_hedging_ind_rep_primary = ('counting_hedging_ind_rep_primary', 'mean'),
).reset_index()

# Creating avg_counting_hedging_smallind_dem_primary and avg_counting_hedging_smallind_rep_primary (Small Individual donors)
print("Creating avg_counting_hedging_smallind_dem_primary and avg_counting_hedging_smallind_rep_primary...")
OUTPUT_8_2_smallind = OUTPUT_1[
    (OUTPUT_1['election.type'] == 'P') & 
    (OUTPUT_1['contributor.type'] == 'I') &
    (OUTPUT_1['later_than_special'] != 1) &
    (OUTPUT_1['amount'] < 200)
    ].groupby(['district', 'cycle', 'party']).agg(
    counting_hedging_smallind_primary = ('bonica.rid', 'nunique'),
).reset_index()

OUTPUT_8_2_smallind = OUTPUT_8_2_smallind.pivot_table(
    index=['district', 'cycle'],
    columns='party',
    values='counting_hedging_smallind_primary',
    fill_value=0
).reset_index().rename(columns = {100.0 : "counting_hedging_smallind_dem_primary", 
                                  200.0 : "counting_hedging_smallind_rep_primary"})
OUTPUT_8_2_smallind = OUTPUT_8_2_smallind[['district', 'cycle', 'counting_hedging_smallind_dem_primary', 'counting_hedging_smallind_rep_primary']] # keeping only Dems and Reps
                                  
OUTPUT_8_2_smallind = OUTPUT_8_2_smallind.groupby(['district', 'cycle']).agg(
    avg_counting_hedging_smallind_dem_primary = ('counting_hedging_smallind_dem_primary', 'mean'),
    avg_counting_hedging_smallind_rep_primary = ('counting_hedging_smallind_rep_primary', 'mean'),
).reset_index()


# Creating hedging_money_general_corp 
print("Creating hedging_money_general_corp and hedging_money_general_ind...")

OUTPUT_8_3_corp = OUTPUT_1[
    (OUTPUT_1['election.type'] == 'G') & 
    (OUTPUT_1['contributor.type'] == 'C') & 
    (OUTPUT_1['later_than_special'] != 1)
    ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
    total_amount = ('amount', 'sum'),
).reset_index()

OUTPUT_8_3_corp = OUTPUT_8_3_corp.pivot_table(
    index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
    columns='party',
    values='total_amount',
    fill_value=0
).reset_index().rename(columns = {100.0 : "total_amount_dem", 
                                  200.0 : "total_amount_rep"})
OUTPUT_8_3_corp = OUTPUT_8_3_corp[['district', 'cycle', 'bonica.cid', 'contributor.name', 'total_amount_dem', 'total_amount_rep']]

OUTPUT_8_3_corp['hedging'] = abs(OUTPUT_8_3_corp['total_amount_dem'] - OUTPUT_8_3_corp['total_amount_rep'])
# OUTPUT_8_3_corp['n_hedging'] = OUTPUT_8_3_corp['hedging'] / (OUTPUT_8_3_corp['total_amount_dem'] + OUTPUT_8_3_corp['total_amount_rep'])

OUTPUT_8_3_corp = OUTPUT_8_3_corp.groupby(['district', 'cycle']).agg(
    hedging_money_general_corp = ('hedging', 'mean'),
    # hedging_money_general_normalized_corp = ('n_hedging', 'mean')
).reset_index()


OUTPUT_8_3_ind = OUTPUT_1[
    (OUTPUT_1['election.type'] == 'G') & 
    (OUTPUT_1['contributor.type'] == 'I') & 
    (OUTPUT_1['later_than_special'] != 1)
    ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
    total_amount = ('amount', 'sum'),
).reset_index()

OUTPUT_8_3_ind = OUTPUT_8_3_ind.pivot_table(
    index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
    columns='party',
    values='total_amount',
    fill_value=0
).reset_index().rename(columns = {100.0 : "total_amount_dem", 
                                  200.0 : "total_amount_rep"})
OUTPUT_8_3_ind = OUTPUT_8_3_ind[['district', 'cycle', 'bonica.cid', 'contributor.name', 'total_amount_dem', 'total_amount_rep']]

OUTPUT_8_3_ind['hedging'] = abs(OUTPUT_8_3_ind['total_amount_dem'] - OUTPUT_8_3_ind['total_amount_rep'])

OUTPUT_8_3_ind = OUTPUT_8_3_ind.groupby(['district', 'cycle']).agg(
    hedging_money_general_ind = ('hedging', 'mean'),
).reset_index()

OUTPUT_8_3_smallind = OUTPUT_1[
    (OUTPUT_1['election.type'] == 'G') & 
    (OUTPUT_1['contributor.type'] == 'I') & 
    (OUTPUT_1['later_than_special'] != 1) &
    (OUTPUT_1['amount'] < 200)
    ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
    total_amount = ('amount', 'sum'),
).reset_index()

OUTPUT_8_3_smallind = OUTPUT_8_3_smallind.pivot_table(
    index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
    columns='party',
    values='total_amount',
    fill_value=0
).reset_index().rename(columns = {100.0 : "total_amount_dem", 
                                  200.0 : "total_amount_rep"})
OUTPUT_8_3_smallind = OUTPUT_8_3_smallind[['district', 'cycle', 'bonica.cid', 'contributor.name', 'total_amount_dem', 'total_amount_rep']]

OUTPUT_8_3_smallind['hedging'] = abs(OUTPUT_8_3_smallind['total_amount_dem'] - OUTPUT_8_3_smallind['total_amount_rep'])

OUTPUT_8_3_smallind = OUTPUT_8_3_smallind.groupby(['district', 'cycle']).agg(
    hedging_money_general_smallind = ('hedging', 'mean'),
).reset_index()


# Creating hedging_money_dem_primary_corp and hedging_money_rep_primary_corp
print("Creating hedging_money_dem_primary_corp, hedging_money_rep_primary_corp, hedging_money_dem_primary_ind, and hedging_money_rep_primary_ind...")

OUTPUT_8_4_corp = OUTPUT_1[
    (OUTPUT_1['election.type'] == 'P') & 
    (OUTPUT_1['contributor.type'] == 'C') & 
    (OUTPUT_1['later_than_special'] != 1)
    ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party', 'bonica.rid']).agg(
    total_amount = ('amount', 'sum'),
).reset_index()
        
OUTPUT_8_4_ind = OUTPUT_1[
    (OUTPUT_1['election.type'] == 'P') & 
    (OUTPUT_1['contributor.type'] == 'I') & 
    (OUTPUT_1['later_than_special'] != 1)
    ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party', 'bonica.rid']).agg(
    total_amount = ('amount', 'sum'),
).reset_index()

OUTPUT_8_4_smallind = OUTPUT_1[
    (OUTPUT_1['election.type'] == 'P') & 
    (OUTPUT_1['contributor.type'] == 'I') & 
    (OUTPUT_1['later_than_special'] != 1) &
    (OUTPUT_1['amount'] < 200)
    ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party', 'bonica.rid']).agg(
    total_amount = ('amount', 'sum'),
).reset_index()

print("Nr of companies that are giving to more than one candidate in the primaries", 
      OUTPUT_8_4_corp[OUTPUT_8_4_corp.duplicated(subset=['district', 'cycle', 'bonica.cid'], keep=False)]['bonica.cid'].nunique())

print("Nr of companies that are giving to more than one candidate in the primaries in the same party", 
      OUTPUT_8_4_corp[OUTPUT_8_4_corp.duplicated(subset=['district', 'cycle', 'bonica.cid', 'party'], keep=False)]['bonica.cid'].nunique())

print("Nr of individuals that are giving to more than one candidate in the primaries", 
      OUTPUT_8_4_ind[OUTPUT_8_4_ind.duplicated(subset=['district', 'cycle', 'bonica.cid'], keep=False)]['bonica.cid'].nunique())

print("Nr of individuals that are giving to more than one candidate in the primaries in the same party", 
      OUTPUT_8_4_ind[OUTPUT_8_4_ind.duplicated(subset=['district', 'cycle', 'bonica.cid', 'party'], keep=False)]['bonica.cid'].nunique())

print("Nr of small individuals that are giving to more than one candidate in the primaries", 
      OUTPUT_8_4_smallind[OUTPUT_8_4_smallind.duplicated(subset=['district', 'cycle', 'bonica.cid'], keep=False)]['bonica.cid'].nunique())

print("Nr of small individuals that are giving to more than one candidate in the primaries in the same party", 
      OUTPUT_8_4_smallind[OUTPUT_8_4_smallind.duplicated(subset=['district', 'cycle', 'bonica.cid', 'party'], keep=False)]['bonica.cid'].nunique())


# Getting duplicate rows, so we understand what parties are contributing to the same party's candidates in the primaries
OUTPUT_8_4_corp = OUTPUT_8_4_corp[OUTPUT_8_4_corp.duplicated(subset=['district', 'cycle', 'bonica.cid', 'party'], keep=False)]
OUTPUT_8_4_ind = OUTPUT_8_4_ind[OUTPUT_8_4_ind.duplicated(subset=['district', 'cycle', 'bonica.cid', 'party'], keep=False)]
OUTPUT_8_4_smallind = OUTPUT_8_4_smallind[OUTPUT_8_4_smallind.duplicated(subset=['district', 'cycle', 'bonica.cid', 'party'], keep=False)]

# We will apply a function for both Dems and Reps separately
# We will apply a function for both Dems and Reps separately
def calculate_party_hedging(data, party_code, party_name, contributor_type):
    """
    Calculate hedging metrics for a specific party's primary candidates.
    
    Parameters:
    -----------
    data : DataFrame
        The source dataframe containing contribution data
    party_code : int
        The code for the party (100 for Democrats, 200 for Republicans)
    party_name : str
        The name to use in output column names (e.g., 'dem' or 'rep')
    contributor_type : str
        The type of contributor ('corp', 'ind', or 'smallind')
        
    Returns:
    --------
    DataFrame
        A dataframe with hedging metrics by district and cycle
    """
    # Filter for specified party
    party_data = data[data['party'] == party_code].copy()
    
    # Sort the data
    party_data = party_data.sort_values(
        ['district', 'cycle', 'bonica.cid', 'party', 'total_amount'], 
        ascending=[True, True, True, True, False]
    )
    
    # Create ranking (useful for pivot)
    party_data['rank'] = party_data.groupby(
        ['district', 'cycle', 'bonica.cid', 'party']
    )['bonica.rid'].transform(lambda x: pd.Series(range(1, len(x) + 1)))
    
    # Select top two candidates
    party_data = party_data.groupby(['district', 'cycle', 'bonica.cid', 'party']).apply(
        lambda x: x.nlargest(2, 'total_amount')
    ).reset_index(drop=True)
    
    # Recompute rank after filtering
    party_data['rank'] = party_data.groupby(
        ['district', 'cycle', 'bonica.cid', 'party']
    ).cumcount() + 1
    
    # Pivot to get contributions to top candidates
    party_data_pivot = party_data.pivot_table(
        index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
        columns='rank',
        values='total_amount',
        fill_value=0
    ).reset_index()
    
    # Rename columns based on party name
    party_data_pivot = party_data_pivot.rename(columns={
        1.0: f"total_amount_{party_name}_first", 
        2.0: f"total_amount_{party_name}_second"
    })
    
    # Calculate hedging metrics
    party_data_pivot['hedging'] = abs(
        party_data_pivot[f"total_amount_{party_name}_first"] - 
        party_data_pivot[f"total_amount_{party_name}_second"]
    )
    
    # Naming variable based on contribution type
    column_name = f"hedging_money_{party_name}_primary_{contributor_type}"

    # Aggregate by district and cycle
    result = party_data_pivot.groupby(['district', 'cycle']).agg(
        **{
            column_name: ('hedging', 'mean')
        }
    ).reset_index()
    
    return result

print("Processing hedging metrics for Dems and Reps separately (corporate)...")
dem_results_corp = calculate_party_hedging(OUTPUT_8_4_corp, 100, 'dem', 'corp')
rep_results_corp = calculate_party_hedging(OUTPUT_8_4_corp, 200, 'rep', 'corp')

print("Processing hedging metrics for Dems and Reps separately (individual)...")
dem_results_ind = calculate_party_hedging(OUTPUT_8_4_ind, 100, 'dem', 'ind')
rep_results_ind = calculate_party_hedging(OUTPUT_8_4_ind, 200, 'rep', 'ind')

print("Processing hedging metrics for Dems and Reps separately (small individual)...")
dem_results_smallind = calculate_party_hedging(OUTPUT_8_4_smallind, 100, 'dem', 'smallind')
rep_results_smallind = calculate_party_hedging(OUTPUT_8_4_smallind, 200, 'rep', 'smallind')


OUTPUT_8_4_corp = pd.merge(
    dem_results_corp,
    rep_results_corp,
    on=['district', 'cycle'],
    how='outer'
)

OUTPUT_8_4_ind = pd.merge(
    dem_results_ind,
    rep_results_ind,
    on=['district', 'cycle'],
    how='outer'
)

OUTPUT_8_4_smallind = pd.merge(
    dem_results_smallind,
    rep_results_smallind,
    on=['district', 'cycle'],
    how='outer'
)
    

OUTPUT_8_corp = OUTPUT_8_1_corp.copy()
for i in range(2, 5):
    print(f"Merging OUTPUT_8_corp with OUTPUT_8_{i}_corp")
    df_name = f"OUTPUT_8_{i}_corp"
    df = globals()[df_name]
    
    # Merge with the current result
    OUTPUT_8_corp = pd.merge(
        OUTPUT_8_corp,
        df,
        on=['cycle', 'district'],
        how='outer'
    )
    if i == range(2, 5)[-1]:
        print("Merge complete!")
    else:
        continue
    
OUTPUT_8_ind = OUTPUT_8_1_ind.copy()
for i in range(2, 5):
    print(f"Merging OUTPUT_8_ind with OUTPUT_8_{i}_ind")
    df_name = f"OUTPUT_8_{i}_ind"
    df = globals()[df_name]
    
    # Merge with the current result
    OUTPUT_8_ind = pd.merge(
        OUTPUT_8_ind,
        df,
        on=['cycle', 'district'],
        how='outer'
    )
    if i == range(2, 5)[-1]:
        print("Merge complete!")
    else:
        continue

OUTPUT_8_smallind = OUTPUT_8_1_smallind.copy()
for i in range(2, 5):
    print(f"Merging OUTPUT_8_smallind with OUTPUT_8_{i}_smallind")
    df_name = f"OUTPUT_8_{i}_smallind"
    df = globals()[df_name]
    
    # Merge with the current result
    OUTPUT_8_smallind = pd.merge(
        OUTPUT_8_smallind,
        df,
        on=['cycle', 'district'],
        how='outer'
    )
    if i == range(2, 5)[-1]:
        print("Merge complete!")
    else:
        continue


OUTPUT_8 = pd.merge(
    OUTPUT_8_corp,
    OUTPUT_8_ind,
    on=['cycle', 'district'],
    how = 'outer'
)

OUTPUT_8 = pd.merge(
    OUTPUT_8,
    OUTPUT_8_smallind,
    on=['cycle', 'district'],
    how = 'outer'
)
# # Merging with districts that have a creation year or discontinuation year or both after 1980
# print("Merging OUTPUT_8 with new_districts_df")
# OUTPUT_8 = pd.merge(
#     OUTPUT_8,
#     new_districts_df,
#     on=['district', 'cycle'],
#     how='left'
#     )


# # Missing values of real_data are replaced with 1, because they are real data (district has existed prior to 1980 and has existed until 2024)
# OUTPUT_8.loc[OUTPUT_8['real_data'].isna(), 'real_data'] = 1
# # Re-ordering data
# OUTPUT_8 = OUTPUT_8.sort_values(by=['district', 'cycle'], ascending=[True, True])
# columns_list = ['district', 'cycle', 'real_data']
# cols = columns_list + [
#     col for col in OUTPUT_8.columns if col not in columns_list]
# OUTPUT_8 = OUTPUT_8[cols]

# # Merging with cartesian product
# print("Merging OUTPUT_8 with OUTPUT_0")
# OUTPUT_8 = pd.merge(
#     OUTPUT_0,
#     OUTPUT_8,
#     on=['cycle', 'district'],
#     how='outer'
#     )
# # Missing values of real_data are replaced with 0, these don't exist but are fake rows to balance our dataset
# OUTPUT_8.loc[OUTPUT_8['real_data'].isna(), 'real_data'] = 0
# # Missing values of other columns are replaced with 0
# mask = OUTPUT_8['real_data'] == 1
# data_columns = [col for col in OUTPUT_8.columns if col not in ['district', 'cycle', 'real_data']]
# OUTPUT_8.loc[mask, data_columns] = OUTPUT_8.loc[mask, data_columns].fillna(0)

# print("Merge complete!")


# # NOTE: We have already created real_data dummy, we get this information from OUTPUT_2 (the 'universe' of district-cycles) and avoid creating a new one for district-cycle individual contributions
# OUTPUT_8 = pd.merge(
#     OUTPUT_2[['district', 'cycle', 'real_data']], # we only need this information from OUTPUT_2
#     OUTPUT_8,
#     on=['cycle', 'district'],
#     how='outer'
#     )

# # Missing values of other columns are replaced with 0
# mask = OUTPUT_3['real_data'] == 1
# data_columns = [col for col in OUTPUT_3.columns if col not in ['district', 'cycle', 'real_data']]
# OUTPUT_3.loc[mask, data_columns] = OUTPUT_3.loc[mask, data_columns].fillna(0)

# # Dataset is balanced and there is no need to merge with OUTPUT_0!

# OUTPUT_3.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_3.csv"), index=False)

# OUTPUT_3 = OUTPUT_3.drop(columns = 'real_data') # no duplicate columns when merging with OUTPUT_2
    


# OUTPUT_8 = pd.merge(
#     OUTPUT_0,
#     OUTPUT_8,
#     on = ['district', 'cycle'],
#     how = 'outer'
#     )


# NOTE: We have already created real_data dummy, we get this information from OUTPUT_2 (the 'universe' of district-cycles) and avoid creating a new one for district-cycle individual contributions
OUTPUT_8 = pd.merge(
    OUTPUT_2[['district', 'cycle', 'real_data']], # we only need this information from OUTPUT_2
    OUTPUT_8,
    on=['cycle', 'district'],
    how='outer'
    )

# Checking...
print("Total length of dataset OUTPUT_8:", len(OUTPUT_8))
print("  - real_data == 1 and (avg_counting_hedging_corp_dem_primary != Nan or avg_counting_hedging_corp_rep_primary != Nan):", len(OUTPUT_8[(OUTPUT_8['real_data'] == 1) & ((~OUTPUT_8['avg_counting_hedging_corp_dem_primary'].isna()) | (~OUTPUT_8['avg_counting_hedging_corp_rep_primary'].isna()))]))
print("  - real_data == 1 and (avg_counting_hedging_corp_dem_primary == Nan or avg_counting_hedging_corp_rep_primary == Nan):", len(OUTPUT_8[(OUTPUT_8['real_data'] == 1) & ((OUTPUT_8['avg_counting_hedging_corp_dem_primary'].isna()) | (OUTPUT_8['avg_counting_hedging_corp_rep_primary'].isna()))]))
print("  - real_data == 0 and (avg_counting_hedging_corp_dem_primary != Nan or avg_counting_hedging_corp_rep_primary != Nan):", len(OUTPUT_8[(OUTPUT_8['real_data'] == 0) & ((~OUTPUT_8['avg_counting_hedging_corp_dem_primary'].isna()) | (~OUTPUT_8['avg_counting_hedging_corp_rep_primary'].isna()))]))
print("  - real_data == 0 and (avg_counting_hedging_corp_dem_primary != Nan or avg_counting_hedging_corp_rep_primary != Nan):", len(OUTPUT_8[(OUTPUT_8['real_data'] == 0) & ((OUTPUT_8['avg_counting_hedging_corp_dem_primary'].isna()) | (OUTPUT_8['avg_counting_hedging_corp_rep_primary'].isna()))]))


# test = OUTPUT_8[(OUTPUT_8['real_data'] == 0) & (~OUTPUT_8['total_amount_ind'].isna())]

# test.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_8_problem.csv"), index=False)

# Missing values of other columns are replaced with 0
# OUTPUT_8_processed = OUTPUT_8.fillna(0)
OUTPUT_8 = OUTPUT_8.fillna(0)
OUTPUT_8.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_8.csv"), index=False)
OUTPUT_8 = OUTPUT_8.drop(columns = 'real_data') # no duplicate columns when merging with OUTPUT_2

#%%

## OUTPUT_9: 
print("Processing OUTPUT_9...")

# each variable has cfscore and cfscore_dyn!

#     1 cfscore_mean_prim_dem: Average CFscore of candidates in the Dem primary (this tells us how much leftist the Dems are in this primary, computed using recipient.cfscores.dyn) 

#     2 cfscore_mean_prim_rep: Average CFscore of candidates in the Rep primary (same as above) 

#     3 cfscore_prim_abs_diff: Absolute value of (CFscore of Dem candidate - CFscore of Rep candidate). This should capture difference in ideology between the two parties in the district. 

#     4 cfscore_gen_dem: CFscore of Dem candidate in general

#     5 cfscore_gen_rep: CFscore of Rep candidate in general

#     6 cfscore_gen_abs_diff: Absolute value of (CFscore of Dem candidate - CFscore of Rep candidate). This should capture difference in ideology between the two parties in the district. 

#     7 cfscore_mean_contrib: Average CFscore of all donors

#     8 cfscore_mean_contrib_dem: Average CFscore of all donors for Democrats

#     9 cfscore_mean_contrib_rep: Average CFscore of all donors for Republicans


# Here we only use rows that were merged in merged_df_2 by filtering out NA values, so that we don't get repeated values for candidates
OUTPUT_9_0_rec = OUTPUT_1[~OUTPUT_1['bonica.rid'].isna()] # suffix: _1, for recipients
OUTPUT_9_0_con = OUTPUT_1[~OUTPUT_1['bonica.cid'].isna()] # suffix: _2, for contributors / donors

# Dropping duplicate rows 
OUTPUT_9_0_rec = OUTPUT_9_0_rec.drop_duplicates(
    subset=['bonica.rid', 'cycle'],
    keep='first' 
)

OUTPUT_9_0_con = OUTPUT_9_0_con.drop_duplicates(
    subset=['bonica.cid', 'cycle'],
    keep='first' 
)

# cfscore_mean_prim_dem
print("Processing cfscore_mean_prim_dem...")
OUTPUT_9_1 = OUTPUT_9_0_rec[
    (OUTPUT_9_0_rec['election.type'] == 'P') &   # primary only
    (OUTPUT_9_0_rec['party'] == 100)             # dems
    ].groupby(['district', 'cycle']).agg(
    count_cfscore=('recipient.cfscore', 'count'),
    count_cfscore_dyn=('recipient.cfscore.dyn', 'count'),
    cfscore_mean_prim_dem=('recipient.cfscore', 'mean'),
    cfscore_dyn_mean_prim_dem=('recipient.cfscore.dyn', 'mean')
).reset_index()

# Replacing missing values of cfscore_dyn with cf_score
OUTPUT_9_1['cfscore_dyn_mean_prim_dem_final'] = np.where(
    OUTPUT_9_1['cfscore_dyn_mean_prim_dem'].isna(),  # Condition: where values are missing
    OUTPUT_9_1['cfscore_mean_prim_dem'],             # Value if condition is True: use cfscore_mean_prim_dem
    OUTPUT_9_1['cfscore_dyn_mean_prim_dem']          # Value if condition is False: keep original value
)

OUTPUT_9_1 = OUTPUT_9_1[['district', 'cycle', 'cfscore_dyn_mean_prim_dem_final']].rename(columns = {'cfscore_dyn_mean_prim_dem_final': 'cfscore_mean_prim_dem'})


# cfscore_mean_prim_rep
print("Processing cfscore_mean_prim_rep...")
OUTPUT_9_2 = OUTPUT_9_0_rec[
    (OUTPUT_9_0_rec['election.type'] == 'P') &   # primary only
    (OUTPUT_9_0_rec['party'] == 200)             # reps
    ].groupby(['district', 'cycle']).agg(
    count_cfscore=('recipient.cfscore', 'count'),
    count_cfscore_dyn=('recipient.cfscore.dyn', 'count'),
    cfscore_mean_prim_rep=('recipient.cfscore', 'mean'),
    cfscore_dyn_mean_prim_rep=('recipient.cfscore.dyn', 'mean')
).reset_index()

OUTPUT_9_2['cfscore_dyn_mean_prim_rep_final'] = np.where(
    OUTPUT_9_2['cfscore_dyn_mean_prim_rep'].isna(),  
    OUTPUT_9_2['cfscore_mean_prim_rep'],             
    OUTPUT_9_2['cfscore_dyn_mean_prim_rep']          
)

OUTPUT_9_2 = OUTPUT_9_2[['district', 'cycle', 'cfscore_dyn_mean_prim_rep_final']].rename(columns = {'cfscore_dyn_mean_prim_rep_final': 'cfscore_mean_prim_rep'})

# cfscore_prim_abs_diff
print("Processing cfscore_prim_abs_diff...")
OUTPUT_9_3 = pd.merge(
    OUTPUT_9_1,
    OUTPUT_9_2,
    on = ['district', 'cycle'],
    how = 'outer'
    )
OUTPUT_9_3['cfscore_prim_abs_diff'] = abs(OUTPUT_9_3['cfscore_mean_prim_dem'] - OUTPUT_9_3['cfscore_mean_prim_rep'])


# cfscore_gen_dem
print("Processing cfscore_gen_dem...")
groupby_vars = ['district', 'cycle', 'bonica.rid']
OUTPUT_9_4 = OUTPUT_9_0_rec[
    (OUTPUT_9_0_rec['election.type'] == 'G') &   # primary only
    (OUTPUT_9_0_rec['party'] == 100)             # dems
    ].groupby(groupby_vars).agg(
    count_cfscore=('recipient.cfscore', 'count'),
    count_cfscore_dyn=('recipient.cfscore.dyn', 'count'),
    cfscore_gen_dem=('recipient.cfscore', 'mean'),
    cfscore_dyn_gen_dem=('recipient.cfscore.dyn', 'mean'),
    gen_vote_pct=('gen.vote.pct', 'sum'),
).reset_index()
        
OUTPUT_9_4['cfscore_dyn_gen_dem_final'] = np.where(
    OUTPUT_9_4['cfscore_dyn_gen_dem'].isna(),  
    OUTPUT_9_4['cfscore_gen_dem'],             
    OUTPUT_9_4['cfscore_dyn_gen_dem']          
)

OUTPUT_9_4 = OUTPUT_9_4[groupby_vars + ['cfscore_dyn_gen_dem_final', 'count_cfscore', 'gen_vote_pct']].rename(columns = {'cfscore_dyn_gen_dem_final': 'cfscore_gen_dem'})

OUTPUT_9_4 = OUTPUT_9_4.sort_values( 
    # Selecting candidates with most votes for each district-cycle-party (if duplicates)
    by=['district', 'cycle', 'gen_vote_pct'], 
    ascending = [True, True, False])
OUTPUT_9_4 = OUTPUT_9_4.drop_duplicates(subset=['district', 'cycle'])
OUTPUT_9_4 = OUTPUT_9_4.drop(columns = ['bonica.rid', 'count_cfscore', 'gen_vote_pct'])

# cfscore_gen_rep
print("Processing cfscore_gen_rep...")
OUTPUT_9_5 = OUTPUT_9_0_rec[
    (OUTPUT_9_0_rec['election.type'] == 'G') &   # primary only
    (OUTPUT_9_0_rec['party'] == 200)             # reps
    ].groupby(groupby_vars).agg(
    count_cfscore=('recipient.cfscore', 'count'),
    count_cfscore_dyn=('recipient.cfscore.dyn', 'count'),
    cfscore_gen_rep=('recipient.cfscore', 'mean'),
    cfscore_dyn_gen_rep=('recipient.cfscore.dyn', 'mean'),
    gen_vote_pct=('gen.vote.pct', 'sum'),
).reset_index()
        
OUTPUT_9_5['cfscore_dyn_gen_rep_final'] = np.where(
    OUTPUT_9_5['cfscore_dyn_gen_rep'].isna(),  
    OUTPUT_9_5['cfscore_gen_rep'],             
    OUTPUT_9_5['cfscore_dyn_gen_rep']          
)

OUTPUT_9_5 = OUTPUT_9_5[groupby_vars + ['cfscore_dyn_gen_rep_final', 'count_cfscore', 'gen_vote_pct']].rename(columns = {'cfscore_dyn_gen_rep_final': 'cfscore_gen_rep'})
        
OUTPUT_9_5 = OUTPUT_9_5.sort_values( 
    # Selecting candidates with most votes for each district-cycle-party (if duplicates)
    by=['district', 'cycle', 'gen_vote_pct'], 
    ascending = [True, True, False])
OUTPUT_9_5 = OUTPUT_9_5.drop_duplicates(subset=['district', 'cycle'])
OUTPUT_9_5 = OUTPUT_9_5.drop(columns = ['bonica.rid', 'count_cfscore', 'gen_vote_pct'])


# cfscore_gen_abs_diff
print("Processing cfscore_gen_abs_diff...")
OUTPUT_9_6 = pd.merge(
    OUTPUT_9_4,
    OUTPUT_9_5,
    on = ['district', 'cycle'],
    how = 'outer'
    )
OUTPUT_9_6['cfscore_gen_abs_diff'] = abs(OUTPUT_9_6['cfscore_gen_dem'] - OUTPUT_9_6['cfscore_gen_rep'])

# cfscore_mean_contrib
print("Processing cfscore_mean_contrib...")
OUTPUT_9_7 = OUTPUT_9_0_con.groupby(['district', 'cycle']).agg(
    count_cfscore=('contributor.cfscore', 'count'),
    count_cfscore_dyn=('contributor.cfscore', 'count'),
    cfscore_mean_contrib=('contributor.cfscore', 'mean'),
    cfscore_dyn_mean_contrib=('contributor.cfscore', 'mean')
).reset_index()

OUTPUT_9_7['cfscore_dyn_mean_contrib_final'] = np.where(
    OUTPUT_9_7['cfscore_dyn_mean_contrib'].isna(),  
    OUTPUT_9_7['cfscore_mean_contrib'],             
    OUTPUT_9_7['cfscore_dyn_mean_contrib']          
)

OUTPUT_9_7 = OUTPUT_9_7[['district', 'cycle', 'cfscore_dyn_mean_contrib_final', 'count_cfscore']].rename(columns = {'cfscore_dyn_mean_contrib_final': 'cfscore_mean_contrib'})


# cfscore_mean_contrib_dem
print("Processing cfscore_mean_contrib_dem...")
OUTPUT_9_8 = OUTPUT_9_0_con[
    (OUTPUT_9_0_con['party'] == 100)
    ].groupby(['district', 'cycle']).agg(
    count_cfscore=('contributor.cfscore', 'count'),
    count_cfscore_dyn=('contributor.cfscore', 'count'),
    cfscore_mean_contrib_dem=('contributor.cfscore', 'mean'),
    cfscore_dyn_mean_contrib_dem=('contributor.cfscore', 'mean')
).reset_index()

OUTPUT_9_8['cfscore_dyn_mean_contrib_dem_final'] = np.where(
    OUTPUT_9_8['cfscore_dyn_mean_contrib_dem'].isna(),  
    OUTPUT_9_8['cfscore_mean_contrib_dem'],             
    OUTPUT_9_8['cfscore_dyn_mean_contrib_dem']          
)

OUTPUT_9_8 = OUTPUT_9_8[['district', 'cycle', 'cfscore_dyn_mean_contrib_dem_final', 'count_cfscore']].rename(columns = {'cfscore_dyn_mean_contrib_dem_final': 'cfscore_mean_contrib_dem'})

# cfscore_mean_contrib_rep
print("Processing cfscore_mean_contrib_rep...")
OUTPUT_9_9 = OUTPUT_9_0_con[
    (OUTPUT_9_0_con['party'] == 200)
    ].groupby(['district', 'cycle']).agg(
    count_cfscore=('contributor.cfscore', 'count'),
    count_cfscore_dyn=('contributor.cfscore', 'count'),
    cfscore_mean_contrib_rep=('contributor.cfscore', 'mean'),
    cfscore_dyn_mean_contrib_rep=('contributor.cfscore', 'mean')
).reset_index()


OUTPUT_9_9['cfscore_dyn_mean_contrib_rep_final'] = np.where(
    OUTPUT_9_9['cfscore_dyn_mean_contrib_rep'].isna(),  
    OUTPUT_9_9['cfscore_mean_contrib_rep'],             
    OUTPUT_9_9['cfscore_dyn_mean_contrib_rep']          
)

OUTPUT_9_9 = OUTPUT_9_9[['district', 'cycle', 'cfscore_dyn_mean_contrib_rep_final', 'count_cfscore']].rename(columns = {'cfscore_dyn_mean_contrib_rep_final': 'cfscore_mean_contrib_rep'})


# Cleaning
OUTPUT_9_1 = OUTPUT_9_1[['district', 'cycle', 'cfscore_mean_prim_dem']]
OUTPUT_9_2 = OUTPUT_9_2[['district', 'cycle', 'cfscore_mean_prim_rep']]
OUTPUT_9_3 = OUTPUT_9_3[['district', 'cycle', 'cfscore_prim_abs_diff']]
OUTPUT_9_4 = OUTPUT_9_4[['district', 'cycle', 'cfscore_gen_dem']]
OUTPUT_9_5 = OUTPUT_9_5[['district', 'cycle', 'cfscore_gen_rep']]
OUTPUT_9_6 = OUTPUT_9_6[['district', 'cycle', 'cfscore_gen_abs_diff']]
OUTPUT_9_7 = OUTPUT_9_7[['district', 'cycle', 'cfscore_mean_contrib']]
OUTPUT_9_8 = OUTPUT_9_8[['district', 'cycle', 'cfscore_mean_contrib_dem']]
OUTPUT_9_9 = OUTPUT_9_9[['district', 'cycle', 'cfscore_mean_contrib_rep']]


# Merging all outputs
print("Merging datasets to OUTPUT_9...")

OUTPUT_9 = OUTPUT_9_1.copy()
for i in range(2, 10):
    print(f"Merging OUTPUT_9 with OUTPUT_9_{i}")
    df_name = f"OUTPUT_9_{i}"
    df = globals()[df_name]
    
    # Merge with the current result
    OUTPUT_9 = pd.merge(
        OUTPUT_9,
        df,
        on=['cycle', 'district'],
        how='outer'
    )
    if i == range(2, 10)[-1]:
        print("Merge complete!")
    
OUTPUT_9 = pd.merge(
    OUTPUT_0,
    OUTPUT_9,
    on = ['district', 'cycle'],
    how = 'outer'
    )

# We have 'recipient.cfscore' and 'recipient.cfscore.dyn', but the latter is clearly better, although has missing values


OUTPUT_9.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_9.csv"), index = False)


#%%

### OUTPUT_2_ext, OUTPUT_3_ext, OUTPUT_4_ext, and OUTPUT_8_ext

### Creating extended datasets with vars that apply the condition of filter contributions between death_date and spec_election_date
# - OUTPUT_2
# - OUTPUT_3
# - OUTPUT_4
# - OUTPUT_8

print("Processing OUTPUT_2_ext, OUTPUT_3_ext, and OUTPUT_4_ext...")

def create_treatment_filtered_outputs(input_df, output_7_df, base_output_df, output_name):
    """
    Create treatment-filtered versions of all variables in base_output_df.
    
    Parameters:
    -----------
    input_df : DataFrame
        The raw contribution data (OUTPUT_1)
    output_7_df : DataFrame
        The treatment data with treat_1, treat_2, treat_3, death_date, special_elections_date
    base_output_df : DataFrame
        The base output (OUTPUT_2, OUTPUT_3, or OUTPUT_4_1/OUTPUT_4_2)
    output_name : str
        Name for the output (e.g., 'OUTPUT_2', 'OUTPUT_3', 'OUTPUT_4_1')
    
    Returns:
    --------
    DataFrame
        Merged dataframe with original variables and _1, _2, _3 versions
    """
    
    print(f"Creating treatment-filtered versions for {output_name}...")
    
    # Start with base output
    result_df = base_output_df.copy()
    
    # Get all variable names (excluding district and cycle)
    base_vars = [col for col in base_output_df.columns if col not in ['district', 'cycle', 'real_data']]
    
    # Step 1: Copy all base variables to create _1, _2, _3 versions
    print("Creating copies of base variables...")
    new_cols = {}
    for var in base_vars:
        new_cols[f'{var}_1'] = result_df[var]
        new_cols[f'{var}_2'] = result_df[var]
        new_cols[f'{var}_3'] = result_df[var]
        
    result_df = pd.concat([result_df, pd.DataFrame(new_cols, index=result_df.index)], axis=1)
    
    # Determine suffix and filter type based on output_name
    if output_name == 'OUTPUT_2':
        filter_type = None
        amount_filter = None
        suffix = ''
    elif output_name == 'OUTPUT_3':
        filter_type = 'C'
        amount_filter = None
        suffix = '_corp'
    elif output_name == 'OUTPUT_4_1':
        filter_type = 'I'
        amount_filter = None
        suffix = '_ind'
    elif output_name == 'OUTPUT_4_2':
        filter_type = 'I'
        amount_filter = 200
        suffix = '_smallind'
        
    # Apply type and amount filters to input_df ONCE at the beginning
    input_df_filtered = input_df.copy()
    if filter_type:
        input_df_filtered = input_df_filtered[input_df_filtered['contributor.type'] == filter_type]
    if amount_filter:
        input_df_filtered = input_df_filtered[input_df_filtered['amount'] < amount_filter]

    # Step 2: For each treatment, filter contributions and recalculate for treatment == 1 rows
    for treat_num in [1, 2, 3]:
        print(f"Processing treat_{treat_num}...")
        
        # Get district-cycles where treatment == 1
        treated_districts = output_7_df[output_7_df[f'treat_{treat_num}'] == 1][['district', 'cycle', 'death_date', 'special_elections_date']]
        
        if treated_districts.empty:
            print(f"No treated districts for treat_{treat_num}")
            continue
        
        # For treat_1 with multiple death districts, only keep the first instance per district
        if treat_num == 1:
            # Filter to only include districts that are in multiple_death_districts
            multiple_death_treated = treated_districts[treated_districts['district'].isin(multiple_death_districts)]
            
            # For multiple death districts, keep only the first occurrence (earliest cycle)
            if not multiple_death_treated.empty:
                first_occurrences = multiple_death_treated.groupby('district')['cycle'].idxmin()
                multiple_death_treated_first = multiple_death_treated.loc[first_occurrences]
                
                # For single death districts, keep all
                single_death_treated = treated_districts[treated_districts['district'].isin(single_death_districts)]
                
                # Combine: first occurrence for multiple deaths + all single deaths
                treated_districts = pd.concat([single_death_treated, multiple_death_treated_first], ignore_index=True)
        
        # Merge filtered input_df with treated districts
        input_treated = pd.merge(
            input_df_filtered.drop(columns=['death_date', 'spec_election_date']),
            treated_districts,
            on=['district', 'cycle'],
            how='inner'
        )
        
        # Filter for contributions between death_date and special_elections_date
        input_treated = input_treated[
            (input_treated['date'] > input_treated['death_date']) &
            (input_treated['date'] < input_treated['special_elections_date'])
        ]
        
        # Now create all 22 aggregations for these treated district-cycles
        treat_suffix = f'_{treat_num}'
         
        # Create aggregations (same 22 as before)
        agg_dict = {}
        
        # 1: All contributions
        agg_1 = input_treated.groupby(['district', 'cycle']).agg(
            **{f'total_amount{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        agg_dict.update(agg_1.set_index(['district', 'cycle']).to_dict('index'))
        
        # 2: Before special election
        agg_2 = input_treated[input_treated['later_than_special'] != 1].groupby(['district', 'cycle']).agg(
            **{f'total_amount_without_LTS1{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_without_LTS1{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_2.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 3: No primary
        agg_3 = input_treated[input_treated['election.type'] != 'P'].groupby(['district', 'cycle']).agg(
            **{f'total_amount_no_primary{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_no_primary{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_3.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 4: No primary, before special
        agg_4 = input_treated[(input_treated['election.type'] != 'P') & (input_treated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
            **{f'total_amount_no_primary_without_LTS1{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_no_primary_without_LTS1{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_4.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 5: Primary only
        agg_5 = input_treated[input_treated['election.type'] == 'P'].groupby(['district', 'cycle']).agg(
            **{f'total_amount_primary{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_primary{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_5.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 6: Primary, before special
        agg_6 = input_treated[(input_treated['election.type'] == 'P') & (input_treated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
            **{f'total_amount_primary_without_LTS1{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_primary_without_LTS1{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_6.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 7: General only
        agg_7 = input_treated[input_treated['election.type'] == 'G'].groupby(['district', 'cycle']).agg(
            **{f'total_amount_gen{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_gen{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_7.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 8: General, before special
        agg_8 = input_treated[(input_treated['election.type'] == 'G') & (input_treated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
            **{f'total_amount_gen_without_LTS1{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_gen_without_LTS1{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_8.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 9: Special only
        agg_9 = input_treated[input_treated['election.type'] == 'S'].groupby(['district', 'cycle']).agg(
            **{f'total_amount_special{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_special{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_9.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 10: Special, before special
        agg_10 = input_treated[(input_treated['election.type'] == 'S') & (input_treated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
            **{f'total_amount_special_without_LTS1{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_special_without_LTS1{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_10.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 11: Dem general
        agg_11 = input_treated[(input_treated['party'] == 100) & (input_treated['election.type'] == 'G')].groupby(['district', 'cycle']).agg(
            **{f'total_amount_dem_gen{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_dem_gen{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_11.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 12: Dem general, before special
        agg_12 = input_treated[(input_treated['party'] == 100) & (input_treated['election.type'] == 'G') & (input_treated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
            **{f'total_amount_dem_gen_without_LTS1{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_dem_gen_without_LTS1{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_12.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 13: Dem primary
        agg_13 = input_treated[(input_treated['party'] == 100) & (input_treated['election.type'] == 'P')].groupby(['district', 'cycle']).agg(
            **{f'total_amount_dem_primary{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_dem_primary{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_13.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 14: Dem primary, before special
        agg_14 = input_treated[(input_treated['party'] == 100) & (input_treated['election.type'] == 'P') & (input_treated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
            **{f'total_amount_dem_primary_without_LTS1{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_dem_primary_without_LTS1{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_14.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 15: Dem special
        agg_15 = input_treated[(input_treated['party'] == 100) & (input_treated['election.type'] == 'S')].groupby(['district', 'cycle']).agg(
            **{f'total_amount_dem_special{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_dem_special{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_15.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 16: Dem special, before special
        agg_16 = input_treated[(input_treated['party'] == 100) & (input_treated['election.type'] == 'S') & (input_treated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
            **{f'total_amount_dem_special_without_LTS1{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_dem_special_without_LTS1{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_16.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 17: Rep general
        agg_17 = input_treated[(input_treated['party'] == 200) & (input_treated['election.type'] == 'G')].groupby(['district', 'cycle']).agg(
            **{f'total_amount_rep_gen{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_rep_gen{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_17.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 18: Rep general, before special
        agg_18 = input_treated[(input_treated['party'] == 200) & (input_treated['election.type'] == 'G') & (input_treated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
            **{f'total_amount_rep_gen_without_LTS1{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_rep_gen_without_LTS1{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_18.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 19: Rep primary
        agg_19 = input_treated[(input_treated['party'] == 200) & (input_treated['election.type'] == 'P')].groupby(['district', 'cycle']).agg(
            **{f'total_amount_rep_primary{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_rep_primary{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_19.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 20: Rep primary, before special
        agg_20 = input_treated[(input_treated['party'] == 200) & (input_treated['election.type'] == 'P') & (input_treated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
            **{f'total_amount_rep_primary_without_LTS1{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_rep_primary_without_LTS1{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_20.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 21: Rep special
        agg_21 = input_treated[(input_treated['party'] == 200) & (input_treated['election.type'] == 'S')].groupby(['district', 'cycle']).agg(
            **{f'total_amount_rep_special{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_rep_special{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_21.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 22: Rep special, before special
        agg_22 = input_treated[(input_treated['party'] == 200) & (input_treated['election.type'] == 'S') & (input_treated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
            **{f'total_amount_rep_special_without_LTS1{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_rep_special_without_LTS1{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_22.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # Step 3: Update result_df with these calculated values
        print(f"Updating values for treat_{treat_num}...")
        for (district, cycle), values in agg_dict.items():
            mask = (result_df['district'] == district) & (result_df['cycle'] == cycle)
            for col, val in values.items():
                if col in result_df.columns:
                    result_df.loc[mask, col] = val
    
    print(f"Finished creating treatment-filtered versions for {output_name}")
    return result_df


print("Processing OUTPUT_8_ext...")

def create_output8_treatment_filtered(input_df, output_7_df, base_output_df):
    """
    Create treatment-filtered versions of OUTPUT_8 variables.
    
    Parameters:
    -----------
    input_df : DataFrame
        The raw contribution data (OUTPUT_1)
    output_7_df : DataFrame
        The treatment data with treat_1, treat_2, treat_3, death_date, special_elections_date
    base_output_df : DataFrame
        The base OUTPUT_8
    
    Returns:
    --------
    DataFrame
        Merged dataframe with original variables and _1, _2, _3 versions
    """
    
    print("Creating treatment-filtered versions for OUTPUT_8...")
    
    # Start with base output
    result_df = base_output_df.copy()
    
    # Get all variable names (excluding district and cycle)
    base_vars = [col for col in base_output_df.columns if col not in ['district', 'cycle', 'real_data']]
    
    # Step 1: Copy all base variables to create _1, _2, _3 versions
    print("Creating copies of base variables...")
    new_cols = {}
    for var in base_vars:
        new_cols[f'{var}_1'] = result_df[var]
        new_cols[f'{var}_2'] = result_df[var]
        new_cols[f'{var}_3'] = result_df[var]
        
    result_df = pd.concat([result_df, pd.DataFrame(new_cols, index=result_df.index)], axis=1)
    
    # Step 2: For each treatment, filter contributions and recalculate for treatment == 1 rows
    for treat_num in [1, 2, 3]:
        print(f"Processing treat_{treat_num} for OUTPUT_8...")
        
        # Get district-cycles where treatment == 1
        treated_districts = output_7_df[output_7_df[f'treat_{treat_num}'] == 1][['district', 'cycle', 'death_date', 'special_elections_date']]
        
        if treated_districts.empty:
            print(f"No treated districts for treat_{treat_num}")
            continue
        
        # For treat_1 with multiple death districts, only keep the first instance per district
        if treat_num == 1:
            # Filter to only include districts that are in multiple_death_districts
            multiple_death_treated = treated_districts[treated_districts['district'].isin(multiple_death_districts)]
            
            # For multiple death districts, keep only the first occurrence (earliest cycle)
            if not multiple_death_treated.empty:
                first_occurrences = multiple_death_treated.groupby('district')['cycle'].idxmin()
                multiple_death_treated_first = multiple_death_treated.loc[first_occurrences]
                
                # For single death districts, keep all
                single_death_treated = treated_districts[treated_districts['district'].isin(single_death_districts)]
                
                # Combine: first occurrence for multiple deaths + all single deaths
                treated_districts = pd.concat([single_death_treated, multiple_death_treated_first], ignore_index=True)
        
        # Merge input_df with treated districts to filter contributions
        input_treated = pd.merge(
            input_df.drop(columns=['death_date', 'spec_election_date']),
            treated_districts,
            on=['district', 'cycle'],
            how='inner'
        )
        
        # Filter for contributions between death_date and special_elections_date
        input_treated = input_treated[
            (input_treated['date'] > input_treated['death_date']) &
            (input_treated['date'] < input_treated['special_elections_date'])
        ]
        
        treat_suffix = f'_{treat_num}'
        
        # === CORPORATE CONTRIBUTORS ===
        print(f"  Processing corporate contributors for treat_{treat_num}...")
        
        # 1. avg_counting_hedging_corp
        temp_corp_1 = input_treated[
            (input_treated['election.type'] == 'G') & 
            (input_treated['contributor.type'] == 'C') & 
            (input_treated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'party']).agg(
            counting_hedging_corp=('bonica.rid', 'nunique')
        ).reset_index()
        
        temp_corp_1 = temp_corp_1.groupby(['district', 'cycle']).agg(
            **{f'avg_counting_hedging_corp{treat_suffix}': ('counting_hedging_corp', 'mean')}
        ).reset_index()
        
        # 2. avg_counting_hedging_corp_dem_primary and avg_counting_hedging_corp_rep_primary
        temp_corp_2 = input_treated[
            (input_treated['election.type'] == 'P') & 
            (input_treated['contributor.type'] == 'C') &
            (input_treated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'party']).agg(
            counting_hedging_corp_primary=('bonica.rid', 'nunique')
        ).reset_index()
        
        temp_corp_2 = temp_corp_2.pivot_table(
            index=['district', 'cycle'],
            columns='party',
            values='counting_hedging_corp_primary',
            fill_value=0
        ).reset_index()
        
        # Rename columns if they exist
        if 100.0 in temp_corp_2.columns:
            temp_corp_2 = temp_corp_2.rename(columns={100.0: 'counting_hedging_corp_dem_primary'})
        else:
            temp_corp_2['counting_hedging_corp_dem_primary'] = 0
            
        if 200.0 in temp_corp_2.columns:
            temp_corp_2 = temp_corp_2.rename(columns={200.0: 'counting_hedging_corp_rep_primary'})
        else:
            temp_corp_2['counting_hedging_corp_rep_primary'] = 0
        
        temp_corp_2 = temp_corp_2[['district', 'cycle', 'counting_hedging_corp_dem_primary', 'counting_hedging_corp_rep_primary']]
        temp_corp_2 = temp_corp_2.groupby(['district', 'cycle']).agg(
            **{f'avg_counting_hedging_corp_dem_primary{treat_suffix}': ('counting_hedging_corp_dem_primary', 'mean'),
               f'avg_counting_hedging_corp_rep_primary{treat_suffix}': ('counting_hedging_corp_rep_primary', 'mean')}
        ).reset_index()
        
        # 3. hedging_money_general_corp
        temp_corp_3 = input_treated[
            (input_treated['election.type'] == 'G') & 
            (input_treated['contributor.type'] == 'C') & 
            (input_treated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
            total_amount=('amount', 'sum')
        ).reset_index()
        
        temp_corp_3 = temp_corp_3.pivot_table(
            index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
            columns='party',
            values='total_amount',
            fill_value=0
        ).reset_index()
        
        # Rename columns if they exist
        if 100.0 in temp_corp_3.columns:
            temp_corp_3 = temp_corp_3.rename(columns={100.0: 'total_amount_dem'})
        else:
            temp_corp_3['total_amount_dem'] = 0
            
        if 200.0 in temp_corp_3.columns:
            temp_corp_3 = temp_corp_3.rename(columns={200.0: 'total_amount_rep'})
        else:
            temp_corp_3['total_amount_rep'] = 0
        
        temp_corp_3['hedging'] = abs(temp_corp_3['total_amount_dem'] - temp_corp_3['total_amount_rep'])
        temp_corp_3 = temp_corp_3.groupby(['district', 'cycle']).agg(
            **{f'hedging_money_general_corp{treat_suffix}': ('hedging', 'mean')}
        ).reset_index()
        
        # 4. hedging_money_dem_primary_corp and hedging_money_rep_primary_corp
        temp_corp_4 = input_treated[
            (input_treated['election.type'] == 'P') & 
            (input_treated['contributor.type'] == 'C') & 
            (input_treated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party', 'bonica.rid']).agg(
            total_amount=('amount', 'sum')
        ).reset_index()
        
        temp_corp_4 = temp_corp_4[temp_corp_4.duplicated(subset=['district', 'cycle', 'bonica.cid', 'party'], keep=False)]
        
        if not temp_corp_4.empty:
            dem_results_corp = calculate_party_hedging(temp_corp_4, 100, 'dem', f'corp{treat_suffix}')
            rep_results_corp = calculate_party_hedging(temp_corp_4, 200, 'rep', f'corp{treat_suffix}')
            temp_corp_4 = pd.merge(dem_results_corp, rep_results_corp, on=['district', 'cycle'], how='outer')
        else:
            temp_corp_4 = pd.DataFrame(columns=['district', 'cycle', 
                                                f'hedging_money_dem_primary_corp{treat_suffix}',
                                                f'hedging_money_rep_primary_corp{treat_suffix}'])
        
        # === INDIVIDUAL CONTRIBUTORS ===
        print(f"  Processing individual contributors for treat_{treat_num}...")
        
        # 1. avg_counting_hedging_ind
        temp_ind_1 = input_treated[
            (input_treated['election.type'] == 'G') & 
            (input_treated['contributor.type'] == 'I') & 
            (input_treated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'party']).agg(
            counting_hedging_ind=('bonica.rid', 'nunique')
        ).reset_index()
        
        temp_ind_1 = temp_ind_1.groupby(['district', 'cycle']).agg(
            **{f'avg_counting_hedging_ind{treat_suffix}': ('counting_hedging_ind', 'mean')}
        ).reset_index()
        
        # 2. avg_counting_hedging_ind_dem_primary and avg_counting_hedging_ind_rep_primary
        temp_ind_2 = input_treated[
            (input_treated['election.type'] == 'P') & 
            (input_treated['contributor.type'] == 'I') &
            (input_treated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'party']).agg(
            counting_hedging_ind_primary=('bonica.rid', 'nunique')
        ).reset_index()
        
        temp_ind_2 = temp_ind_2.pivot_table(
            index=['district', 'cycle'],
            columns='party',
            values='counting_hedging_ind_primary',
            fill_value=0
        ).reset_index()
        
        if 100.0 in temp_ind_2.columns:
            temp_ind_2 = temp_ind_2.rename(columns={100.0: 'counting_hedging_ind_dem_primary'})
        else:
            temp_ind_2['counting_hedging_ind_dem_primary'] = 0
            
        if 200.0 in temp_ind_2.columns:
            temp_ind_2 = temp_ind_2.rename(columns={200.0: 'counting_hedging_ind_rep_primary'})
        else:
            temp_ind_2['counting_hedging_ind_rep_primary'] = 0
        
        temp_ind_2 = temp_ind_2[['district', 'cycle', 'counting_hedging_ind_dem_primary', 'counting_hedging_ind_rep_primary']]
        temp_ind_2 = temp_ind_2.groupby(['district', 'cycle']).agg(
            **{f'avg_counting_hedging_ind_dem_primary{treat_suffix}': ('counting_hedging_ind_dem_primary', 'mean'),
               f'avg_counting_hedging_ind_rep_primary{treat_suffix}': ('counting_hedging_ind_rep_primary', 'mean')}
        ).reset_index()
        
        # 3. hedging_money_general_ind
        temp_ind_3 = input_treated[
            (input_treated['election.type'] == 'G') & 
            (input_treated['contributor.type'] == 'I') & 
            (input_treated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
            total_amount=('amount', 'sum')
        ).reset_index()
        
        temp_ind_3 = temp_ind_3.pivot_table(
            index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
            columns='party',
            values='total_amount',
            fill_value=0
        ).reset_index()
        
        if 100.0 in temp_ind_3.columns:
            temp_ind_3 = temp_ind_3.rename(columns={100.0: 'total_amount_dem'})
        else:
            temp_ind_3['total_amount_dem'] = 0
            
        if 200.0 in temp_ind_3.columns:
            temp_ind_3 = temp_ind_3.rename(columns={200.0: 'total_amount_rep'})
        else:
            temp_ind_3['total_amount_rep'] = 0
        
        temp_ind_3['hedging'] = abs(temp_ind_3['total_amount_dem'] - temp_ind_3['total_amount_rep'])
        temp_ind_3 = temp_ind_3.groupby(['district', 'cycle']).agg(
            **{f'hedging_money_general_ind{treat_suffix}': ('hedging', 'mean')}
        ).reset_index()
        
        # 4. hedging_money_dem_primary_ind and hedging_money_rep_primary_ind
        temp_ind_4 = input_treated[
            (input_treated['election.type'] == 'P') & 
            (input_treated['contributor.type'] == 'I') & 
            (input_treated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party', 'bonica.rid']).agg(
            total_amount=('amount', 'sum')
        ).reset_index()
        
        temp_ind_4 = temp_ind_4[temp_ind_4.duplicated(subset=['district', 'cycle', 'bonica.cid', 'party'], keep=False)]
        
        if not temp_ind_4.empty:
            dem_results_ind = calculate_party_hedging(temp_ind_4, 100, 'dem', f'ind{treat_suffix}')
            rep_results_ind = calculate_party_hedging(temp_ind_4, 200, 'rep', f'ind{treat_suffix}')
            temp_ind_4 = pd.merge(dem_results_ind, rep_results_ind, on=['district', 'cycle'], how='outer')
        else:
            temp_ind_4 = pd.DataFrame(columns=['district', 'cycle', 
                                              f'hedging_money_dem_primary_ind{treat_suffix}',
                                              f'hedging_money_rep_primary_ind{treat_suffix}'])
        
        # === SMALL INDIVIDUAL CONTRIBUTORS ===
        print(f"  Processing small individual contributors for treat_{treat_num}...")
        
        # 1. avg_counting_hedging_smallind
        temp_smallind_1 = input_treated[
            (input_treated['election.type'] == 'G') & 
            (input_treated['contributor.type'] == 'I') & 
            (input_treated['later_than_special'] != 1) &
            (input_treated['amount'] < 200)
        ].groupby(['district', 'cycle', 'party']).agg(
            counting_hedging_smallind=('bonica.rid', 'nunique')
        ).reset_index()
        
        temp_smallind_1 = temp_smallind_1.groupby(['district', 'cycle']).agg(
            **{f'avg_counting_hedging_smallind{treat_suffix}': ('counting_hedging_smallind', 'mean')}
        ).reset_index()
        
        # 2. avg_counting_hedging_smallind_dem_primary and avg_counting_hedging_smallind_rep_primary
        temp_smallind_2 = input_treated[
            (input_treated['election.type'] == 'P') & 
            (input_treated['contributor.type'] == 'I') &
            (input_treated['later_than_special'] != 1) &
            (input_treated['amount'] < 200)
        ].groupby(['district', 'cycle', 'party']).agg(
            counting_hedging_smallind_primary=('bonica.rid', 'nunique')
        ).reset_index()
        
        temp_smallind_2 = temp_smallind_2.pivot_table(
            index=['district', 'cycle'],
            columns='party',
            values='counting_hedging_smallind_primary',
            fill_value=0
        ).reset_index()
        
        if 100.0 in temp_smallind_2.columns:
            temp_smallind_2 = temp_smallind_2.rename(columns={100.0: 'counting_hedging_smallind_dem_primary'})
        else:
            temp_smallind_2['counting_hedging_smallind_dem_primary'] = 0
            
        if 200.0 in temp_smallind_2.columns:
            temp_smallind_2 = temp_smallind_2.rename(columns={200.0: 'counting_hedging_smallind_rep_primary'})
        else:
            temp_smallind_2['counting_hedging_smallind_rep_primary'] = 0
        
        temp_smallind_2 = temp_smallind_2[['district', 'cycle', 'counting_hedging_smallind_dem_primary', 'counting_hedging_smallind_rep_primary']]
        temp_smallind_2 = temp_smallind_2.groupby(['district', 'cycle']).agg(
            **{f'avg_counting_hedging_smallind_dem_primary{treat_suffix}': ('counting_hedging_smallind_dem_primary', 'mean'),
               f'avg_counting_hedging_smallind_rep_primary{treat_suffix}': ('counting_hedging_smallind_rep_primary', 'mean')}
        ).reset_index()
        
        # 3. hedging_money_general_smallind
        temp_smallind_3 = input_treated[
            (input_treated['election.type'] == 'G') & 
            (input_treated['contributor.type'] == 'I') & 
            (input_treated['later_than_special'] != 1) &
            (input_treated['amount'] < 200)
        ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
            total_amount=('amount', 'sum')
        ).reset_index()
        
        temp_smallind_3 = temp_smallind_3.pivot_table(
            index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
            columns='party',
            values='total_amount',
            fill_value=0
        ).reset_index()
        
        if 100.0 in temp_smallind_3.columns:
            temp_smallind_3 = temp_smallind_3.rename(columns={100.0: 'total_amount_dem'})
        else:
            temp_smallind_3['total_amount_dem'] = 0
            
        if 200.0 in temp_smallind_3.columns:
            temp_smallind_3 = temp_smallind_3.rename(columns={200.0: 'total_amount_rep'})
        else:
            temp_smallind_3['total_amount_rep'] = 0
        
        temp_smallind_3['hedging'] = abs(temp_smallind_3['total_amount_dem'] - temp_smallind_3['total_amount_rep'])
        temp_smallind_3 = temp_smallind_3.groupby(['district', 'cycle']).agg(
            **{f'hedging_money_general_smallind{treat_suffix}': ('hedging', 'mean')}
        ).reset_index()
        
        # 4. hedging_money_dem_primary_smallind and hedging_money_rep_primary_smallind
        temp_smallind_4 = input_treated[
            (input_treated['election.type'] == 'P') & 
            (input_treated['contributor.type'] == 'I') & 
            (input_treated['later_than_special'] != 1) &
            (input_treated['amount'] < 200)
        ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party', 'bonica.rid']).agg(
            total_amount=('amount', 'sum')
        ).reset_index()
        
        temp_smallind_4 = temp_smallind_4[temp_smallind_4.duplicated(subset=['district', 'cycle', 'bonica.cid', 'party'], keep=False)]
        
        if not temp_smallind_4.empty:
            dem_results_smallind = calculate_party_hedging(temp_smallind_4, 100, 'dem', f'smallind{treat_suffix}')
            rep_results_smallind = calculate_party_hedging(temp_smallind_4, 200, 'rep', f'smallind{treat_suffix}')
            temp_smallind_4 = pd.merge(dem_results_smallind, rep_results_smallind, on=['district', 'cycle'], how='outer')
        else:
            temp_smallind_4 = pd.DataFrame(columns=['district', 'cycle', 
                                                   f'hedging_money_dem_primary_smallind{treat_suffix}',
                                                   f'hedging_money_rep_primary_smallind{treat_suffix}'])
        
        # Merge all temp dataframes for this treatment
        print(f"  Merging all variables for treat_{treat_num}...")
        treat_result = temp_corp_1.copy()
        for temp_df in [temp_corp_2, temp_corp_3, temp_corp_4, 
                       temp_ind_1, temp_ind_2, temp_ind_3, temp_ind_4,
                       temp_smallind_1, temp_smallind_2, temp_smallind_3, temp_smallind_4]:
            treat_result = pd.merge(treat_result, temp_df, on=['district', 'cycle'], how='outer')
        
        # Update result_df with calculated values
        print(f"  Updating result_df for treat_{treat_num}...")
        for idx, row in treat_result.iterrows():
            mask = (result_df['district'] == row['district']) & (result_df['cycle'] == row['cycle'])
            for col in treat_result.columns:
                if col not in ['district', 'cycle'] and col in result_df.columns:
                    result_df.loc[mask, col] = row[col]
    
    print("Finished creating treatment-filtered versions for OUTPUT_8")
    return result_df



# Usage:
OUTPUT_2_ext = create_treatment_filtered_outputs(OUTPUT_1, OUTPUT_7, OUTPUT_2, 'OUTPUT_2')
OUTPUT_3_ext = create_treatment_filtered_outputs(OUTPUT_1, OUTPUT_7, OUTPUT_3, 'OUTPUT_3')
OUTPUT_4_1_ext = create_treatment_filtered_outputs(OUTPUT_1, OUTPUT_7, OUTPUT_4_1, 'OUTPUT_4_1')
OUTPUT_4_2_ext = create_treatment_filtered_outputs(OUTPUT_1, OUTPUT_7, OUTPUT_4_2, 'OUTPUT_4_2')

OUTPUT_8_ext = create_output8_treatment_filtered(OUTPUT_1, OUTPUT_7, OUTPUT_8)





print("Adding treatment variables...")
for df_name, df in [('OUTPUT_2_ext', OUTPUT_2_ext), ('OUTPUT_3_ext', OUTPUT_3_ext), 
                     ('OUTPUT_4_1_ext', OUTPUT_4_1_ext), ('OUTPUT_4_2_ext', OUTPUT_4_2_ext),
                     ('OUTPUT_8_ext', OUTPUT_8_ext)]:
    # Merge treatment variables
    df_merged = pd.merge(
        df,
        OUTPUT_7[['district', 'cycle', 'treat_1', 'treat_2', 'treat_3', 'death_date', 'special_elections_date']],
        on=['district', 'cycle'],
        how='left'
    )
    
    # Convert all columns to numeric except district and date columns
    print(f"Converting {df_name} columns to numeric...")
    for col in df_merged.columns:
        if col not in ['district', 'death_date', 'special_elections_date']:
            df_merged[col] = pd.to_numeric(df_merged[col], errors='ignore')
    
    # Ensure date columns are datetime
    for col in df_merged.columns:
        if col in ['death_date', 'special_elections_date']:
            df_merged[col] = pd.to_datetime(df_merged[col], errors='ignore')
    
    # Reorder columns: district, cycle, treat_1, treat_2, treat_3, then the rest
    first_cols = ['district', 'cycle', 'treat_1', 'treat_2', 'treat_3', 'death_date', 'special_elections_date']
    other_cols = [col for col in df_merged.columns if col not in first_cols]
    df_merged = df_merged[first_cols + other_cols]
    df_merged = df_merged.sort_values(by = ['district', 'cycle'])
    
    # Reassign
    if df_name == 'OUTPUT_2_ext':
        OUTPUT_2_ext = df_merged
    elif df_name == 'OUTPUT_3_ext':
        OUTPUT_3_ext = df_merged
    elif df_name == 'OUTPUT_4_1_ext':
        OUTPUT_4_1_ext = df_merged
    elif df_name == 'OUTPUT_4_2_ext':
        OUTPUT_4_2_ext = df_merged
    elif df_name == 'OUTPUT_8_ext':
        OUTPUT_8_ext = df_merged    

print("Finished processing all extended outputs!")


# test = OUTPUT_2_ext[[
#     'district', 'cycle', 'treat_1', 'treat_2', 'treat_3', 'real_data',
#     'death_date', 'special_elections_date',
#     'total_amount', 'tran_count', 
#     'total_amount_1', 'tran_count_1',
#     'total_amount_2', 'tran_count_2',
#     'total_amount_3', 'tran_count_3',
#     ]]

# test = OUTPUT_3_ext[[
#     'district', 'cycle', 'treat_1', 'treat_2', 'treat_3', # 'real_data',
#     'death_date', 'special_elections_date',
#     'total_amount_corp', 'tran_count_corp', 
#     'total_amount_corp_1', 'tran_count_corp_1',
#     'total_amount_corp_2', 'tran_count_corp_2',
#     'total_amount_corp_3', 'tran_count_corp_3',
#     ]]

# test = OUTPUT_4_1_ext[[
#     'district', 'cycle', 'treat_1', 'treat_2', 'treat_3', # 'real_data',
#     'death_date', 'special_elections_date',
#     'total_amount_ind', 'tran_count_ind', 
#     'total_amount_ind_1', 'tran_count_ind_1',
#     'total_amount_ind_2', 'tran_count_ind_2',
#     'total_amount_ind_3', 'tran_count_ind_3',
#     ]]

test = OUTPUT_8_ext[[
    'district', 'cycle', 'treat_1', 'treat_2', 'treat_3', # 'real_data',
    'death_date', 'special_elections_date',
    'avg_counting_hedging_corp', 
    'avg_counting_hedging_corp_1', 
    'avg_counting_hedging_corp_2',
    'avg_counting_hedging_corp_3',
    'avg_counting_hedging_ind',
    'avg_counting_hedging_ind_1',
    'avg_counting_hedging_ind_2',
    'avg_counting_hedging_ind_3',
    ]]

print("OUTPUT_2_ext shape:", OUTPUT_2_ext.shape)
print("OUTPUT_3_ext shape:", OUTPUT_3_ext.shape)
print("OUTPUT_4_1_ext shape:", OUTPUT_4_1_ext.shape)
print("OUTPUT_4_2_ext shape:", OUTPUT_4_2_ext.shape)
print("OUTPUT_8_ext shape:", OUTPUT_8_ext.shape)

OUTPUT_2_ext = OUTPUT_2_ext.fillna(0)
OUTPUT_3_ext = OUTPUT_3_ext.fillna(0)
OUTPUT_4_1_ext = OUTPUT_4_1_ext.fillna(0)
OUTPUT_4_2_ext = OUTPUT_4_2_ext.fillna(0)
OUTPUT_8_ext = OUTPUT_8_ext.fillna(0)

print("Saving extended output datasets...")
OUTPUT_2_ext.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_2_ext.csv"), index=False)
OUTPUT_3_ext.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_3_ext.csv"), index=False)
OUTPUT_4_1_ext.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_4_1_ext.csv"), index=False)
OUTPUT_4_2_ext.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_4_2_ext.csv"), index=False)
OUTPUT_8_ext.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_8_ext.csv"), index=False)
print("All extended datasets saved successfully!")



OUTPUT_3_ext = OUTPUT_3_ext.drop(
    columns = 
    ['treat_1', 'treat_2', 'treat_3', 'death_date',
    'special_elections_date']
    ) # no duplicate columns when merging with OUTPUT_2

OUTPUT_4_1_ext = OUTPUT_4_1_ext.drop(
    columns = 
    ['treat_1', 'treat_2', 'treat_3', 'death_date',
    'special_elections_date']
    ) # no duplicate columns when merging with OUTPUT_2

OUTPUT_4_2_ext = OUTPUT_4_2_ext.drop(
    columns = 
    ['treat_1', 'treat_2', 'treat_3', 'death_date',
    'special_elections_date']
    ) # no duplicate columns when merging with OUTPUT_2

OUTPUT_8_ext = OUTPUT_8_ext.drop(
    columns = 
    ['treat_1', 'treat_2', 'treat_3', 'death_date',
    'special_elections_date']
    ) # no duplicate columns when merging with OUTPUT_2


#%%

# # Get the death_date and special_elections_date for CA05 1988
# ca05_1988_info = OUTPUT_7[(OUTPUT_7['district'] == 'CA05') & (OUTPUT_7['cycle'] == 1988)]
# death_date = ca05_1988_info['death_date'].iloc[0]
# spec_date = ca05_1988_info['special_elections_date'].iloc[0]

# print(f"Death date: {death_date}")
# print(f"Special election date: {spec_date}")
# print()

# # Get all contributions for CA05 in 1988
# ca05_1988_contribs = OUTPUT_1[
#     (OUTPUT_1['district'] == 'CA05') & 
#     (OUTPUT_1['cycle'] == 1988)
# ].copy()

# # Convert dates
# ca05_1988_contribs['date'] = pd.to_datetime(ca05_1988_contribs['date'])

# # Check contributions by date range
# print("Total contributions for CA05 1988:", len(ca05_1988_contribs))
# print("Contributions BEFORE death:", len(ca05_1988_contribs[ca05_1988_contribs['date'] < death_date]))
# print("Contributions BETWEEN death and special election:", len(ca05_1988_contribs[
#     (ca05_1988_contribs['date'] > death_date) & 
#     (ca05_1988_contribs['date'] < spec_date)
# ]))
# print("Contributions AFTER special election:", len(ca05_1988_contribs[ca05_1988_contribs['date'] > spec_date]))
# print()

# # Now filter for corporate contributors in general election, not after special
# ca05_1988_corp_gen = ca05_1988_contribs[
#     (ca05_1988_contribs['election.type'] == 'G') & 
#     (ca05_1988_contribs['contributor.type'] == 'C') & 
#     (ca05_1988_contribs['later_than_special'] != 1)
# ]

# print("Corporate general election contributions (later_than_special != 1):", len(ca05_1988_corp_gen))
# print()

# # Filter for contributions between death and special election
# ca05_1988_corp_gen_filtered = ca05_1988_corp_gen[
#     (ca05_1988_corp_gen['date'] > death_date) & 
#     (ca05_1988_corp_gen['date'] < spec_date)
# ]

# print("Corporate general election contributions BETWEEN death and special:", len(ca05_1988_corp_gen_filtered))
# print()

# # Calculate avg_counting_hedging_corp for the full period
# test_full = ca05_1988_corp_gen.groupby(['district', 'cycle', 'party']).agg(
#     counting_hedging_corp=('bonica.rid', 'nunique')
# ).reset_index()
# test_full_avg = test_full.groupby(['district', 'cycle']).agg(
#     avg_counting_hedging_corp=('counting_hedging_corp', 'mean')
# ).reset_index()
# print("Base avg_counting_hedging_corp (full period):")
# print(test_full_avg)
# print()

# # Calculate avg_counting_hedging_corp for the filtered period
# if len(ca05_1988_corp_gen_filtered) > 0:
#     test_filtered = ca05_1988_corp_gen_filtered.groupby(['district', 'cycle', 'party']).agg(
#         counting_hedging_corp=('bonica.rid', 'nunique')
#     ).reset_index()
#     test_filtered_avg = test_filtered.groupby(['district', 'cycle']).agg(
#         avg_counting_hedging_corp=('counting_hedging_corp', 'mean')
#     ).reset_index()
#     print("Filtered avg_counting_hedging_corp (between death and special):")
#     print(test_filtered_avg)
# else:
#     print("No contributions found between death and special election dates!")
#     print("This explains the NaN values in OUTPUT_8_ext")


# # Check what election types exist for CA05 1988 between death and special election
# ca05_1988_between = OUTPUT_1[
#     (OUTPUT_1['district'] == 'CA05') & 
#     (OUTPUT_1['cycle'] == 1988) &
#     (pd.to_datetime(OUTPUT_1['date']) > death_date) &
#     (pd.to_datetime(OUTPUT_1['date']) < spec_date)
# ].copy()

# print("Election types during the death-to-special period:")
# print(ca05_1988_between['election.type'].value_counts())
# print()

# print("Contributor types during the death-to-special period:")
# print(ca05_1988_between['contributor.type'].value_counts())
# print()

# # Check corporate contributions by election type
# print("Corporate contributions by election type:")
# corp_contribs = ca05_1988_between[ca05_1988_between['contributor.type'] == 'C']
# print(corp_contribs['election.type'].value_counts())


# # Check individual contributions for CA05 1988
# test_ind = OUTPUT_8_ext[
#     (OUTPUT_8_ext['district'] == 'CA05') & 
#     (OUTPUT_8_ext['cycle'] == 1988)
# ][['district', 'cycle', 'treat_1', 'treat_2', 'treat_3',
#    'avg_counting_hedging_ind', 'avg_counting_hedging_ind_1', 
#    'avg_counting_hedging_ind_2', 'avg_counting_hedging_ind_3',
#    'avg_counting_hedging_smallind', 'avg_counting_hedging_smallind_1',
#    'avg_counting_hedging_smallind_2', 'avg_counting_hedging_smallind_3']]

# print(test_ind)

#%%

## FINAL MERGE OF ALL OUTPUTS

print("..." * 5)
print("Merging all outputs: OUTPUTS_2 to OUTPUTS_9")

OUTPUT_1_final_collapsed = OUTPUT_2.copy()
for i in range(3, 10):
    print(f"Merging OUTPUT_2 with OUTPUT_{i}")
    df_name = f"OUTPUT_{i}"
    df = globals()[df_name]
    
    # Merge with the current result
    OUTPUT_1_final_collapsed = pd.merge(
        OUTPUT_1_final_collapsed,
        df,
        on=['cycle', 'district'],
        how='outer'
    )
    if i == range(3, 10)[-1]:
        print("Merge complete!")
    
print("\n\nFINAL CHECK:\nLength of final dataset:", len(OUTPUT_1_final_collapsed), 
      "should be the same as all these datasets")
for i in range(2, 10):
    
    df_name = f"OUTPUT_{i}"
    df = globals()[df_name]
    
    print(f"Length of {df_name}: {len(df)}")
    

print("..." * 5)
print("Merging all extended datasets: OUTPUTS_2_ext, OUTPUT_3_ext, OUTPUT_4_1_ext, OUTPUT_4_2_ext, and OUTPUT_8_ext")

OUTPUT_1_final_collapsed_ext = OUTPUT_2_ext.copy()

ext_datasets = [
    ('OUTPUT_3_ext', OUTPUT_3_ext),
    ('OUTPUT_4_1_ext', OUTPUT_4_1_ext),
    ('OUTPUT_4_2_ext', OUTPUT_4_2_ext),
    ('OUTPUT_8_ext', OUTPUT_8_ext)
]

for df_name, df in ext_datasets:
    print(f"Merging OUTPUT_1_final_collapsed_ext with {df_name}")
    OUTPUT_1_final_collapsed_ext = pd.merge(
        OUTPUT_1_final_collapsed_ext,
        df,
        on=['cycle', 'district'],
        how='outer'
    )

print("EXT merge complete!")

print("\n\nFINAL EXT CHECK:\nLength of final EXT dataset:", len(OUTPUT_1_final_collapsed_ext),
      "should be the same as all these EXT datasets")

for df_name in ['OUTPUT_2_ext', 'OUTPUT_3_ext', 'OUTPUT_4_1_ext', 'OUTPUT_4_2_ext', 'OUTPUT_8_ext']:
    df = globals()[df_name]
    print(f"Length of {df_name}: {len(df)}")


    
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
            'description': "Since we balance the panel data (i.e., have every cycle for each district), we use this dummy variable to indicate whether the district-cycle existed (value: 1) or not (value: 0), by scraping information from Wikipedia. We create the dummy when merging the grouped-by district and cycle contribution-level dataset (depending on the variables, either OUTPUT_2, OUTPUT_3, OUTPUT_4, or OUTPUT_8) with a dataset that contains all district and cycle combinations that were either created or discontinued after 1980 (new_districts_filtered.csv), since every DIME contribution comes after this election year. If the district-cycle was created before 1980 and has no discontinuation year, it implies the district's persistent existence in our period of interest (i.e., 1980 to 2024), meaning that the district will always receive 1 and we don't have to deal with this. If a district-cycle was either created or discontinued or both after 1980, then this district-cycle receives 1 if the cycle is between the district's creation and discontinuation year, while it receives 0 if the cycle is before the district's creation year or later than the district's discontinuation year. Missing values coming from 'fake districts' (real_data == 0) were replaced with zeros.",
            'source': 'Wikipedia',
            'origin_dataset': 'new_districts.html, new_districts.csv, new_districts_filtered.csv',
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
    
}


# Create a dictionary with each variable having four attributes
rows = []
for var_name, attributes in OUTPUT_1_final_collapsed_dict.items():
    rows.append({
        'variable_name': var_name,
        'description': attributes['description'],
        'source': attributes['source'],
        'origin_dataset': attributes['origin_dataset'],
        'relevant_URLs': attributes['relevant_URLs'],
        'output_relation': attributes['output_relation'],
    })

OUTPUT_1_final_collapsed_dict_df = pd.DataFrame(rows)

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
            'description': "Since we balance the panel data (i.e., have every cycle for each district), we use this dummy variable to indicate whether the district-cycle existed (value: 1) or not (value: 0), by scraping information from Wikipedia. We create the dummy when merging the grouped-by district and cycle contribution-level dataset (depending on the variables, either OUTPUT_2, OUTPUT_3, OUTPUT_4, or OUTPUT_8) with a dataset that contains all district and cycle combinations that were either created or discontinued after 1980 (new_districts_filtered.csv), since every DIME contribution comes after this election year. If the district-cycle was created before 1980 and has no discontinuation year, it implies the district's persistent existence in our period of interest (i.e., 1980 to 2024), meaning that the district will always receive 1 and we don't have to deal with this. If a district-cycle was either created or discontinued or both after 1980, then this district-cycle receives 1 if the cycle is between the district's creation and discontinuation year, while it receives 0 if the cycle is before the district's creation year or later than the district's discontinuation year. Missing values coming from 'fake districts' (real_data == 0) were replaced with zeros.",
            'source': 'Wikipedia',
            'origin_dataset': 'new_districts.html, new_districts.csv, new_districts_filtered.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_United_States_congressional_districts',
            'output_relation': 'OUTPUT_2_ext, OUTPUT_3_ext, OUTPUT_4_ext, OUTPUT_8_ext (Variables measured in or related to dollar amounts)'
            },       
    
    # OUTPUT_{i}_ext
    '_suffix': {
            'description': 'For treated district-cycle observations, we apply condition that keeps only contributions coming between death date and date of special elections. Suffix determines what treatment we used to apply this condition (either treat_1, treat_2, or treat_3); No suffix indicates original variable (i.e. filtering condition not applied). For single-death district, condition applied similarly for all variables (_1, _2, _3) when first treatment occurs. In multiple-death districts, we apply condition for _1 variables only when first death occurs, for _2 and _3 variables we apply the condition when the death(s) occur(s) again.',
            'source': '(see original var)',
            'origin_dataset': '(see original var)',
            'relevant_URLs': '(see original var)',
            'output_relation': '(see original var)'
            },       
    
    }

# Create a dictionary with each variable having four attributes
rows = []
for var_name, attributes in OUTPUT_1_final_collapsed_ext_dict.items():
    rows.append({
        'variable_name': var_name,
        'description': attributes['description'],
        'source': attributes['source'],
        'origin_dataset': attributes['origin_dataset'],
        'relevant_URLs': attributes['relevant_URLs'],
        'output_relation': attributes['output_relation'],
    })

OUTPUT_1_final_collapsed_ext_dict_df = pd.DataFrame(rows)


#%%
OUTPUT_1_final_collapsed.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_1_final_collapsed.csv"), index = False)
OUTPUT_1_final_collapsed_dict_df.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_1_final_collapsed_dict.csv"), index = False)

OUTPUT_1_final_collapsed_ext.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_1_final_collapsed_ext.csv"), index = False)
OUTPUT_1_final_collapsed_ext_dict_df.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_1_final_collapsed_dict_ext.csv"), index = False)

### END OF SCRIPT!

print("\nEnd of script!")


# OUTPUT_1_final_collapsed = pd.read_csv(
#     data_folder + "/OUTPUTS/OUTPUT_1_final_collapsed.csv", 
#     encoding='latin-1'
#     )


# OUTPUT_1_final_collapsed_dup = OUTPUT_1_final_collapsed.duplicated(subset=['district', 'cycle'])

# OUTPUT_1_final_collapsed_dup = OUTPUT_1_final_collapsed.groupby(['district', 'cycle']).size()
# OUTPUT_1_final_collapsed_dup[OUTPUT_1_final_collapsed_dup > 1]

# "LA06 2002"
