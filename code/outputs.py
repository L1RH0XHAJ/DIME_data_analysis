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
import importlib.util
import sys

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
parent_folder = os.path.dirname(code_folder)

data_folder = os.path.join(parent_folder, "data")

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


# ## OUTPUT_0: a Cartesian product of unique values of district and cycles (useful for merging with other OUTPUT data)
# ## OUTPUT_0_2: similar to OUTPUT_0, but has an additional categorisation by party as well
# print("Reading OUTPUT_0 and OUTPUT_0_2...")

# OUTPUT_0 = pd.read_csv(
#     data_folder + "/OUTPUTS/OUTPUT_0.csv", 
#     encoding='latin-1'
#     )

# OUTPUT_0_2 = pd.read_csv(
#     data_folder + "/OUTPUTS/OUTPUT_0_2.csv", 
#     encoding='latin-1'
#     )

## OUTPUT 1: contribution-day level dataset
print("Reading OUTPUT_1...")

OUTPUT_1 = pd.read_csv(
    os.path.join(data_folder, "OUTPUTS/OUTPUT_1.csv"), 
    encoding='latin-1'
    )

## Special elections data and death districts
print("Reading special elections data...")
special_elections = pd.read_csv(os.path.join(data_folder, "special_elections_final.csv"), encoding='latin-1')

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
election_dates_df = pd.read_csv(os.path.join(data_folder, "election_dates.csv"), encoding='latin-1')

#%%

### Reading and processing new data here
#     1. MIT_eMIT elections data
#     2. new_districts_df: newly created districts

## 1. MIT_eMIT US House 1976 - 2024 elections data (source: https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/IG0UN2)
# print("Reading MIT US House 1976 - 2022 elections data...")
# gen_elect_df = pd.read_csv(data_folder + "/1976-2022-house.csv", encoding='latin-1')
print("Reading MIT US House 1976 - 2024 elections data...")
gen_elect_df = pd.read_csv(os.path.join(data_folder, "1976-2024-house.csv"), encoding='utf-8')

# Renaming columns
gen_elect_df = gen_elect_df.rename(columns = {'year' : 'cycle'})

gen_elect_df['cycle'] = pd.to_numeric(gen_elect_df['cycle'], errors='coerce')  # will convert None values to NaN

# Dropping rows
gen_elect_df = gen_elect_df[gen_elect_df['cycle'].isin(OUTPUT_1['cycle'].unique())] # cycle in OUTPUT_1
gen_elect_df = gen_elect_df[gen_elect_df['stage']=='GEN'] # very few observations for primary ('PRI'), no point in using them
gen_elect_df = gen_elect_df[gen_elect_df['special']!= True] # very few observations for special elections
gen_elect_df = gen_elect_df[gen_elect_df['runoff']!= True] # very few observations for runoff elections
gen_elect_df = gen_elect_df.drop(columns = ['runoff'])
gen_elect_df['runoff'] = False # Assuming everything else is not a runoff
# gen_elect_df = gen_elect_df[(gen_elect_df['party']=='DEMOCRAT') | (gen_elect_df['party']=='REPUBLICAN')] # only dems and reps
# print(gen_elect_df['cycle'].value_counts())
print(gen_elect_df['cycle'].unique())

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
print(gen_elect_df['party'].value_counts())

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

gen_elect_df.loc[
    (gen_elect_df['district'] == 'CT01') & 
    (gen_elect_df['cycle'] == 2016) & 
    (gen_elect_df['candidate'] == 'WRITEIN'),
    'candidatevotes'
] = 2 # was originally 1


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
check_maxtotalvotes = votes_check[votes_check['candidatevotes'] != votes_check['totalvotes']]
print(check_maxtotalvotes)

gen_elect_df_check = gen_elect_df.merge(
    check_maxtotalvotes[['district', 'cycle']].drop_duplicates(),
    on=['district', 'cycle'],
    how='inner'
)

# Detailed verification for each district-cycle combination
print("\n" + "="*80)
print("Detailed check for each district-cycle combination:")
print("="*80)

for _, row in check_maxtotalvotes.iterrows():
    district = row['district']
    cycle = row['cycle']
    expected_total = row['totalvotes']
    actual_sum = row['candidatevotes']
    
    # Get all candidates for this district-cycle
    district_cycle_data = gen_elect_df[
        (gen_elect_df['district'] == district) & 
        (gen_elect_df['cycle'] == cycle)
    ]
    
    print(f"\nDistrict: {district}, Cycle: {cycle}")
    print(f"Expected totalvotes: {expected_total:,.0f}")
    print(f"Actual sum of candidatevotes: {actual_sum:,.0f}")
    print(f"Difference: {expected_total - actual_sum:,.0f}")
    print(f"Number of candidates: {len(district_cycle_data)}")
    print("\nCandidate breakdown:")
    # print(district_cycle_data[['candidate', 'candidatevotes', 'totalvotes']].to_string())
    print("-"*80)


# For each district-cycle in check_maxtotalvotes, overwrite totalvotes with sum of candidatevotes
for _, row in check_maxtotalvotes.iterrows():
    district = row['district']
    cycle = row['cycle']
    correct_total = row['candidatevotes']  # This is the sum from our check
    
    # Update all rows for this district-cycle
    mask = (gen_elect_df['district'] == district) & (gen_elect_df['cycle'] == cycle)
    gen_elect_df.loc[mask, 'totalvotes'] = correct_total
    

print(f"Updated totalvotes for {len(check_maxtotalvotes)} district-cycle combinations")

# Verify
votes_check_after = gen_elect_df.groupby(['district', 'cycle']).agg({
    'candidatevotes': 'sum',
    'totalvotes': 'first',
}).reset_index()

remaining_mismatches = votes_check_after[votes_check_after['candidatevotes'] != votes_check_after['totalvotes']]
print(f"Remaining mismatches: {len(remaining_mismatches)}")



## Final changes (problems with raw data)

# IMPORTANT NOTE: cases when candidate votes (one candidate) = totalvotes (1 = 1) indicate uncontested candidate
# For special case, MARIO DIAZ-BALART

# Update candidatevotes only where it's currently -1
gen_elect_df.loc[
    (gen_elect_df['candidate'] == "MARIO DIAZ-BALART") & 
    (gen_elect_df['candidatevotes'] == -1), 
    'candidatevotes'
] = 1

# Update totalvotes only where it's currently -1
gen_elect_df.loc[
    (gen_elect_df['candidate'] == "MARIO DIAZ-BALART") & 
    (gen_elect_df['totalvotes'] == -1), 
    'totalvotes'
] = 1

print("Summary of candidatevotes\n", gen_elect_df['candidatevotes'].describe()) # checking
print("Count of negative candidatevotes", len(gen_elect_df[gen_elect_df['candidatevotes'] < 0])) # checking
print("Count of negative candidatevotes", len(gen_elect_df[gen_elect_df['totalvotes'] < 0])) # checking

print(gen_elect_df[gen_elect_df['candidate'] == 'MARIO DIAZ-BALART'])



# Creating variable for OUTPUT_6 (since every row is each candidate in each district-cycle, we don't need to pivot or do anything else with the data to generate this data)
gen_elect_df['gen_vote_pct'] = gen_elect_df['candidatevotes'] / gen_elect_df['totalvotes']

# We keep only dems and reps
gen_elect_df = gen_elect_df[gen_elect_df['party'] != 300]

## Checking if any duplicates
# display_duplicates(gen_elect_df, ['district', 'cycle', 'party', 'candidate'])
# display_duplicates(gen_elect_df, ['district', 'cycle', 'candidate'])

print("Summary of gen_vote_pct\n", gen_elect_df['gen_vote_pct'].describe()) # checking


#%%


## 2. Newly created districts data (from convert_html_to_csv_2.py)
print("Reading data of newly created districts...")
# new_districts_df = pd.read_csv(data_folder + "/new_districts_filtered.csv", encoding='latin-1')
new_districts_df = pd.read_csv(os.path.join(data_folder, "new_districts_filtered_all.csv"), encoding='latin-1')

print("Processing new_districts_df_0...") # cartesian product of districts in new_districts_df and all cycles (all election years)

unique_districts = new_districts_df['district'].dropna().unique()  # universe of districts
unique_cycles = list(range(1980, 2026, 2))  # hard-coded
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

# For districts that always exist in 1980 - 2024 period
for idx in new_districts_df.index:
    row = new_districts_df.loc[idx]
    if row['exist_always'] == 1:
        new_districts_df.loc[idx, 'real_data'] = 1
    else:
        continue

for idx in new_districts_df.index:
    row = new_districts_df.loc[idx]
    if row['exist_always'] == 0:

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


# MT02 SPECIAL CASES (district discontinued and created within 1980 - 2024 period)
# -> 2nd district: 1919–1993, 2023–present

cycles_to_update = list(range(1992, 2022, 2))
new_districts_df.loc[
    (new_districts_df['district'] == 'MT02') & 
    (new_districts_df['cycle'].isin(cycles_to_update)), 
    'real_data'
] = 0

territorial_districts = ['AS01', 'DC01', 'GU01', 'MP01', 'PR01', 'VI01']

new_districts_df['territorial'] = 0
new_districts_df.loc[
    new_districts_df['district'].isin(territorial_districts),
    'territorial'
] = 1

# Checks

# Filter data cycle in OUTPUT_1[cycle].unique()
print("Filter data cycle in OUTPUT_1[cycle].unique()")
new_districts_df = new_districts_df[new_districts_df['cycle'].isin(OUTPUT_1['cycle'].unique())]
print("Earliest year", new_districts_df['cycle'].min())
print("Latest year", new_districts_df['cycle'].max())

new_districts_df_real1 = new_districts_df[new_districts_df['real_data'] == 1]
new_districts_df_real1_and_terr0 = new_districts_df_real1[new_districts_df_real1['territorial'] == 0]

print("Unique district count by cycle (all data)")
print(new_districts_df.groupby('cycle')['district'].count())
print("-" * 20)
print("Unique district count by cycle (real_data == 1)")
print(new_districts_df_real1.groupby('cycle')['district'].count())
print("-" * 20)
print("Unique district count by cycle (real_data == 1 & territorial == 0)")
print(new_districts_df_real1_and_terr0.groupby('cycle')['district'].count())
print("=" * 20)


new_districts_df = new_districts_df[['district', 'cycle', 'real_data', 'territorial']]

new_districts_df.to_csv(os.path.join(data_folder, "new_districts_filtered_universe.csv"), index = False)

## new_districts_df_party: a Cartesian product of unique values of district and cycles and party
print("Processing new_districts_df_by_party...")

unique_districts = new_districts_df['district'].dropna().unique()
unique_cycles = new_districts_df['cycle'].dropna().unique()
unique_parties = [100, 200] # hard coding this for Dems and Reps only
parties_list = []
districts_list = []
cycles_list = []

for district in unique_districts:
    for cycle in unique_cycles:
        for party in unique_parties:
            districts_list.append(district)
            cycles_list.append(cycle)
            parties_list.append(party)

new_districts_df_party = pd.DataFrame({
    'district': districts_list,
    'cycle': cycles_list,
    'party': parties_list
})

new_districts_df_party = new_districts_df_party.sort_values(['district', 'cycle', 'party']).reset_index(drop=True)

# Adding real_data and territorial
new_districts_df_party = pd.merge(
    new_districts_df_party,
    new_districts_df,
    on = ['district', 'cycle'],
    how = 'left'
    )

new_districts_df_party.to_csv(os.path.join(data_folder, "new_districts_filtered_universe_party.csv"), index = False)

new_districts_df_party = new_districts_df_party[['district', 'cycle', 'party']]


#%%

def compare_districts(df1, df2, df1_name="df1", df2_name="df2", district_col='district'):
    districts_df1 = set(df1[district_col].dropna())
    districts_df2 = set(df2[district_col].dropna())
    
    only_in_df1 = districts_df1 - districts_df2
    only_in_df2 = districts_df2 - districts_df1
    in_both = districts_df1 & districts_df2
    
    print("=" * 60)
    print(f"District Comparison: {df1_name} vs {df2_name}")
    print("=" * 60)
    print(f"\nTotal districts in {df1_name}: {len(districts_df1)}")
    print(f"Total districts in {df2_name}: {len(districts_df2)}")
    print(f"Districts in both: {len(in_both)}")
    
    print(f"\n{'-' * 60}")
    print(f"Districts only in {df1_name} (not in {df2_name}): {len(only_in_df1)}")
    if only_in_df1:
        print(sorted(only_in_df1))
    
    print(f"\n{'-' * 60}")
    print(f"Districts only in {df2_name} (not in {df1_name}): {len(only_in_df2)}")
    if only_in_df2:
        print(sorted(only_in_df2))
        
    print('\n')
    
    return {
        'only_in_df1': sorted(only_in_df1),
        'only_in_df2': sorted(only_in_df2),
        'in_both': sorted(in_both),
        'df1_count': len(districts_df1),
        'df2_count': len(districts_df2),
        'both_count': len(in_both)
    }

# Usage example:
for year in range(1980, 2026, 2):
    selected_year = year
    new_districts_df_filter_year = new_districts_df_real1_and_terr0[new_districts_df_real1_and_terr0['cycle'] == selected_year]
    gen_elect_df_filter_year = gen_elect_df[gen_elect_df['cycle'] == selected_year]
    
    result = compare_districts(
        new_districts_df_filter_year, 
        gen_elect_df_filter_year, 
        df1_name=f"new_districts_df_filter_{selected_year}", 
        df2_name=f"gen_elect_df_{selected_year}"
    )
    
    # Access the results
    extra_districts_1980 = result['only_in_df1']
    extra_districts_1980_reverse = result['only_in_df2']



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

#     7 total_amount_gen: Sum of contributions in the district/cycle. Only considering general

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


for year in range(1980, 2026, 2):
    selected_year = year
    new_districts_df_filter_year = new_districts_df_real1[new_districts_df_real1['cycle'] == selected_year]
    OUTPUT_2_filter_year = OUTPUT_2[OUTPUT_2['cycle'] == selected_year]
    
    result = compare_districts(
        new_districts_df_filter_year, 
        OUTPUT_2_filter_year, 
        df1_name=f"new_districts_df_filter_{selected_year}", 
        df2_name=f"OUTPUT_2_filter_{selected_year}"
    )
    
    # Access the results
    extra_districts_1980 = result['only_in_df1']
    extra_districts_1980_reverse = result['only_in_df2']


# new_districts_df['const_1'] = 1
# OUTPUT_2['const_2'] = 1

# OUTPUT_2_test = pd.merge(
#     new_districts_df,
#     OUTPUT_2,
#     on=['district', 'cycle'],
#     how='left'
#     )

# print(OUTPUT_2_test['const_1'].value_counts(dropna = False))
# print(OUTPUT_2_test['const_2'].value_counts(dropna = False))


# print("Unique district count by cycle (all data)")
# print(OUTPUT_2_test.groupby('cycle')['district'].count())
# print("-" * 20)
# print("Unique district count by cycle (real_data == 1)")
# print(OUTPUT_2_test[OUTPUT_2_test['real_data'] == 1].groupby('cycle')['district'].count())
# print("-" * 20)
# print("Unique district count by cycle (real_data == 1 & territorial == 0)")
# print(OUTPUT_2_test[(OUTPUT_2_test['real_data'] == 1) & 
#                     (OUTPUT_2_test['territorial'] == 0)].groupby('cycle')['district'].count())
# print("=" * 20)


## WE MERGE WITH UNIVERSE OF DISTRICTS
OUTPUT_2 = pd.merge(
    new_districts_df,
    OUTPUT_2,
    on=['district', 'cycle'],
    how='left'
    )



# # Missing values of real_data are replaced with 1, because they are real data (district has existed prior to 1980 and has existed until 2024)
# OUTPUT_2.loc[OUTPUT_2['real_data'].isna(), 'real_data'] = 1
# # Re-ordering data
# OUTPUT_2 = OUTPUT_2.sort_values(by=['district', 'cycle'], ascending=[True, True])
# columns_list = ['district', 'cycle', 'real_data']
# cols = columns_list + [
#     col for col in OUTPUT_2.columns if col not in columns_list]
# OUTPUT_2 = OUTPUT_2[cols]

# print("Merging OUTPUTS with OUTPUT_0")
# OUTPUT_2 = pd.merge(
#     OUTPUT_0,
#     OUTPUT_2,
#     on=['cycle', 'district'],
#     how='outer'
#     )

# # Missing values of real_data are replaced with 0, these don't exist but are fake rows to balance our dataset
# OUTPUT_2.loc[OUTPUT_2['real_data'].isna(), 'real_data'] = 0

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
    new_districts_df, # we only need this information from OUTPUT_2
    OUTPUT_3,
    on=['cycle', 'district'],
    how='left'
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
OUTPUT_3 = OUTPUT_3.drop(columns = ['real_data', 'territorial']) # no duplicate columns when merging with OUTPUT_2


#%%

### OUTPUT_4: 
print("Processing OUTPUT_4...")

    
# all variables are similar to OUTPUT_2 but apply only to contributions coming from individuals

OUTPUT_4_1 = create_aggregated_outputs(OUTPUT_1, 'OUTPUT_4_1', filter_type='I', suffix='_ind')

OUTPUT_4_2 = create_aggregated_outputs(OUTPUT_1, 'OUTPUT_4_2', filter_type='I', amount_filter=200, suffix='_smallind')

# NOTE: We have already created real_data dummy, we get this information from OUTPUT_2 (the 'universe' of district-cycles) and avoid creating a new one for district-cycle individual contributions
OUTPUT_4_1 = pd.merge(
    new_districts_df, # we only need this information from OUTPUT_2
    OUTPUT_4_1,
    on=['cycle', 'district'],
    how='left'
    )

OUTPUT_4_2 = pd.merge(
    new_districts_df, # we only need this information from OUTPUT_2
    OUTPUT_4_2,
    on=['cycle', 'district'],
    how='left'
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
OUTPUT_4_1 = OUTPUT_4_1.drop(columns = ['real_data', 'territorial']) # no duplicate columns when merging with OUTPUT_2

# OUTPUT_4_2_processed = OUTPUT_4_2.fillna(0)
OUTPUT_4_2 = OUTPUT_4_2.fillna(0)
OUTPUT_4_2.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_4_2.csv"), index=False)
OUTPUT_4_2 = OUTPUT_4_2.drop(columns = ['real_data', 'territorial']) # no duplicate columns when merging with OUTPUT_2

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

# OUTPUT_5 = pd.merge(
#     OUTPUT_0,
#     OUTPUT_5,
#     on=['cycle', 'district'],
#     how='left'
#     )

OUTPUT_5 = pd.merge(
    new_districts_df,
    OUTPUT_5,
    on=['cycle', 'district'],
    how='left'
    )


OUTPUT_5.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_5.csv"), index = False)

OUTPUT_5 = OUTPUT_5[['district', 'cycle', 'district_color_35_65', 'district_color_30_70', 'district_color_40_60']]

#%%

## OUTPUT_6: 
print("Processing OUTPUT_6...")

# THESE ARE OLD VAR NAMES



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
        
        # NEW CONDITION: If G_dem or G_rep equals 1, set G_dispersion to 1 (this means they won elections uncontested)
        if 'G_dem' in result and 'G_rep' in result:
            if result['G_dem'] == 1 or result['G_rep'] == 1:
                result['G_dispersion'] = 1

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
    

# Creating datasets
print("Filtering OUTPUT_6_1_1...")
# OUTPUT_6_1_1 = OUTPUT_1.groupby(['district', 'cycle']).apply(
#     lambda x: calculate_metrics(x, 'gen.vote.pct')
# ).reset_index()

OUTPUT_6_1_1 = gen_elect_df.groupby(['district', 'cycle', 'totalvotes']).apply(
    lambda x: calculate_metrics(x, 'gen_vote_pct')
).reset_index()


OUTPUT_6_1_1 = pd.merge(
    new_districts_df,
    OUTPUT_6_1_1,
    on=['cycle', 'district'],
    how='left'
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
    new_districts_df_party[['district', 'cycle', 'party']],
    OUTPUT_6_1_2,
    on=['cycle', 'district', 'party'],
    how='left'
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




# Balancing datasets
OUTPUT_6_2_dem = pd.merge(
    OUTPUT_2[['district', 'cycle']],
    OUTPUT_6_2_dem,
    on=['cycle', 'district'],
    how='left'
    )
OUTPUT_6_2_rep = pd.merge(
    OUTPUT_2[['district', 'cycle']],
    OUTPUT_6_2_rep,
    on=['cycle', 'district'],
    how='left'
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



OUTPUT_6.to_csv(os.path.join(data_folder, 'OUTPUTS', 'OUTPUT_6.csv'), index = False)

OUTPUT_6 = OUTPUT_6.drop(columns = ['real_data', 'territorial'])


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
    new_districts_df,
    OUTPUT_7,
    how = 'left',
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

OUTPUT_7 = OUTPUT_7.drop(columns = ['real_data', 'territorial'])


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

# NOTE: We have already created real_data dummy, we get this information from OUTPUT_2 (the 'universe' of district-cycles) and avoid creating a new one for district-cycle individual contributions
OUTPUT_8 = pd.merge(
    new_districts_df, # we only need this information from OUTPUT_2
    OUTPUT_8,
    on=['cycle', 'district'],
    how='left'
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
OUTPUT_8 = OUTPUT_8.drop(columns = ['real_data', 'territorial']) # no duplicate columns when merging with OUTPUT_2



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
    new_districts_df,
    OUTPUT_9,
    on = ['district', 'cycle'],
    how = 'left'
    )

# We have 'recipient.cfscore' and 'recipient.cfscore.dyn', but the latter is clearly better, although has missing values


OUTPUT_9.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_9.csv"), index = False)

OUTPUT_9 = OUTPUT_9.drop(columns = ['real_data', 'territorial'])

#%%

### OUTPUT_2_ext, OUTPUT_3_ext, OUTPUT_4_ext, and OUTPUT_8_ext

### Creating extended datasets with vars that apply the condition of filter contributions between death_date and spec_election_date
# - OUTPUT_2
# - OUTPUT_3
# - OUTPUT_4
# - OUTPUT_8

print("3-step processing of _ext_ (extended) datasets")

def import_module_from_path(module_name, file_path):
    """Import a module from a specific file path"""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

script_path = os.path.join(code_folder, "outputs_scripts", "create_ext_vars.py")
CEV = import_module_from_path("CEV", script_path)
create_treatment_filtered_outputs = CEV.create_treatment_filtered_outputs
create_output8_treatment_filtered = CEV.create_output8_treatment_filtered
add_gen_np_variables = CEV.add_gen_np_variables
add_gen_np_variables_output8 = CEV.add_gen_np_variables_output8
add_gen_np_spec_variables = CEV.add_gen_np_spec_variables
add_gen_np_spec_variables_output8 = CEV.add_gen_np_spec_variables_output8

if __name__ == '__main__':

    # Execution
    
    # 1. create_treatment_filtered_outputs and create_output8_treatment_filtered
    print("***** STEP 1 *****")
    print("Processing OUTPUT_2_ext, OUTPUT_3_ext, and OUTPUT_4_ext...")
    OUTPUT_2_ext = create_treatment_filtered_outputs(OUTPUT_1, OUTPUT_7, OUTPUT_2, 'OUTPUT_2', single_death_districts, multiple_death_districts)  # latter two need to be redefined for module
    OUTPUT_3_ext = create_treatment_filtered_outputs(OUTPUT_1, OUTPUT_7, OUTPUT_3, 'OUTPUT_3', single_death_districts, multiple_death_districts)
    OUTPUT_4_1_ext = create_treatment_filtered_outputs(OUTPUT_1, OUTPUT_7, OUTPUT_4_1, 'OUTPUT_4_1', single_death_districts, multiple_death_districts)
    OUTPUT_4_2_ext = create_treatment_filtered_outputs(OUTPUT_1, OUTPUT_7, OUTPUT_4_2, 'OUTPUT_4_2', single_death_districts, multiple_death_districts)
    
    print("Processing OUTPUT_8_ext...")
    OUTPUT_8_ext = create_output8_treatment_filtered(OUTPUT_1, OUTPUT_7, OUTPUT_8, single_death_districts, multiple_death_districts)

    # 2. add_gen_np_variables and add_gen_np_variables_output8    
    print("***** STEP 2 *****")
    print("Adding gen_np variables to OUTPUT_2_ext, OUTPUT_3_ext, and OUTPUT_4_ext...")
    OUTPUT_2_ext = add_gen_np_variables(OUTPUT_1, OUTPUT_7, OUTPUT_2_ext, 'OUTPUT_2', single_death_districts, multiple_death_districts)
    OUTPUT_3_ext = add_gen_np_variables(OUTPUT_1, OUTPUT_7, OUTPUT_3_ext, 'OUTPUT_3', single_death_districts, multiple_death_districts)
    OUTPUT_4_1_ext = add_gen_np_variables(OUTPUT_1, OUTPUT_7, OUTPUT_4_1_ext, 'OUTPUT_4_1', single_death_districts, multiple_death_districts)
    OUTPUT_4_2_ext = add_gen_np_variables(OUTPUT_1, OUTPUT_7, OUTPUT_4_2_ext, 'OUTPUT_4_2', single_death_districts, multiple_death_districts)
    
    print("Adding gen_np variables to OUTPUT_8_ext...")    
    OUTPUT_8_ext = add_gen_np_variables_output8(OUTPUT_1, OUTPUT_7, OUTPUT_8_ext, single_death_districts, multiple_death_districts)
    
    # 3. add_gen_np_spec_variables and add_gen_np_spec_variables_output8    
    print("***** STEP 3 *****")
    print("Adding gen_np_spec variables to OUTPUT_2_ext, OUTPUT_3_ext, and OUTPUT_4_ext...")
    OUTPUT_2_ext = add_gen_np_spec_variables(OUTPUT_1, OUTPUT_7, OUTPUT_2_ext, 'OUTPUT_2', single_death_districts, multiple_death_districts)
    OUTPUT_3_ext = add_gen_np_spec_variables(OUTPUT_1, OUTPUT_7, OUTPUT_3_ext, 'OUTPUT_3', single_death_districts, multiple_death_districts)
    OUTPUT_4_1_ext = add_gen_np_spec_variables(OUTPUT_1, OUTPUT_7, OUTPUT_4_1_ext, 'OUTPUT_4_1', single_death_districts, multiple_death_districts)
    OUTPUT_4_2_ext = add_gen_np_spec_variables(OUTPUT_1, OUTPUT_7, OUTPUT_4_2_ext, 'OUTPUT_4_2', single_death_districts, multiple_death_districts)
    print("Adding gen_np_spec variables to OUTPUT_8_ext...")
    OUTPUT_8_ext = add_gen_np_spec_variables_output8(OUTPUT_1, OUTPUT_7, OUTPUT_8_ext, single_death_districts, multiple_death_districts)

    

# test = OUTPUT_2_ext[[
#     'district', 'cycle', 
#     # 'treat_1', 'treat_2', 'treat_3', 'real_data',
#     # 'death_date', 'special_elections_date',
#     'total_amount_gen', 'tran_count_gen', 
#     'total_amount_gen_1', 'tran_count_gen_1',
#     'total_amount_gen_2', 'tran_count_gen_2',
#     'total_amount_gen_3', 'tran_count_gen_3',
#     # 'total_amount_gen_np', 'tran_count_gen_np',
#     'total_amount_gen_np_1', 'tran_count_gen_np_1',
#     'total_amount_gen_np_2', 'tran_count_gen_np_2',
#     'total_amount_gen_np_3', 'tran_count_gen_np_3',
#     'total_amount_gen_np_spec_1', 'tran_count_gen_np_spec_1',
#     'total_amount_gen_np_spec_2', 'tran_count_gen_np_spec_2',
#     'total_amount_gen_np_spec_3', 'tran_count_gen_np_spec_3'
    
#     ]]

# test = OUTPUT_3_ext[[
#     'district', 'cycle', 
#     # 'treat_1', 'treat_2', 'treat_3', 'real_data',
#     # 'death_date', 'special_elections_date',
#     'total_amount_gen_corp', 'tran_count_gen_corp', 
#     'total_amount_gen_corp_1', 'tran_count_gen_corp_1',
#     'total_amount_gen_corp_2', 'tran_count_gen_corp_2',
#     'total_amount_gen_corp_3', 'tran_count_gen_corp_3',
#     # 'total_amount_gen_np', 'tran_count_gen_np',
#     'total_amount_gen_np_corp_1', 'tran_count_gen_np_corp_1',
#     'total_amount_gen_np_corp_2', 'tran_count_gen_np_corp_2',
#     'total_amount_gen_np_corp_3', 'tran_count_gen_np_corp_3',
#     'total_amount_gen_np_spec_corp_1', 'tran_count_gen_np_spec_corp_1',
#     'total_amount_gen_np_spec_corp_2', 'tran_count_gen_np_spec_corp_2',
#     'total_amount_gen_np_spec_corp_3', 'tran_count_gen_np_spec_corp_3'
    
#     ]]

# test2_corp = OUTPUT_8_ext[[
#     'district', 'cycle', 
#     # 'treat_1', 'treat_2', 'treat_3', # 'real_data',
#     # 'death_date', 'special_elections_date',
#     'avg_counting_hedging_corp', 
#     'avg_counting_hedging_corp_1', 
#     'avg_counting_hedging_corp_2',
#     'avg_counting_hedging_corp_3',

#     # 'avg_counting_hedging_np_corp',
#     'avg_counting_hedging_np_corp_1',
#     'avg_counting_hedging_np_corp_2',
#     'avg_counting_hedging_np_corp_3',

#     # 'avg_counting_hedging_np_spec_corp',
#     'avg_counting_hedging_np_spec_corp_1',
#     'avg_counting_hedging_np_spec_corp_2',
#     'avg_counting_hedging_np_spec_corp_3',
    
#     ]]


# test2_ind = OUTPUT_8_ext[[
#     'district', 'cycle', 
#     # 'treat_1', 'treat_2', 'treat_3', # 'real_data',
#     # 'death_date', 'special_elections_date',
#     'avg_counting_hedging_ind', 
#     'avg_counting_hedging_ind_1', 
#     'avg_counting_hedging_ind_2',
#     'avg_counting_hedging_ind_3',

#     # 'avg_counting_hedging_np_ind',
#     'avg_counting_hedging_np_ind_1',
#     'avg_counting_hedging_np_ind_2',
#     'avg_counting_hedging_np_ind_3',

#     # 'avg_counting_hedging_np_spec_ind',
#     'avg_counting_hedging_np_spec_ind_1',
#     'avg_counting_hedging_np_spec_ind_2',
#     'avg_counting_hedging_np_spec_ind_3',
    
#     ]]



#%%


# OUTPUT_2_ext_2 = OUTPUT_2_ext.copy()
# OUTPUT_3_ext_2 = OUTPUT_3_ext.copy()
# OUTPUT_4_1_ext_2 = OUTPUT_4_1_ext.copy()
# OUTPUT_4_2_ext_2 = OUTPUT_4_2_ext.copy()
# OUTPUT_8_ext_2 = OUTPUT_8_ext.copy()


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
    
    print(df_merged.columns)
    
    # df_merged = df.copy()
    
    # Convert all columns to numeric except district and date columns
    print(f"Converting {df_name} columns to numeric...")
    for col in df_merged.columns:
        if col not in ['district', 'death_date', 'special_elections_date']:
            df_merged[col] = pd.to_numeric(df_merged[col], errors='ignore')
    
    # Ensure date columns are datetime
    for col in df_merged.columns:
        if col in ['death_date', 'special_elections_date']:
            df_merged[col] = pd.to_datetime(df_merged[col], errors='ignore')
    

    # if df_name == 'OUTPUT_2_ext':
    #     first_cols = ['district', 'cycle', 'treat_1', 'treat_2', 'treat_3', 'death_date', 'special_elections_date']
    # else:
    #     first_cols = ['district', 'cycle', ]

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


# Adding 'real_data' and 'territorial'
OUTPUT_3_ext = pd.merge(
    OUTPUT_2_ext[['district', 'cycle', 'real_data', 'territorial']],
    OUTPUT_3_ext,
    on = ['district', 'cycle'],
    how = 'left'
    )

OUTPUT_4_1_ext = pd.merge(
    OUTPUT_2_ext[['district', 'cycle', 'real_data', 'territorial']],
    OUTPUT_4_1_ext,
    on = ['district', 'cycle'],
    how = 'left'
    )

OUTPUT_4_2_ext = pd.merge(
    OUTPUT_2_ext[['district', 'cycle', 'real_data', 'territorial']],
    OUTPUT_4_2_ext,
    on = ['district', 'cycle'],
    how = 'left'
    )

OUTPUT_8_ext = pd.merge(
    OUTPUT_2_ext[['district', 'cycle', 'real_data', 'territorial']],
    OUTPUT_8_ext,
    on = ['district', 'cycle'],
    how = 'left'
    )


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
    ['real_data', 'territorial',
     'treat_1', 'treat_2', 'treat_3', 'death_date',
     'special_elections_date']
    ) # no duplicate columns when merging with OUTPUT_2

OUTPUT_4_1_ext = OUTPUT_4_1_ext.drop(
    columns = 
    ['real_data', 'territorial',
     'treat_1', 'treat_2', 'treat_3', 'death_date',
     'special_elections_date']
    ) # no duplicate columns when merging with OUTPUT_2

OUTPUT_4_2_ext = OUTPUT_4_2_ext.drop(
    columns = 
    ['real_data', 'territorial',
     'treat_1', 'treat_2', 'treat_3', 'death_date',
     'special_elections_date']
    ) # no duplicate columns when merging with OUTPUT_2

OUTPUT_8_ext = OUTPUT_8_ext.drop(
    columns = 
    ['real_data', 'territorial',
     'treat_1', 'treat_2', 'treat_3', 'death_date',
     'special_elections_date']
    ) # no duplicate columns when merging with OUTPUT_2

#%%


def get_amount_sum(df, district=None, cycle=None, electiontype=None):
    """
    Get the sum of 'amount' variable for a given dataframe with optional filtering.
    
    # Get total amount for CA05 across all cycles
    get_amount_sum(OUTPUT_1, district='CA05')
    
    # Get total amount for cycle 1988 across all districts
    get_amount_sum(OUTPUT_1, cycle=1988)
    
    # Get total amount for CA05 in 1988
    get_amount_sum(OUTPUT_1, district='CA05', cycle=1988)
        
    # Get total amount for multiple districts and cycles
    get_amount_sum(OUTPUT_1, district=['CA05', 'NY01'], cycle=[1984, 1988])
    """
    
    # Start with a copy to avoid modifying original
    filtered_df = df.copy()
    
    # Apply district filter if provided
    if district is not None:
        if isinstance(district, list):
            filtered_df = filtered_df[filtered_df['district'].isin(district)]
        else:
            filtered_df = filtered_df[filtered_df['district'] == district]
    
    # Apply cycle filter if provided
    if cycle is not None:
        if isinstance(cycle, list):
            filtered_df = filtered_df[filtered_df['cycle'].isin(cycle)]
        else:
            filtered_df = filtered_df[filtered_df['cycle'] == cycle]
            
    # Apply election type filter if provided
    if electiontype is not None:
        if isinstance(electiontype, list):
            filtered_df = filtered_df[filtered_df['election.type'].isin(electiontype)]
        else:
            filtered_df = filtered_df[filtered_df['election.type'] == electiontype]
    
    # Check if 'amount' column exists
    if 'amount' not in filtered_df.columns:
        print("Warning: 'amount' column not found in dataframe")
        return 0
    
    # Calculate and return sum
    total = filtered_df['amount'].sum()
    
    # Print summary
    print("Filters applied:")
    print(f"  District: {district if district is not None else 'All'}")
    print(f"  Cycle: {cycle if cycle is not None else 'All'}")
    print(f"  Election Type: {electiontype if electiontype is not None else 'All'}")
    print(f"  Number of rows: {len(filtered_df):,}")
    print(f"  Total amount: ${total:,.2f}")
    
    return total

# get_amount_sum(OUTPUT_1, district=['CA32'], cycle=[2002], electiontype=['G', 'S'])


#%%

## FINAL MERGE OF ALL OUTPUTS


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


print("Also, merging variables from _ext datasets (OUTPUT_2_ext, OUTPUT_3_ext, OUTPUT_4_1_ext, OUTPUT_4_2_ext, and OUTPUT_8_ext)")

# Define the gen_np variables for OUTPUT_2, 3, 4_1, 4_2
gen_np_vars_outputs_2_to_4 = [
    'total_amount_gen_np',
    'tran_count_gen_np',
    'total_amount_gen_np_without_LTS1',
    'tran_count_gen_np_without_LTS1',
    'total_amount_dem_gen_np',
    'tran_count_dem_gen_np',
    'total_amount_dem_gen_np_without_LTS1',
    'tran_count_dem_gen_np_without_LTS1',
    'total_amount_rep_gen_np',
    'tran_count_rep_gen_np',
    'total_amount_rep_gen_np_without_LTS1',
    'tran_count_rep_gen_np_without_LTS1'
]

# Define the gen_np variables for OUTPUT_8
gen_np_vars_output8 = [
    'hedging_money_general_np_corp',
    'avg_counting_hedging_np_corp',
    'hedging_money_general_np_ind',
    'avg_counting_hedging_np_ind',
    'hedging_money_general_np_smallind',
    'avg_counting_hedging_np_smallind',
]

# Dictionary mapping datasets to their respective variable lists
ext_datasets = {
    'OUTPUTS_2_ext': (OUTPUT_2_ext, gen_np_vars_outputs_2_to_4),
    'OUTPUT_3_ext': (OUTPUT_3_ext, gen_np_vars_outputs_2_to_4),
    'OUTPUT_4_1_ext': (OUTPUT_4_1_ext, gen_np_vars_outputs_2_to_4),
    'OUTPUT_4_2_ext': (OUTPUT_4_2_ext, gen_np_vars_outputs_2_to_4),
    'OUTPUT_8_ext': (OUTPUT_8_ext, gen_np_vars_output8)
}

# Merge each dataset
for dataset_name, (dataset, var_patterns) in ext_datasets.items():
    print(f"  Merging {dataset_name}...")
    
    # Select columns that contain any of the variable patterns
    cols_to_keep = ['district', 'cycle']
    for col in dataset.columns:
        if any(pattern in col for pattern in var_patterns):
            print(f"    From {dataset_name}, keeping variable: {col}")
            cols_to_keep.append(col)
    
    # Check if we found all expected variables
    # Note: We expect at least as many columns as patterns (could be more with suffixes)
    expected_count = len(var_patterns)
    actual_count = len(cols_to_keep) - 2  # Subtract 'district' and 'cycle'
    
    if actual_count >= expected_count:
        print(f"Found {actual_count} variables (expected at least {expected_count})")
    else:
        print(f"Warning: Found only {actual_count} variables, expected at least {expected_count}")
        print(f"    Missing patterns might include: {var_patterns}")
    
    # Merge with the current result
    OUTPUT_1_final_collapsed = pd.merge(
        OUTPUT_1_final_collapsed,
        dataset[cols_to_keep],
        on=['cycle', 'district'],
        how='left'
    )
    
    print(f"\nMerged {len(cols_to_keep) - 2} variables from {dataset_name}")
    print()

print("Merge complete!")
print(f"OUTPUT_1_final_collapsed now has {len(OUTPUT_1_final_collapsed.columns)} columns")


    
#%%

# Manually creating dictionary of variables

print("Identifying /outputs_scripts/create_dict.py...")

# Check if the file exists
create_dict_path = os.path.join(code_folder, "outputs_scripts", "create_dict.py")

if os.path.exists(create_dict_path):
    print(f"Found: {create_dict_path}")
    sys.path.insert(0, os.path.join(code_folder, "outputs_scripts"))
    from create_dict import OUTPUT_1_final_collapsed_dict_df, OUTPUT_1_final_collapsed_ext_dict_df
    print("Successfully imported dictionaries from create_dict.py")
    print(" -> OUTPUT_1_final_collapsed_dict_df")
    print(" -> OUTPUT_1_final_collapsed_ext_dict_df")
else:
    print(f"ERROR: File not found at {create_dict_path}")
    print("Please check that the file exists and the path is correct.")
    sys.exit(1)

#%%

print("\nFinal step: Saving collapsed OUTPUTS...")
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