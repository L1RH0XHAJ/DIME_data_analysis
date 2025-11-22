#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 12 11:18:52 2025

@author: lirhoxhaj
"""

### LIBRARIES

# pip install polars

import os
import inspect
# import polars as pl
import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns

#%%

### SETUP

# These lines will get the location of this file '\code\main.py'. Please ensure file is saved in folder \code. 

# This line does not work in interactive environment (e.g., Jupyter Notebook or interpreters like IDLE)
# code_folder = os.path.dirname(os.path.abspath(__file__))

# Get the directory where the current script is located
# code_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

# If all fails define working folder manually and run the lines here
code_folder = "/Users/lirhoxhaj/Library/CloudStorage/OneDrive-ImperialCollegeLondon/Desktop/RA/Tommaso/Contributions_Paper/working_folder_lir/code"
code_folder = r"C:\Users\lhoxhaj\OneDrive - Imperial College London\Desktop\RA\Tommaso\Contributions_Paper\working_folder_lir\code"

# This is your working folder where folders '\code' and '\data' are saved
parent_folder = os.path.dirname(code_folder)

data_folder = os.path.join(parent_folder, "data")

print("Parent folder:", parent_folder, "\n")
print("Code folder:", code_folder, "\n")
print("Data folder:", data_folder, "\n")

#%%

# TRY OUT THIS CODE !

# contribDB_2022 = pd.read_csv(os.path.join(data_folder, "contribDB_2022.csv"), chunksize=50000)
# for idx, chunk in enumerate(contribDB_2022):
#     if idx == 0:
#         nr = 1
#         df = pd.DataFrame(chunk)
#         for i in chunk.columns:
#             print(nr, i)
#             nr += 1
#     else:
#         break

# change this !!

# cols = pd.DataFrame(columns=["final_price", "image_url", "title", "url", "categories"])
# cols.to_csv("modified_data.csv", mode="a", encoding="utf-8", index=False)

# data = pd.read_csv("bd_amazon.csv", chunksize= 50000, usecols=["final_price", "image_url", "title", "url", "categories"])

# for idx, chunk in enumerate(data):
#     chunk.to_csv("modified_data.csv", mode="a", encoding="utf-8", index=False, header=False)






#%%

### READING DATASETS

# This for loop saves all datasets in a dictionary, we can refer to specific datasets as 

## INPUT 1: DIME database on contributions

contribDB_dict = {}
# for year in [1980, 1982, 1984, 1986, 1988, 1990, 1992, 2006, 2008]:
for year in range(1980, 2006, 2):
    file_path = Path(data_folder) / f"contribDB_{year}.csv"
    try:
        print(f"\nReading contribDB_{year}.csv ...")
        # excluding columns: 
        #    'contributor.lname', 'contributor.fname', 'contributor.mname',
        #    'contributor.suffix', 'contributor.title', 'contributor.ffname', 
        #    'contributor.address', 'contributor.city', 'contributor.state', 'contributor.zipcode', 
        #    'contributor.occupation', 'contributor.employer', 'occ.standardized', 'is.corp', 
        #    'recipient.party', 'recipient.type', 'recipient.state',
        #    'censustract', 
        #    'efec.memo', 'efec.memo2', 'efec.transaction.id.orig' 
        #    'bk.ref.transaction.id', 'efec.org.orig', 'efec.comid.orig', 
        #    'efec.form.type', 'excluded.from.scaling',

        df = pd.read_csv(
            file_path, 
            encoding='latin-1',
            usecols = ['cycle', 'transaction.id', 'transaction.type', 'amount', 'date',
                   'bonica.cid', 'contributor.name', 'contributor.type',
                   'contributor.gender', 'recipient.name',
                   'bonica.rid', 'seat', 'election.type', 'gis.confidence',
                   'contributor.district', 'latitude', 'longitude', 
                   'contributor.cfscore', 'candidate.cfscore'] 
            )
        
        # Filter for House seats and corporate / individual contributions
        df = df[
            ((df['seat'] == 'federal:house') & (df['contributor.type'] == 'C')) |
            ((df['seat'] == 'federal:house') & (df['contributor.type'] == 'I'))
        ]
        df = df[df['amount'] >= 0]
        df = df.dropna(subset = ['amount', 'date']) # dropping missing values for these vars
        # dropping contributions with absurd dates after 2024

        df['date'] = pd.to_datetime(df['date'], format = '%Y-%m-%d', errors = 'coerce') # need to use errors = 'coerce' to include incorrect dates
                
        df = df[
            (df['date'].dt.year - 2 <= df['cycle']) &
            # (df['date'].dt.year <= df['cycle'] + 2)
        ]
        
        contribDB_dict[year] = df
    except FileNotFoundError:
        print(f"Warning: File not found for year {year}")
    except pd.errors.EmptyDataError:
        print(f"Warning: Empty file for year {year}")

# to make sure they were all read correctly
for year in contribDB_dict:
    print(f"\nData for year {year}:")
    print(contribDB_dict[year][['cycle', 'bonica.rid']].head(3))
    print(contribDB_dict[year].shape)

# Appending all datasets in one place
print("Appending all datasets...")
contribDB_all = pd.concat(contribDB_dict.values(), axis=0, ignore_index=True)
# Checking rows that have different cycle and date years
mismatch_rows = contribDB_all[contribDB_all['cycle'] != contribDB_all['date'].dt.year]
print(mismatch_rows['cycle'].describe())
print(mismatch_rows['date'].dt.year.describe())


## INPUT 2: Recipients’ database

print("Reading recipients data...")

# excluding columns:
#    'lname', 'ffname', 'fname', 'mname', 
#    'bonica.cid',     CONFUSED WITH THE CONTRIBUTIONS DATA
#    'distcyc', 'contributor.cfscore',
#    'recipient.type', NOT NEEDED IF FILTERED FOR FEDERAL:HOUSE
#    'igcat',          TOO MANY MISSING VALUES
#    .

recipients = pd.read_csv(
    data_folder + "/dime_recipients_1979_2024.csv", 
    encoding='latin-1',
    usecols = [
        'election', 'cycle', 'fecyear', 'bonica.rid', 'name',
        'title', 'suffix', 'party',
        'state', 'seat', 'district', 'ico.status', 'cand.gender',
        
        'recipient.cfscore', 'recipient.cfscore.dyn', 
        'dwdime', 'dwnom1', 'dwnom2', 'ps.dwnom1', 'ps.dwnom2', 'irt.cfscore',
        'composite.score', 
        
        'num.givers', 'num.givers.total', 'total.receipts',
        'total.disbursements', 'total.indiv.contribs', 'total.unitemized',
        'total.pac.contribs', 'total.party.contribs',
        'total.contribs.from.candidate', 
        
        'ind.exp.support', 'ind.exp.oppose',
        'prim.vote.pct', 'pwinner', 'gen.vote.pct', 'gwinner', 's.elec.stat',
        'r.elec.stat', 'district.pres.vs', 'fec.cand.status', 
        
        'comtype', 'ICPSR', 'ICPSR2', 'Cand.ID', 'FEC.ID', 'NID',
        'before.switch.ICPSR', 'after.switch.ICPSR', 'party.orig',
        'nimsp.party', 'nimsp.candidate.ICO.code', 'nimsp.district',
        'nimsp.office', 'nimsp.candidate.status'
        ]
    )

recipients = recipients[recipients['seat'] == 'federal:house']

recipients['party'] = pd.to_numeric(recipients['party'], errors='coerce')  # need to use here as well, since some numbers were saved as string

# saving district values to .txt file for further analysis
all_recp_dist = recipients['district'].unique()
all_recp_dist_df = pd.DataFrame(all_recp_dist)
np.savetxt(os.path.join(data_folder, 'district_in_federalhouse_recipients.txt'), 
           all_recp_dist, 
           fmt='%s')  # Use string format instead of default float format

short_districts = []
for dist in recipients['district'].unique():
    if isinstance(dist, str) and len(dist) < 4:
        print("Districts that are less than four characters:")
        print(dist)
        short_districts.append(dist)
    elif not isinstance(dist, str):
        print("Non-string district value:")
        print(dist)
        short_districts.append(dist)
        
recp_short_dist = recipients[recipients['district'].isin(short_districts)]
recp_short_dist.to_csv(os.path.join(data_folder, "district_in_federalhouse_recipients_problem.csv"), index = False)

# i = 1
# for idx, row in recp_short_dist.iterrows():
#     print(i)
#     print(row['name'])
#     print(row['cycle'])
#     print(row['district'])
#     print()
#     i += 1
    
# Manually replacing values
rec_dist_dict = {
    # Included candidates means they ran for House of Rep in this cycle; if excluded, they did run (e.g. ran for Governor)   
    "bonior, david e": {"district": "MI10", "cycle": 2002},
    "holloway, clyde c": {"district": "LA08", "cycle": 2004},
    "jefferson, william j": {"district": "LA02", "cycle": 2000},
    "clayburgh, rick": {"district": "ND01", "cycle": 2004},
    "morenoff, dan": {"district": np.nan, "cycle": 2012},    # needs to be dropped, Texas house of reps, not US (represented District 114)
    "ross, michael p": {"district": np.nan, "cycle": 2012},    # needs to be dropped, decided not to run    
    "rainwater, marilyn": {"district": "OK01", "cycle": 2014},
    "vivoni, miguel hernandez": {"district": "PR01", "cycle": 2014},
    "colantuono, thomas p": {"district": "NH01", "cycle": 2000},
    "lindbeck, steve": {"district": "AK01", "cycle": 2016},
    "hunkin finau, vaitinasa salu": {"district": "AS01", "cycle": 2016},
    "whalen iii, john j": {"district": "DE01", "cycle": 2024}
}   
    
for name, data in rec_dist_dict.items():
    # Updating recp_short_dist
    mask_short = (recp_short_dist['name'].str.contains(name, case=False, na=False) & 
                 (recp_short_dist['cycle'] == data['cycle']))    
    recp_short_dist.loc[mask_short, 'district'] = data['district']
    
    # Updating recipients dataframe
    mask_recipients = (recipients['name'].str.contains(name, case=False, na=False) & 
                       (recipients['cycle'] == data['cycle']))
    recipients.loc[mask_recipients, 'district'] = data['district']
    
    
recp_short_dist.to_csv(os.path.join(data_folder, "district_in_federalhouse_recipients_solved.csv"), index = False)

recipients = recipients[~recipients['district'].isna()]



## INPUT 3: Self-constructed dataset of deaths, using special_elections.csv (see convert_html_to_csv.py)
print("Reading special elections data...")
special_elections = pd.read_csv(data_folder + "/special_elections_final.csv", encoding='latin-1')

# Looking at which district have multiple deaths
for dist in special_elections['district'].unique():
    df_filter = special_elections[(special_elections['cause_vacancy'] == 'Death') & (special_elections['district'] == dist)]
    if len(df_filter) > 1:
        print(f"Number of deaths in district {dist}: {len(df_filter)}")
        print(df_filter[["resign_member", "spec_election_date"]])
        print()
    else:
        continue
# VERDICT: Districts that have multiple deaths of incumbents: CA05, TX18, VA01, VA04, NJ10 


## INPUT 4: Manually created general elections dataset for each year (cycle) in our dataset
print("Reading elections dates data...")
election_dates = pd.read_csv(data_folder + "/election_dates.csv", encoding='latin-1')


#%%

### MERGING DATASETS


## MERGE 1: Contributions dataset (Input 1) with recipients dataset (Input 2) using id (“bonica.rid”) and “cycle” as keys

print("\nMERGE 1: Merging contributions and recipients data...")
merged_df_1 = pd.merge(
    contribDB_all, 
    recipients,
    on=['bonica.rid', 'cycle'],  
    how='inner'                  # only keep matching rows (use option 'outer' to keep unmatched rows as well)
)

# check how many rows were matched (the sum of these two should match to the total!)
nonna_rows = len(merged_df_1[~merged_df_1['name'].isna()]) 
na_rows = len(merged_df_1[merged_df_1['name'].isna()]) 
print("Total number of rows in merged_df_1 =", len(merged_df_1))
print("  - Number of merged rows", nonna_rows)
print("  - Number of missing rows:", na_rows)


## MERGE 2: Merged dataset from MERGE 1 and special_elections using 'district' (do not drop all observations)

print("\nMERGE 2: Merging with special elections data...")
merged_df_2 = pd.merge(
    merged_df_1, 
    special_elections,
    on=['district'],  
    how='outer'                  # keeping unmatched rows
)

# check how many rows were matched (the sum of these two should match to the total!)
nonna_rows = len(merged_df_2[~merged_df_2['resign_member'].isna()]) 
na_rows = len(merged_df_2[merged_df_2['resign_member'].isna()]) 
print("Total number of rows in merged_df_2 =", len(merged_df_2))
print("  - Number of merged rows", nonna_rows)
print("  - Number of missing rows:", na_rows)


## MERGE 3: Get election datasets (used for creating treatment dummies and dealing with complex districts)

print("\nMERGE 3: Merging with election dates...")
merged_df_3 = pd.merge(
    merged_df_2,
    election_dates,
    on='cycle',
    how='left'
)

# check how many rows were matched (the sum of these two should match to the total!)
nonna_rows = len(merged_df_3[~merged_df_3['election_date_in_cycle'].isna()]) 
na_rows = len(merged_df_3[merged_df_3['election_date_in_cycle'].isna()]) 
print("Total number of rows in merged_df_3 =", len(merged_df_3))
print("  - Number of merged rows", nonna_rows)
print("  - Number of missing rows:", na_rows) # unmatched row because district not in merged_df_1!


#%%

### CREATING NEW VARIABLES

# - later_than_special
# - days_to_nearest_death


## 1. 

## New dummy variable: “later_than_special”, =1 if the contribution is given in the same cycle of the death, but later than the special election (these are contributions given to the general election).

merged_df_3['cycle'] = pd.to_numeric(merged_df_3['cycle'], errors='coerce')  # will convert None values to NaN
merged_df_3['date'] = pd.to_datetime(merged_df_3['date'], errors = 'coerce') # need to use errors = 'coerce' to include incorrect dates
merged_df_3['spec_election_date'] = pd.to_datetime(merged_df_3['spec_election_date'], errors = 'coerce') # need to use errors = 'coerce' to include incorrect dates
merged_df_3['resign_date'] = pd.to_datetime(merged_df_3['resign_date']) # no errors here, because these dates were created manually in special_elections.csv!
merged_df_3['death_date'] = pd.to_datetime(merged_df_3['death_date']) # no errors here, because these dates were created manually in special_elections.csv!
merged_df_3['election_date_in_cycle'] = pd.to_datetime(merged_df_3['election_date_in_cycle']) # no errors here, because these dates were created manually in election_dates.csv!


merged_df_3['later_than_special'] = 0  # Initialize with zeros

# Set to 1 where:
#   1. Contribution year is between cycle-2 and cycle+2
#   2. Contribution date is later than special election date


mask = (
    # Filtering for within election cycle
    (merged_df_3['date'].dt.year >= merged_df_3['cycle'] - 2) &
    (merged_df_3['date'].dt.year < merged_df_3['cycle'] + 1) &
    # Filtering for within election cycle, in terms of years between spec_election_date and year
    (merged_df_3['date'].dt.year >= merged_df_3['spec_election_date'].dt.year - 1) &
    (merged_df_3['date'].dt.year < merged_df_3['spec_election_date'].dt.year + 1) &
    # Main condition!
    (merged_df_3['date'] >= merged_df_3['spec_election_date'])
)

# mask = merged_df_3['date'] > merged_df_3['spec_election_date']

# Apply the mask
merged_df_3.loc[mask, 'later_than_special'] = 1
# Handle NaN values in spec_election_date (keep as 0)
merged_df_3.loc[merged_df_3['spec_election_date'].isna(), 'later_than_special'] = 0

print("Value counts for later than special:")
print(merged_df_3['later_than_special'].value_counts())

# Testing for election.type == 'S'
merged_df_3['later_than_special_S'] = 0
merged_df_3.loc[merged_df_3['election.type'] == 'S', 'later_than_special_S'] = 1

print("Value counts for later_than_special_S:")
print(merged_df_3['later_than_special_S'].value_counts())

# Discrepancy! Gets resolved (or significantly smaller when duplicates contributions are dropped)

## 2.

## For each contribution in a district where an incumbent died:
    
#   2. Create a variable “days_to_nearest_death” which counts the number of days between the contribution (variable date in Input 1 dataset) and the date of the nearest death in the district (from Input 3 dataset)
#      - <0 for contributions earlier than death, >0 for contributions later than death
#      - Be careful! There are districts where more than one incumbent died (here, “nearest” death is key!)



# For each district-cycle combination, find all death dates
district_cycle_deaths = merged_df_3[merged_df_3['resign_member'].notna()][
    ['district', 'cycle', 'death_date']].drop_duplicates()



# # Initialize the days_to_nearest_death column
# merged_df_3['days_to_nearest_death'] = np.nan

# # For each district with deaths
# for district in district_cycle_deaths['district'].unique():
#     # Get all cycles for this district
#     district_cycles = district_cycle_deaths[district_cycle_deaths['district'] == district]['cycle'].unique()
    
#     # For each contribution in the district
#     district_mask = merged_df_3['district'] == district
#     for idx in merged_df_3[district_mask].index:
#         contrib_date = merged_df_3.loc[idx, 'date']
#         contrib_cycle = merged_df_3.loc[idx, 'cycle']
        
#         if pd.isna(contrib_date):
#             continue
            
#         # Get relevant death dates (same cycle or next cycle)
#         relevant_deaths = district_cycle_deaths[
#             (district_cycle_deaths['district'] == district) & 
#             (district_cycle_deaths['cycle'].isin([contrib_cycle, contrib_cycle + 2]))  # include current and next cycle
#         ]['death_date']
        
#         # Calculate days to all relevant deaths
#         days_to_deaths = [(contrib_date - death_date).days for death_date in relevant_deaths]
        
#         # Find the nearest death (smallest absolute difference)
#         if days_to_deaths:
#             nearest_days = min(days_to_deaths, key=abs)
#             merged_df_3.loc[idx, 'days_to_nearest_death'] = nearest_days




# We calculate days to nearest death for all rows, and the absolute value as well
merged_df_3['days_to_nearest_death'] = (merged_df_3['date'] - merged_df_3['death_date']).dt.days
merged_df_3['abs_days_to_death'] = merged_df_3['days_to_nearest_death'].abs()
# Then, we sort by some variables, as well as absolute death, which brings up the contributions closest to death dates first

duplicate_columns = ['cycle', 'date', 'transaction.id', 'abs_days_to_death']
merged_df_3 = merged_df_3.sort_values(
    by=duplicate_columns,
    ascending=[True, True, True, True]  # 4 values to match the 4 columns
)

merged_df_3_VA01 = merged_df_3[merged_df_3['district'] == 'VA01']


# Now keep only the first occurrence of each unique contribution (closest to death)
# This allows us to have unique rows for each contributions (rather than duplicates), even for problematic rows
merged_df_3 = merged_df_3.drop_duplicates(
    subset=['cycle', 'date', 'transaction.id'],
    keep='first' 
)

merged_df_3_VA01_2 = merged_df_3[merged_df_3['district'] == 'VA01']

dropped_indices = set(merged_df_3_VA01.index) - set(merged_df_3_VA01_2.index)
dropped_rows = merged_df_3_VA01.loc[list(dropped_indices)]


merged_df_3_CA05 = merged_df_3[merged_df_3['district'] == 'CA05']



# Verify results with some examples

def print_district_summary(district):
    print("\nExample of multiple deaths in a district:")
    print(f"\nDeaths in district {district}:")
    print(district_cycle_deaths[district_cycle_deaths['district'] == district][['cycle', 'death_date']].sort_values('cycle'))
    print("\nSample contributions and their days to nearest death:")
    print("First 10 contributions:")
    print(merged_df_3[(merged_df_3['district'] == district) & (merged_df_3['days_to_nearest_death'].notna())][['cycle', 'date', 'amount', 'death_date', 'days_to_nearest_death']].sort_values('date').head(10))
    print("\nLast 10 contributions:")
    print(merged_df_3[(merged_df_3['district'] == district) & (merged_df_3['days_to_nearest_death'].notna())][['cycle', 'date', 'amount', 'death_date', 'days_to_nearest_death']].sort_values('date').tail(10))
    print()
    print("=" * 20)
    print()
    
# Print examples
print("\nSINGLE DEATH DISTRICT:")
print_district_summary("AL03")
print("\nMULTIPLE DEATH DISTRICT:")
print_district_summary("TX18") 


# Summary stats for days_to_nearest_death and histograms
print(merged_df_3['days_to_nearest_death'].describe())

plt.figure(figsize=(10, 6))
sns.histplot(data=merged_df_3, x='days_to_nearest_death', bins=30)
plt.axvline(x=0, color='red', linewidth=3, label = 'Difference = 0')
plt.axvline(x=merged_df_3['days_to_nearest_death'].mean(), color='blue', linewidth=3, label = 'Mean value')  # adds vertical line
plt.title('Distribution of days_to_nearest_death')
plt.xlabel('Value')
plt.ylabel('Count')
plt.legend()
plt.show()

#%%

# Checking discrepancy between later_than_special and 

# let's check for later_than_special == 1
merged_df_3_lts1 = merged_df_3[merged_df_3['later_than_special'] == 1][
    ['cycle', 'transaction.id', 'transaction.type', 'amount', 'date',
       'bonica.cid', 'contributor.name', 'contributor.type',
       'contributor.gender', 'recipient.name', 'bonica.rid',
       'spec_election_year', 'resign_member', 'resign_party', 'resign_date',
       'death_date', 'spec_election_date', 'cause_vacancy', 'death_unexpected',
       'resign_year', 'spec_election_Original_candidate',
       'spec_election_Cause_of_vacancy', 'spec_Winner', 'original_district',
       'election_date_in_cycle', 'later_than_special', 'later_than_special_S']
    ]
    
print("Value counts for later_than_special_S when later_than_special == 1:")
print(merged_df_3_lts1['later_than_special_S'].value_counts()) 

# different cases

merged_df_3_diff = merged_df_3[merged_df_3['later_than_special'] != merged_df_3['later_than_special_S']][
    ['cycle', 'transaction.id', 'transaction.type', 'amount', 'date',
       'bonica.cid', 'contributor.name', 'contributor.type',
       'contributor.gender', 'recipient.name', 'bonica.rid',
       'spec_election_year', 'resign_member', 'resign_party', 'resign_date',
       'death_date', 'spec_election_date', 'cause_vacancy', 'death_unexpected',
       'resign_year', 'spec_election_Original_candidate',
       'spec_election_Cause_of_vacancy', 'spec_Winner', 'original_district',
       'election_date_in_cycle', 'later_than_special', 'later_than_special_S']
    ]
print("Value counts for later_than_special_S and later_than_special when they are not equal to each other:")
print(merged_df_3_diff['later_than_special_S'].value_counts()) 
print(merged_df_3_diff['later_than_special'].value_counts())

merged_df_3_diff.to_csv(os.path.join(data_folder, "discrepancy_btw_lts_dummies.csv"), index = False)

#%%

## 3.

## For each contribution in a district where an incumbent died:

#   3. Create two dummy variable "treatment", 
#      3.1. one for all districts with a single death of an incumbent in "special_elections" data where they died, not resigned
#           - treatment == 0 for all contributions (rows) coming before death of incumbent; == 1 for contributions coming post-death
#      3.2. one for districts that have multiple deaths
#           - for 'problematic' districts (multiple deaths of incumbents), 
#               - we assign 0 and 1 accordingly, but also have a 'reset' at every election cycle
#               - we manually find election dates (or ask AI to do this for us) from 1980 to 2024
#               - assuming multiple dead incumbents in district, we will have 0 values before death of FIRST incumbent, then 1 values after death of FIRST incumbent UNTIL NEXT ELECTION DATE where values reset to 0



# Count deaths per district and identify single/multiple death districts
death_counts = merged_df_3[merged_df_3['cause_vacancy'] == 'Death'].groupby('district')['resign_member'].nunique().reset_index()
single_death_districts = death_counts[death_counts['resign_member'] == 1]['district'].tolist()
multiple_death_districts = death_counts[death_counts['resign_member'] > 1]['district'].tolist()

# Create treat_1 (simple case - single death districts)
merged_df_3['treat_1'] = 0
for district in single_death_districts:
    # First check if there are any non-null death dates for this district
    district_death_dates = merged_df_3[
        (merged_df_3['district'] == district) & 
        (merged_df_3['death_date'].notna())
    ]['death_date']
    
    # Only proceed if there's at least one death date
    if not district_death_dates.empty:
        death_date = district_death_dates.iloc[0]
        
        merged_df_3.loc[
            (merged_df_3['district'] == district) & 
            (merged_df_3['date'] > death_date), 
            'treat_1'] = 1
    else:
        print(f"Warning: No death date found for district {district}")



# Create treat_2 (complex case - multiple death districts)
merged_df_3['treat_2'] = 0

# Process each multiple-death district separately
for district in multiple_death_districts:
    # Get all deaths for this district
    district_deaths = merged_df_3[
        (merged_df_3['district'] == district) & 
        (merged_df_3['death_date'].notna())
    ][['death_date', 'cycle']].drop_duplicates().sort_values('death_date')
    
    # Get all election dates, so that we know when a reset of values happens
    election_dates = merged_df_3['election_date_in_cycle'].dropna().unique()
    #election_dates = pd.to_datetime(election_dates).sort_values()
    
    # For each contribution in the district
    district_mask = merged_df_3['district'] == district
    for idx in merged_df_3[district_mask].index:
        contrib_date = merged_df_3.loc[idx, 'date']
        if pd.isna(contrib_date):
            continue
        
        # Default state is 0
        state = 0
        
        # Check each death to determine correct state
        for _, death_row in district_deaths.iterrows():
            death_date = death_row['death_date']
            
            # If contribution is after death
            if contrib_date > death_date:
                # Find next election after this death
                next_election = election_dates[election_dates > death_date].min() if any(election_dates > death_date) else pd.NaT
                
                # If no election after this death or contribution is before next election
                if pd.isna(next_election) or contrib_date < next_election:
                    state = 1
                    break
        
        merged_df_3.loc[idx, 'treat_2'] = state        



# checking ...
def plot_scatter(picked_district, treatment):
    merged_df_3_sample = merged_df_3[merged_df_3['district'] == picked_district]
    death_dates = merged_df_3_sample['death_date'].dropna().unique()
    
    # Define election_dates here so it's available in both branches
    election_dates = merged_df_3_sample['election_date_in_cycle'].dropna().unique()
    
    if treatment == 'treat_1':
        # Plot for treat_1
        plt.figure(figsize=(10, 6))
        plt.scatter(merged_df_3_sample['date'], merged_df_3_sample['treat_1'], c='blue', label = 'Contributions', s = 0.5)
        # Add a vertical line for each death date
        for i, death_date in enumerate(death_dates):
            plt.axvline(x=death_date, color='purple', linewidth=3, 
                        label='Death date' if i == 0 else "_")  # Only one label in legend
        
        # Add lines for election dates if you have them
        for i, election_date in enumerate(election_dates):
            plt.axvline(x=election_date, color='green', linestyle='--', linewidth=2,
                        label='Election date' if i == 0 else "_")
        
        plt.title(f'Scatterplot: treat_1 across time (for {picked_district})')
        plt.xlabel('Date')
        plt.ylabel('Treatment')
        plt.legend()
        plt.show()
    
    else:
        # Similar approach for treat_2
        plt.figure(figsize=(10, 6))
        plt.scatter(merged_df_3_sample['date'], merged_df_3_sample['treat_2'], c='red', label = 'Contributions', s = 0.5)
        for i, death_date in enumerate(death_dates):
            plt.axvline(x=death_date, color='purple', linewidth=3, 
                        label='Death date' if i == 0 else "_")
        
        for i, election_date in enumerate(election_dates):
            plt.axvline(x=election_date, color='green', linestyle='--', linewidth=2,
                        label='Election date' if i == 0 else "_")
        
        plt.title(f'Scatterplot: treat_2 across time (for {picked_district})')
        plt.xlabel('Date')
        plt.ylabel('Treatment')
        plt.legend()
        plt.show()    

# single death example
plot_scatter('AL03', 'treat_1')
# multiple death example
plot_scatter('VA01', 'treat_2')
plot_scatter('CA05', 'treat_2')


    

# Testing...
def print_treatment_example(district, treatment_var):
    print(f"\nExample for district {district}:")
    print("Deaths in this district:")
    print(merged_df_3[
        (merged_df_3['district'] == district) & 
        (merged_df_3['death_date'].notna())
    ][['cycle', 'death_date']].drop_duplicates().sort_values('cycle'))
    
    print(f"\nFirst 10 observations ({treatment_var}):")
    print(merged_df_3[
        (merged_df_3['district'] == district) & (merged_df_3['days_to_nearest_death'].notna())
    ][['date', 'cycle', 'days_to_nearest_death', 'election_date_in_cycle', treatment_var]].sort_values('date').head(10))
    print("\nLast 10 observations:")
    print(merged_df_3[
        (merged_df_3['district'] == district) & (merged_df_3['days_to_nearest_death'].notna())
    ][['date', 'cycle', 'days_to_nearest_death', 'election_date_in_cycle', treatment_var]].sort_values('date').tail(10))

# Print examples
print("\nSINGLE DEATH DISTRICT:")
print_treatment_example("IL21", 'treat_1')
print("\nMULTIPLE DEATH DISTRICT:")
print_treatment_example("CA05", 'treat_2')



#%%

### CLEANING DATASET FROM DUPLICATES

# Important to know that we have multiple merged rows for districts that have multiple deaths

# Solution for entire datset (dealing with districts that have multiple deaths)

OUTPUT_1 = merged_df_3.sort_values(
    ['transaction.id', 'district', 'days_to_nearest_death']
).drop_duplicates( 
    subset=['transaction.id', 'district'], 
    keep='first'
)

    
duplicate_columns = ['cycle', 'date', 'bonica.cid', 'bonica.rid', 'abs_days_to_death', 'resign_member']

# Finding what rows are being dropped
dropped_indices = set(merged_df_3.index) - set(OUTPUT_1.index)
dropped_rows = merged_df_3.loc[list(dropped_indices)]    
dropped_rows_CA05 = dropped_rows[dropped_rows['district'] == 'CA05']

# Using duplicates function
duplicates = merged_df_3[merged_df_3.duplicated(subset=['transaction.id', 'district'], keep=False)]

# Checking
print(f"Number of transactions with duplicates: {duplicates['transaction.id'].nunique()}")
print(dropped_rows['transaction.id'].nunique(), "... should be same number as above")   


# Note: duplicates are because some values are different for the same contribution but different values in the 'recepients' data. Districts are NAN
print(f"Districts that have these duplicates: {duplicates['district'].unique()}")


#%%

### OUTPUTS

print("Processing OUTPUT_1...")
## OUTPUT 1: contribution-day level dataset

# Sort the rows by date and transaction.id
sort_vars = ['date', 'district', 'transaction.id', 'cycle']
OUTPUT_1 = OUTPUT_1.sort_values(sort_vars)
# Reorder columns to put date and transaction.id first
cols = sort_vars + [col for col in OUTPUT_1.columns if col not in sort_vars]
OUTPUT_1 = OUTPUT_1[cols]

# OUTPUT_1.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_1.csv"), index = False)
# OUTPUT_1_corp.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_1_corp.csv"), index = False)
# OUTPUT_1_ind.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_1_ind.csv"), index = False)


#%%

print("Processing OUTPUT_2...")
## OUTPUT_2: 

# NOTE: (100 = Dem, 200 = Rep, 328 = Ind)

#     1 total_amount: Sum of contributions in the district/cycle. We don't consider contributions to primaries

#     2 total_amount_without_special_elections: For district/cycle with multiple elections, here we consider only those contributions that are earlier than the date of the special election. For all other districts/cycles,  this coincides with total_amount. We don't consider contributions to primaries

#     3 total_amount_dem_gen: Sum of contributions in the district/cycle to Democratic candidate in the general election

#     4 total_amount_dem_gen_without_special: Same as total_amount_dem_gen, but we don't consider contributions later than special elections (when they occur)

#     5 total_amount_dem_primary: Sum of contributions to Democratic candidates in primary

#     6 total_amount_rep_gen: Same as total_amount_dem_gen, but for Republican candidate

#     7 total_amount_rep_gen_without_special: Same as total_amount_dem_gen_without_special but for Republican candidate

#     8 total_amount_rep_primary: Same as total_amount_dem_primary but for Republican primary


print("Filtering OUTPUT_2_1...")
OUTPUT_2_1 = OUTPUT_1[
    OUTPUT_1['election.type'] != 'P'
    ].groupby(
    ['district', 'cycle']).agg(
        total_amount = ('amount', 'sum'),
        tran_count = ('transaction.id', 'count'),
        avg_amount = ('amount', 'mean'),
).reset_index()

print("Filtering OUTPUT_2_2...")
OUTPUT_2_2 = OUTPUT_1[
    (OUTPUT_1['election.type'] != 'P') & 
    (OUTPUT_1['election.type'] != 'S')
    ].groupby(
    ['district', 'cycle']).agg(
        total_amount_without_special_elections = ('amount', 'sum'),
        tran_count_without_special_elections = ('transaction.id', 'count'),
        avg_amount_without_special_elections = ('amount', 'mean'),
).reset_index()

print("Filtering OUTPUT_2_3...")
OUTPUT_2_3 = OUTPUT_1[
    (OUTPUT_1['election.type'] == 'G') & 
    (OUTPUT_1['party'] == 100)
    ].groupby(
    ['district', 'cycle']).agg(
        total_amount_dem_gen = ('amount', 'sum'),
        tran_count_dem_gen = ('transaction.id', 'count'),
        avg_amount_dem_gen = ('amount', 'mean'),
).reset_index()
        
print("Filtering OUTPUT_2_4...")
OUTPUT_2_4 = OUTPUT_1[
    (OUTPUT_1['election.type'] != 'P') & 
    (OUTPUT_1['election.type'] != 'S') & 
    (OUTPUT_1['party'] == 100)].groupby(
    ['district', 'cycle']).agg(
        total_amount_dem_gen_without_special = ('amount', 'sum'),
        tran_count_dem_gen_without_special = ('transaction.id', 'count'),
        avg_amount_dem_gen_without_special = ('amount', 'mean'),
).reset_index()
        
print("Filtering OUTPUT_2_5...")
OUTPUT_2_5 = OUTPUT_1[
    (OUTPUT_1['election.type'] == 'P') &
    (OUTPUT_1['party'] == 100)].groupby(
    ['district', 'cycle']).agg(
        total_amount_dem_primary = ('amount', 'sum'),
        tran_count_dem_primary = ('transaction.id', 'count'),
        avg_amount_dem_primary = ('amount', 'mean'),
).reset_index()

print("Filtering OUTPUT_2_6...")
OUTPUT_2_6 = OUTPUT_1[
    (OUTPUT_1['election.type'] != 'P') & 
    (OUTPUT_1['party'] == 200)
    ].groupby(
    ['district', 'cycle']).agg(
        total_amount_rep_gen = ('amount', 'sum'),
        tran_count_rep_gen = ('transaction.id', 'count'),
        avg_amount_rep_gen = ('amount', 'mean'),
).reset_index()
        
print("Filtering OUTPUT_2_7...")
OUTPUT_2_7 = OUTPUT_1[
    (OUTPUT_1['election.type'] != 'P') & 
    (OUTPUT_1['election.type'] != 'S') & 
    (OUTPUT_1['party'] == 200)].groupby(
    ['district', 'cycle']).agg(
        total_amount_rep_gen_without_special = ('amount', 'sum'),
        tran_count_rep_gen_without_special = ('transaction.id', 'count'),
        avg_amount_rep_gen_without_special = ('amount', 'mean'),
).reset_index()
        
print("Filtering OUTPUT_2_8...")
OUTPUT_2_8 = OUTPUT_1[
    (OUTPUT_1['election.type'] == 'P') &
    (OUTPUT_1['party'] == 200)].groupby(
    ['district', 'cycle']).agg(
        total_amount_rep_primary = ('amount', 'sum'),
        tran_count_rep_primary = ('transaction.id', 'count'),
        avg_amount_rep_primary = ('amount', 'mean'),
).reset_index()

OUTPUT_2 = OUTPUT_2_1.copy()
for i in range(2, 9):
    print(f"Merging OUTPUT_2 with OUTPUT_2_{i}")
    df_name = f"OUTPUT_2_{i}"
    df = globals()[df_name]
    
    # Merge with the current result
    OUTPUT_2 = pd.merge(
        OUTPUT_2,
        df,
        on=['cycle', 'district'],
        how='outer'
    )
    if i == 8:
        print("Merge complete!")
    else:
        continue

OUTPUT_2.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_2.csv"), index = False)


#%%

print("Processing OUTPUT_3...")
## OUTPUT_3: 
    
#     1 total_amount_corp: Same as total_amount but considering only corporate contributions

#     2 total_amount_without_special_elections_corp: Same as total_amount but considering only corporate contributions

#     3 total_amount_dem_gen_corp: Same as total_amount_dem_gen but considering only corporate contributions

#     4 total_amount_dem_gen_without_special_corp: Same as total_amount_dem_gen_without_special but considering only corporate contributions

#     5 total_amount_dem_primary_corp: Same as total_amount_dem_primary but considering only corporate contributions

#     6 total_amount_rep_gen_corp: Same as total_amount_rep_gen but considering only corporate contributions

#     7 total_amount_rep_gen_without_special_corp: Same as total_amount_dem_rep_without_special but considering only corporate contributions

#     8 total_amount_rep_primary_corp: Same as total_amount_rep_primary but considering only corporate contributions



OUTPUT_1_corp = OUTPUT_1[OUTPUT_1['contributor.type'] == 'C']

print("Filtering OUTPUT_3_1...")
OUTPUT_3_1 = OUTPUT_1_corp[
    (OUTPUT_1_corp['election.type'] != 'P')
    ].groupby(
    ['district', 'cycle']).agg(
        total_amount_corp = ('amount', 'sum'),
        tran_count_corp = ('transaction.id', 'count'),
        avg_amount_corp = ('amount', 'mean'),
).reset_index()

print("Filtering OUTPUT 3_2...")
OUTPUT_3_2 = OUTPUT_1_corp[
    (OUTPUT_1_corp['election.type'] != 'P') & 
    (OUTPUT_1_corp['election.type'] != 'S')
    ].groupby(
    ['district', 'cycle']).agg(
        total_amount_without_special_elections_corp = ('amount', 'sum'),
        tran_count_without_special_elections_corp = ('transaction.id', 'count'),
        avg_amount_without_special_elections_corp = ('amount', 'mean'),
).reset_index()

print("Filtering OUTPUT 3_3...")
OUTPUT_3_3 = OUTPUT_1_corp[
    (OUTPUT_1_corp['election.type'] != 'P') & 
    (OUTPUT_1_corp['party'] == 100)
    ].groupby(
    ['district', 'cycle']).agg(
        total_amount_dem_gen_corp = ('amount', 'sum'),
        tran_count_dem_gen_corp = ('transaction.id', 'count'),
        avg_amount_dem_gen_corp = ('amount', 'mean'),
).reset_index()
        
print("Filtering OUTPUT 3_4...")
OUTPUT_3_4 = OUTPUT_1_corp[
    (OUTPUT_1_corp['election.type'] != 'P') & 
    (OUTPUT_1_corp['election.type'] != 'S') & 
    (OUTPUT_1_corp['party'] == 100)].groupby(
    ['district', 'cycle']).agg(
        total_amount_dem_gen_without_special_corp = ('amount', 'sum'),
        tran_count_dem_gen_without_special_corp = ('transaction.id', 'count'),
        avg_amount_dem_gen_without_special_corp = ('amount', 'mean'),
).reset_index()
        
print("Filtering OUTPUT 3_5...")
OUTPUT_3_5 = OUTPUT_1_corp[
    (OUTPUT_1_corp['election.type'] == 'P') &
    (OUTPUT_1_corp['party'] == 100)].groupby(
    ['district', 'cycle']).agg(
        total_amount_dem_primary_corp = ('amount', 'sum'),
        tran_count_dem_primary_corp = ('transaction.id', 'count'),
        avg_amount_dem_primary_corp = ('amount', 'mean'),
).reset_index()

print("Filtering OUTPUT 3_6...")
OUTPUT_3_6 = OUTPUT_1_corp[
    (OUTPUT_1_corp['election.type'] != 'P') & 
    (OUTPUT_1_corp['party'] == 200)
    ].groupby(
    ['district', 'cycle']).agg(
        total_amount_rep_gen_corp = ('amount', 'sum'),
        tran_count_rep_gen_corp = ('transaction.id', 'count'),
        avg_amount_rep_gen_corp = ('amount', 'mean'),
).reset_index()
        
print("Filtering OUTPUT 3_7...")
OUTPUT_3_7 = OUTPUT_1_corp[
    (OUTPUT_1_corp['election.type'] != 'P') & 
    (OUTPUT_1_corp['election.type'] != 'S') & 
    (OUTPUT_1_corp['party'] == 200)].groupby(
    ['district', 'cycle']).agg(
        total_amount_rep_gen_without_special_corp = ('amount', 'sum'),
        tran_count_rep_gen_without_special_corp = ('transaction.id', 'count'),
        avg_amount_rep_gen_without_special_corp = ('amount', 'mean'),
).reset_index()
        
print("Filtering OUTPUT 3_8...")
OUTPUT_3_8 = OUTPUT_1_corp[
    (OUTPUT_1_corp['election.type'] == 'P') &
    (OUTPUT_1_corp['party'] == 200)].groupby(
    ['district', 'cycle']).agg(
        total_amount_rep_primary_corp = ('amount', 'sum'),
        tran_count_rep_primary_corp = ('transaction.id', 'count'),
        avg_amount_rep_primary_corp = ('amount', 'mean'),
).reset_index()

OUTPUT_3 = OUTPUT_3_1.copy()
for i in range(2, 9):
    print(f"Merging OUTPUT_3 with OUTPUT_3_{i}")
    df_name = f"OUTPUT_3_{i}"
    df = globals()[df_name]
    
    # Merge with the current result
    OUTPUT_3 = pd.merge(
        OUTPUT_3,
        df,
        on=['cycle', 'district'],
        how='outer'
    )
    if i == 8:
        print("Merge complete!")
    else:
        continue

OUTPUT_3.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_3.csv"), index = False)

#%%

print("Processing OUTPUT_4...")
## OUTPUT_4: 
    
#     1 total_amount_corp_ind:  Same as total_amount but considering only individual contributions

#     2 total_amount_without_special_elections_ind: Same as total_amount but considering only individual contributions

#     3 total_amount_dem_gen_ind: Same as total_amount_dem_gen but considering only individual contributions

#     4 total_amount_dem_gen_without_special_ind: Same as total_amount_dem_gen_without_special but considering only individual contributions

#     5 total_amount_dem_primary_ind: Same as total_amount_dem_primary but considering only individual contributions

#     6 total_amount_rep_gen_ind: Same as total_amount_rep_gen but considering only individual contributions

#     7 total_amount_rep_gen_without_special_ind: Same as total_amount_dem_rep_without_special but considering only individual contributions

#     8 total_amount_rep_primary_ind: Same as total_amount_rep_primary but considering only individual contributions


OUTPUT_1_ind = OUTPUT_1[OUTPUT_1['contributor.type'] == 'I']

print("Filtering OUTPUT_4_1...")
OUTPUT_4_1 = OUTPUT_1_ind[
    (OUTPUT_1_ind['election.type'] != 'P')
    ].groupby(
    ['district', 'cycle']).agg(
        total_amount_ind = ('amount', 'sum'),
        tran_count_ind = ('transaction.id', 'count'),
        avg_amount_ind = ('amount', 'mean'),
).reset_index()

print("Filtering OUTPUT 4_2...")
OUTPUT_4_2 = OUTPUT_1_ind[
    (OUTPUT_1_ind['election.type'] != 'P') & 
    (OUTPUT_1_ind['election.type'] != 'S')
    ].groupby(
    ['district', 'cycle']).agg(
        total_amount_without_special_elections_ind = ('amount', 'sum'),
        tran_count_without_special_elections_ind = ('transaction.id', 'count'),
        avg_amount_without_special_elections_ind = ('amount', 'mean'),
).reset_index()

print("Filtering OUTPUT 4_3...")
OUTPUT_4_3 = OUTPUT_1_ind[
    (OUTPUT_1_ind['election.type'] != 'P') & 
    (OUTPUT_1_ind['party'] == 100)
    ].groupby(
    ['district', 'cycle']).agg(
        total_amount_dem_gen_ind = ('amount', 'sum'),
        tran_count_dem_gen_ind = ('transaction.id', 'count'),
        avg_amount_dem_gen_ind = ('amount', 'mean'),
).reset_index()
        
print("Filtering OUTPUT 4_4...")
OUTPUT_4_4 = OUTPUT_1_ind[
    (OUTPUT_1_ind['election.type'] != 'P') & 
    (OUTPUT_1_ind['election.type'] != 'S') & 
    (OUTPUT_1_ind['party'] == 100)].groupby(
    ['district', 'cycle']).agg(
        total_amount_dem_gen_without_special_ind = ('amount', 'sum'),
        tran_count_dem_gen_without_special_ind = ('transaction.id', 'count'),
        avg_amount_dem_gen_without_special_ind = ('amount', 'mean'),
).reset_index()
        
print("Filtering OUTPUT 4_5...")
OUTPUT_4_5 = OUTPUT_1_ind[
    (OUTPUT_1_ind['election.type'] == 'P') &
    (OUTPUT_1_ind['party'] == 100)].groupby(
    ['district', 'cycle']).agg(
        total_amount_dem_primary_ind = ('amount', 'sum'),
        tran_count_dem_primary_ind = ('transaction.id', 'count'),
        avg_amount_dem_primary_ind = ('amount', 'mean'),
).reset_index()

print("Filtering OUTPUT 4_6...")
OUTPUT_4_6 = OUTPUT_1_ind[
    (OUTPUT_1_ind['election.type'] != 'P') & 
    (OUTPUT_1_ind['party'] == 200)
    ].groupby(
    ['district', 'cycle']).agg(
        total_amount_rep_gen_ind = ('amount', 'sum'),
        tran_count_rep_gen_ind = ('transaction.id', 'count'),
        avg_amount_rep_gen_ind = ('amount', 'mean'),
).reset_index()
        
print("Filtering OUTPUT 4_7...")
OUTPUT_4_7 = OUTPUT_1_ind[
    (OUTPUT_1_ind['election.type'] != 'P') & 
    (OUTPUT_1_ind['election.type'] != 'S') & 
    (OUTPUT_1_ind['party'] == 200)].groupby(
    ['district', 'cycle']).agg(
        total_amount_rep_gen_without_special_ind = ('amount', 'sum'),
        tran_count_rep_gen_without_special_ind = ('transaction.id', 'count'),
        avg_amount_rep_gen_without_special_ind = ('amount', 'mean'),
).reset_index()
        
print("Filtering OUTPUT 4_8...")
OUTPUT_4_8 = OUTPUT_1_ind[
    (OUTPUT_1_ind['election.type'] == 'P') &
    (OUTPUT_1_ind['party'] == 200)].groupby(
    ['district', 'cycle']).agg(
        total_amount_rep_primary_ind = ('amount', 'sum'),
        tran_count_rep_primary_ind = ('transaction.id', 'count'),
        avg_amount_rep_primary_ind = ('amount', 'mean'),
).reset_index()

OUTPUT_4 = OUTPUT_4_1.copy()
for i in range(2, 9):
    print(f"Merging OUTPUT_4 with OUTPUT_4_{i}")
    df_name = f"OUTPUT_4_{i}"
    df = globals()[df_name]
    
    # Merge with the current result
    OUTPUT_4 = pd.merge(
        OUTPUT_4,
        df,
        on=['cycle', 'district'],
        how='outer'
    )
    if i == 8:
        print("Merge complete!")
    else:
        continue

OUTPUT_4.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_4.csv"), index = False)


#%%

print("Processing OUTPUT_5...")
## OUTPUT_5: 
    
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

OUTPUT_5.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_5.csv"), index = False)


#%%

print("Processing OUTPUT_6...")
## OUTPUT_6: 

#     - dist_cycle_comp_35_65: We compute the average across candidates of the vote shares obtained in the general election. We consider only the two main candidates for this. Second, if this is between 35 and 65, we assign value 1, and 0 otherwise.

#     - dist_cycle_comp_30_70: Same as dist_cycle_comp_35_65 but using 30 and 70 as thresholds

#     - dist_cycle_comp_40_60: Same as dist_cycle_comp_35_65 but using 40 and 60 as thresholds

#     - dem_prim_cycle_comp_35_65: Same as dist_cycle_comp_35_65 but considering only (two main) candidates in Dem primary

#     - dem_prim_cycle_comp_30_70: Same as dist_cycle_comp_30_70 but considering only (two main) candidates in Dem primary

#     - dem_prim_cycle_comp_40_60: Same as dist_cycle_comp_40_60 but considering only (two main) candidates in Dem primary

#     - rep_prim_cycle_comp_35_65: Same as dist_cycle_comp_35_65 but considering only (two main) candidates in Rep primary

#     - rep_prim_cycle_comp_30_70: Same as dist_cycle_comp_30_70 but considering only (two main) candidates in Rep primary

#     - rep_prim_cycle_comp_40_60: Same as dist_cycle_comp_40_60 but considering only (two main) candidates in Rep primary

#     - Election_day: Day in which the seat is assigned


def calculate_metrics(group):
    result = {}
    
    """
    Using both gen.vote.pct (G) and prim.vote.pct (P)
    """
    
    # Calculate metrics for general election votes
    if 'gen.vote.pct' in group.columns:
        # Sort by general election vote percentage (descending)
        gen_sorted = group.sort_values(by='gen.vote.pct', ascending=False)
        
        # Get top 2 candidates (by unique bonica.rid)
        gen_top_candidates = gen_sorted.drop_duplicates(subset='bonica.rid').head(2)
        
        if len(gen_top_candidates) > 0:
            result['G_max'] = gen_top_candidates['gen.vote.pct'].iloc[0]
            if len(gen_top_candidates) > 1:
                result['G_min'] = gen_top_candidates['gen.vote.pct'].iloc[1]
                result['G_dispersion'] = result['G_max'] - result['G_min']
            else:
                result['G_min'] = np.nan
                result['G_dispersion'] = np.nan
    
    # Calculate metrics for primary election votes
    if 'prim.vote.pct' in group.columns:
        # Sort by primary election vote percentage (descending)
        prim_sorted = group.sort_values(by='prim.vote.pct', ascending=False)
        
        # Get top 2 candidates (by unique bonica.rid)
        prim_top_candidates = prim_sorted.drop_duplicates(subset='bonica.rid').head(2)
        
        if len(prim_top_candidates) > 0:
            result['P_max'] = prim_top_candidates['prim.vote.pct'].iloc[0]
            if len(prim_top_candidates) > 1:
                result['P_min'] = prim_top_candidates['prim.vote.pct'].iloc[1]
                result['P_dispersion'] = result['P_max'] - result['P_min']
            else:
                result['P_min'] = np.nan
                result['P_dispersion'] = np.nan
    
    # Count unique candidates
    result['num_candidates'] = group['bonica.rid'].nunique()
    
    return pd.Series(result)

# Apply the function to each district-cycle group
print("Filtering OUTPUT_6_1...")
OUTPUT_6_1 = OUTPUT_1.groupby(['district', 'cycle']).apply(calculate_metrics).reset_index()
print("Filtering OUTPUT_6_2...")
OUTPUT_6_2 = OUTPUT_1.groupby(['district', 'cycle', 'party']).apply(calculate_metrics).reset_index()
OUTPUT_6_2_dem = OUTPUT_6_2[OUTPUT_6_2['party'] == 100].drop(columns = 'party')
OUTPUT_6_2_rep = OUTPUT_6_2[OUTPUT_6_2['party'] == 200].drop(columns = 'party')

# Applying a function to create variables using a for loop for each dicitonary
print("Creating categorical variables for district in general...")
label_values = [0, 1, 0]

OUTPUT_6_1_dict_G = {
    'dist_cycle_comp_35_65_G': [0, 35, 65, 100],
    'dist_cycle_comp_30_70_G': [0, 30, 70, 100],
    'dist_cycle_comp_40_60_G': [0, 40, 60, 100]
    }

OUTPUT_6_1_dict_P = {
    'dist_cycle_comp_35_65_P': [0, 0.35, 0.65, 1],
    'dist_cycle_comp_30_70_P': [0, 0.30, 0.70, 1],
    'dist_cycle_comp_40_60_P': [0, 0.40, 0.60, 1]
    }

OUTPUT_6_2_dict_dem_G = {
    'dem_prim_cycle_comp_35_65_G': [0, 35, 65, 100],
    'dem_prim_cycle_comp_30_70_G': [0, 30, 70, 100],
    'dem_prim_cycle_comp_40_60_G': [0, 40, 60, 100]
    }

OUTPUT_6_2_dict_dem_P = {
    'dem_prim_cycle_comp_35_65_P': [0, 0.35, 0.65, 1],
    'dem_prim_cycle_comp_30_70_P': [0, 0.30, 0.70, 1],
    'dem_prim_cycle_comp_40_60_P': [0, 0.40, 0.60, 1]
    }

OUTPUT_6_2_dict_rep_G = {
    'rep_prim_cycle_comp_35_65_G': [0, 35, 65, 100],
    'rep_prim_cycle_comp_30_70_G': [0, 30, 70, 100],
    'rep_prim_cycle_comp_40_60_G': [0, 40, 60, 100]
    }

OUTPUT_6_2_dict_rep_P = {
    'rep_prim_cycle_comp_35_65_P': [0, 0.35, 0.65, 1],
    'rep_prim_cycle_comp_30_70_P': [0, 0.30, 0.70, 1],
    'rep_prim_cycle_comp_40_60_P': [0, 0.40, 0.60, 1]
    }

def create_cat_vars(data, dictionary, var, labels):
    result_data = data.copy()
    for key, value in dictionary.items():
        print(key, value)
        result_data[key] = pd.cut(
            result_data[var], 
            bins=value, 
            labels=labels, 
            ordered=False,
            include_lowest=True
        )
    return result_data
    

OUTPUT_6_1 = create_cat_vars(OUTPUT_6_1, OUTPUT_6_1_dict_G, 'G_dispersion', label_values)
OUTPUT_6_1 = create_cat_vars(OUTPUT_6_1, OUTPUT_6_1_dict_P, 'P_dispersion', label_values)
OUTPUT_6_2_dem = create_cat_vars(OUTPUT_6_2_dem, OUTPUT_6_2_dict_dem_G, 'G_dispersion', label_values)
OUTPUT_6_2_dem = create_cat_vars(OUTPUT_6_2_dem, OUTPUT_6_2_dict_dem_P, 'P_dispersion', label_values)
OUTPUT_6_2_rep = create_cat_vars(OUTPUT_6_2_rep, OUTPUT_6_2_dict_rep_G, 'G_dispersion', label_values)
OUTPUT_6_2_rep = create_cat_vars(OUTPUT_6_2_rep, OUTPUT_6_2_dict_rep_P, 'P_dispersion', label_values)

# Saving datasets before dropping columns
OUTPUT_6_1.to_csv(os.path.join(data_folder, 'OUTPUTS', 'OUTPUT_6_1.csv'), index = False)
OUTPUT_6_2_dem.to_csv(os.path.join(data_folder, 'OUTPUTS', 'OUTPUT_6_2_dem.csv'), index = False)
OUTPUT_6_2_rep.to_csv(os.path.join(data_folder, 'OUTPUTS', 'OUTPUT_6_2_rep.csv'), index = False)


# Dropping columns
dataframes = [OUTPUT_6_1, OUTPUT_6_2_dem, OUTPUT_6_2_rep]
for i in range(len(dataframes)):
    dataframes[i].drop(
        columns=[
            'G_max', 'G_min', 'G_dispersion', 'P_max', 'P_min',
            'P_dispersion', 'num_candidates'
        ],
        inplace=True
    )

# Merging all datasets to one final dataset
print("Merging datasets to OUTPUT_6...")

OUTPUT_6 = pd.merge(
    OUTPUT_6_1,
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
print("Merge complete!")

OUTPUT_6.to_csv(os.path.join(data_folder, 'OUTPUTS', 'OUTPUT_6.csv'), index = False)



#%%

print("Processing OUTPUT_7...")
## OUTPUT_7: 
    
#     - treatment_1: We assign 1 for district/cycles after death of an incumbent. We assign 0 otherwise

#     - treatment_2: We assign 1 for district/cycles for the first cycle after death of an incumbent. We assign 0 otherwise

#     - death_unexpected: We take this dummy from Deaths.xlsx dataset. For missing deaths only: You can assign this dummy following my logic (expected deaths come after a long illness situations). For districts with multiple deaths: Before the first death, consider the first death. Same for first cycle after the death. Then start considering the second, and so on...

#     - death_age: Age of dead incumbent. For districts with multiple deaths, follow same logiuc as death_unexpected

#     - death_party_member: Party of dead incumbent. For districts with multiple deaths, follow same logiuc as death_unexpected



OUTPUT_7_0 = OUTPUT_1[['district', 'cycle']].drop_duplicates()
all_districts = OUTPUT_7_0['district'].unique()

# Creating single and multiple death districts
no_death_districts = []
single_death_districts = []
multiple_death_districts = []
for dist in special_elections['district'].unique():
    df_filter = special_elections[(special_elections['cause_vacancy'] == 'Death') & (special_elections['district'] == dist)]
    if len(df_filter) > 1:
        print(f"Number of deaths in district {dist}: {len(df_filter)}")
        print()
        multiple_death_districts.append(dist)
    elif len(df_filter) == 1:
        print(f"Number of deaths in district {dist}: {len(df_filter)}")
        print()
        single_death_districts.append(dist)
    else:
        print(f"Number of deaths in district {dist}: {len(df_filter)}")
        print()
        no_death_districts.append(dist) # these are people that resigned, but not died

print("Length of no_death_districts:", len(no_death_districts))
print("Length of single_death_districts:", len(single_death_districts))
print("Length of multiple_death_districts:", len(multiple_death_districts))
print("Length of total districts in final output:", OUTPUT_7_0['district'].nunique())


# Creating treat_1
print("Creating treat_1...")
OUTPUT_7_0['treat_1'] = 0
for district in single_death_districts:
    # Get death year for this district
    district_death_dates = OUTPUT_1[
        (OUTPUT_1['district'] == district) & 
        (OUTPUT_1['death_date'].notna())
    ]['death_date']
    
    if not district_death_dates.empty:
        death_date = district_death_dates.iloc[0]
        death_year = pd.to_datetime(death_date).year
        
        # Assign treatment to cycles after death year
        OUTPUT_7_0.loc[
            (OUTPUT_7_0['district'] == district) & 
            (OUTPUT_7_0['cycle'] > death_year), 
            'treat_1'] = 1
    else:
        print(f"Warning: No death date found for district {district}")

# Creating treat_2
print("Creating treat_2...")
OUTPUT_7_0['treat_2'] = 0
for district in multiple_death_districts:
    # Get all deaths for this district with their years
    district_deaths = OUTPUT_1[
        (OUTPUT_1['district'] == district) & 
        (OUTPUT_1['death_date'].notna())
    ][['death_date', 'cycle']].drop_duplicates()
    
    district_deaths['death_year'] = pd.to_datetime(district_deaths['death_date']).dt.year
    
    if not district_death_dates.empty:
    
        # Get all special election years for this district
        special_elections_death_dates = special_elections[
            (special_elections['district'] == district)
        ]['spec_election_date'].unique()
        
        special_elections_years = pd.to_datetime(special_elections_death_dates).year
        
        print(f"Distict: {district}, Special elections years: {special_elections_years}")
        print()
        
        # For each cycle in this district (sorted)
        district_cycles = sorted(OUTPUT_7_0[OUTPUT_7_0['district'] == district]['cycle'])       
        
        for i, cycle in enumerate(district_cycles):
            # Default state is 0
            state = 0

            # Check each death
            for _, death_row in district_deaths.iterrows():
                death_year = death_row['death_year']
                
                # If current cycle is the first cycle after death
                if cycle > death_year:
                    # Check if there was a previous cycle after death but before current cycle
                    if i > 0 and district_cycles[i-1] > death_year:
                        # Not the first cycle after death, skip
                        continue
                    
                    # Check if there's a special election between death and this cycle
                    intervening_special = any((special_year > death_year) & (special_year <= cycle) 
                                             for special_year in special_elections_years)
                    
                    # If no intervening special election and this is the first cycle after death
                    if not intervening_special:
                        state = 1
                        break
            
            OUTPUT_7_0.loc[
                (OUTPUT_7_0['district'] == district) & 
                (OUTPUT_7_0['cycle'] == cycle), 
                'treat_2'] = state
        
    else:         
        print(f"Warning: No death date found for district {district}")        
        
        


# Creating death_unexpected, death_age, and death_party_member
print("Creating death_unexpected, death_age, and death_party_member...")
OUTPUT_7_0['death_unexpected'] = np.nan
OUTPUT_7_0['death_age'] = np.nan
OUTPUT_7_0['resign_party'] = np.nan

# Process each district
for district in single_death_districts + multiple_death_districts:
    # Get all deaths for this district with their metadata
    district_deaths = OUTPUT_1[
        (OUTPUT_1['district'] == district) & 
        (OUTPUT_1['death_date'].notna())
    ][['death_date', 'cycle', 'death_unexpected', 'death_age', 'resign_party']].drop_duplicates()
    
    if district_deaths.empty:
        print(f"Warning: No death data found for district {district}")
        continue
    
    # Convert death_date to datetime and sort by date
    district_deaths['death_date'] = pd.to_datetime(district_deaths['death_date'])
    district_deaths = district_deaths.sort_values('death_date')
    district_deaths['death_year'] = district_deaths['death_date'].dt.year
    
    # Get all special election dates for this district if it's a multiple death district
    if district in multiple_death_districts:
        special_elections_dates = pd.to_datetime(special_elections[
            (special_elections['district'] == district)
        ]['spec_election_date'].unique())
        special_elections_years = pd.Series([date.year for date in special_elections_dates if not pd.isna(date)])
    
    # Get all cycles for this district (sorted)
    district_cycles = sorted(OUTPUT_7_0[OUTPUT_7_0['district'] == district]['cycle'])
    
    # For single death districts, process is simpler
    if district in single_death_districts:
        death_row = district_deaths.iloc[0]  # Get the only death row
        
        # For ALL cycles in this district, assign the first death's attributes
        for cycle in district_cycles:
            OUTPUT_7_0.loc[
                (OUTPUT_7_0['district'] == district) & 
                (OUTPUT_7_0['cycle'] == cycle), 
                ['death_unexpected', 'death_age', 'resign_party']
            ] = [
                death_row['death_unexpected'],
                death_row['death_age'],
                death_row['resign_party']
            ]
    
    # For multiple death districts
    else:
        # Get the first death's data
        first_death = district_deaths.iloc[0]
        first_death_year = first_death['death_year']
        
        # For each cycle
        for cycle in district_cycles:
            # Default to using first death's data
            relevant_death = first_death
            
            # If we're after the first death, check if we should use a different death
            if cycle > first_death_year:
                for idx, death_row in district_deaths.iterrows():
                    death_year = death_row['death_year']
                    
                    # If death is before this cycle
                    if death_year < cycle:
                        # Check if there's a special election between this death and the cycle
                        intervening_special = any(
                            (special_year > death_year) & (special_year <= cycle) 
                            for special_year in special_elections_years
                        )
                        
                        # If no intervening special election, this death is relevant
                        if not intervening_special:
                            relevant_death = death_row
            
            # Assign the relevant death's attributes
            OUTPUT_7_0.loc[
                (OUTPUT_7_0['district'] == district) & 
                (OUTPUT_7_0['cycle'] == cycle), 
                ['death_unexpected', 'death_age', 'resign_party']
            ] = [
                relevant_death['death_unexpected'],
                relevant_death['death_age'],
                relevant_death['resign_party']
            ]



OUTPUT_7_0.to_csv(os.path.join(data_folder, 'OUTPUTS', 'OUTPUT_7.csv'), index = False)







#%%

print("Processing OUTPUT_8...")
## OUTPUT_8: 
    
#     1 avg_counting_hedging_corp: Average number of candidates (in general election only, and before the special election) funded by corporations in the district/cycle

#     2 avg_counting_hedging_corp_dem_primary: Same as avg_counting_hedging_corp but for candidates to Dem primary only

#     3 avg_counting_hedging_corp_rep_primary: Same as avg_counting_hedging_corp but for candidates to Rep primary only

#     4 hedging_money_general: The index of extensive-margin hedging is computed as the absolute difference between a corporation’s contributions to Republican and Democratic candidates in a given district and election cycle (only for general election, before the special election). This captures the extent to which a firm biases its contributions toward one party over the other. The index is constructed taking the average of this difference across corporations in the same district and cycle. See the keynote file strategies_hedging

#     5 hedging_money_general_normalized: The normalized index of extensive-margin hedging scales the absolute difference between a corporation’s contributions to Republican and Democratic candidates by the total amount contributed to both parties. See the keynote file strategies_hedging

#     6 hedging_money_dem_primary: Same as hedging_money_general but computed using contributions to two main candidates in Dem primary only

#     7 hedging_money_rep_primary: Same as hedging_money_general but computed using contributions to two main candidates in Rep primary only

# Creating avg_counting_hedging_corp
print("Creating avg_counting_hedging_corp...")
OUTPUT_8_1 = OUTPUT_1[
    (OUTPUT_1['election.type'] == 'G') & 
    (OUTPUT_1['contributor.type'] == 'C')
    ].groupby(['district', 'cycle', 'party']).agg(
    counting_hedging_corp = ('bonica.rid', 'nunique'),
).reset_index()

OUTPUT_8_1 = OUTPUT_8_1.groupby(['district', 'cycle']).agg(
    avg_counting_hedging_corp = ('counting_hedging_corp', 'mean'),
).reset_index()
        
# Creating avg_counting_hedging_corp_dem_primary and avg_counting_hedging_corp_rep_primary
print("Creating avg_counting_hedging_corp_dem_primary and avg_counting_hedging_corp_rep_primary...")
OUTPUT_8_2 = OUTPUT_1[
    (OUTPUT_1['election.type'] == 'P') & 
    (OUTPUT_1['contributor.type'] == 'C')
    ].groupby(['district', 'cycle', 'party']).agg(
    counting_hedging_corp_primary = ('bonica.rid', 'nunique'),
).reset_index()

OUTPUT_8_2 = OUTPUT_8_2.pivot_table(
    index=['district', 'cycle'],
    columns='party',
    values='counting_hedging_corp_primary',
    fill_value=0
).reset_index().rename(columns = {100.0 : "counting_hedging_corp_dem_primary", 
                                  200.0 : "counting_hedging_corp_rep_primary"})
OUTPUT_8_2 = OUTPUT_8_2[['district', 'cycle', 'counting_hedging_corp_dem_primary', 'counting_hedging_corp_rep_primary']] # keeping only Dems and Reps
                                  
OUTPUT_8_2 = OUTPUT_8_2.groupby(['district', 'cycle']).agg(
    avg_counting_hedging_corp_dem_primary = ('counting_hedging_corp_dem_primary', 'mean'),
    avg_counting_hedging_corp_rep_primary = ('counting_hedging_corp_rep_primary', 'mean'),
).reset_index()


# Creating hedging_money_general and hedging_money_general_normalized
print("Creating hedging_money_general and hedging_money_general_normalized...")

OUTPUT_8_3 = OUTPUT_1[
    (OUTPUT_1['election.type'] == 'G') & 
    (OUTPUT_1['contributor.type'] == 'C')
    ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
    total_amount = ('amount', 'sum'),
).reset_index()

OUTPUT_8_3 = OUTPUT_8_3.pivot_table(
    index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
    columns='party',
    values='total_amount',
    fill_value=0
).reset_index().rename(columns = {100.0 : "total_amount_dem", 
                                  200.0 : "total_amount_rep"})
OUTPUT_8_3 = OUTPUT_8_3[['district', 'cycle', 'bonica.cid', 'contributor.name', 'total_amount_dem', 'total_amount_rep']]

OUTPUT_8_3['hedging'] = abs(OUTPUT_8_3['total_amount_dem'] - OUTPUT_8_3['total_amount_rep'])
OUTPUT_8_3['n_hedging'] = OUTPUT_8_3['hedging'] / (OUTPUT_8_3['total_amount_dem'] + OUTPUT_8_3['total_amount_rep'])

OUTPUT_8_3 = OUTPUT_8_3.groupby(['district', 'cycle']).agg(
    hedging_money_general = ('hedging', 'mean'),
    hedging_money_general_normalized = ('n_hedging', 'mean')
).reset_index()


# Creating hedging_money_dem_primary and hedging_money_rep_primary
print("Creating hedging_money_general and hedging_money_general_normalized...")

OUTPUT_8_4 = OUTPUT_1[
    (OUTPUT_1['election.type'] == 'P') & 
    (OUTPUT_1['contributor.type'] == 'C')
    ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party', 'bonica.rid']).agg(
    total_amount = ('amount', 'sum'),
).reset_index()


print("Nr of companies that are giving to more than one candidate in the primaries", 
      OUTPUT_8_4[OUTPUT_8_4.duplicated(subset=['district', 'cycle', 'bonica.cid'], keep=False)]['bonica.cid'].nunique())

print("Nr of companies that are giving to more than one candidate in the primaries in the same party", 
      OUTPUT_8_4[OUTPUT_8_4.duplicated(subset=['district', 'cycle', 'bonica.cid', 'party'], keep=False)]['bonica.cid'].nunique())

# Getting duplicate rows, so we understand what parties are contributing to the same party's candidates in the primaries
OUTPUT_8_4 = OUTPUT_8_4[OUTPUT_8_4.duplicated(subset=['district', 'cycle', 'bonica.cid', 'party'], keep=False)]

# We will apply a function for both Dems and Reps separately
def calculate_party_hedging(data, party_code, party_name):
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
    
    total = (
        party_data_pivot[f"total_amount_{party_name}_first"] + 
        party_data_pivot[f"total_amount_{party_name}_second"]
    )
    
    # Handle division by zero
    party_data_pivot['n_hedging'] = party_data_pivot['hedging'] / total.replace(0, np.nan)
    
    # Aggregate by district and cycle
    result = party_data_pivot.groupby(['district', 'cycle']).agg(
        **{
            f"hedging_money_{party_name}_primary": ('hedging', 'mean'),
            f"hedging_money_{party_name}_primary_normalized": ('n_hedging', 'mean')
        }
    ).reset_index()
    
    return result

print("Processing hedging metrics for Dems and Reps separately...")
dem_results = calculate_party_hedging(OUTPUT_8_4, 100, 'dem')
rep_results = calculate_party_hedging(OUTPUT_8_4, 200, 'rep')


OUTPUT_8_4 = pd.merge(
    dem_results,
    rep_results,
    on=['district', 'cycle'],
    how='outer'
)
        

OUTPUT_8 = OUTPUT_8_1.copy()
for i in range(2, 5):
    print(f"Merging OUTPUT_8 with OUTPUT_8_{i}")
    df_name = f"OUTPUT_8_{i}"
    df = globals()[df_name]
    
    # Merge with the current result
    OUTPUT_8 = pd.merge(
        OUTPUT_8,
        df,
        on=['cycle', 'district'],
        how='outer'
    )
    if i == 8:
        print("Merge complete!")
    else:
        continue

OUTPUT_8.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_8.csv"), index = False)


#%%

print("Processing OUTPUT_9...")

## OUTPUT_9: 
    
#     1 mean_cfscore: Average CFscore for entire district-cycle

#     2 mean_cfscore_dyn: Average CFscore (period-specific) for entire district-cycle

#     3 mean_cfscore_dem: Same as mean_cfscore but only for Dems

#     4 mean_cfscore_dyn_dem: Same as mean_cfscore_dyn but only for Dems

#     5 mean_cfscore_rep: Same as mean_cfscore but only for Reps

#     6 mean_cfscore_dyn_rep: Same as mean_cfscore_dyn but only for Reps


# Here we only use rows that were merged in merged_df_2 by filtering out NA values
OUTPUT_9_0 = OUTPUT_1[~OUTPUT_1['resign_member'].isna()]
# Dropping duplicate rows (bonica.rid is more unique)
OUTPUT_9_0 = OUTPUT_9_0.drop_duplicates(
    subset=['bonica.rid', 'cycle'],
    keep='first' 
)

OUTPUT_9_1 = OUTPUT_9_0.groupby(['district', 'cycle']).agg(
    # count_cfscore=('recipient.cfscore', 'count'),
    # count_cfscore_dyn=('recipient.cfscore.dyn', 'count'),
    mean_cfscore=('recipient.cfscore', 'mean'),
    mean_cfscore_dyn=('recipient.cfscore.dyn', 'mean')
).reset_index()


OUTPUT_9_2 = OUTPUT_9_0.groupby(['district', 'cycle', 'party']).agg(
    # count_cfscore=('recipient.cfscore', 'count'),
    # count_cfscore_dyn=('recipient.cfscore.dyn', 'count'),
    mean_cfscore=('recipient.cfscore', 'mean'),
    mean_cfscore_dyn=('recipient.cfscore.dyn', 'mean')
).reset_index()

# Then use pd.melt to create a long-format dataframe with variable names
OUTPUT_9_2_melted = pd.melt(
    OUTPUT_9_2,
    id_vars=['district', 'cycle', 'party'],
    value_vars=['mean_cfscore', 'mean_cfscore_dyn']
)

# Now pivot with both party and variable as columns
OUTPUT_9_2 = OUTPUT_9_2_melted.pivot_table(
    index=['district', 'cycle'],
    columns=['party', 'variable'],
    values='value',
    fill_value=0
).reset_index()

# Flatten the column hierarchy to more readable names
OUTPUT_9_2.columns = [
    f"{col[1]}_{col[0]}" if isinstance(col, tuple) and col[0] != "" 
    else col for col in OUTPUT_9_2.columns
]

OUTPUT_9_2 = OUTPUT_9_2.rename(columns = 
                               {'_district':'district',
                                '_cycle':'cycle',
                                'mean_cfscore_100.0':'mean_cfscore_dem',
                                'mean_cfscore_dyn_100.0':'mean_cfscore_dyn_dem',
                                'mean_cfscore_200.0':'mean_cfscore_rep',
                                'mean_cfscore_dyn_200.0':'mean_cfscore_dyn_rep',
                                   })

OUTPUT_9_2 = OUTPUT_9_2[['district', 'cycle', 'mean_cfscore_dem', 'mean_cfscore_dyn_dem',
       'mean_cfscore_rep', 'mean_cfscore_dyn_rep']]

print("Merging datasets to OUTPUT_9...")

OUTPUT_9 = pd.merge(
    OUTPUT_9_1,
    OUTPUT_9_2,
    on=['cycle', 'district'],
    how='outer'
    )


# We have 'recipient.cfscore' and 'recipient.cfscore.dyn', but the latter is clearly better, although has missing values


OUTPUT_9.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_9.csv"), index = False)



#%%


### END OF SCRIPT!
print("..." * 5)
print("\nEnd of script!")







