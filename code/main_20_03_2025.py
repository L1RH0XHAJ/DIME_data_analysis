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

### READING DATASETS

# This for loop saves all datasets in a dictionary, we can refer to specific datasets as 

## INPUT 1: DIME database on contributions
print("\n")
print("*" * 30)
print("INPUT_1")

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
        
        print("\n-> Length of raw dataset:", len(df))
        
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
            (df['date'].dt.year >= df['cycle'] - 2) &
            (df['date'].dt.year <= df['cycle'] + 2)
        ]
        
        print("-> Length of dataset after filtering:", len(df))
        
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
print(contribDB_all[contribDB_all['cycle'] != contribDB_all['date'].dt.year]['cycle'].describe())
print(contribDB_all[contribDB_all['cycle'] != contribDB_all['date'].dt.year]['date'].dt.year.describe())


## INPUT 2: Recipients’ database
print("\n")
print("*" * 30)
print("INPUT_2")
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
print("\n")
print("*" * 30)
print("INPUT_3")
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
print("\n")
print("*" * 30)
print("INPUT_4")
print("Reading elections dates data...")
election_dates_df = pd.read_csv(data_folder + "/election_dates.csv", encoding='latin-1')


#%%

### MERGING DATASETS


## MERGE 1: Contributions dataset (Input 1) with recipients dataset (Input 2) using id (“bonica.rid”) and “cycle” as keys
print("\n")
print("*" * 30)
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
print("\n")
print("*" * 30)
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
print("\n")
print("*" * 30)
print("\nMERGE 3: Merging with election dates...")
merged_df_3 = pd.merge(
    merged_df_2,
    election_dates_df,
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

## New dummy variable: “later_than_special”, =1 if the contribution is given in the same cycle of the death, but later than the special election (date of contribution is later than date of special election in that cycle).

merged_df_3['cycle'] = pd.to_numeric(merged_df_3['cycle'], errors='coerce')  # will convert None values to NaN
merged_df_3['date'] = pd.to_datetime(merged_df_3['date'], errors = 'coerce') # need to use errors = 'coerce' to include incorrect dates
merged_df_3['spec_election_date'] = pd.to_datetime(merged_df_3['spec_election_date'], errors = 'coerce') # need to use errors = 'coerce' to include incorrect dates
merged_df_3['resign_date'] = pd.to_datetime(merged_df_3['resign_date']) # no errors here, because these dates were created manually in special_elections.csv!
merged_df_3['death_date'] = pd.to_datetime(merged_df_3['death_date']) # no errors here, because these dates were created manually in special_elections.csv!
merged_df_3['election_date_in_cycle'] = pd.to_datetime(merged_df_3['election_date_in_cycle']) # no errors here, because these dates were created manually in election_dates.csv!

merged_df_3_2 = merged_df_3.copy()

merged_df_3_2['later_than_special'] = 0  # Initialize with zeros, districts that did not have special elections simply receive a zero


mask = (
    # Filtering for within election cycle, in terms of years between spec_election_date and year
    # (merged_df_3_2['date'] <= merged_df_3_2['spec_election_date'] + pd.DateOffset(months=6)) &
    # (merged_df_3_2['date'] < merged_df_3_2['election_date_in_cycle']) &
    (merged_df_3_2['cycle'] == merged_df_3_2['spec_cycle']) &
    # Main condition!
    (merged_df_3_2['date'] >= merged_df_3_2['spec_election_date'])
)


# Apply the mask and handle NaN values in spec_election_date (keep as 0)
merged_df_3_2.loc[mask, 'later_than_special'] = 1
merged_df_3_2.loc[merged_df_3_2['spec_election_date'].isna(), 'later_than_special'] = 0
print("Value counts for later than special:")
print(merged_df_3_2['later_than_special'].value_counts())
print(f"Types of election for later_than_special == 0 (total observations: {len(merged_df_3_2[merged_df_3_2['later_than_special'] == 0])})")
print(merged_df_3_2[merged_df_3_2['later_than_special'] == 0]['election.type'].value_counts())
print(f"Types of election for later_than_special == 1 (total observations: {len(merged_df_3_2[merged_df_3_2['later_than_special'] == 1])})")
print(merged_df_3_2[merged_df_3_2['later_than_special'] == 1]['election.type'].value_counts())

# Creating variable for election.type == 'S'
merged_df_3_2['election_type_S'] = 0
merged_df_3_2.loc[merged_df_3_2['election.type'] == 'S', 'election_type_S'] = 1

print("Value counts for election_type_S:")
print(merged_df_3_2['election_type_S'].value_counts())

print("Contributions going to special elections before special elections date:", len(merged_df_3_2[(merged_df_3_2['election_type_S'] == 1) & (merged_df_3_2['date'] < merged_df_3_2['spec_election_date'])]))
print("Contributions going to special elections on and after special elections date:", len(merged_df_3_2[(merged_df_3_2['election_type_S'] == 1) & (merged_df_3_2['date'] >= merged_df_3_2['spec_election_date'])]))
print("Contributions going to special elections that have missing special elections date:", len(merged_df_3_2[(merged_df_3_2['election_type_S'] == 1) & (merged_df_3_2['date'].isna())]))

# Discrepancy! Gets resolved (or significantly smaller when duplicates contributions are dropped)


## 2.

## For each contribution in a district where an incumbent died:
    
#   2. Create a variable “days_to_nearest_death” which counts the number of days between the contribution (variable date in Input 1 dataset) and the date of the nearest death in the district (from Input 3 dataset)
#      - <0 for contributions earlier than death, >0 for contributions later than death
#      - Be careful! There are districts where more than one incumbent died (here, “nearest” death is key!)



# For each district-cycle combination, find all death dates
district_cycle_deaths = merged_df_3_2[merged_df_3_2['resign_member'].notna()][
    ['district', 'cycle', 'death_date']].drop_duplicates()



# # Initialize the days_to_nearest_death column
# merged_df_3_2['days_to_nearest_death'] = np.nan

# # For each district with deaths
# for district in district_cycle_deaths['district'].unique():
#     # Get all cycles for this district
#     district_cycles = district_cycle_deaths[district_cycle_deaths['district'] == district]['cycle'].unique()
    
#     # For each contribution in the district
#     district_mask = merged_df_3_2['district'] == district
#     for idx in merged_df_3_2[district_mask].index:
#         contrib_date = merged_df_3_2.loc[idx, 'date']
#         contrib_cycle = merged_df_3_2.loc[idx, 'cycle']
        
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
#             merged_df_3_2.loc[idx, 'days_to_nearest_death'] = nearest_days




# We calculate days to nearest death for all rows, and the absolute value as well
merged_df_3_2['days_to_nearest_death'] = (merged_df_3_2['date'] - merged_df_3_2['death_date']).dt.days
merged_df_3_2['abs_days_to_death'] = merged_df_3_2['days_to_nearest_death'].abs()
# Then, we sort by some variables, as well as absolute death, which brings up the contributions closest to death dates first

duplicate_columns = ['cycle', 'date', 'transaction.id', 'abs_days_to_death']
merged_df_3_2 = merged_df_3_2.sort_values(
    by=duplicate_columns,
    ascending=[True, True, True, True]  # 4 values to match the 4 columns
)


# Now keep only the first occurrence of each unique contribution (closest to death)
# This allows us to have unique rows for each contributions (rather than duplicates), even for problematic rows
merged_df_3_2 = merged_df_3_2.drop_duplicates(
    subset=['cycle', 'date', 'transaction.id'],
    keep='first' 
)

# Checking for districts with deaths

# merged_df_3_VA01_2 = merged_df_3[merged_df_3['district'] == 'VA01']
# dropped_indices = set(merged_df_3_VA01.index) - set(merged_df_3_VA01_2.index)
# dropped_rows = merged_df_3_VA01.loc[list(dropped_indices)]
# merged_df_3_CA05 = merged_df_3[merged_df_3['district'] == 'CA05']



STOP


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

# Checking discrepancy between later_than_special and election_type_S

special_elections_districts = special_elections.groupby('district')['resign_member'].nunique().reset_index()

# for district in special_elections_districts['district'].unique():
for district in ['MA01']:
    df_filtered = merged_df_3[merged_df_3['district'] == district]
    df_filtered_1 = df_filtered[df_filtered['resign_member'].isna()]
    df_filtered_2 = df_filtered[~df_filtered['resign_member'].isna()]
    print(district, "\nLength of df_filtered_1:", len(df_filtered_1), "\nLength of df_filtered_2:", len(df_filtered_2))
    for cycle in df_filtered_2['cycle'].unique():
        print(cycle)
        df_filtered_2_1 = df_filtered_2[df_filtered_2['cycle'] == cycle]
        print("later_than_special == 0", len(df_filtered_2_1[df_filtered_2_1['later_than_special'] == 0]))
        print("later_than_special == 1", len(df_filtered_2_1[df_filtered_2_1['later_than_special'] == 1]))
        print()
        print("election_type_S == 1", len(df_filtered_2_1[df_filtered_2_1['election_type_S'] == 1]))
        print("- before special election date:", len(df_filtered_2_1[(df_filtered_2_1['election_type_S'] == 1) & (df_filtered_2_1['date'] < df_filtered_2_1['spec_election_date'])]))
        print("- in special election date:", len(df_filtered_2_1[(df_filtered_2_1['election_type_S'] == 1) & (df_filtered_2_1['date'] == df_filtered_2_1['spec_election_date'])]))
        print("- after special election date:", len(df_filtered_2_1[(df_filtered_2_1['election_type_S'] == 1) & (df_filtered_2_1['date'] > df_filtered_2_1['spec_election_date'])]))    
        print("*" * 40)


# test = merged_df_3[merged_df_3['district'] == 'MA01'] 
# test = test[(test['election_type_S'] == 1) & (test['cycle'] == 1994)]
# test['date'].describe()
# print(test['spec_election_date'].unique())



#%%

## 3.

## For each contribution in a district where an incumbent died:

#   3. Create two dummy variable "treatment", 
#      - treat_1: simple case. 
#        -> For single death districts, assign values 0 before death, 1 after death. 
#        -> For multiple death districts, we assume first death is the only one
#      - treat_2: complex case. 
#        -> For single death districts, repeats logic from treat_1. 
#        -> For multiple death districts, we assign 0 before death, 1 after death, and then reset to 0 after election_cycle, and repeat until next death

# NOTE: these are used in the OUTPUTS later
special_elections_districts = special_elections.groupby('district')['resign_member'].nunique().reset_index()
death_counts = special_elections[special_elections['cause_vacancy'] == 'Death'].groupby('district')['resign_member'].nunique().reset_index()
single_death_districts = death_counts[death_counts['resign_member'] == 1]['district'].tolist()
multiple_death_districts = death_counts[death_counts['resign_member'] > 1]['district'].tolist()
no_death_districts = special_elections[special_elections['cause_vacancy'] == 'Resigned']['district'].tolist()
print("Total number of districts with special elections", len(special_elections_districts))
print("Total number of districts with one death", len(single_death_districts))
print("Total number of districts with multiple deaths", len(multiple_death_districts))
print("Nr of districts with resignations", len(no_death_districts))

# Create treat_1 (simple case)
merged_df_3['treat_1'] = 0
for district in single_death_districts + multiple_death_districts:
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



# Create treat_2 (complex case)
merged_df_3['treat_2'] = 0
for district in single_death_districts + multiple_death_districts:
    district_deaths = merged_df_3[
        (merged_df_3['district'] == district) & 
        (merged_df_3['death_date'].notna())
    ][['death_date', 'cycle']].drop_duplicates().sort_values('death_date')
    
    # Get all election dates, so that we know when a reset of values happens
    election_dates = merged_df_3['election_date_in_cycle'].dropna().unique()
    
    # For each contribution in the district
    district_mask = merged_df_3['district'] == district
    
    if district in single_death_districts:
        print(f"{district} is single death")
        merged_df_3.loc[district_mask, 'treat_2'] = merged_df_3.loc[district_mask, 'treat_1']
        
    elif district in multiple_death_districts:
        print(f"{district} is multiple death")
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
        
    else:
        print(f"{district} not found in single_death_districts nor in multiple_death_districts")


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
plot_scatter('AL03', 'treat_2')
# multiple death example
plot_scatter('VA01', 'treat_1')
plot_scatter('VA01', 'treat_2')
plot_scatter('CA05', 'treat_1')
plot_scatter('CA05', 'treat_2')


    

# # Testing...
# def print_treatment_example(district, treatment_var):
#     print(f"\nExample for district {district}:")
#     print("Deaths in this district:")
#     print(merged_df_3[
#         (merged_df_3['district'] == district) & 
#         (merged_df_3['death_date'].notna())
#     ][['cycle', 'death_date']].drop_duplicates().sort_values('cycle'))
    
#     print(f"\nFirst 10 observations ({treatment_var}):")
#     print(merged_df_3[
#         (merged_df_3['district'] == district) & (merged_df_3['days_to_nearest_death'].notna())
#     ][['date', 'cycle', 'days_to_nearest_death', 'election_date_in_cycle', treatment_var]].sort_values('date').head(10))
#     print("\nLast 10 observations:")
#     print(merged_df_3[
#         (merged_df_3['district'] == district) & (merged_df_3['days_to_nearest_death'].notna())
#     ][['date', 'cycle', 'days_to_nearest_death', 'election_date_in_cycle', treatment_var]].sort_values('date').tail(10))

# # Print examples
# print("\nSINGLE DEATH DISTRICT:")
# print_treatment_example("IL21", 'treat_1')
# print("\nMULTIPLE DEATH DISTRICT:")
# print_treatment_example("CA05", 'treat_2')



#%%

### OUTPUTS

## OUTPUT 1: contribution-day level dataset
print("Processing OUTPUT_1...")

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


# Sort the rows by date and transaction.id
sort_vars = ['date', 'district', 'transaction.id', 'cycle']
OUTPUT_1 = OUTPUT_1.sort_values(sort_vars)
# Reorder columns to put date and transaction.id first
cols = sort_vars + [col for col in OUTPUT_1.columns if col not in sort_vars]
OUTPUT_1 = OUTPUT_1[cols]

# OUTPUT_1.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_1.csv"), index = False)
# OUTPUT_1_corp.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_1_corp.csv"), index = False)
# OUTPUT_1_ind.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_1_ind.csv"), index = False)

OUTPUT_1 = pd.read_csv(
    data_folder + "/OUTPUTS/OUTPUT_1.csv", 
    encoding='latin-1'
    )

## OUTPUT_0: a Cartesian product of unique values of district and cycles (useful for merging with other OUTPUT data)
print("Processing OUTPUT_0...")

unique_districts = OUTPUT_1['district'].dropna().unique()
unique_cycles = OUTPUT_1['cycle'].dropna().unique()
unique_parties = [100, 200] # hard coding this for Dems and Reps only
parties_list = []
districts_list = []
cycles_list = []

for district in unique_districts:
    for cycle in unique_cycles:
        districts_list.append(district)
        cycles_list.append(cycle)

OUTPUT_0 = pd.DataFrame({
    'district': districts_list,
    'cycle': cycles_list
})

OUTPUT_0 = OUTPUT_0.sort_values(['district', 'cycle']).reset_index(drop=True)

# Clean list
parties_list = []
districts_list = []
cycles_list = []

for district in unique_districts:
    for cycle in unique_cycles:
        for party in unique_parties:
            districts_list.append(district)
            cycles_list.append(cycle)
            parties_list.append(party)

OUTPUT_0_2 = pd.DataFrame({
    'district': districts_list,
    'cycle': cycles_list,
    'party': parties_list
})

OUTPUT_0_2 = OUTPUT_0_2.sort_values(['district', 'cycle', 'party']).reset_index(drop=True)

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


def create_aggregated_outputs(input_df, output_prefix, filter_type=None, suffix=''):
    # Apply filter if specified
    if filter_type:
        filtered_df = input_df[input_df['contributor.type'] == filter_type]
    else:
        filtered_df = input_df.copy()
    
    print(f"Processing {output_prefix}...")
    
    # Create output dataframes with appropriate filters
    output_dfs = {}
    
    # 1: All non-primary elections
    print(f"Filtering {output_prefix}_1...")
    output_dfs[f"{output_prefix}_1"] = filtered_df[
        filtered_df['election.type'] != 'P'
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount{suffix}': ('amount', 'sum'),
           f'tran_count{suffix}': ('transaction.id', 'count'),
           f'avg_amount{suffix}': ('amount', 'mean')}
    ).reset_index()
    
    # 2: All non-primary elections before special elections
    print(f"Filtering {output_prefix}_2...")
    output_dfs[f"{output_prefix}_2"] = filtered_df[
        (filtered_df['election.type'] != 'P') &
        (filtered_df['later_than_special'] == 0)
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_without_special_elections{suffix}': ('amount', 'sum'),
           f'tran_count_without_special_elections{suffix}': ('transaction.id', 'count'),
           f'avg_amount_without_special_elections{suffix}': ('amount', 'mean')}
    ).reset_index()
    
    # 3: General elections for Democrats
    print(f"Filtering {output_prefix}_3...")
    output_dfs[f"{output_prefix}_3"] = filtered_df[
        (filtered_df['election.type'] == 'G') &
        (filtered_df['party'] == 100)
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_dem_gen{suffix}': ('amount', 'sum'),
           f'tran_count_dem_gen{suffix}': ('transaction.id', 'count'),
           f'avg_amount_dem_gen{suffix}': ('amount', 'mean')}
    ).reset_index()
    
    # 4: General elections for Democrats before special elections
    print(f"Filtering {output_prefix}_4...")
    output_dfs[f"{output_prefix}_4"] = filtered_df[
        (filtered_df['election.type'] == 'G') &
        (filtered_df['later_than_special'] == 0) &
        (filtered_df['party'] == 100)
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_dem_gen_without_special{suffix}': ('amount', 'sum'),
           f'tran_count_dem_gen_without_special{suffix}': ('transaction.id', 'count'),
           f'avg_amount_dem_gen_without_special{suffix}': ('amount', 'mean')}
    ).reset_index()
    
    # 5: Primary elections for Democrats
    print(f"Filtering {output_prefix}_5...")
    output_dfs[f"{output_prefix}_5"] = filtered_df[
        (filtered_df['election.type'] == 'P') &
        (filtered_df['party'] == 100)
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_dem_primary{suffix}': ('amount', 'sum'),
           f'tran_count_dem_primary{suffix}': ('transaction.id', 'count'),
           f'avg_amount_dem_primary{suffix}': ('amount', 'mean')}
    ).reset_index()
    
    # 6: General elections for Republicans
    print(f"Filtering {output_prefix}_6...")
    output_dfs[f"{output_prefix}_6"] = filtered_df[
        (filtered_df['election.type'] == 'G') &
        (filtered_df['party'] == 200)
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_rep_gen{suffix}': ('amount', 'sum'),
           f'tran_count_rep_gen{suffix}': ('transaction.id', 'count'),
           f'avg_amount_rep_gen{suffix}': ('amount', 'mean')}
    ).reset_index()
    
    # 7: General elections for Republicans before special elections
    print(f"Filtering {output_prefix}_7...")
    output_dfs[f"{output_prefix}_7"] = filtered_df[
        (filtered_df['election.type'] == 'G') &
        (filtered_df['later_than_special'] == 0) &
        (filtered_df['party'] == 200)
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_rep_gen_without_special{suffix}': ('amount', 'sum'),
           f'tran_count_rep_gen_without_special{suffix}': ('transaction.id', 'count'),
           f'avg_amount_rep_gen_without_special{suffix}': ('amount', 'mean')}
    ).reset_index()
    
    # 8: Primary elections for Republicans
    print(f"Filtering {output_prefix}_8...")
    output_dfs[f"{output_prefix}_8"] = filtered_df[
        (filtered_df['election.type'] == 'P') &
        (filtered_df['party'] == 200)
    ].groupby(['district', 'cycle']).agg(
        **{f'total_amount_rep_primary{suffix}': ('amount', 'sum'),
           f'tran_count_rep_primary{suffix}': ('transaction.id', 'count'),
           f'avg_amount_rep_primary{suffix}': ('amount', 'mean')}
    ).reset_index()
    
    # Merge all outputs
    final_output = output_dfs[f"{output_prefix}_1"].copy()
    for i in range(2, 9):
        print(f"Merging {output_prefix} with {output_prefix}_{i}")
        df = output_dfs[f"{output_prefix}_{i}"]
        
        # Merge with the current result
        final_output = pd.merge(
            final_output,
            df,
            on=['cycle', 'district'],
            how='outer'
        )
    
    print("Merge complete!")
    return final_output

OUTPUT_2 = create_aggregated_outputs(OUTPUT_1, 'OUTPUT_2')

OUTPUT_2 = pd.merge(
    OUTPUT_0,
    OUTPUT_2,
    on=['cycle', 'district'],
    how='outer'
    )
OUTPUT_2.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_2.csv"), index=False)


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


OUTPUT_3 = create_aggregated_outputs(OUTPUT_1, 'OUTPUT_3', filter_type='C', suffix='_corp')

OUTPUT_3 = pd.merge(
    OUTPUT_0,
    OUTPUT_3,
    on=['cycle', 'district'],
    how='outer'
    )
OUTPUT_3.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_3.csv"), index=False)



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


OUTPUT_4 = create_aggregated_outputs(OUTPUT_1, 'OUTPUT_4', filter_type='I', suffix='_ind')

OUTPUT_4 = pd.merge(
    OUTPUT_0,
    OUTPUT_4,
    on=['cycle', 'district'],
    how='outer'
    )
OUTPUT_4.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_4.csv"), index=False)


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

OUTPUT_5 = pd.merge(
    OUTPUT_0,
    OUTPUT_5,
    on=['cycle', 'district'],
    how='outer'
    )
OUTPUT_5.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_5.csv"), index = False)


#%%

print("Processing OUTPUT_6...")
## OUTPUT_6: 

#     - dist_cycle_comp_35_65: We compute the average across candidates of the vote shares obtained in the general election. We consider only the two main candidates for this. Second, if this is between 35 and 65, we assign value 1, and 0 otherwise.

#     - dist_cycle_comp_30_70: Same as dist_cycle_comp_35_65 but using 30 and 70 as thresholds

#     - avg_gen_vote_pct_dem: Average gen.vote.pct values for district in previous cycles, for Dems

#     - avg_gen_vote_pct_rep: Average gen.vote.pct values for district in previous cycles, for Reps

#     - dist_cycle_comp_40_60: Same as dist_cycle_comp_35_65 but using 40 and 60 as thresholds

#     - dem_prim_cycle_comp_35_65: Same as dist_cycle_comp_35_65 but considering only (two main) candidates in Dem primary

#     - dem_prim_cycle_comp_30_70: Same as dist_cycle_comp_30_70 but considering only (two main) candidates in Dem primary

#     - dem_prim_cycle_comp_40_60: Same as dist_cycle_comp_40_60 but considering only (two main) candidates in Dem primary

#     - rep_prim_cycle_comp_35_65: Same as dist_cycle_comp_35_65 but considering only (two main) candidates in Rep primary

#     - rep_prim_cycle_comp_30_70: Same as dist_cycle_comp_30_70 but considering only (two main) candidates in Rep primary

#     - rep_prim_cycle_comp_40_60: Same as dist_cycle_comp_40_60 but considering only (two main) candidates in Rep primary

#     - Election_day: Day in which the seat is assigned


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
                    
    if measure_var == 'prim.vote.pct':
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
    
# 3.
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
    

    
# Creating datasets
print("Filtering OUTPUT_6_1_1...")
OUTPUT_6_1_1 = OUTPUT_1.groupby(['district', 'cycle']).apply(
    lambda x: calculate_metrics(x, 'gen.vote.pct')
).reset_index()
OUTPUT_6_1_1 = pd.merge(
    OUTPUT_0,
    OUTPUT_6_1_1,
    on=['cycle', 'district'],
    how='outer'
    )

# We groupby party as well, to get G_max for Dems and Reps (their vote share)
print("Filtering OUTPUT_6_1_2...")
OUTPUT_6_1_2 = OUTPUT_1.groupby(['district', 'cycle', 'party']).apply(
    lambda x: calculate_metrics(x, 'gen.vote.pct')
).reset_index()

OUTPUT_6_1_2 = pd.merge(
    OUTPUT_0_2,
    OUTPUT_6_1_2,
    on=['cycle', 'district', 'party'],
    how='outer'
    )
OUTPUT_6_1_2 = OUTPUT_6_1_2[OUTPUT_6_1_2['party'].isin(unique_parties)]

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

# Applying a function to create variables using a for loop for each dicitonary
print("Creating categorical variables for district in general...")
label_values = [0, 1, 0]

# NOTE: gen.vote.pct and prim.vote.pct use different scales for measuring percentages, labels adjusted accordingly
OUTPUT_6_1_dict_G = {
    'dist_cycle_comp_35_65': [0, 35, 65, 100],
    'dist_cycle_comp_30_70': [0, 30, 70, 100],
    'dist_cycle_comp_40_60': [0, 40, 60, 100]
    }

OUTPUT_6_2_dict_dem_P = {
    'dem_prim_cycle_comp_35_65': [0, 0.35, 0.65, 1],
    'dem_prim_cycle_comp_30_70': [0, 0.30, 0.70, 1],
    'dem_prim_cycle_comp_40_60': [0, 0.40, 0.60, 1]
    }

OUTPUT_6_2_dict_rep_P = {
    'rep_prim_cycle_comp_35_65': [0, 0.35, 0.65, 1],
    'rep_prim_cycle_comp_30_70': [0, 0.30, 0.70, 1],
    'rep_prim_cycle_comp_40_60': [0, 0.40, 0.60, 1]
    }


OUTPUT_6_1_1 = create_cat_vars(OUTPUT_6_1_1, OUTPUT_6_1_dict_G, 'G_dispersion', label_values)
OUTPUT_6_2_dem = create_cat_vars(OUTPUT_6_2_dem, OUTPUT_6_2_dict_dem_P, 'P_dispersion', label_values)
OUTPUT_6_2_rep = create_cat_vars(OUTPUT_6_2_rep, OUTPUT_6_2_dict_rep_P, 'P_dispersion', label_values)

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


# Saving datasets before dropping columns
OUTPUT_6_1_1.to_csv(os.path.join(data_folder, 'OUTPUTS', 'OUTPUT_6_1_1.csv'), index = False)
OUTPUT_6_1_3.to_csv(os.path.join(data_folder, 'OUTPUTS', 'OUTPUT_6_1_3.csv'), index = False)
OUTPUT_6_2_dem.to_csv(os.path.join(data_folder, 'OUTPUTS', 'OUTPUT_6_2_dem.csv'), index = False)
OUTPUT_6_2_rep.to_csv(os.path.join(data_folder, 'OUTPUTS', 'OUTPUT_6_2_rep.csv'), index = False)

# Dropping columns
dataframes = [OUTPUT_6_1_1, OUTPUT_6_1_3, OUTPUT_6_2_dem, OUTPUT_6_2_rep]
drop_columns = ['G_max', 'G_min', 'G_dispersion', 'G_average',
                'P_max', 'P_min', 'P_dispersion', 'P_average',
                'num_candidates']
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



#%%

print("Processing OUTPUT_7...")
## OUTPUT_7: 
    
#     - treatment_1: We assign 1 for district/cycles after death of an incumbent. We assign 0 otherwise

#     - treatment_2: We assign 1 for district/cycles for the first cycle after death of an incumbent. We assign 0 otherwise

#     - death_unexpected: We take this dummy from Deaths.xlsx dataset. For missing deaths only: You can assign this dummy following my logic (expected deaths come after a long illness situations). For districts with multiple deaths: Before the first death, consider the first death. Same for first cycle after the death. Then start considering the second, and so on...

#     - death_age: Age of dead incumbent. For districts with multiple deaths, follow same logiuc as death_unexpected

#     - death_party_member: Party of dead incumbent. For districts with multiple deaths, follow same logiuc as death_unexpected


OUTPUT_7 = OUTPUT_1[['district', 'cycle']].drop_duplicates()
print("Balancing dataset...")
OUTPUT_7 = pd.merge(
    OUTPUT_0,
    OUTPUT_7,
    how = 'outer',
    on = ['district', 'cycle']
    )


# Creating treat_1
print("Creating treat_1...")
OUTPUT_7['treat_1'] = 0
for district in single_death_districts + multiple_death_districts:
    # Get death year for this district
    district_death_dates = OUTPUT_1[
        (OUTPUT_1['district'] == district) & 
        (OUTPUT_1['death_date'].notna())
    ]['death_date']
    
    if not district_death_dates.empty:
        death_date = district_death_dates.iloc[0]
        death_year = pd.to_datetime(death_date).year
        
        # Assign treatment to cycles after death year
        OUTPUT_7.loc[
            (OUTPUT_7['district'] == district) & 
            (OUTPUT_7['cycle'] > death_year), 
            'treat_1'] = 1
    else:
        print(f"Warning: No death date found for district {district}")
print(f"Finished processing treat_1")

# Creating treat_2
print("Creating treat_2...")
OUTPUT_7['treat_2'] = 0
for district in single_death_districts + multiple_death_districts:
    # Get all deaths for this district with their years
    district_deaths = OUTPUT_1[
        (OUTPUT_1['district'] == district) & 
        (OUTPUT_1['death_date'].notna())
    ][['death_date', 'cycle']].drop_duplicates()
    
    district_deaths['death_year'] = pd.to_datetime(district_deaths['death_date']).dt.year
    
    # For each contribution in the district
    district_mask = OUTPUT_7['district'] == district
    
    if not district_death_dates.empty:
        
        if district in single_death_districts:
            
            # Copying treat_1 to treat_2
            OUTPUT_7.loc[district_mask, 'treat_2'] = OUTPUT_7.loc[district_mask, 'treat_1']
        
        # elif district in multiple_death_districts:
        #     print(f"{district} is multiple death")
        #     # Get all special election years for this district
        #     special_elections_death_dates = special_elections[
        #         (special_elections['district'] == district)
        #     ]['spec_election_date'].unique()
            
        #     special_elections_years = pd.to_datetime(special_elections_death_dates).year
            
        #     print(f"Distict: {district}, Special elections years: {special_elections_years}")
        #     print()
            
        #     # For each cycle in this district (sorted)
        #     district_cycles = sorted(OUTPUT_7[OUTPUT_7['district'] == district]['cycle'])       
            
        #     for i, cycle in enumerate(district_cycles):
        #         state = 0 # default state is 0
    
        #         # Check each death
        #         for _, death_row in district_deaths.iterrows():
        #             death_year = death_row['death_year']
                    
        #             # If current cycle is the first cycle after death
        #             if cycle > death_year:
        #                 # Check if there was a previous cycle after death but before current cycle
        #                 if i > 0 and district_cycles[i-1] > death_year:
        #                     # Not the first cycle after death, skip
        #                     continue
                        
        #                 # Check if there's a special election between death and this cycle
        #                 intervening_special = any((special_year > death_year) & (special_year <= cycle) 
        #                                          for special_year in special_elections_years)
                        
        #                 # If no intervening special election and this is the first cycle after death
        #                 if not intervening_special:
        #                     state = 1
        #                     break
                
        #         OUTPUT_7.loc[
        #             (OUTPUT_7['district'] == district) & 
        #             (OUTPUT_7['cycle'] == cycle), 
        #             'treat_2'] = state
                
        #         print(f"Finished processing treat_2 for {district}")
                
            
        elif district in multiple_death_districts:
            
            # Get all special election years for this district
            special_elections_death_dates = special_elections[
                (special_elections['district'] == district)
            ]['spec_election_date'].unique()
            
            special_elections_years = pd.to_datetime(special_elections_death_dates).year
            
            # For each cycle in this district (sorted)
            cycle_mask = OUTPUT_7['district'] == district
            district_cycles = sorted(OUTPUT_7[cycle_mask]['cycle'])       
            
            for cycle in district_cycles:
                state = 0  # default state is 0
                cycle_idx = district_cycles.index(cycle)
    
                # Check each death
                for _, death_row in district_deaths.iterrows():
                    death_year = death_row['death_year']
                    
                    # If current cycle is after death
                    if cycle > death_year:
                        # Check if there was a previous cycle after death but before current cycle
                        if cycle_idx > 0 and district_cycles[cycle_idx-1] > death_year:
                            # Not the first cycle after death, skip
                            continue
                        
                        # Check if there's a special election between death and this cycle
                        intervening_special = any((special_year > death_year) & (special_year <= cycle) 
                                                 for special_year in special_elections_years)
                        
                        # If no intervening special election and this is the first cycle after death
                        if not intervening_special:
                            state = 1
                            break
                
                # Use the combined mask for district and cycle
                combined_mask = district_mask & (OUTPUT_7['cycle'] == cycle)
                OUTPUT_7.loc[combined_mask, 'treat_2'] = state
                
        else:
            print("District not found in single_death_districts nor in multiple_death_districts")
        
    else:         
        print(f"Warning: No death date found for district {district}")        
print(f"Finished processing treat_2")   
        


# Creating death_unexpected, death_age, and death_party_member
print("Creating death_unexpected_1 (and _2), death_age_1 (and _2), and death_party_member_1 (and _2)...")
OUTPUT_7['death_unexpected_1'] = np.nan
OUTPUT_7['death_age_1'] = np.nan
OUTPUT_7['death_party_1'] = np.nan
OUTPUT_7['death_unexpected_2'] = np.nan
OUTPUT_7['death_age_2'] = np.nan
OUTPUT_7['death_party_2'] = np.nan

# for district in single_death_districts + multiple_death_districts:
#     # Get all deaths for this district with their metadata
#     district_deaths = OUTPUT_1[
#         (OUTPUT_1['district'] == district) & 
#         (OUTPUT_1['death_date'].notna())
#     ][['death_date', 'cycle', 'death_unexpected', 'death_age', 'resign_party', 'treat_1', 'treat_2']].drop_duplicates()
    
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
#                 ['death_unexpected', 'death_age', 'resign_party']
#             ] = [
#                 death_row['death_unexpected'],
#                 death_row['death_age'],
#                 death_row['resign_party']
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
#                 ['death_unexpected', 'death_age', 'resign_party']
#             ] = [
#                 relevant_death['death_unexpected'],
#                 relevant_death['death_age'],
#                 relevant_death['resign_party']
#             ]


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
    
    # Get all special election dates for this district
    special_elections_dates = pd.to_datetime(special_elections[
        (special_elections['district'] == district)
    ]['spec_election_date'].unique())
    special_elections_years = pd.Series([date.year for date in special_elections_dates if not pd.isna(date)])
    
    # Get all cycles for this district (sorted)
    district_cycles = sorted(OUTPUT_7[OUTPUT_7['district'] == district]['cycle'])
    
    # Initialize columns for this district if not already present
    for col in ['death_unexpected_1', 'death_age_1', 'death_party_1', 
                'death_unexpected_2', 'death_age_2', 'death_party_2']:
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
        first_death['resign_party']
    ]
    
        
    # IF DEATH ATTRIBUTES NEED TO BE ASSIGNED VALUES OF FIRST DEATH FOR ZERO VALUES OF TREAT_2
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
            relevant_death['resign_party']
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
    #             relevant_death['resign_party']
    #         ]
    #     else:
    #         # If no relevant death for treat_2, set to null
    #         OUTPUT_7.loc[
    #             (OUTPUT_7['district'] == district) & 
    #             (OUTPUT_7['cycle'] == cycle), 
    #             ['death_unexpected_2', 'death_age_2', 'death_party_2']
    #         ] = [np.nan, np.nan, np.nan]
    
print(f"Finished processing death attributes")

# Creating special_elections and special_elections_cause data
print("Creating special_elections and special_elections_cause variables")

# Subset of vars from special_elections data, latter only to show what has been merged
special_elections_2 = special_elections[['district', 'spec_cycle', 'resign_member', 'cause_vacancy']].rename( 
    columns = {'spec_cycle': 'cycle',
               'cause_vacancy': 'special_elections_cause'}) # rename for merging

OUTPUT_7 = pd.merge(
    OUTPUT_7,
    special_elections_2,
    how = 'left',
    on = ['district', 'cycle']
    )

OUTPUT_7['special_elections'] = np.where(OUTPUT_7['resign_member'].notna(), 1, 0)

test = OUTPUT_7[OUTPUT_7['district'] == 'CA05']

OUTPUT_7.to_csv(os.path.join(data_folder, 'OUTPUTS', 'OUTPUT_7.csv'), index = False)







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
    if i == 4:
        print("Merge complete!")
    else:
        continue
    
OUTPUT_8 = pd.merge(
    OUTPUT_0,
    OUTPUT_8,
    on = ['district', 'cycle'],
    how = 'outer'
    )

OUTPUT_8.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_8.csv"), index = False)


#%%

print("Processing OUTPUT_9...")

## OUTPUT_9: 
    
# each variable has cfscore and cfscore_dyn!

#     1 cfscore_mean_prim_dem: Average CFscore of candidates in the Dem primary (this tells us how much leftist the Dems are in this primary, computed using recipient.cfscores.dyn) 

#     2 cfscore_mean_prim_rep: Average CFscore of candidates in the Rep primary (same as above) 

#     3 cfscore_gen_dem: CFscore of Dem candidate in general

#     4 cfscore_gen_rep: CFscore of Rep candidate in general

#     5 cfscore_abs_diff: Absolute value of (CFscore of Dem candidate - CFscore of Rep candidate). This should capture difference in ideology between the two parties in the district. 

#     6 cfscore_mean_contrib: Average CFscore of all donors

#     7 cfscore_mean_contrib_dem: Average CFscore of all donors for Democrats

#     8 cfscore_mean_contrib_rep: Average CFscore of all donors for Republicans


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

OUTPUT_9_1 = OUTPUT_9_1[['district', 'cycle', 'cfscore_dyn_mean_prim_dem_final', 'count_cfscore']].rename(columns = {'cfscore_dyn_mean_prim_dem_final': 'cfscore_mean_prim_dem'})

# cfscore_mean_prim_rep
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

OUTPUT_9_2 = OUTPUT_9_2[['district', 'cycle', 'cfscore_dyn_mean_prim_rep_final', 'count_cfscore']].rename(columns = {'cfscore_dyn_mean_prim_rep_final': 'cfscore_mean_prim_rep'})


# cfscore_gen_dem
OUTPUT_9_3 = OUTPUT_9_0_rec[
    (OUTPUT_9_0_rec['election.type'] == 'G') &   # primary only
    (OUTPUT_9_0_rec['party'] == 100)             # dems
    ].groupby(['district', 'cycle', 'bonica.rid', 'recipient.name']).agg(
    count_cfscore=('recipient.cfscore', 'count'),
    count_cfscore_dyn=('recipient.cfscore.dyn', 'count'),
    cfscore_gen_dem=('recipient.cfscore', 'mean'),
    cfscore_dyn_gen_dem=('recipient.cfscore.dyn', 'mean')
).reset_index()
        
OUTPUT_9_3['cfscore_dyn_gen_dem_final'] = np.where(
    OUTPUT_9_3['cfscore_dyn_gen_dem'].isna(),  
    OUTPUT_9_3['cfscore_gen_dem'],             
    OUTPUT_9_3['cfscore_dyn_gen_dem']          
)

OUTPUT_9_3 = OUTPUT_9_3[['district', 'cycle', 'bonica.rid', 'recipient.name', 'cfscore_dyn_gen_dem_final', 'count_cfscore']].rename(columns = {'cfscore_dyn_gen_dem_final': 'cfscore_gen_dem'})

# Need to give canidate rank _1 and _2 so that we can pivot data 
OUTPUT_9_3['candidate_rank'] = OUTPUT_9_3.groupby(['district', 'cycle'])['bonica.rid'].transform(
lambda x: pd.Series(range(1, len(x) + 1), index=x.index))
# Add underline for suffix (useful for var ordering later)   
suffixes = ['_' + str(rank) for rank in OUTPUT_9_3['candidate_rank'].unique().tolist()]

OUTPUT_9_3.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_9_3_dems_to_check.csv"), index = False)
        
OUTPUT_9_3 = OUTPUT_9_3.pivot_table(
    index=['district', 'cycle'],
    columns='candidate_rank',
    values=['count_cfscore', 'cfscore_gen_dem']
).reset_index()

# Renaming columns
OUTPUT_9_3.columns = [
    f'{col[0]}_{col[1]}' if col[1] != '' else col[0] 
    for col in OUTPUT_9_3.columns
]

# Reordering columns
OUTPUT_9_3_cols = OUTPUT_9_3.columns.tolist()

base_cols = [col for col in OUTPUT_9_3_cols if not any(col.endswith(suffix) for suffix in suffixes)]

cols_suffix_list = []
for suffix in suffixes:
    cols_suffix = [col for col in OUTPUT_9_3_cols if col.endswith(suffix)]
    cols_suffix_list.extend(cols_suffix)  
new_col_order = base_cols + cols_suffix_list

OUTPUT_9_3 = OUTPUT_9_3[new_col_order]


# cfscore_gen_rep
OUTPUT_9_4 = OUTPUT_9_0_rec[
    (OUTPUT_9_0_rec['election.type'] == 'G') &   # primary only
    (OUTPUT_9_0_rec['party'] == 200)             # dems
    ].groupby(['district', 'cycle', 'bonica.rid', 'recipient.name']).agg(
    count_cfscore=('recipient.cfscore', 'count'),
    count_cfscore_dyn=('recipient.cfscore.dyn', 'count'),
    cfscore_gen_rep=('recipient.cfscore', 'mean'),
    cfscore_dyn_gen_rep=('recipient.cfscore.dyn', 'mean')
).reset_index()
        
OUTPUT_9_4['cfscore_dyn_gen_rep_final'] = np.where(
    OUTPUT_9_4['cfscore_dyn_gen_rep'].isna(),  
    OUTPUT_9_4['cfscore_gen_rep'],             
    OUTPUT_9_4['cfscore_dyn_gen_rep']          
)

OUTPUT_9_4 = OUTPUT_9_4[['district', 'cycle', 'bonica.rid', 'recipient.name', 'cfscore_dyn_gen_rep_final', 'count_cfscore']].rename(columns = {'cfscore_dyn_gen_rep_final': 'cfscore_gen_rep'})
        
OUTPUT_9_4['candidate_rank'] = OUTPUT_9_4.groupby(['district', 'cycle'])['bonica.rid'].transform(
lambda x: pd.Series(range(1, len(x) + 1), index=x.index)
)   
suffixes = ['_' + str(rank) for rank in OUTPUT_9_4['candidate_rank'].unique().tolist()]

OUTPUT_9_4.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_9_4_reps_to_check.csv"), index = False)
        
        
OUTPUT_9_4 = OUTPUT_9_4.pivot_table(
    index=['district', 'cycle'],
    columns='candidate_rank',
    values=['count_cfscore', 'cfscore_gen_rep']
).reset_index()
OUTPUT_9_4.columns = [
    f'{col[0]}_{col[1]}' if col[1] != '' else col[0] 
    for col in OUTPUT_9_4.columns
]

OUTPUT_9_4_cols = OUTPUT_9_4.columns.tolist()
base_cols = [col for col in OUTPUT_9_4_cols if not any(col.endswith(suffix) for suffix in suffixes)]
cols_suffix_list = []
for suffix in suffixes:
    cols_suffix = [col for col in OUTPUT_9_4_cols if col.endswith(suffix)]
    cols_suffix_list.extend(cols_suffix)  
new_col_order = base_cols + cols_suffix_list
OUTPUT_9_4 = OUTPUT_9_4[new_col_order]


# cfscore_abs_diff
OUTPUT_9_5 = pd.merge(
    OUTPUT_9_3,
    OUTPUT_9_4,
    on = ['district', 'cycle'],
    how = 'outer'
    )

OUTPUT_9_5['cfscore_abs_diff'] = abs(OUTPUT_9_5['cfscore_gen_dem_1'] - OUTPUT_9_5['cfscore_gen_rep_1'])



# cfscore_mean_contrib
OUTPUT_9_6 = OUTPUT_9_0_con.groupby(['district', 'cycle']).agg(
    count_cfscore=('contributor.cfscore', 'count'),
    count_cfscore_dyn=('contributor.cfscore', 'count'),
    cfscore_mean_contrib=('contributor.cfscore', 'mean'),
    cfscore_dyn_mean_contrib=('contributor.cfscore', 'mean')
).reset_index()

OUTPUT_9_6['cfscore_dyn_mean_contrib_final'] = np.where(
    OUTPUT_9_6['cfscore_dyn_mean_contrib'].isna(),  
    OUTPUT_9_6['cfscore_mean_contrib'],             
    OUTPUT_9_6['cfscore_dyn_mean_contrib']          
)

OUTPUT_9_6 = OUTPUT_9_6[['district', 'cycle', 'cfscore_dyn_mean_contrib_final', 'count_cfscore']].rename(columns = {'cfscore_dyn_mean_contrib_final': 'cfscore_mean_contrib'})


# cfscore_mean_contrib_dem
OUTPUT_9_7 = OUTPUT_9_0_con[
    (OUTPUT_9_0_con['party'] == 100)
    ].groupby(['district', 'cycle']).agg(
    count_cfscore=('contributor.cfscore', 'count'),
    count_cfscore_dyn=('contributor.cfscore', 'count'),
    cfscore_mean_contrib_dem=('contributor.cfscore', 'mean'),
    cfscore_dyn_mean_contrib_dem=('contributor.cfscore', 'mean')
).reset_index()

OUTPUT_9_7['cfscore_dyn_mean_contrib_dem_final'] = np.where(
    OUTPUT_9_7['cfscore_dyn_mean_contrib_dem'].isna(),  
    OUTPUT_9_7['cfscore_mean_contrib_dem'],             
    OUTPUT_9_7['cfscore_dyn_mean_contrib_dem']          
)

OUTPUT_9_7 = OUTPUT_9_7[['district', 'cycle', 'cfscore_dyn_mean_contrib_dem_final', 'count_cfscore']].rename(columns = {'cfscore_dyn_mean_contrib_dem_final': 'cfscore_mean_contrib_dem'})

# cfscore_mean_contrib_rep
OUTPUT_9_8 = OUTPUT_9_0_con[
    (OUTPUT_9_0_con['party'] == 200)
    ].groupby(['district', 'cycle']).agg(
    count_cfscore=('contributor.cfscore', 'count'),
    count_cfscore_dyn=('contributor.cfscore', 'count'),
    cfscore_mean_contrib_rep=('contributor.cfscore', 'mean'),
    cfscore_dyn_mean_contrib_rep=('contributor.cfscore', 'mean')
).reset_index()


OUTPUT_9_8['cfscore_dyn_mean_contrib_rep_final'] = np.where(
    OUTPUT_9_8['cfscore_dyn_mean_contrib_rep'].isna(),  
    OUTPUT_9_8['cfscore_mean_contrib_rep'],             
    OUTPUT_9_8['cfscore_dyn_mean_contrib_rep']          
)

OUTPUT_9_8 = OUTPUT_9_8[['district', 'cycle', 'cfscore_dyn_mean_contrib_rep_final', 'count_cfscore']].rename(columns = {'cfscore_dyn_mean_contrib_rep_final': 'cfscore_mean_contrib_rep'})



OUTPUT_9_1.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_9_1.csv"), index = False)
OUTPUT_9_2.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_9_2.csv"), index = False)
OUTPUT_9_3.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_9_3.csv"), index = False)
OUTPUT_9_4.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_9_4.csv"), index = False)
OUTPUT_9_5.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_9_5.csv"), index = False)
OUTPUT_9_6.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_9_6.csv"), index = False)
OUTPUT_9_7.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_9_7.csv"), index = False)
OUTPUT_9_8.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_9_8.csv"), index = False)






print("Merging datasets to OUTPUT_9...")

OUTPUT_9 = OUTPUT_9_1.copy()
for i in range(2, 9):
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
    if i == 8:
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

print("Final merge: Merging everything together")

OUTPUT_9 = OUTPUT_2.copy() # initialise with OUTPUT_2
for i in range(3, 9):
    print(f"Merging OUTPUT_9 with OUTPUT_9_{i}")
    df_name = f"OUTPUT_9_{i}"
    df = globals()[df_name]

    OUTPUT_9 = pd.merge(
        OUTPUT_9,
        df,
        on=['cycle', 'district'],
        how='outer'
    )
    if i == 8:
        print("Merge complete!")



### END OF SCRIPT!
print("..." * 5)
print("\nEnd of script!")







