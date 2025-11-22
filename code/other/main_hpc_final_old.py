#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 12 11:18:52 2025

@author: lirhoxhaj
"""

## PURPOSE OF FILE: Processing DIME data and merging with other relevant data to get OUTPUT_0, OUTPUT_0_2, OUTPUT_1, OUTPUT_1_corp and OUTPUT_1_ind 


### LIBRARIES

# pip install polars

import os
# import inspect
# import polars as pl
import pandas as pd
import numpy as np
# from pathlib import Path
# import matplotlib.pyplot as plt
# import seaborn as sns

#%%

### SETUP

# These lines will get the location of this file '\code\main.py'. Please ensure file is saved in folder \code. 

# This line does not work in interactive environment (e.g., Jupyter Notebook or interpreters like IDLE)
# code_folder = os.path.dirname(os.path.abspath(__file__))

# Get the directory where the current script is located
# code_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

# If all fails define working folder manually and run the lines here
# code_folder = "/Users/lirhoxhaj/Library/CloudStorage/OneDrive-ImperialCollegeLondon/Desktop/RA/Tommaso/Contributions_Paper/working_folder_lir/code"
# code_folder = r"C:\Users\lhoxhaj\OneDrive - Imperial College London\Desktop\RA\Tommaso\Contributions_Paper\working_folder_lir\code"

code_folder = "/rds/general/user/lhoxhaj/home/code/Contributions_Paper/"
parent_folder = "/rds/general/user/lhoxhaj/home/"
data_folder = "/rds/general/user/lhoxhaj/home/data/"


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
for year in range(1980, 2026, 2):
    file_path = os.path.join(data_folder, f"contribDB_{year}.csv")
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
        #    'candidate.cfscore'   # NOT NEEDED, EXISTS IN RECIPIENTS DATA

        df = pd.read_csv(
            file_path, 
            encoding='latin-1',
            usecols = ['cycle', 'transaction.id', 'transaction.type', 'amount', 'date',
                   'bonica.cid', 'contributor.name', 'contributor.type',
                   'contributor.gender', 'recipient.name',
                   'bonica.rid', 'seat', 'election.type', 'gis.confidence',
                   'contributor.district', 'latitude', 'longitude', 
                   'contributor.cfscore'] 
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
        print(df_filter[["spec_member", "spec_election_date"]])
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
nonna_rows = len(merged_df_2[~merged_df_2['spec_member'].isna()]) 
na_rows = len(merged_df_2[merged_df_2['spec_member'].isna()]) 
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
# - treat_1, treat_2, and treat_3
# - btw_death_and_spec_1, btw_death_and_spec_2, and btw_death_and_spec_3

## 1. later_than_special

## New dummy variable: “later_than_special”, =1 if the contribution is given in the same cycle of the death, but later than the special election (date of contribution is later than date of special election in that cycle).

merged_df_3['cycle'] = pd.to_numeric(merged_df_3['cycle'], errors='coerce')  # will convert None values to NaN
merged_df_3['date'] = pd.to_datetime(merged_df_3['date'], errors = 'coerce') # need to use errors = 'coerce' to include incorrect dates
merged_df_3['spec_election_date'] = pd.to_datetime(merged_df_3['spec_election_date'], errors = 'coerce') # need to use errors = 'coerce' to include incorrect dates
merged_df_3['resign_date'] = pd.to_datetime(merged_df_3['resign_date']) # no errors here, because these dates were created manually in special_elections.csv!
merged_df_3['death_date'] = pd.to_datetime(merged_df_3['death_date']) # no errors here, because these dates were created manually in special_elections.csv!
# merged_df_3['gen_elect_date'] = pd.to_datetime(merged_df_3['gen_elect_date']) # no errors here, because these dates were created manually in special_elections.csv!
merged_df_3['spec_gen_elect_date'] = pd.to_datetime(merged_df_3['spec_gen_elect_date']) # no errors here, because these dates were created manually in special_elections.csv!
merged_df_3['death_gen_elect_date'] = pd.to_datetime(merged_df_3['death_gen_elect_date']) # no errors here, because these dates were created manually in special_elections.csv!
merged_df_3['election_date_in_cycle'] = pd.to_datetime(merged_df_3['election_date_in_cycle']) # no errors here, because these dates were created manually in election_dates.csv!


merged_df_3['later_than_special'] = np.nan  # Initialize with zeros, districts that did not have special elections simply receive a zero


mask1 = (
    # Filtering for within election cycle, in terms of years between spec_election_date and year
    # (merged_df_3['date'] <= merged_df_3['spec_election_date'] + pd.DateOffset(months=6)) &
    # (merged_df_3['date'] < merged_df_3['election_date_in_cycle']) &
    (merged_df_3['cycle'] == merged_df_3['spec_cycle']) &
    # Main condition!
    (merged_df_3['date'] > merged_df_3['spec_election_date'])
)


mask0 = (
    # Filtering for within election cycle, in terms of years between spec_election_date and year
    # (merged_df_3['date'] <= merged_df_3['spec_election_date'] + pd.DateOffset(months=6)) &
    # (merged_df_3['date'] < merged_df_3['election_date_in_cycle']) &
    (merged_df_3['cycle'] == merged_df_3['spec_cycle']) &
    # Main condition!
    (merged_df_3['date'] <= merged_df_3['spec_election_date'])
)



# Apply the mask and handle NaN values in spec_election_date (keep as 0)
merged_df_3.loc[mask1, 'later_than_special'] = 1
merged_df_3.loc[mask0, 'later_than_special'] = 0
merged_df_3.loc[merged_df_3['spec_election_date'].isna(), 'later_than_special'] = np.nan

print("Value counts for later than special:")
print(merged_df_3['later_than_special'].value_counts())
print(f"Types of election for later_than_special == 0 (total observations: {len(merged_df_3[merged_df_3['later_than_special'] == 0])})")
print(merged_df_3[merged_df_3['later_than_special'] == 0]['election.type'].value_counts())
print(f"Types of election for later_than_special == 1 (total observations: {len(merged_df_3[merged_df_3['later_than_special'] == 1])})")
print(merged_df_3[merged_df_3['later_than_special'] == 1]['election.type'].value_counts())
print()

# Creating variable for election.type == 'S'
merged_df_3['election_type_S'] = 0
merged_df_3.loc[merged_df_3['election.type'] == 'S', 'election_type_S'] = 1

print("Value counts for election_type_S:")
print(merged_df_3['election_type_S'].value_counts())
print()
print("Contributions going to special elections before special elections date:", len(merged_df_3[(merged_df_3['election_type_S'] == 1) & (merged_df_3['date'] < merged_df_3['spec_election_date'])]),
      f"(Percent of total: {round(100 * len(merged_df_3[(merged_df_3['election_type_S'] == 1) & (merged_df_3['date'] < merged_df_3['spec_election_date'])]) / len(merged_df_3[merged_df_3['election_type_S'] == 1]), 2)}%)")
print("Contributions going to special elections on special elections date:", len(merged_df_3[(merged_df_3['election_type_S'] == 1) & (merged_df_3['date'] == merged_df_3['spec_election_date'])]),
      f"(Percent of total: {round(100 * len(merged_df_3[(merged_df_3['election_type_S'] == 1) & (merged_df_3['date'] == merged_df_3['spec_election_date'])]) / len(merged_df_3[merged_df_3['election_type_S'] == 1]), 2)}%)")
print("Contributions going to special elections after special elections date:", len(merged_df_3[(merged_df_3['election_type_S'] == 1) & (merged_df_3['date'] > merged_df_3['spec_election_date'])]),
      f"(Percent of total: {round(100 * len(merged_df_3[(merged_df_3['election_type_S'] == 1) & (merged_df_3['date'] > merged_df_3['spec_election_date'])]) / len(merged_df_3[merged_df_3['election_type_S'] == 1]), 2)}%)")


# Discrepancy! The contributions going to special elections according to Bonica's raw data are mostly coming before the special election date, although some 
print()
# print("Number of contributions going to special elect")


#%%
## 2. days_to_nearest_death

## For each contribution in a district where an incumbent died:
    
#   2. Create a variable “days_to_nearest_death” which counts the number of days between the contribution (variable date in Input 1 dataset) and the date of the nearest death in the district (from Input 3 dataset)
#      - <0 for contributions earlier than death, >0 for contributions later than death
#      - Be careful! There are districts where more than one incumbent died (here, “nearest” death is key!)



# For each district-cycle combination, find all death dates
district_cycle_deaths = merged_df_3[merged_df_3['spec_member'].notna()][
    ['district', 'cycle', 'death_date']].drop_duplicates()

# We calculate days to nearest death for all rows, and the absolute value as well
merged_df_3['days_to_nearest_death'] = (merged_df_3['date'] - merged_df_3['death_date']).dt.days
merged_df_3['abs_days_to_death'] = merged_df_3['days_to_nearest_death'].abs()
# Then, we sort by some variables, as well as absolute death, which brings up the contributions closest to death dates first

duplicate_columns = ['cycle', 'date', 'transaction.id', 'abs_days_to_death']
merged_df_3 = merged_df_3.sort_values(
    by=duplicate_columns,
    ascending=[True, True, True, True]  # 4 values to match the 4 columns
)


# Now keep only the first occurrence of each unique contribution (closest to death)
# This allows us to have unique rows for each contributions (rather than duplicates), even for problematic rows
merged_df_3 = merged_df_3.drop_duplicates(
    subset=['cycle', 'date', 'transaction.id'],
    keep='first' 
)


print("Value counts for later than special (AFTER DROPPING DUPLICATES):")
print(merged_df_3['later_than_special'].value_counts())
print(f"Types of election for later_than_special == 0 (total observations: {len(merged_df_3[merged_df_3['later_than_special'] == 0])})")
print(merged_df_3[merged_df_3['later_than_special'] == 0]['election.type'].value_counts())
print(f"Types of election for later_than_special == 1 (total observations: {len(merged_df_3[merged_df_3['later_than_special'] == 1])})")
print(merged_df_3[merged_df_3['later_than_special'] == 1]['election.type'].value_counts())
print()
print("Value counts for election_type_S (AFTER DROPPING DUPLICATES):")
print(merged_df_3['election_type_S'].value_counts())
print()
print("Contributions going to special elections before special elections date:", len(merged_df_3[(merged_df_3['election_type_S'] == 1) & (merged_df_3['date'] < merged_df_3['spec_election_date'])]),
      f"(Percent of total: {round(100 * len(merged_df_3[(merged_df_3['election_type_S'] == 1) & (merged_df_3['date'] < merged_df_3['spec_election_date'])]) / len(merged_df_3[merged_df_3['election_type_S'] == 1]), 2)}%)")
print("Contributions going to special elections on special elections date:", len(merged_df_3[(merged_df_3['election_type_S'] == 1) & (merged_df_3['date'] == merged_df_3['spec_election_date'])]),
      f"(Percent of total: {round(100 * len(merged_df_3[(merged_df_3['election_type_S'] == 1) & (merged_df_3['date'] == merged_df_3['spec_election_date'])]) / len(merged_df_3[merged_df_3['election_type_S'] == 1]), 2)}%)")
print("Contributions going to special elections after special elections date:", len(merged_df_3[(merged_df_3['election_type_S'] == 1) & (merged_df_3['date'] > merged_df_3['spec_election_date'])]),
      f"(Percent of total: {round(100 * len(merged_df_3[(merged_df_3['election_type_S'] == 1) & (merged_df_3['date'] > merged_df_3['spec_election_date'])]) / len(merged_df_3[merged_df_3['election_type_S'] == 1]), 2)}%)")

# Checking for later_than_special masked rows
print("Nr of districts with contributions that either have later_than_special == 0:", merged_df_3[merged_df_3['later_than_special'] == 0]['district'].nunique())
print("Nr of districts with contributions that either have later_than_special == 1:", merged_df_3[merged_df_3['later_than_special'] == 1]['district'].nunique())
print("Nr of districts with contributions that either have later_than_special == 1 or == 0:", merged_df_3[(merged_df_3['later_than_special'] == 0) | (merged_df_3['later_than_special'] == 1)]['district'].nunique())
print("Total nr of districts in data that have special election:", merged_df_3[~merged_df_3['spec_election_date'].isna()]['district'].nunique())
print("Total nr of districts in data:", merged_df_3['district'].nunique())




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


# # Summary stats for days_to_nearest_death and histograms
# print(merged_df_3['days_to_nearest_death'].describe())

# plt.figure(figsize=(10, 6))
# sns.histplot(data=merged_df_3, x='days_to_nearest_death', bins=30)
# plt.axvline(x=0, color='red', linewidth=3, label = 'Difference = 0')
# plt.axvline(x=merged_df_3['days_to_nearest_death'].mean(), color='blue', linewidth=3, label = 'Mean value')  # adds vertical line
# plt.title('Distribution of days_to_nearest_death')
# plt.xlabel('Value')
# plt.ylabel('Count')
# plt.legend()
# plt.show()



#%%

## 3. treat_1, treat_2 and treat_3

## For each contribution in a district where an incumbent died:

#   3. Create two dummy variable "treatment", 
#      - treat_1: simple case. 
#        -> For single death districts, assign values 0 before death, 1 after death. 
#        -> For multiple death districts, we assume first death is the only one. 
#      - treat_2: complex case. 
#        -> For single death districts, repeats logic from treat_1. 
#        -> For multiple death districts, we assign 0 before death, 1 after death, and then reset to 0 after election_cycle, and repeat until next death.

# NOTE: these are used in the OUTPUTS later
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


# Create treat_3 (single death districts only treated within same cycle, multiple district logic is the same as treat_2)
merged_df_3['treat_3'] = 0

for district in single_death_districts + multiple_death_districts:
    district_mask = merged_df_3['district'] == district
    
    if district in single_death_districts:
        print(f"{district} is single death")
        # Get the death date and cycle for this district
        district_death_info = merged_df_3[
            (merged_df_3['district'] == district) & 
            (merged_df_3['death_date'].notna())
        ][['death_date', 'spec_cycle']].drop_duplicates()
        
        if not district_death_info.empty:
            death_date = district_death_info.iloc[0]['death_date']
            death_cycle = district_death_info.iloc[0]['spec_cycle']
            
            # Set treat_3 = 1 only if date > death_date AND in the same cycle
            merged_df_3.loc[
                (merged_df_3['district'] == district) & 
                (merged_df_3['date'] > death_date) &
                (merged_df_3['cycle'] == death_cycle), 
                'treat_3'] = 1
        else:
            print(f"Warning: No death date found for district {district}")
            
    elif district in multiple_death_districts:
        print(f"{district} is multiple death")
        # For multiple death districts, replicate treat_2
        merged_df_3.loc[district_mask, 'treat_3'] = merged_df_3.loc[district_mask, 'treat_2']
    else:
        print(f"{district} not found in single_death_districts nor in multiple_death_districts")


# # checking ...
# def plot_scatter(picked_district, treatment):
#     merged_df_3_sample = merged_df_3[merged_df_3['district'] == picked_district]
#     death_dates = merged_df_3_sample['death_date'].dropna().unique()
    
#     # Define election_dates here so it's available in all branches
#     election_dates = merged_df_3_sample['election_date_in_cycle'].dropna().unique()
    
#     if treatment == 'treat_1':
#         # Plot for treat_1
#         plt.figure(figsize=(10, 6))
#         plt.scatter(merged_df_3_sample['date'], merged_df_3_sample['treat_1'], c='blue', label = 'Contributions', s = 0.5)
#         # Add a vertical line for each death date
#         for i, death_date in enumerate(death_dates):
#             plt.axvline(x=death_date, color='purple', linewidth=3, 
#                         label='Death date' if i == 0 else "_")  # Only one label in legend
        
#         # Add lines for election dates if you have them
#         for i, election_date in enumerate(election_dates):
#             plt.axvline(x=election_date, color='green', linestyle='--', linewidth=2, alpha=0.5,
#                         label='Election date' if i == 0 else "_")
        
#         plt.title(f'Scatterplot: treat_1 across time (for {picked_district})')
#         plt.xlabel('Date')
#         plt.ylabel('Treatment')
#         plt.legend()
#         plt.show()
    
#     elif treatment == 'treat_2':
#         # Plot for treat_2
#         plt.figure(figsize=(10, 6))
#         plt.scatter(merged_df_3_sample['date'], merged_df_3_sample['treat_2'], c='red', label = 'Contributions', s = 0.5)
#         for i, death_date in enumerate(death_dates):
#             plt.axvline(x=death_date, color='purple', linewidth=3, 
#                         label='Death date' if i == 0 else "_")
        
#         for i, election_date in enumerate(election_dates):
#             plt.axvline(x=election_date, color='green', linestyle='--', linewidth=2, alpha=0.5,
#                         label='Election date' if i == 0 else "_")
        
#         plt.title(f'Scatterplot: treat_2 across time (for {picked_district})')
#         plt.xlabel('Date')
#         plt.ylabel('Treatment')
#         plt.legend()
#         plt.show()
    
#     elif treatment == 'treat_3':
#         # Plot for treat_3
#         plt.figure(figsize=(10, 6))
#         plt.scatter(merged_df_3_sample['date'], merged_df_3_sample['treat_3'], c='orange', label = 'Contributions', s = 0.5)
#         for i, death_date in enumerate(death_dates):
#             plt.axvline(x=death_date, color='purple', linewidth=3, 
#                         label='Death date' if i == 0 else "_")
        
#         for i, election_date in enumerate(election_dates):
#             plt.axvline(x=election_date, color='green', linestyle='--', linewidth=2, alpha=0.5,
#                         label='Election date' if i == 0 else "_")
        
#         plt.title(f'Scatterplot: treat_3 across time (for {picked_district})')
#         plt.xlabel('Date')
#         plt.ylabel('Treatment')
#         plt.legend()
#         plt.show()

# # single death example
# plot_scatter('AL03', 'treat_1')
# plot_scatter('AL03', 'treat_2')
# plot_scatter('AL03', 'treat_3')

# plot_scatter('MS05', 'treat_1')
# plot_scatter('MS05', 'treat_2')
# plot_scatter('MS05', 'treat_3')


# # multiple death example
# plot_scatter('VA01', 'treat_1')
# plot_scatter('VA01', 'treat_2')
# plot_scatter('VA01', 'treat_3')

# plot_scatter('CA05', 'treat_1')
# plot_scatter('CA05', 'treat_2')
# plot_scatter('CA05', 'treat_3')


    

# Testing...
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
# print_treatment_example("IL21", 'treat_2')
# print_treatment_example("IL21", 'treat_3')
# print("\nMULTIPLE DEATH DISTRICT:")
# print_treatment_example("CA05", 'treat_2')


#%%

## 4. btw_death_and_spec_1, btw_death_and_spec_2, and btw_death_and_spec_3

# Create btw_death_and_spec_1 (treat_1 = 1 AND date between death_date and spec_election_date)
merged_df_3['btw_death_and_spec_1'] = 0
merged_df_3.loc[
    (merged_df_3['treat_1'] == 1) & 
    (merged_df_3['date'] > merged_df_3['death_date']) & 
    (merged_df_3['date'] < merged_df_3['spec_election_date']),
    'btw_death_and_spec_1'
] = 1

# Create btw_death_and_spec_2 (treat_2 = 1 AND date between death_date and spec_election_date)
merged_df_3['btw_death_and_spec_2'] = 0
merged_df_3.loc[
    (merged_df_3['treat_2'] == 1) & 
    (merged_df_3['date'] > merged_df_3['death_date']) & 
    (merged_df_3['date'] < merged_df_3['spec_election_date']),
    'btw_death_and_spec_2'
] = 1

# Create btw_death_and_spec_3 (treat_3 = 1 AND date between death_date and spec_election_date)
merged_df_3['btw_death_and_spec_3'] = 0
merged_df_3.loc[
    (merged_df_3['treat_3'] == 1) & 
    (merged_df_3['date'] > merged_df_3['death_date']) & 
    (merged_df_3['date'] < merged_df_3['spec_election_date']),
    'btw_death_and_spec_3'
] = 1


# def plot_scatter_2(picked_district, treatment):
#     merged_df_3_sample = merged_df_3[merged_df_3['district'] == picked_district]
#     death_dates = merged_df_3_sample['death_date'].dropna().unique()
    
#     # Define election_dates and special election dates
#     election_dates = merged_df_3_sample['election_date_in_cycle'].dropna().unique()
#     spec_election_dates = merged_df_3_sample['spec_election_date'].dropna().unique()
    
#     if treatment == 'btw_death_and_spec_1':
#         # Plot for btw_death_and_spec_1
#         plt.figure(figsize=(10, 6))
#         plt.scatter(merged_df_3_sample['date'], merged_df_3_sample['btw_death_and_spec_1'], c='blue', label='Contributions', s=0.5)
#         # Add a vertical line for each death date
#         for i, death_date in enumerate(death_dates):
#             plt.axvline(x=death_date, color='purple', linewidth=3, 
#                         label='Death date' if i == 0 else "_")
        
#         # Add lines for regular election dates
#         for i, election_date in enumerate(election_dates):
#             plt.axvline(x=election_date, color='green', linestyle='--', linewidth=2, alpha=0.5,
#                         label='Election date' if i == 0 else "_")
        
#         # Add lines for special election dates
#         for i, spec_election_date in enumerate(spec_election_dates):
#             plt.axvline(x=spec_election_date, color='red', linestyle=':', linewidth=2,
#                         label='Special election date' if i == 0 else "_")
        
#         plt.title(f'Scatterplot: btw_death_and_spec_1 across time (for {picked_district})')
#         plt.xlabel('Date')
#         plt.ylabel('Treatment')
#         plt.legend()
#         plt.show()
    
#     elif treatment == 'btw_death_and_spec_2':
#         # Plot for btw_death_and_spec_2
#         plt.figure(figsize=(10, 6))
#         plt.scatter(merged_df_3_sample['date'], merged_df_3_sample['btw_death_and_spec_2'], c='red', label='Contributions', s=0.5)
#         for i, death_date in enumerate(death_dates):
#             plt.axvline(x=death_date, color='purple', linewidth=3, 
#                         label='Death date' if i == 0 else "_")
        
#         for i, election_date in enumerate(election_dates):
#             plt.axvline(x=election_date, color='green', linestyle='--', linewidth=2, alpha=0.5,
#                         label='Election date' if i == 0 else "_")
        
#         for i, spec_election_date in enumerate(spec_election_dates):
#             plt.axvline(x=spec_election_date, color='red', linestyle=':', linewidth=2,
#                         label='Special election date' if i == 0 else "_")
        
#         plt.title(f'Scatterplot: btw_death_and_spec_2 across time (for {picked_district})')
#         plt.xlabel('Date')
#         plt.ylabel('Treatment')
#         plt.legend()
#         plt.show()
    
#     elif treatment == 'btw_death_and_spec_3':
#         # Plot for btw_death_and_spec_3
#         plt.figure(figsize=(10, 6))
#         plt.scatter(merged_df_3_sample['date'], merged_df_3_sample['btw_death_and_spec_3'], c='orange', label='Contributions', s=0.5)
#         for i, death_date in enumerate(death_dates):
#             plt.axvline(x=death_date, color='purple', linewidth=3, 
#                         label='Death date' if i == 0 else "_")
        
#         for i, election_date in enumerate(election_dates):
#             plt.axvline(x=election_date, color='green', linestyle='--', linewidth=2, alpha=0.5,
#                         label='Election date' if i == 0 else "_")
        
#         for i, spec_election_date in enumerate(spec_election_dates):
#             plt.axvline(x=spec_election_date, color='red', linestyle=':', linewidth=2,
#                         label='Special election date' if i == 0 else "_")
        
#         plt.title(f'Scatterplot: btw_death_and_spec_3 across time (for {picked_district})')
#         plt.xlabel('Date')
#         plt.ylabel('Treatment')
#         plt.legend()
#         plt.show()

# # single death example
# plot_scatter_2('AL03', 'btw_death_and_spec_1')
# plot_scatter_2('AL03', 'btw_death_and_spec_2')
# plot_scatter_2('AL03', 'btw_death_and_spec_3')
# plot_scatter_2('MS05', 'btw_death_and_spec_1')
# plot_scatter_2('MS05', 'btw_death_and_spec_2')
# plot_scatter_2('MS05', 'btw_death_and_spec_3')

# # case where spec_elect_date = gen_elect_date
# plot_scatter_2('IL04', 'btw_death_and_spec_1')
# plot_scatter_2('IL04', 'btw_death_and_spec_2')
# plot_scatter_2('IL04', 'btw_death_and_spec_3')


# # multiple death example
# # plot_scatter_2('VA01', 'btw_death_and_spec_1')
# # plot_scatter_2('VA01', 'btw_death_and_spec_2')
# # plot_scatter_2('VA01', 'btw_death_and_spec_3')
# plot_scatter_2('CA05', 'btw_death_and_spec_1')
# plot_scatter_2('CA05', 'btw_death_and_spec_2')
# plot_scatter_2('CA05', 'btw_death_and_spec_3')



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

    
duplicate_columns = ['cycle', 'date', 'bonica.cid', 'bonica.rid', 'abs_days_to_death', 'spec_member']

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

## OUTPUT_1_dict: A dictionary of OUTPUT_1 variables, their description and their source
    
OUTPUT_1_dict = {
            
    'date': {
            'description': 'Transaction date of the contribution',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'district': {
            'description': 'District code: two-letter state code followed by congressional district number. Used to merge various datasets',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'transaction.id': {
            'description': 'Unique ID for transaction',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'cycle': {
            'description': 'Year of election cycle. Used to merge various datasets',
            'source': 'DIME data ; ChatGPT ; Wikipedia',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'transaction.type': {
            'description': 'FEC code for transaction type',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'amount': {
            'description': 'Dollar amount of the contribution',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'bonica.cid': {
            'description': 'A unique contributor ID assigned to each contributor (individual and organization)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'contributor.name': {
            'description': 'Complete name of contributor (last, first) ; suffix and title removed.',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'contributor.type': {
            'description': 'Indicator of contributor type: individual (I) or organization (C)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'contributor.gender': {
            'description': 'Gender of contributor, if contributor is individual',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'recipient.name': {
            'description': 'Name of the recipient candidate or committee',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'bonica.rid': {
            'description': 'Unique ID for recipients. Used to merge various contribution datasets with recipients data',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'seat_x': {
            'description': 'Elected office sought by candidate, as per the contributions data',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'election.type': {
            'description': 'Type of election contribution is going towards: primary (P), general (G), special (S), runoff (R)',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'latitude': {
            'description': 'Geographic latitude from where the contribution is coming from',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'longitude': {
            'description': 'Geographic longitude from where the contribution is coming from',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'gis.confidence': {
            'description': 'A measure of confidence of the accuracy of the GIS coordinates',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'contributor.district': {
            'description': 'District of contributor only',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'contributor.cfscore': {
            'description': 'Contributor’s ideal CFscore, as a measure of their political ideology or beliefs',
            'source': 'DIME data',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'election': {
            'description': 'Election cycle preceded by two-letter state code. Federal candidates have ’fd’ as the state code',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'fecyear': {
            'description': 'Year listed by the FEC indicating the year of campaign’s the target election.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'name': {
            'description': 'Name of the candidate/recipient',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'title': {
            'description': 'Title of the candidate/recipient  (e.g. Mr., Mrs., Dr., Esq)',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'suffix': {
            'description': 'Suffix of the candidate/recipient',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'party': {
            'description': 'Party of the candidate/ recipient (100 = Dem, 200 = Rep, 328 = Ind)',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'state': {
            'description': 'Two-letter state abbreviations',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'seat_y': {
            'description': 'Elected office sought by candidate, as per the recipients data',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'ico.status': {
            'description': 'Incumbency status. (I = Incumbent, C = Challenger, O = Open Seat Candidate, blank - not up for election).',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'cand.gender': {
            'description': 'Candidate gender codings',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'recipient.cfscore': {
            'description': 'Recipient’s CFscore, as a measure of their political ideology or beliefs, based on donations received',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'recipient.cfscore.dyn': {
            'description': 'Period-specific estimates of ideology. (Candidate/recipient scores are re-estimated in each election cycle while holding contributor scores constant.)',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'dwdime': {
            'description': 'Ideological measures for recipients',
            'source': 'DIME data ; voteview.com',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://doi.org/10.1111/ajps.12376 ; https://www.voteview.com/'
            },
    
    'dwnom1': {
            'description': '(voteview.com) First dimension common-space DW-NOMINATE score. (Based on joint scaling of the 1st to the 117th Congresses.)',
            'source': 'DIME data ; voteview.com',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://doi.org/10.1111/ajps.12376 ; https://www.voteview.com/'
            },
    
    'dwnom2': {
            'description': '(voteview.com) Second dimension common-space DW-NOMINATE score.',
            'source': 'DIME data ; voteview.com',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://doi.org/10.1111/ajps.12376 ; https://www.voteview.com/'
            },
    
    'ps.dwnom1': {
            'description': '(voteview.com) First dimension Nokken-Poole period-specific DW-NOMINATE score. (Scores for the House and Senate are scaled separately and thus should not be directly compared.)',
            'source': 'DIME data ; voteview.com',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://doi.org/10.1111/ajps.12376 ; https://www.voteview.com/'
            },
    
    'ps.dwnom2': {
            'description': '(voteview.com) Second dimension Nokken-Poole period-specific DW-NOMINATE score.',
            'source': 'DIME data ; voteview.com',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://doi.org/10.1111/ajps.12376 ; https://www.voteview.com/'
            },
    
    'irt.cfscore': {
            'description': 'Estimates of ideology for recipients/candidates from IRT count-model applied to PAC data.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://doi.org/10.1111/ajps.12014'
            },
    
    'composite.score': {
            'description': 'Composite ideological score for recipients, combining information from multiple sources',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'num.givers': {
            'description': 'Number of distinct donors that gave to the candidate during a specific election cycle',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'num.givers.total': {
            'description': 'Number of distinct donors that gave to the candidate/recipient aggregating over the candidate/recipient’s career.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'total.receipts': {
            'description': 'Sum total of contributions raised during an election cycle',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'total.disbursements': {
            'description': ' Sum total campaign disbursements during an election cycle.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'total.indiv.contribs': {
            'description': 'Sum total of itemized contributions from individuals raised during an election cycle',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'total.unitemized': {
            'description': 'Sum of unitemized contributions from individuals raised during an election cycle.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'total.pac.contribs': {
            'description': 'Sum total contributions raised from contributions from PACs and other committees.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'total.party.contribs': {
            'description': 'Sum total raised from party committees.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'total.contribs.from.candidate': {
            'description': 'Sum total raised from candidate self-contributions.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'ind.exp.support': {
            'description': 'Sum total independent expenditures made to support the candidate.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'ind.exp.oppose': {
            'description': 'Sum total independent expenditures made to oppose the candidate.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'prim.vote.pct': {
            'description': 'FEC reported vote share in primary election. Primary election outcomes are only coded for federal congressional candidates.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'pwinner': {
            'description': 'Primary election outcome (’W’ = won election; ’L’ = lost election). Primary election outcomes are only coded for federal congressional candidates.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'gen.vote.pct': {
            'description': 'FEC reported vote share in general election.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'gwinner': {
            'description': '...',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    's.elec.stat': {
            'description': 'FEC special election code (W = Win) (L = Lose).',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'r.elec.stat': {
            'description': 'FEC run-off election code (W = Win) (L = Lose).',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'district.pres.vs': {
            'description': 'District-level percentage of the two-party vote share won by the Democratic presidential nominee in the most recent (or concurrent) presidential election.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'fec.cand.status': {
            'description': 'Indicates the status of the candidate’s campaign assigned by the FEC. (C = Statutory candidate; F = Statutory candidate for future election; N = Not yet a statutory candidate; P = Statutory candidate in prior cycle)',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'comtype': {
            'description': 'FEC code for type of committee.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'ICPSR': {
            'description': 'Adjusted ICPSR legislator ID. Candidates that have never served in Congress are assigned IDs based off of their candidate IDs assigned by the FEC, NIMSP, or state reporting agencies. The four-digit election cycle is appended to the ID. Candidates that are active in multiple election cycles (or file to run for multiple seats during a single election cycle) will appear multiple times. This variable provides a unique row identifier',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'ICPSR2': {
            'description': 'Adjusted ICPSR legislator ID. Candidates that have never served in Congress are assigned IDs based off of their candidate IDs assigned by the FEC, NIMSP, or state reporting agencies. Party switchers are assigned new ICPSR2 IDs after switching parties. (See before.switch.ICPSR and after.switch.ICPSR.)',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'Cand.ID': {
            'description': 'The candidate ID assigned by the FEC or state reporting agency.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'FEC.ID': {
            'description': 'The committee ID assigned by the FEC or state reporting agency campaign committee.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'NID': {
            'description': '(CRP/NIMSP) Unique candidate IDs assigned by the Center for Responsive Politics/National Institute for Money in State Politics.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'before.switch.ICPSR': {
            'description': 'ICPSR ID prior to switching parties (included for party-switchers only).',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'after.switch.ICPSR': {
            'description': 'ICPSR ID after switching parties (included for party-switchers only).',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'party.orig': {
            'description': 'Original party before switch.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'nimsp.party': {
            'description': '(nimsp) three-letter party code assigned by the NIMSP',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'nimsp.candidate.ICO.code': {
            'description': '(nimsp) incumbency status assigned by the NIMSP.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'nimsp.district': {
            'description': '(nimsp) district number assigned by the NIMSP.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'nimsp.office': {
            'description': '(nimsp) state-office sought.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'nimsp.candidate.status': {
            'description': '(nimsp) election outcome.',
            'source': 'DIME data',
            'origin_dataset': 'dime_recipients_1979_2024.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime'
            },
    
    'spec_election_year': {
            'description': 'Year of special election, if special election occurred because of resignation or death of incumbent',
            'source': 'ChatGPT and Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'spec_cycle': {
            'description': 'Election cycle year of special election, if special election occurred because of resignation or death of incumbent',
            'source': 'ChatGPT and Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'spec_member': {
            'description': 'Full name of incumbent (candidate) that either resigned or passed away',
            'source': 'ChatGPT and Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'spec_party': {
            'description': 'Party of incumbent (candidate) that either resigned or passed away',
            'source': 'ChatGPT and Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'resign_date': {
            'description': 'Date of resignation of incumbent (candidate), includes death dates as well. Note: Not retrieved for all candidates that resigned, but applies to all candidates that passed away.',
            'source': 'ChatGPT and Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'death_date': {
            'description': 'Date of death of incumbent (candidate). Note: Retrieved for all candidates that passed away.',
            'source': 'ChatGPT and Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'spec_election_date': {
            'description': 'Date special election date was held in for district-cycle, following resignation or death of candidate. Note: Not retrieved for all candidates that resigned, but applies to all candidates that passed away.',
            'source': 'ChatGPT and Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'cause_vacancy': {
            'description': 'Cause of vacancy (Death or Resigned)',
            'source': 'ChatGPT and Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'death_age': {
            'description': 'Age of candidate when they passed away',
            'source': 'ChatGPT and Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'death_cause': {
            'description': 'Cause that led to death of candidate. Contains description that was used to construct death_unexpected',
            'source': 'ChatGPT and Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'death_unexpected': {
            'description': 'Dummy variable indicating if death of candidate was unexpected or not (1 if yes, 0 if not), based on the cause of the death.',
            'source': 'ChatGPT and Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'resign_year': {
            'description': 'Year of resignation or death. Note: Not retrieved for all candidates that resigned, but applies to all candidates that passed away.',
            'source': 'ChatGPT and Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'spec_election_Congress': {
            'description': 'Number of congress election was held retrieved at from Wikipedia page. Note: not retrieved for all candidates.',
            'source': 'Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'spec_election_Original_candidate': {
            'description': 'Number of original candidate retrieved from Wikipedia page. Note: not retrieved for all candidates.',
            'source': 'Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'spec_election_Cause_of_vacancy': {
            'description': 'Cause of vacancy retrieved from Wikipedia page. Note: not retrieved for all candidates.',
            'source': 'Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'spec_Winner': {
            'description': 'Winner of special elections, retrieved from Wikipedia page. Note: not retrieved for all candidates.',
            'source': 'Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'original_district': {
            'description': 'District of candidate retrieved from Wikipedia. Note: not retrieved for all candidates.',
            'source': 'Wikipedia',
            'origin_dataset': 'special_elections_final.csv',
            'relevant_URLs': 'https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'election_date_in_cycle': {
            'description': 'Date of the general election held at that election cycle. Used to enhance logic of treatment variables',
            'source': 'ChatGPT',
            'origin_dataset': 'election_dates.csv',
            'relevant_URLs': ''
            },
    
    'later_than_special': {
            'description': 'Dummy variable that indicates if a contribution in the same cycle of the death came later than the date of the special election (1), or earlier (0). Missing value indicates special elections did not occur in that specific district-cycle (we know if a special election happened in that district-cycle if the DIME contributions and recipients data is merged with the special_elections data).',
            'source': 'Author (from merged datasets)',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'election_type_S': {
            'description': "Dummy variable that indicates if a contribution is going towards a special election, as indicated by Bonica's election.type variable. Gets value 1 if election.type == 'S', else 0 (belongs to G, P, or R).",
            'source': 'Author (from merged datasets)',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'days_to_nearest_death': {
            'description': 'Counts the number of days between the contribution date and the date of the nearest death in the district',
            'source': 'Author (from merged datasets)',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'abs_days_to_death': {
            'description': 'Absolute value of days_to_nearest_death',
            'source': 'Author (from merged datasets)',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'treat_1': {
            'description': "First dummy we use to measure the impact of the death of an incumbent on contributions. For single death districts, we assign values of 0 before death of the incumbent, and 1 after death (based on whether the contribution is coming before or after death_date). For multiple death districts, we assume first death is the only death, hence we assign 0 to all contributions coming before the first death of an incumbent in that district's history, and 1 to contributions coming after the death.",
            'source': 'Author (from merged datasets)',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            },
    
    'treat_2': {
            'description': "Second dummy we use to measure the impact of the death of an incumbent on contributions. For single death districts, we repeat the logic we used for these districts in treat_1. For multiple death districts, we assign 0 to all contributions coming before the first death of an incumbent in that district's history, 1 to contributions coming after the first death up until the end of the election cycle in which the special elections happened. In the new election cycle the values are set back to 0. This is repeated for the second death, and all other deaths.",
            'source': 'Author (from merged datasets)',
            'origin_dataset': 'contribDB_1980.csv to contribDB_2024.csv ; dime_recipients_1979_2024.csv ; special_elections_final.csv, election_dates.csv',
            'relevant_URLs': 'https://data.stanford.edu/dime ; https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives'
            }
    }

rows = []
for var_name, attributes in OUTPUT_1_dict.items():
    rows.append({
        'variable_name': var_name,
        'description': attributes['description'],
        'source': attributes['source'],
        'origin_dataset': attributes['origin_dataset'],
        'relevant_URLs': attributes['relevant_URLs'],
    })

OUTPUT_1_dict_df = pd.DataFrame(rows)

#%%
# Saving datasets for usage in other script
print("Saving OUTPUT_0, OUTPUT_0_2, OUTPUT_1, OUTPUT_1_corp, and OUTPUT_1_ind")
OUTPUT_0.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_0.csv"), index = False)
OUTPUT_0_2.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_0_2.csv"), index = False)
OUTPUT_1.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_1.csv"), index = False)
OUTPUT_1[OUTPUT_1['contributor.type'] == 'C'].to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_1_corp.csv"), index = False)
OUTPUT_1[OUTPUT_1['contributor.type'] == 'I'].to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_1_ind.csv"), index = False)
OUTPUT_1_dict_df.to_csv(os.path.join(data_folder, "OUTPUTS", "OUTPUT_1_dict.csv"), index = False)


print("\nEnd of main.py script!")


