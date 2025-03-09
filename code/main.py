#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 12 11:18:52 2025

@author: lirhoxhaj
"""

### LIBRARIES

import os
import inspect
import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns

#%%

## PURPOSE OF FILE: Merging DIME contributions and recipients data with deaths/resignation data, and special elections data.


### SETUP

# These lines will get the location of this file '\code\main.py'. Please ensure file is saved in folder \code. 

# This line may not work in interactive environment (e.g., Jupyter Notebook or interpreters like IDLE)
code_folder = os.path.dirname(os.path.abspath(__file__))

# Get the directory where the current script is located
code_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

# This is your working folder where folders '\code' and '\data' are saved
parent_folder = os.path.dirname(code_folder)

data_folder = os.path.join(parent_folder, "data")

print("Parent folder:", parent_folder, "\n")
print("Code folder:", code_folder, "\n")
print("Data folder:", data_folder, "\n")


#%%

### READING DATASETS

# This for loop saves all datasets in a dictionary

## INPUT 1: DIME database on contributions

contribDB_dict = {}
# Define range based on years we want to work with:
for year in range(1980, 2006, 2): # selecting sample of data
    file_path = Path(data_folder) / f"contribDB_{year}.csv"
    try:
        print(f"\nReading contribDB_{year}.csv ...")
        # excluding columns: 
        #    'contributor.lname', 'contributor.fname', 'contributor.mname',
        #    'contributor.suffix', 'contributor.title', 'contributor.ffname', 
        #    'contributor.address', 'contributor.city', 'contributor.state', 'contributor.zipcode', 
        #    'contributor.occupation', 'contributor.employer', 'occ.standardized', 'is.corp', 
        #    'recipient.party', 'recipient.type', 'recipient.state',
        #    'latitude', 'longitude', 'censustract', 
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
                   'contributor.district', 
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
            (df['date'].dt.year >= df['cycle'] - 2) &
            (df['date'].dt.year <= df['cycle'] + 2)
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

# print("Unique number of recipients:")
# print("  - bonica.rid:", recipients['bonica.rid'].nunique())
# print("  - name:", recipients['name'].nunique())

# recipients_duplicate = recipients.copy()

# recipients_duplicate['bid_year'] = recipients_duplicate['bonica.rid'] + recipients_duplicate['cycle'].astype(str)

# prob_bid = []
# for bid in recipients_duplicate['bonica.rid'].unique():
#     df_filter = recipients_duplicate[recipients_duplicate['bonica.rid']==bid]
#     if df_filter['name'].nunique() != 1:
#         print("bonica.rid:", bid)
#         print("Nr unique names:", df_filter['name'].nunique())
#         print("Unique names:", df_filter['name'].unique())
#         print("\n\n")
#         prob_bid.append(bid)
#     else:
#         continue

# recipients_duplicate_2 = recipients_duplicate[recipients_duplicate['bonica.rid'].isin(prob_bid)]

# recipients_duplicate_2 = recipients_duplicate_2 = recipients_duplicate_2.sort_values(
#     by=['bonica.rid', 'cycle', 'name'],
#     ascending=[True, True, True]  # 4 values to match the 4 columns
# )


# saving district values to .txt file for further analysis
all_recp_dist = recipients[recipients['seat']=='federal:house']['district'].unique()
all_recp_dist_df = pd.DataFrame(all_recp_dist)
np.savetxt(os.path.join(data_folder, 'district_in_federalhouse.txt'), 
           all_recp_dist, 
           fmt='%s')  # Use string format instead of default float format

short_districts = []
for dist in recipients[recipients['seat']=='federal:house']['district'].unique():
    if isinstance(dist, str) and len(dist) < 4:
        print("Districts that are less than four characters:")
        print(dist)
        short_districts.append(dist)
    elif not isinstance(dist, str):
        print("Non-string district value:")
        print(dist)
        
recp_short_dist = recipients[(recipients['seat']=='federal:house') & (recipients['district'].isin(short_districts))]
recp_short_dist.to_csv(os.path.join(data_folder, "short_district_in_federalhouse.csv"))




## INPUT 3: Self-constructed dataset of deaths and resignations, using special_elections.csv (see convert_html_to_csv.py)
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
    (merged_df_3['date'].dt.year >= merged_df_3['cycle'] - 2) &
    (merged_df_3['date'].dt.year < merged_df_3['cycle'] + 2) &
    (merged_df_3['date'] > merged_df_3['spec_election_date'])
)

mask = merged_df_3['date'] > merged_df_3['spec_election_date']

# Apply the mask
merged_df_3.loc[mask, 'later_than_special'] = 1
# Handle NaN values in spec_election_date (keep as 0)
merged_df_3.loc[merged_df_3['spec_election_date'].isna(), 'later_than_special'] = 0

print("Value counts for later than special:")
print(merged_df_3['later_than_special'].value_counts())


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
    
    print(f"\First 10 observations ({treatment_var}):")
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

merged_df_3_clean = merged_df_3.sort_values(
    ['transaction.id', 'district', 'days_to_nearest_death']
).drop_duplicates( 
    subset=['transaction.id', 'district'], 
    keep='first'
)

    
duplicate_columns = ['cycle', 'date', 'bonica.cid', 'bonica.rid', 'abs_days_to_death', 'resign_member']

# Finding what rows are being dropped
dropped_indices = set(merged_df_3.index) - set(merged_df_3_clean.index)
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

print("Processing output_1...")

## OUTPUT 1: contribution-day level dataset

# Sort the rows by date and transaction.id
output_1 = merged_df_3_clean.sort_values(['date', 'district', 'transaction.id'])
# Reorder columns to put date and transaction.id first
cols = ['date', 'district', 'transaction.id'] + [col for col in output_1.columns if col not in ['date', 'district', 'transaction.id']]
output_1 = output_1[cols]



# output_1.to_csv(os.path.join(data_folder, "output_1_1980to2000.csv"), index = False)



#%%

print("Processing output_2...")

## OUTPUT 2_1 and 2_2 (and _muted):
#     - OUTPUT_2_merged: OUTPUT_2 (collapsing OUTPUT 1 for district-cycle and getting sum of amount and count of contributions) and OUTPUT_2_muted (has muted vars)
#     - OUTPUT_2_corp_merged: like OUTPUT_2_merged but for contributions coming from corporates only! (OUTPUT_2_corp + OUTPUT_2_corp_muted)
#     - OUTPUT_2_ind_merged: like OUTPUT_2_merged but for contributions coming from individuals only! (OUTPUT_2_ind + OUTPUT_2_ind_muted)


# OUTPUT_2_merged
output_2 = output_1.groupby(['cycle', 'district']).agg({
    'amount': 'sum',
    'transaction.id': 'count'
}).reset_index()
output_2 = output_2.sort_values(['cycle', 'district'])
output_2['avg_amount_per_contribution'] = output_2['amount'] / output_2['transaction.id']


output_2_muted = output_1[output_1['later_than_special'] == 0].groupby(['cycle', 'district']).agg({
    'amount': 'sum',
    'transaction.id': 'count'
}).reset_index()
output_2_muted = output_2_muted.sort_values(['cycle', 'district'])
output_2_muted['avg_amount_per_contribution'] = output_2_muted['amount'] / output_2_muted['transaction.id']


output_2_merged = pd.merge(
    output_2,
    output_2_muted,
    on=['cycle', 'district'],
    how = 'left',
    suffixes=['', '_muted']
)



# OUTPUT_2_corp_merged
output_2_corp = output_1[
    output_1['contributor.type'] == 'C'
    ].groupby(['cycle', 'district']).agg({
    'amount': 'sum',
    'transaction.id': 'count'
}).reset_index()
output_2_corp = output_2_corp.sort_values(['cycle', 'district'])
output_2_corp['avg_amount_per_contribution'] = output_2_corp['amount'] / output_2_corp['transaction.id']


output_2_corp_muted = output_1[
    (output_1['contributor.type'] == 'C') & (output_1['later_than_special'] == 0)
    ].groupby(['cycle', 'district']).agg({
    'amount': 'sum',
    'transaction.id': 'count'
}).reset_index()
output_2_corp_muted = output_2_corp_muted.sort_values(['cycle', 'district'])
output_2_corp_muted['avg_amount_per_contribution'] = output_2_corp_muted['amount'] / output_2_corp_muted['transaction.id']


output_2_corp_merged = pd.merge(
    output_2_corp,
    output_2_corp_muted,
    on=['cycle', 'district'],
    how = 'left',
    suffixes=['', '_muted']
)



# OUTPUT_2_ind_merged
output_2_ind = output_1[
    output_1['contributor.type'] == 'I'
    ].groupby(['cycle', 'district']).agg({
    'amount': 'sum',
    'transaction.id': 'count'
}).reset_index()
output_2_ind = output_2_ind.sort_values(['cycle', 'district'])
output_2_ind['avg_amount_per_contribution'] = output_2_ind['amount'] / output_2_ind['transaction.id']


output_2_ind_muted = output_1[
    (output_1['contributor.type'] == 'I') & (output_1['later_than_special'] == 0)
    ].groupby(['cycle', 'district']).agg({
    'amount': 'sum',
    'transaction.id': 'count'
}).reset_index()
output_2_ind_muted = output_2_ind_muted.sort_values(['cycle', 'district'])
output_2_ind_muted['avg_amount_per_contribution'] = output_2_ind_muted['amount'] / output_2_ind_muted['transaction.id']


output_2_ind_merged = pd.merge(
    output_2_ind,
    output_2_ind_muted,
    on=['cycle', 'district'],
    how = 'left',
    suffixes=['', '_muted']
)



# output_2_merged.to_csv(os.path.join(data_folder, "output_2_merged.csv"), index = False)
# output_2_corp_merged.to_csv(os.path.join(data_folder, "output_2_corp_merged.csv"), index = False)
# output_2_ind_merged.to_csv(os.path.join(data_folder, "output_2_ind_merged.csv"), index = False)



#%%

print("Processing output_3...")

## OUPUT 3: 
#     - OUTPUT_3: grouping by district and election cycle to get dispersion index


def calculate_metrics(group, vote_share_variable):
    # Sort the group by 'gen.vote.pct' in descending order
    sorted_group = group.sort_values(by=vote_share_variable, ascending=False)
    
    # Select the top 2 candidates (assuming Dems and Reps, ignoring independent)
    unique_group = sorted_group.drop_duplicates(subset=vote_share_variable, keep='first')
    top_candidates = unique_group.head(2)
    
    # Calculate metrics
    vote_share_max = top_candidates[vote_share_variable].iloc[0] if len(top_candidates) > 0 else np.nan
    vote_share_min = top_candidates[vote_share_variable].iloc[1] if len(top_candidates) > 1 else top_candidates[vote_share_variable].min()
    dispersion_index = vote_share_max - vote_share_min if len(top_candidates) > 1 else np.nan
    num_candidates = group['bonica.rid'].nunique()
    total_candidates = len(group['bonica.rid'].unique())
    
    return pd.Series({
        'vote_share_max': vote_share_max,
        'vote_share_min': vote_share_min,
        'dispersion_index': dispersion_index,
        'num_candidates': num_candidates,
        'total_candidates': total_candidates
    })

# Apply the function to each group
output_3_G = output_1.groupby(['district', 'cycle']).apply(lambda x: calculate_metrics(x, 'gen.vote.pct')).reset_index()
output_3_P = output_1.groupby(['district', 'cycle']).apply(lambda x: calculate_metrics(x, 'prim.vote.pct')).reset_index()


# Checking (make sure to cross check using output_3 as well)
# df_filtered_1 = merged_df_3_clean[
#     (merged_df_3_clean['district'] == 'CA03') & 
#     (merged_df_3_clean['cycle'] == 1980)
#     ]

# df_filtered_2 = merged_df_3_clean[
#     (merged_df_3_clean['district'] == 'CA03') & 
#     (merged_df_3_clean['cycle'] == 2000)
#     ]

# candidate_stats_1 = df_filtered_1.groupby('bonica.rid')['gen.vote.pct'].agg([
#     'count', 'mean', 'std', 'min', 'max'
# ]).reset_index().sort_values('mean', ascending=False)
# print(candidate_stats_1)

# candidate_stats_2 = df_filtered_2.groupby('bonica.rid')['gen.vote.pct'].agg([
#     'count', 'mean', 'std', 'min', 'max'
# ]).reset_index().sort_values('mean', ascending=False)
# print(candidate_stats_2)




# output_3.to_csv(os.path.join(data_folder, "output_3.csv"), index = False)



#%%

print("Processing output_4...")

## OUPUT 4: 
#     - OUTPUT_4: collapse district/cycle, to Dems and Reps, 
#     - OUTPUT_4_corp: OUTPUT_4 for corps
#     - OUTPUT_4_ind: OUTPUT_4 for individuals

# NOTE: (100 = Dem, 200 = Rep, 328 = Ind)

# OUTPUT_4
output_4 = output_1.groupby(['cycle', 'district', 'party']).agg({
    'amount': 'sum',
    'transaction.id': 'count'
}).reset_index()
output_4 = output_4.sort_values(['cycle', 'district', 'party'])
output_4['avg_amount_per_contribution'] = output_4['amount'] / output_4['transaction.id']


# OUTPUT_4_corp
output_4_corp = output_1[
    output_1['contributor.type'] == 'C'
    ].groupby(['cycle', 'district', 'party']).agg({
    'amount': 'sum',
    'transaction.id': 'count'
}).reset_index()
output_4_corp = output_4_corp.sort_values(['cycle', 'district', 'party'])
output_4_corp['avg_amount_per_contribution'] = output_4_corp['amount'] / output_4_corp['transaction.id']


# OUTPUT_4_ind
output_4_ind = output_1[
    output_1['contributor.type'] == 'I'
    ].groupby(['cycle', 'district', 'party']).agg({
    'amount': 'sum',
    'transaction.id': 'count'
}).reset_index()
output_4_ind = output_4_ind.sort_values(['cycle', 'district', 'party'])
output_4_ind['avg_amount_per_contribution'] = output_4_ind['amount'] / output_4_ind['transaction.id']


# output_4.to_csv(os.path.join(data_folder, "output_4.csv"), index = False)
# output_4_corp.to_csv(os.path.join(data_folder, "output_4_corp.csv"), index = False)
# output_4_ind.to_csv(os.path.join(data_folder, "output_4_ind.csv"), index = False)




#%%

print("Processing output_5...")

## OUTPUT 5
#     - OUTPUT_5_G_recip_and_party: for general elections, counting unique recipients and parties that a given contributor funded in each district / cycle
#     - OUTPUT_5_G_avg: for OUTPUT_5_G_recip_ and _party, average across contributing corps district / cycle

#     - OUTPUT_5_P_recip_and_party: for primary elections, counting unique recipients that a given contributor funded in each district / cycle
#     - OUTPUT_5_P_avg: for OUTPUT_5_P_recip_and_party, average across contributing corps district / cycle


def create_election_stats(data, election_type):
    # Filter data for specific election type and corporate contributors
    filtered_data = data[
        (data['election.type'] == election_type) & 
        (data['contributor.type'] == 'C')
    ]
    
    # Group by cycle, district, and contributor name to get recipient and party counts
    recipient_counts = filtered_data.groupby(['cycle', 'district', 'contributor.name']).agg({
        'bonica.rid': 'count',
        'party': 'count'
    }).reset_index()
    
    # Rename columns for clarity
    recipient_counts = recipient_counts.rename(columns={
        'bonica.rid': 'nr_recipients',
        'party': 'nr_parties'
    })
    
    # Sort the data
    recipient_counts = recipient_counts.sort_values(['cycle', 'district', 'contributor.name'])
    
    # Calculate district-level averages
    district_averages = recipient_counts.groupby(['cycle', 'district']).agg({
        'nr_recipients': 'mean',
        'nr_parties': 'mean'
    }).reset_index()
    
    # Rename columns for clarity
    district_averages = district_averages.rename(columns={
        'nr_recipients': f'avg_nr_recipients_{election_type}',
        'nr_parties': f'avg_nr_parties_{election_type}'
    })
    
    # Sort the data
    district_averages = district_averages.sort_values(['cycle', 'district'])
    
    return recipient_counts, district_averages

# Using funciton
output_5_G_recip_and_party, output_5_G_avg = create_election_stats(output_1, 'G')
output_5_P_recip_and_party, output_5_P_avg = create_election_stats(output_1, 'P')


# output_5_G_recip_and_party.to_csv(os.path.join(data_folder, "output_5_G_recip_and_party.csv"), index = False)
# output_5_G_avg.to_csv(os.path.join(data_folder, "output_5_G_avg.csv"), index = False)
# output_5_P_recip_and_party.to_csv(os.path.join(data_folder, "output_5_P_recip_and_party.csv"), index = False)
# output_5_P_avg.to_csv(os.path.join(data_folder, "output_5_P_avg.csv"), index = False)


#%%

print("Processing output_6...")

## OUTPUT_6 (for corp contributions only!!)
#     - OUTPUT_6
#       1. calculating hedging (difference btw. amount given to Dems candidate and Reps candidate, in non-primary)
#       2. calculating normalized hedging (hedging / sum of amounts)
#     - OUTPUT_6_avg
#       3. get avg of both


output_6 = output_1[
    (output_1['election.type'] != 'P') & 
    (output_1['contributor.type'] == 'C')
    ].groupby(['cycle', 'district', 'bonica.cid', 'contributor.name', 'party']).agg({
    'amount': 'sum'
}).reset_index()

output_6_pivot = output_6.pivot_table(
    index=['cycle', 'district', 'bonica.cid', 'contributor.name'],
    columns='party',
    values='amount',
    fill_value=0
).reset_index()

# Keeping only dems and reps!

output_6_pivot = output_6_pivot.rename(columns = {100.0 : "dems", 200.0 : "reps"})

output_6_pivot = output_6_pivot[['cycle', 'district', 'bonica.cid', 'contributor.name', 'dems', "reps"]]
output_6_pivot['hedging'] = abs(output_6_pivot['dems'] - output_6_pivot['reps'])
output_6_pivot['n_hedging'] = output_6_pivot['hedging'] / (output_6_pivot['dems'] + output_6_pivot['reps'])


output_6_avg = output_6_pivot.groupby(['cycle', 'district']).agg({
    'dems': 'mean',
    'reps': 'mean',
    'hedging': 'mean',
    'n_hedging': 'mean',
    }).reset_index()

output_6_avg = output_6_avg.rename(columns = {
    'dems': 'avg_dems',
    'reps': 'avg_dems',
    'hedging': 'avg_hedging',
    'n_hedging': 'avg_n_hedging'
    })

# output_6_pivot.to_csv(os.path.join(data_folder, "output_6.csv"), index = False)
# output_6_avg.to_csv(os.path.join(data_folder, "output_6_avg.csv"), index = False)


#%%

print("Processing output_7...")

## OUTPUT_7 (for recipients)
#     - OUTPUT_7_1: for district-cycle, get average CFscore
#     - OUTPUT_7_2: for district-cycle, get average CFscore for parties

# Here we only use rows that were merged in merged_df_2 by filtering out NA values
nonna_rows = output_1[~output_1['resign_member'].isna()]
# Dropping duplicate rows (bonica.rid is more unique)
nonna_rows = nonna_rows.drop_duplicates(
    subset=['bonica.rid', 'cycle'],
    keep='first' 
)

output_7_1 = nonna_rows.groupby(['district', 'cycle']).agg(
    count_cfscore=('recipient.cfscore', 'count'),
    count_cfscore_dyn=('recipient.cfscore.dyn', 'count'),
    mean_cfscore=('recipient.cfscore', 'mean'),
    mean_cfscore_dyn=('recipient.cfscore.dyn', 'mean')
).reset_index()


output_7_2 = nonna_rows.groupby(['district', 'cycle', 'party']).agg(
    count_cfscore=('recipient.cfscore', 'count'),
    count_cfscore_dyn=('recipient.cfscore.dyn', 'count'),
    mean_cfscore=('recipient.cfscore', 'mean'),
    mean_cfscore_dyn=('recipient.cfscore.dyn', 'mean')
).reset_index()



# We have 'recipient.cfscore' and 'recipient.cfscore.dyn', but the latter is clearly better, although has missing values



# output_7_1.to_csv(os.path.join(data_folder, "output_7_1.csv"), index = False)
# output_7_2.to_csv(os.path.join(data_folder, "output_7_2.csv"), index = False)


#%%










