#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 30 16:58:39 2025

@author: lirhoxhaj
"""

import os
import inspect
# import polars as pl
import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime


code_folder = "/Users/lirhoxhaj/Library/CloudStorage/OneDrive-ImperialCollegeLondon/Desktop/RA/Tommaso/Contributions_Paper/working_folder_lir/code"
parent_folder = os.path.dirname(code_folder)

data_folder = os.path.join(parent_folder, "data")

print("Parent folder:", parent_folder, "\n")
print("Code folder:", code_folder, "\n")
print("Data folder:", data_folder, "\n")

#%%

## INPUT 1: DIME database on contributions
print("\n")
print("*" * 30)
print("INPUT_1")

df_1988 = pd.read_csv(
    Path(data_folder) / f"contribDB_1988.csv",
    encoding='latin-1',
    usecols = ['cycle', 'transaction.id', 'transaction.type', 'amount', 'date',
           'bonica.cid', 'contributor.name', 'contributor.type',
           'contributor.gender', 'recipient.name',
           'bonica.rid', 'seat', 'election.type', 'gis.confidence',
           'contributor.district', 'latitude', 'longitude', 
           'contributor.cfscore'] 
    )

# Reading oth88.zip (itoth.txt)
df_1 = pd.read_csv(os.path.join(data_folder, 'Bonica_FEC_raw_data/main/itoth.txt'), delimiter='|', header=None)
df_1 = df_1.rename(columns = {0:"committee_id",
                              1:"amendment_inidicator",
                              2:"report_type",
                              3:"election_type",
                              4:"image_number",
                              5:"contribution_code",
                              6:"empty_column_1",
                              7:"contributor_name",
                              8:"contributor_city",
                              9:"contributor_state",
                              10:"contributor_zipcode",
                              11:"occupation",
                              12:"empty_column_2",
                              13:"date",
                              14:"amount",
                              15:"contributor_id",
                              16:"dummy_1",
                              17:"empty_column_3",
                              18:"empty_column_4",
                              19:"contribution_code_description",
                              20:"transaction_id",
                              })
# Reading pas288.zip (itpas2.txt)
df_2 = pd.read_csv(os.path.join(data_folder, 'Bonica_FEC_raw_data/main/itpas2.txt'), delimiter='|', header=None)
df_2 = df_2.rename(columns = {0:"committee_id",
                              1:"amendment_inidicator",
                              2:"report_type",
                              3:"election_type",
                              4:"image_number",
                              5:"contribution_code",
                              6:"empty_column_1",
                              7:"contributor_name",
                              8:"contributor_city",
                              9:"contributor_state",
                              10:"contributor_zipcode",
                              11:"occupation",
                              12:"empty_column_2",
                              13:"date",
                              14:"amount",
                              15:"contributor_id",
                              16:"recipient_id",
                              17:"dummy_1",
                              18:"empty_column_3",
                              19:"empty_column_4",
                              20:"contribution_code_description",
                              21:"transaction_id",
                              })

# Reading indiv88.zip (itcont.txt)
df_3 = pd.read_csv(os.path.join(data_folder, 'Bonica_FEC_raw_data/main/itcont.txt'), delimiter='|', header=None)
df_3 = df_3.rename(columns = {0:"committee_id",
                              1:"amendment_inidicator",
                              2:"report_type",
                              3:"election_type",
                              4:"image_number",
                              5:"contribution_code",
                              6:"empty_column_1",
                              7:"contributor_name",
                              8:"contributor_city",
                              9:"contributor_state",
                              10:"contributor_zipcode",
                              11:"occupation",
                              12:"empty_column_2",
                              13:"date",
                              14:"amount",
                              15:"recipient_id",
                              16:"dummy_1",
                              17:"empty_column_3",
                              18:"empty_column_4",
                              19:"contribution_code_description",
                              20:"transaction_id",
                              })

FEC_1988_house = pd.read_csv(os.path.join(data_folder, 'Bonica_FEC_raw_data/main/FEC_1988_house.csv'))

df_1 = df_1.reset_index(drop=True)
df_2 = df_2.reset_index(drop=True)
df_3 = df_3.reset_index(drop=True)

df_1.to_csv(os.path.join(data_folder, "Bonica_FEC_raw_data/main/itoth.csv"), index = False)
df_2.to_csv(os.path.join(data_folder, "Bonica_FEC_raw_data/main/itpas2.csv"), index = False)
df_3.to_csv(os.path.join(data_folder, "Bonica_FEC_raw_data/main/itcont.csv"), index = False)

FEC_1988_house = FEC_1988_house.reset_index(drop=True)

df_all = pd.concat([df_1, df_2, df_3], axis=0, ignore_index=True, join='outer')

# First, convert the numbers to strings
df_all['date_str'] = df_all['date'].astype(str)
def parse_custom_date(date_str):
    try:
        # Get year (last 4 digits)
        year = int(date_str[-4:])
        
        # Get day and month based on remaining length
        remaining = date_str[:-4]
        
        # if len(remaining) == 1:
        #     # Format is 1YYYY (day=1, month=1)
        #     day = 1
        #     month = int(remaining)
        if len(remaining) == 2:
            day = int(remaining[1])
            month = int(remaining[0])
        elif len(remaining) == 3:
            day = int(remaining[1:])
            month = int(remaining[0])
        else:  # len(remaining) == 4
            # Format is DDMMYYYY
            day = int(remaining[3:])
            month = int(remaining[0:2])
            
        # Create datetime object
        return pd.Timestamp(year=year, month=month, day=day)
    except:
        return pd.NaT  # Return NaT (Not a Time) for any errors

# Apply the function to create a new datetime column
df_all['date_proper'] = df_all['date_str'].apply(parse_custom_date)

# If you want to drop the intermediate string column
df_all = df_all.drop(columns=['date_str'])


dropvalues = ['0', '2', '4', '6', '8']
# df_all = df_all[(~df_all["election_type"].isna()) | (~df_all["election_type"].isin(dropvalues))]
df_all = df_all[~df_all["election_type"].isna()]
df_all = df_all[~df_all["election_type"].isin(dropvalues)]

duplicate_transaction_id = df_all[df_all.duplicated(subset=['transaction_id'])]['transaction_id'].unique()


duplicate_counts = df_all['transaction_id'].value_counts()
duplicated_ids = duplicate_counts[duplicate_counts > 1].index.tolist()



# Create a copy of the original DataFrame before deduplication
df_all_original = df_all.copy()

# Method to prioritize rows with non-missing recipient_id
# Step 1: Split data into duplicates and non-duplicates
duplicates = df_all[df_all['transaction_id'].isin(duplicated_ids)]
non_duplicates = df_all[~df_all['transaction_id'].isin(duplicated_ids)]

# Step 2: For each duplicated transaction_id, keep the best row
# We'll use a groupby approach and sort within each group to prioritize non-missing recipient_id
# Sort within each group: rows with non-missing recipient_id first
duplicates = duplicates.sort_values(['transaction_id', 'recipient_id'], 
                                   na_position='last')

# Keep the first occurrence of each transaction_id (which will be the one with non-missing recipient_id if available)
deduped_rows = duplicates.drop_duplicates('transaction_id', keep='first')

# Step 3: Combine the non-duplicates with the deduped rows
df_all_deduped = pd.concat([non_duplicates, deduped_rows], ignore_index=True)

# Verify the results
print(f"Original data shape: {df_all.shape}")
print(f"After deduplication: {df_all_deduped.shape}")
print(f"Duplicate transaction_ids removed: {len(duplicated_ids)}")

# Check if all duplicated_ids still exist in the deduped DataFrame (they should, but only once each)
still_present = [id in df_all_deduped['transaction_id'].values for id in duplicated_ids]
print(f"All duplicated_ids still represented: {all(still_present)}")

df_merged = pd.merge(
    df_all_deduped, # this has the house contributions only 
    FEC_1988_house,
    on=['committee_id'],  
    how='left'                  # only keep matching rows (use option 'outer' to keep unmatched rows as well)
)


contribDB_dict = {}
for year in [1980, 1982, 1984, 1986, 1988, 1990, 1992, 1994, 1996]:
# for year in range(1980, 2026, 2):
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
        #    'candidate.cfscore'   # NOT NEEDED, EXISTS IN RECIPIENTS DATA

        df_raw = pd.read_csv(
            file_path, 
            encoding='latin-1',
            usecols = ['cycle', 'transaction.id', 'transaction.type', 'amount', 'date',
                   'bonica.cid', 'contributor.name', 'contributor.type',
                   'contributor.gender', 'recipient.name',
                   'bonica.rid', 'seat', 'election.type', 'gis.confidence',
                   'contributor.district', 'latitude', 'longitude', 
                   'contributor.cfscore'] 
            )
        
        print("\n-> Length of raw dataset:", len(df_raw))
        
        print("\n-> Unique values for 'cycle' variable for this election year {year} data:", df_raw['cycle'].unique())
        
        # Filter for House seats and corporate / individual contributions
        df = df_raw[
            ((df_raw['seat'] == 'federal:house') & (df_raw['contributor.type'] == 'C')) |
            ((df_raw['seat'] == 'federal:house') & (df_raw['contributor.type'] == 'I'))
        ]
        print("\n-> Length of dataset after selecting only corporate and individual contributions':", len(df), f"({round(len(df)/len(df_raw)*100, 2)}% of raw data)")
        
        df = df[df['amount'] >= 0]
        df = df.dropna(subset = ['amount', 'date']) # dropping missing values for these vars
        print("\n-> Length of dataset after dropping contributions that have negative amounts (less than 0)':", len(df), f"({round(len(df)/len(df_raw)*100, 2)}% of raw data)")
        
        # dropping contributions with absurd dates after 2024
        df['date'] = pd.to_datetime(df['date'], format = '%Y-%m-%d', errors = 'coerce') # need to use errors = 'coerce' to include incorrect dates
              
        
        print(f"\n-> Unique values of year contributions are coming from: {df['date'].dt.year.unique()}")
        print("Value counts:")
        print("NaN values:", df['date'].isna().sum())
        print("Other years")
        print(df['date'].dt.year.value_counts())
        
        
        cycle_year = year
        cycle_year_two_years_before = year - 2
        cycle_year_two_years_after = year + 2
        
        # df = df[
        #     (df['date'].dt.year >= cycle_year_two_years_before) &
        #     (df['date'].dt.year <= cycle_year_two_years_after)
        # ]
        
        # print("-> Length of dataset after filtering:", len(df))
        
        df = df[
            df['date'].dt.year >= cycle_year_two_years_before
        ]

        print(f"\n-> Length of dataset after dropping contributions that have dates later than {cycle_year_two_years_before} for election cycle year {cycle_year}':", len(df), f"({round(len(df)/len(df_raw)*100, 2)}% of raw data)")
                
        df = df[
            df['date'].dt.year <= cycle_year_two_years_after
        ]
        
        print(f"\n-> Length of dataset after dropping contributions that have dates before than {cycle_year_two_years_after} for election cycle year {cycle_year}':", len(df), f"({round(len(df)/len(df_raw)*100, 2)}% of raw data)")
        
        print("*" * 40)
        
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
OUTPUT_1 = pd.merge(
    contribDB_all, 
    recipients,
    on=['bonica.rid', 'cycle'],  
    how='inner'                  # only keep matching rows (use option 'outer' to keep unmatched rows as well)
)

# check how many rows were matched (the sum of these two should match to the total!)
# nonna_rows = len(merged_df_1[~merged_df_1['name'].isna()]) 
# na_rows = len(merged_df_1[merged_df_1['name'].isna()]) 
# print("Total number of rows in merged_df_1 =", len(merged_df_1))
# print("  - Number of merged rows", nonna_rows)
# print("  - Number of missing rows:", na_rows)


## MERGE 2: Merged dataset from MERGE 1 and special_elections using 'district' (do not drop all observations)
print("\n")
print("*" * 30)
print("\nMERGE 2: Merging with special elections data...")
OUTPUT_1 = pd.merge(
    OUTPUT_1, 
    special_elections,
    on=['district'],  
    how='outer'                  # keeping unmatched rows
)

# check how many rows were matched (the sum of these two should match to the total!)
# nonna_rows = len(merged_df_2[~merged_df_2['spec_member'].isna()]) 
# na_rows = len(merged_df_2[merged_df_2['spec_member'].isna()]) 
# print("Total number of rows in merged_df_2 =", len(merged_df_2))
# print("  - Number of merged rows", nonna_rows)
# print("  - Number of missing rows:", na_rows)


## MERGE 3: Get election datasets (used for creating treatment dummies and dealing with complex districts)
print("\n")
print("*" * 30)
print("\nMERGE 3: Merging with election dates...")
OUTPUT_1 = pd.merge(
    OUTPUT_1,
    election_dates_df,
    on='cycle',
    how='left'
)

# check how many rows were matched (the sum of these two should match to the total!)
# nonna_rows = len(OUTPUT_1[~OUTPUT_1['election_date_in_cycle'].isna()]) 
# na_rows = len(OUTPUT_1[OUTPUT_1['election_date_in_cycle'].isna()]) 
# print("Total number of rows in OUTPUT_1 =", len(OUTPUT_1))
# print("  - Number of merged rows", nonna_rows)
# print("  - Number of missing rows:", na_rows) # unmatched row because district not in merged_df_1!


#%%

### CREATING NEW VARIABLES

# - later_than_special
# - days_to_nearest_death
# - treat_1 and treat_2

## 1. later_than_special

## New dummy variable: “later_than_special”, =1 if the contribution is given in the same cycle of the death, but later than the special election (date of contribution is later than date of special election in that cycle).

OUTPUT_1['cycle'] = pd.to_numeric(OUTPUT_1['cycle'], errors='coerce')  # will convert None values to NaN
OUTPUT_1['date'] = pd.to_datetime(OUTPUT_1['date'], errors = 'coerce') # need to use errors = 'coerce' to include incorrect dates
OUTPUT_1['spec_election_date'] = pd.to_datetime(OUTPUT_1['spec_election_date'], errors = 'coerce') # need to use errors = 'coerce' to include incorrect dates
OUTPUT_1['resign_date'] = pd.to_datetime(OUTPUT_1['resign_date']) # no errors here, because these dates were created manually in special_elections.csv!
OUTPUT_1['death_date'] = pd.to_datetime(OUTPUT_1['death_date']) # no errors here, because these dates were created manually in special_elections.csv!
OUTPUT_1['election_date_in_cycle'] = pd.to_datetime(OUTPUT_1['election_date_in_cycle']) # no errors here, because these dates were created manually in election_dates.csv!


OUTPUT_1['later_than_special'] = np.nan  # Initialize with zeros, districts that did not have special elections simply receive a zero


mask1 = (
    # Filtering for within election cycle, in terms of years between spec_election_date and year
    # (OUTPUT_1['date'] <= OUTPUT_1['spec_election_date'] + pd.DateOffset(months=6)) &
    # (OUTPUT_1['date'] < OUTPUT_1['election_date_in_cycle']) &
    (OUTPUT_1['cycle'] == OUTPUT_1['spec_cycle']) &
    # Main condition!
    (OUTPUT_1['date'] > OUTPUT_1['spec_election_date'])
)


mask0 = (
    # Filtering for within election cycle, in terms of years between spec_election_date and year
    # (OUTPUT_1['date'] <= OUTPUT_1['spec_election_date'] + pd.DateOffset(months=6)) &
    # (OUTPUT_1['date'] < OUTPUT_1['election_date_in_cycle']) &
    (OUTPUT_1['cycle'] == OUTPUT_1['spec_cycle']) &
    # Main condition!
    (OUTPUT_1['date'] <= OUTPUT_1['spec_election_date'])
)



# Apply the mask and handle NaN values in spec_election_date (keep as 0)
OUTPUT_1.loc[mask1, 'later_than_special'] = 1
OUTPUT_1.loc[mask0, 'later_than_special'] = 0
OUTPUT_1.loc[OUTPUT_1['spec_election_date'].isna(), 'later_than_special'] = np.nan


for year in OUTPUT_1['cycle'].unique():
    df_filtered = OUTPUT_1[OUTPUT_1['cycle'] == year]
    df_filtered_ind = df_filtered[df_filtered['contributor.type'] == 'I']
    df_filtered_corp = df_filtered[df_filtered['contributor.type'] == 'C']
    print(f"Length of dataset filtered for election year {year}: {len(df_filtered)} ({round(len(df_filtered)/len(OUTPUT_1)*100, 2)}% of total OUTPUT_1 dataset)")
    print(f"  -> only ind contributions: {len(df_filtered_ind)} ({round(len(df_filtered_ind)/len(df_filtered)*100, 2)}% of this year's data)")
    print(f"  -> only corp contributions: {len(df_filtered_corp)} ({round(len(df_filtered_corp)/len(df_filtered)*100, 2)}% of this year's data)")
    print()


