### LIBRARIES

import os
# import inspect
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

# If all fails define working folder manually and run the lines here:
# code_folder = "/Users/lirhoxhaj/Library/CloudStorage/OneDrive-ImperialCollegeLondon/Desktop/RA/Tommaso/Contributions_Paper/working_folder_lir/code"
# code_folder = r"C:\Users\lhoxhaj\OneDrive - Imperial College London\Desktop\RA\Tommaso\Contributions_Paper\working_folder_lir\code"

code_folder = "/rds/general/user/lhoxhaj/home/code/"
parent_folder = "/rds/general/user/lhoxhaj/home/"
data_folder = "/rds/general/user/lhoxhaj/home/data/"

# This is your working folder where folders '\code' and '\data' are saved
# parent_folder = os.path.dirname(code_folder)

# data_folder = os.path.join(parent_folder, "data")

print("Parent folder:", parent_folder, "\n")
print("Code folder:", code_folder, "\n")
print("Data folder:", data_folder, "\n")

#%%

### READING DATASETS

# This for loop saves all datasets in a dictionary, we can refer to specific datasets as 

## INPUT 1: DIME database on contributions

# file_path_1 = os.path.join(data_folder, "contribDB_1980.csv")
# file_path_2 = Path(data_folder) / f"contribDB_2024.csv"
# df_1 = pd.read_csv(file_path_1, encoding='latin-1')


contribDB_dict = {}
for year in range(1980, 2026, 2):
    #file_path = Path(data_folder) / f"contribDB_{year}.csv"
    file_path = os.path.join(data_folder, f"contribDB_{year}.csv")
    try:
        print(f"\nReading contribDB_{year}.csv ...")
        df = pd.read_csv(file_path, encoding='latin-1')
        # Filter for House seats and corporate / individual contributions
        df = df[
            ((df['seat'] == 'federal:house') & (df['is.corp'] == 'corp')) |
            ((df['seat'] == 'federal:house') & (df['contributor.type'] == 'I'))
        ]
        df = df[df['amount'] >= 0]
        df = df.dropna(subset = ['amount', 'date']) # dropping missing values for these vars
        # dropping contributions with absurd dates after 2024

        df['date'] = pd.to_datetime(df['date'], errors = 'coerce') # need to use errors = 'coerce' to include incorrect dates
        
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
contribDB_all = pd.concat(contribDB_dict.values(), axis=0, ignore_index=True)
# Checking rows that have different cycle and date years
mismatch_rows = contribDB_all[contribDB_all['cycle'] != contribDB_all['date'].dt.year]
print(mismatch_rows['cycle'].describe())
print(mismatch_rows['date'].dt.year.describe())


## INPUT 2: Recipients’ database

recipients = pd.read_csv(data_folder + "/dime_recipients_1979_2024.csv", encoding='latin-1')


## INPUT 3: Self-constructed dataset of deaths (using deaths_merged.csv)

deaths_merged = pd.read_csv(data_folder + "/deaths_merged.csv", encoding='latin-1')

# Looking at which district have multiple deaths_merged
for dist in deaths_merged['district'].unique():
    df_filter = deaths_merged[deaths_merged['district'] == dist]
    if len(df_filter) > 1:
        print(f"Number of deaths_merged in district {dist}: {len(df_filter)}")
        print(df_filter[["death_member", "death_date"]])
        print()
    else:
        continue
    
# Districts that have multiple deaths_merged of incumbents: CA05, TX18, VA01, VA04, NJ10 


## INPUT 4: Manually creating elections dataset for each year (cycle) in our dataset

election_dates = pd.read_csv(data_folder + "/election_dates.csv", encoding='latin-1')


#%%

### MERGING DATASETS


## MERGE 1: Contributions dataset (Input 1) with recipients dataset (Input 2) using id (“bonica.rid”) and “cycle” as keys

merged_df_1 = pd.merge(
    contribDB_all, 
    recipients,
    on=['bonica.rid', 'cycle'],  
    how='inner'                  # only keep matching rows (use option 'outer' to keep unmatched rows as well)
)

## MERGE 2: Merged dataset from MERGE 1 and deaths_merged using 'district' (do not drop all observations)

merged_df_2 = pd.merge(
    merged_df_1, 
    deaths_merged,
    on=['district'],  
    how='outer'                  # keeping unmatched rows
)

# check how many rows were matched (the sum of these two should match to the total!)
nonna_rows = len(merged_df_2[~merged_df_2['death_member'].isna()]) 
na_rows = len(merged_df_2[merged_df_2['death_member'].isna()]) 
print("Total number of rows in merged_df_2 =", len(merged_df_2))
print("  - Number of merged rows", nonna_rows)
print("  - Number of missing rows:", na_rows)


## MERGE 3: Get election datasets (used for creating treatment dummies and dealing with complex districts)
merged_df_3 = pd.merge(
    merged_df_2,
    election_dates,
    on='cycle',
    how='left'
)


#%%

### CREATING NEW VARIABLES

# - later_than_special
# - days_to_nearest_death


## 1. 

## New dummy variable: “later_than_special”, =1 if the contribution is given in the same cycle of the death, but later than the special election (these are contributions given to the general election).

merged_df_3['cycle'] = pd.to_numeric(merged_df_3['cycle'], errors='coerce')  # will convert None values to NaN
merged_df_3['date'] = pd.to_datetime(merged_df_3['date'], errors = 'coerce') # need to use errors = 'coerce' to include incorrect dates
merged_df_3['special_election_date'] = pd.to_datetime(merged_df_3['special_election_date'], errors = 'coerce') # need to use errors = 'coerce' to include incorrect dates
merged_df_3['death_date'] = pd.to_datetime(merged_df_3['death_date']) # no errors here, because these dates were created manually in deaths_merged.csv!
merged_df_3['election_date_in_cycle'] = pd.to_datetime(merged_df_3['election_date_in_cycle']) # no errors here, because these dates were created manually in election_dates.csv!


merged_df_3['later_than_special'] = 0  # Initialize with zeros

# Set to 1 where:
#   1. Contribution year is between cycle and cycle+2
#   2. Contribution date is later than special election date
mask = (
    (merged_df_3['date'].dt.year >= merged_df_3['cycle']) &
    (merged_df_3['date'].dt.year < merged_df_3['cycle'] + 2) &
    (merged_df_3['date'] > merged_df_3['special_election_date'])
)
# Apply the mask
merged_df_3.loc[mask, 'later_than_special'] = 1
# Handle NaN values in special_election_date (keep as 0)
merged_df_3.loc[merged_df_3['special_election_date'].isna(), 'later_than_special'] = 0

merged_df_3_sample = merged_df_3[merged_df_3['later_than_special'] == 1]
merged_df_3_sample.to_csv(os.path.join(data_folder, "check_later_than_special_values.csv"), index = False)




## 2.

## For each contribution in a district where an incumbent died:
    
#   2. Create a variable “days_to_nearest_death” which counts the number of days between the contribution (variable date in Input 1 dataset) and the date of the nearest death in the district (from Input 3 dataset)
#      - <0 for contributions earlier than death, >0 for contributions later than death
#      - Be careful! There are districts where more than one incumbent died (here, “nearest” death is key!)



# For each district-cycle combination, find all death dates
district_cycle_deaths = merged_df_3[merged_df_3['death_member'].notna()][['district', 'cycle', 'death_date']].drop_duplicates()



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
merged_df_3 = merged_df_3.sort_values(
    by=['cycle', 'date', 'bonica.cid_x', 'bonica.rid', 'abs_days_to_death'],
    ascending=[True, True, True, True, True]
)
# Now keep only the first occurrence of each unique contribution (closest to death)
# This allows us to have unique rows for each contributions (rather than duplicates), even for problematic rows
merged_df_3 = merged_df_3.drop_duplicates(
    subset= merged_df_1.columns,
    keep='first'
)


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
print_district_summary("HI01")
print("\nMULTIPLE DEATH DISTRICT:")
print_district_summary("CA05")


# Summary stats for days_to_nearest_death and histograms
print(merged_df_3['days_to_nearest_death'].describe())

# plt.figure(figsize=(10, 6))
# sns.histplot(data=merged_df_3, x='days_to_nearest_death', bins=30)
# plt.axvline(x=0, color='red', linewidth=3)  # adds vertical line
# plt.title('Distribution of days_to_nearest_death')
# plt.xlabel('Value')
# plt.ylabel('Count')
# plt.show()




#%%

## 3.

## For each contribution in a district where an incumbent died:

#   3. Create two dummy variable "treatment", 
#      3.1. one for all districts with a single death of an incumbent in "deaths_merged" data
#           - treatment == 0 for all contributions (rows) coming before death of incumbent; == 1 for contributions coming post-death
#      3.2. one for districts that have multiple deaths
#           - for 'problematic' districts (multiple deaths of incumbents), 
#               - we assign 0 and 1 accordingly, but also have a 'reset' at every election cycle
#               - we manually find election dates (or ask AI to do this for us) from 1980 to 2024
#               - assuming multiple dead incumbents in district, we will have 0 values before death of FIRST incumbent, then 1 values after death of FIRST incumbent UNTIL NEXT ELECTION DATE where values reset to 0



# Count deaths per district and identify single/multiple death districts
death_counts = merged_df_3.groupby('district')['death_member'].nunique().reset_index()
single_death_districts = death_counts[death_counts['death_member'] == 1]['district'].tolist()
multiple_death_districts = death_counts[death_counts['death_member'] > 1]['district'].tolist()

# Create treat_1 (simple case - single death districts)
merged_df_3['treat_1'] = 0
for district in single_death_districts:
    death_date = merged_df_3[
        (merged_df_3['district'] == district) & 
        (merged_df_3['death_date'].notna())
    ]['death_date'].iloc[0]
    
    merged_df_3.loc[
        (merged_df_3['district'] == district) & 
        (merged_df_3['date'] > death_date), 
        'treat_1'
    ] = 1

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
merged_df_3_sample = merged_df_3[merged_df_3['district'] == 'CA05'][['date', 'treat_1', 'treat_2']]

        
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

duplicates = merged_df_3[merged_df_3.duplicated(subset=['transaction.id', 'district'], keep=False)]
print(f"Number of transactions with duplicates: {duplicates['transaction.id'].nunique()}")
print(len(merged_df_3) - len(merged_df_3_clean), "... should be same number as above")   


# Note: duplicates are because some values are different for the same contribution but different values in the 'recepients' data. Districts are NAN
print(f"Districts that have these duplicates: {duplicates['district'].unique()}")


#%%

### OUTPUTS


## OUTPUT 1: contribution-day level dataset

# Sort the rows by date and transaction.id
output_1 = merged_df_3_clean.sort_values(['date', 'district', 'transaction.id'])
# Reorder columns to put date and transaction.id first
cols = ['date', 'district', 'transaction.id'] + [col for col in output_1.columns if col not in ['date', 'district', 'transaction.id']]
output_1 = output_1[cols]



## OUTPUT 2_1 and 2_2: collapsing OUTPUT 1 for district-cycle and getting sum of amount and count of contributions, and then this but 'muted' by dropping observations for later_than_special == 1

# Group by district-cycle
output_2_1 = output_1.groupby(['cycle', 'district']).agg({
    'amount': 'sum',
    'transaction.id': 'count'
}).reset_index()
# Sort the result if needed
output_2_1 = output_2_1.sort_values(['cycle', 'district'])
output_2_1['avg_amount_per_contribution'] = output_2_1['amount'] / output_2_1['transaction.id']

# Same, but dropping contributions that came after special elections
output_2_2 = output_1[output_1['later_than_special'] != 1].groupby(['cycle', 'district']).agg({
    'amount': 'sum',
    'transaction.id': 'count'
}).reset_index()

output_2_2 = output_2_2.sort_values(['cycle', 'district'])
output_2_2['avg_amount_per_contribution'] = output_2_2['amount'] / output_2_2['transaction.id']


output_2_merge = pd.merge(
    output_2_1, 
    output_2_2,
    on=['district', 'cycle'],  
    how='outer'                  # keeping unmatched rows
)

output_2_merge_noNA = output_2_merge.dropna() # two missing vars, since output_2_2 is shorter
print(np.corrcoef(output_2_merge_noNA['amount_x'], output_2_merge_noNA['amount_y']))


## OUPUT 3: grouping by district and election cycle to get dispersion index

def calculate_metrics(group):
    # Sort the group by 'gen.vote.pct' in descending order
    sorted_group = group.sort_values(by='gen.vote.pct', ascending=False)
    
    # Select the top 2 candidates
    unique_group = sorted_group.drop_duplicates(subset='gen.vote.pct', keep='first')
    top_candidates = unique_group.head(2)
    
    # Calculate metrics
    vote_share_max = top_candidates['gen.vote.pct'].iloc[0] if len(top_candidates) > 0 else np.nan
    vote_share_min = top_candidates['gen.vote.pct'].iloc[1] if len(top_candidates) > 1 else top_candidates['gen.vote.pct'].min()
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
output_3 = merged_df_3_clean.groupby(['district', 'cycle']).apply(calculate_metrics).reset_index()


# Checking (make sure to cross check using output_3 as well)
df_filtered_1 = merged_df_3_clean[
    (merged_df_3_clean['district'] == 'CA03') & 
    (merged_df_3_clean['cycle'] == 1980)
    ]

df_filtered_2 = merged_df_3_clean[
    (merged_df_3_clean['district'] == 'CA03') & 
    (merged_df_3_clean['cycle'] == 2000)
    ]

candidate_stats_1 = df_filtered_1.groupby('bonica.rid')['gen.vote.pct'].agg([
    'count', 'mean', 'std', 'min', 'max'
]).reset_index().sort_values('mean', ascending=False)
print(candidate_stats_1)

candidate_stats_2 = df_filtered_2.groupby('bonica.rid')['gen.vote.pct'].agg([
    'count', 'mean', 'std', 'min', 'max'
]).reset_index().sort_values('mean', ascending=False)
print(candidate_stats_2)



#%%
### SAVING DATASETS

output_1.to_csv(os.path.join(data_folder, "output_1_hpc_final.csv"), index = False)
output_2_merge.to_csv(os.path.join(data_folder, "output_2_both_hpc_final.csv"), index = False)
output_3.to_csv(os.path.join(data_folder, "output_3_hpc_final.csv"), index = False)








