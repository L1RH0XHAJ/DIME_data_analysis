#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 12 11:18:52 2025

@author: lirhoxhaj
"""

## PURPOSE OF FILE: Processing the _ext_ (extended) outputs and creating new variables


### LIBRARIES

import os
import pandas as pd
import numpy as np

#%%


## Preliminary functions

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




#%%

### OUTPUT_2_ext, OUTPUT_3_ext, OUTPUT_4_ext, and OUTPUT_8_ext

### Creating extended datasets with vars that apply the condition of filter contributions between death_date and spec_election_date
# - OUTPUT_2
# - OUTPUT_3
# - OUTPUT_4
# - OUTPUT_8

## Here, we create vars with suffix _1, _2, _3. 


# print("Processing OUTPUT_2_ext, OUTPUT_3_ext, and OUTPUT_4_ext...")

def create_treatment_filtered_outputs(input_df, output_7_df, base_output_df, output_name, single_death_districts, multiple_death_districts):
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

#%%
# print("Processing OUTPUT_8_ext...")

def create_output8_treatment_filtered(input_df, output_7_df, base_output_df, single_death_districts, multiple_death_districts):
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

#%%
# Also, for 'gen' variables, we create 'gen_np' (general elections non-primary) variables, 
#  -> we keep contributions going towards general elections or special elections in treated district-cycles, 
#  -> whereas in untreated ones we select only the ones going towards general elections

# def add_gen_np_variables(input_df, output_7_df, ext_df, output_name):
#     """
#     Add _gen_np suffix variables to _ext datasets.
    
#     For rows where treat_3 == 1: select contributions with election.type != 'P' between death_date and special_elections_date
#     For other rows: keep election.type == 'G'
    
#     Parameters:
#     -----------
#     input_df : DataFrame
#         The raw contribution data (OUTPUT_1)
#     output_7_df : DataFrame
#         The treatment data with treat_3, death_date, special_elections_date
#     ext_df : DataFrame
#         The extended output dataset (OUTPUT_2_ext, OUTPUT_3_ext, etc.)
#     output_name : str
#         Name for the output (e.g., 'OUTPUT_2', 'OUTPUT_3', 'OUTPUT_4_1')
    
#     Returns:
#     --------
#     DataFrame
#         DataFrame with additional _gen_np variables
#     """
    
#     print(f"Adding _gen_np variables for {output_name}_ext...")
    
#     # Start with the existing ext_df
#     result_df = ext_df.copy()
    
#     # Determine filter type based on output_name
#     if output_name == 'OUTPUT_2':
#         filter_type = None
#         amount_filter = None
#         suffix = ''
#     elif output_name == 'OUTPUT_3':
#         filter_type = 'C'
#         amount_filter = None
#         suffix = '_corp'
#     elif output_name == 'OUTPUT_4_1':
#         filter_type = 'I'
#         amount_filter = None
#         suffix = '_ind'
#     elif output_name == 'OUTPUT_4_2':
#         filter_type = 'I'
#         amount_filter = 200
#         suffix = '_smallind'
    
#     # Apply type and amount filters to input_df
#     input_df_filtered = input_df.copy()
#     if filter_type:
#         input_df_filtered = input_df_filtered[input_df_filtered['contributor.type'] == filter_type]
#     if amount_filter:
#         input_df_filtered = input_df_filtered[input_df_filtered['amount'] < amount_filter]
    
#     # Get base variables that contain 'gen' (excluding district, cycle, and treatment variables)
#     # exclude_cols = ['district', 'cycle', 'real_data', 'treat_1', 'treat_2', 'treat_3', 
#     #                 'death_date', 'special_elections_date']
#     # base_vars = [col for col in ext_df.columns if col not in exclude_cols 
#     #              and not col.endswith('_1') and not col.endswith('_2') and not col.endswith('_3')
#     #              and 'gen' in col]  # Only variables with 'gen' in their names
    
#     # # Initialize _gen_np columns with NaN (only for variables with 'gen')
#     # for var in base_vars:
#     #     result_df[f'{var}_gen_np'] = np.nan
    
    
#     # Just initialize the specific _gen_np columns we're going to create
#     gen_np_vars = [
#         f'total_amount_gen_np{suffix}',
#         f'tran_count_gen_np{suffix}',
#         f'total_amount_gen_np_without_LTS1{suffix}',
#         f'tran_count_gen_np_without_LTS1{suffix}',
#         f'total_amount_dem_gen_np{suffix}',
#         f'tran_count_dem_gen_np{suffix}',
#         f'total_amount_dem_gen_np_without_LTS1{suffix}',
#         f'tran_count_dem_gen_np_without_LTS1{suffix}',
#         f'total_amount_rep_gen_np{suffix}',
#         f'tran_count_rep_gen_np{suffix}',
#         f'total_amount_rep_gen_np_without_LTS1{suffix}',
#         f'tran_count_rep_gen_np_without_LTS1{suffix}'
#     ]
    
#     # Initialize _gen_np columns with NaN
#     for var in gen_np_vars:
#         result_df[var] = np.nan
    
#     # Process treated rows (treat_3 == 1)
#     treated_districts = output_7_df[output_7_df['treat_3'] == 1][['district', 'cycle', 'death_date', 'special_elections_date']]
    
#     if not treated_districts.empty:
#         print(f"  Processing {len(treated_districts)} treated district-cycles...")
        
#         # Merge filtered input_df with treated districts
#         input_treated = pd.merge(
#             input_df_filtered.drop(columns=['death_date', 'spec_election_date'], errors='ignore'),
#             treated_districts,
#             on=['district', 'cycle'],
#             how='inner'
#         )
        
#         # Filter for contributions with election.type != 'P' between death_date and special_elections_date
#         input_treated = input_treated[
#             (input_treated['date'] > input_treated['death_date']) &
#             (input_treated['date'] < input_treated['special_elections_date']) &
#             (input_treated['election.type'] != 'P')
#         ]
        
#         # Create aggregations for treated rows
#         agg_dict = {}
        
#         # 1: All contributions (no primary) - includes both G and S
#         agg_1 = input_treated.groupby(['district', 'cycle']).agg(
#             **{f'total_amount_gen_np{suffix}': ('amount', 'sum'),
#                f'tran_count_gen_np{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         agg_dict.update(agg_1.set_index(['district', 'cycle']).to_dict('index'))
        
#         # 2: Before special election - includes both G and S
#         agg_2 = input_treated[input_treated['later_than_special'] != 1].groupby(['district', 'cycle']).agg(
#             **{f'total_amount_gen_np_without_LTS1{suffix}': ('amount', 'sum'),
#                f'tran_count_gen_np_without_LTS1{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         for idx, row in agg_2.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict:
#                 agg_dict[key] = {}
#             agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
#         # 3: Dem general
#         agg_3 = input_treated[input_treated['party'] == 100].groupby(['district', 'cycle']).agg(
#             **{f'total_amount_dem_gen_np{suffix}': ('amount', 'sum'),
#                f'tran_count_dem_gen_np{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         for idx, row in agg_3.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict:
#                 agg_dict[key] = {}
#             agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
#         # 4: Dem general, before special
#         agg_4 = input_treated[(input_treated['party'] == 100) & (input_treated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
#             **{f'total_amount_dem_gen_np_without_LTS1{suffix}': ('amount', 'sum'),
#                f'tran_count_dem_gen_np_without_LTS1{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         for idx, row in agg_4.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict:
#                 agg_dict[key] = {}
#             agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
#         # 5: Rep general
#         agg_5 = input_treated[input_treated['party'] == 200].groupby(['district', 'cycle']).agg(
#             **{f'total_amount_rep_gen_np{suffix}': ('amount', 'sum'),
#                f'tran_count_rep_gen_np{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         for idx, row in agg_5.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict:
#                 agg_dict[key] = {}
#             agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
#         # 6: Rep general, before special
#         agg_6 = input_treated[(input_treated['party'] == 200) & (input_treated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
#             **{f'total_amount_rep_gen_np_without_LTS1{suffix}': ('amount', 'sum'),
#                f'tran_count_rep_gen_np_without_LTS1{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         for idx, row in agg_6.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict:
#                 agg_dict[key] = {}
#             agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
#         # Update result_df with calculated values for treated rows
#         print("  Updating result_df with _gen_np values for treated rows...")
#         for (district, cycle), values in agg_dict.items():
#             mask = (result_df['district'] == district) & (result_df['cycle'] == cycle)
#             for col, val in values.items():
#                 if col in result_df.columns:
#                     result_df.loc[mask, col] = val
    
#     # Process untreated rows (treat_3 == 0 or NaN) - use election.type == 'G'
#     print("  Processing untreated rows with election.type == 'G'...")
    
#     # Get all district-cycle combinations
#     all_districts = result_df[['district', 'cycle']].copy()
    
#     # Merge with treatment info
#     all_districts = pd.merge(
#         all_districts,
#         output_7_df[['district', 'cycle', 'treat_3']],
#         on=['district', 'cycle'],
#         how='left'
#     )
    
#     # Select untreated rows (treat_3 == 0 or NaN)
#     untreated_districts = all_districts[(all_districts['treat_3'] == 0) | (all_districts['treat_3'].isna())][['district', 'cycle']]
    
#     if not untreated_districts.empty:
#         # Merge with input data
#         input_untreated = pd.merge(
#             input_df_filtered,
#             untreated_districts,
#             on=['district', 'cycle'],
#             how='inner'
#         )
        
#         # Filter for general elections only
#         input_untreated = input_untreated[input_untreated['election.type'] == 'G']
        
#         # Create aggregations for untreated rows (only for general elections)
#         agg_dict_untreated = {}
        
#         # 1: All general election contributions
#         agg_gen_all = input_untreated.groupby(['district', 'cycle']).agg(
#             **{f'total_amount_gen_np{suffix}': ('amount', 'sum'),
#                f'tran_count_gen_np{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         agg_dict_untreated.update(agg_gen_all.set_index(['district', 'cycle']).to_dict('index'))
        
#         # 2: General elections, before special election (with LTS1 filter)
#         agg_gen_lts = input_untreated[input_untreated['later_than_special'] != 1].groupby(['district', 'cycle']).agg(
#             **{f'total_amount_gen_np_without_LTS1{suffix}': ('amount', 'sum'),
#                f'tran_count_gen_np_without_LTS1{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         for idx, row in agg_gen_lts.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict_untreated:
#                 agg_dict_untreated[key] = {}
#             agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
#         # Party-specific aggregations
#         agg_dem = input_untreated[input_untreated['party'] == 100].groupby(['district', 'cycle']).agg(
#             **{f'total_amount_dem_gen_np{suffix}': ('amount', 'sum'),
#                f'tran_count_dem_gen_np{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         for idx, row in agg_dem.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict_untreated:
#                 agg_dict_untreated[key] = {}
#             agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
#         agg_dem_lts = input_untreated[(input_untreated['party'] == 100) & (input_untreated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
#             **{f'total_amount_dem_gen_np_without_LTS1{suffix}': ('amount', 'sum'),
#                f'tran_count_dem_gen_np_without_LTS1{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         for idx, row in agg_dem_lts.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict_untreated:
#                 agg_dict_untreated[key] = {}
#             agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
#         agg_rep = input_untreated[input_untreated['party'] == 200].groupby(['district', 'cycle']).agg(
#             **{f'total_amount_rep_gen_np{suffix}': ('amount', 'sum'),
#                f'tran_count_rep_gen_np{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         for idx, row in agg_rep.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict_untreated:
#                 agg_dict_untreated[key] = {}
#             agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
#         agg_rep_lts = input_untreated[(input_untreated['party'] == 200) & (input_untreated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
#             **{f'total_amount_rep_gen_np_without_LTS1{suffix}': ('amount', 'sum'),
#                f'tran_count_rep_gen_np_without_LTS1{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         for idx, row in agg_rep_lts.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict_untreated:
#                 agg_dict_untreated[key] = {}
#             agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
#         # Update result_df with calculated values for untreated rows
#         print("  Updating result_df with _gen_np values for untreated rows...")
#         for (district, cycle), values in agg_dict_untreated.items():
#             mask = (result_df['district'] == district) & (result_df['cycle'] == cycle)
#             for col, val in values.items():
#                 if col in result_df.columns:
#                     result_df.loc[mask, col] = val
    
#     print(f"Finished adding _gen_np variables for {output_name}_ext")
#     return result_df



# def add_gen_np_variables_output8(input_df, output_7_df, output8_ext):
#     """
#     Add _gen_np suffix variables to OUTPUT_8_ext dataset.
    
#     For rows where treat_3 == 1: select contributions with election.type != 'P' between death_date and special_elections_date
#     For other rows: keep election.type == 'G'
    
#     Parameters:
#     -----------
#     input_df : DataFrame
#         The raw contribution data (OUTPUT_1)
#     output_7_df : DataFrame
#         The treatment data with treat_3, death_date, special_elections_date
#     output8_ext : DataFrame
#         The OUTPUT_8_ext dataset
    
#     Returns:
#     --------
#     DataFrame
#         DataFrame with additional _gen_np variables
#     """
    
#     print("Adding _gen_np variables for OUTPUT_8_ext...")
    
#     # Start with the existing output8_ext
#     result_df = output8_ext.copy()
    
#     # Define the _gen_np variables we want to create (only "general" variables)
#     gen_np_vars = [
#         'hedging_money_general_np_corp',
#         'avg_counting_hedging_np_corp',
        
#         'hedging_money_general_np_ind',
#         'avg_counting_hedging_np_ind',
        
#         'hedging_money_general_np_smallind',
#         'avg_counting_hedging_np_smallind',
#     ]
    
#     # Initialize _gen_np columns with NaN
#     for var in gen_np_vars:
#         result_df[var] = np.nan
    
#     # Process treated rows (treat_3 == 1)
#     treated_districts = output_7_df[output_7_df['treat_3'] == 1][['district', 'cycle', 'death_date', 'special_elections_date']]
    
#     if not treated_districts.empty:
#         print(f"  Processing {len(treated_districts)} treated district-cycles...")
        
#         # Merge input_df with treated districts
#         input_treated = pd.merge(
#             input_df.drop(columns=['death_date', 'spec_election_date'], errors='ignore'),
#             treated_districts,
#             on=['district', 'cycle'],
#             how='inner'
#         )
        
#         # Filter for contributions with election.type != 'P' between death_date and special_elections_date
#         input_treated = input_treated[
#             (input_treated['date'] > input_treated['death_date']) &
#             (input_treated['date'] < input_treated['special_elections_date']) &
#             (input_treated['election.type'] != 'P')
#         ]
        
#         # Create aggregations for treated rows
#         agg_dict = {}
        
#         ### Corporate contributors
        
#         ## Hedging
#         temp_corp = input_treated[
#             (input_treated['contributor.type'] == 'C') & 
#             (input_treated['later_than_special'] != 1)
#         ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
#             total_amount=('amount', 'sum')
#         ).reset_index()
        
#         temp_corp = temp_corp.pivot_table(
#             index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
#             columns='party',
#             values='total_amount',
#             fill_value=0
#         ).reset_index()
        
#         if 100.0 in temp_corp.columns:
#             temp_corp = temp_corp.rename(columns={100.0: 'total_amount_dem'})
#         else:
#             temp_corp['total_amount_dem'] = 0
            
#         if 200.0 in temp_corp.columns:
#             temp_corp = temp_corp.rename(columns={200.0: 'total_amount_rep'})
#         else:
#             temp_corp['total_amount_rep'] = 0
        
#         temp_corp['hedging'] = abs(temp_corp['total_amount_dem'] - temp_corp['total_amount_rep'])
        
#         temp_corp_result = temp_corp.groupby(['district', 'cycle']).agg(
#             hedging_money_general_np_corp=('hedging', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_corp_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict:
#                 agg_dict[key] = {}
#             agg_dict[key]['hedging_money_general_np_corp'] = row['hedging_money_general_np_corp']
            
           
#         ## Avg_count
#         temp_corp_count = input_treated[
#             (input_treated['contributor.type'] == 'C') & 
#             (input_treated['later_than_special'] != 1)
#         ].groupby(['district', 'cycle', 'party']).agg(
#             counting_hedging_corp=('bonica.rid', 'nunique')
#         ).reset_index()

#         temp_corp_count_result = temp_corp_count.groupby(['district', 'cycle']).agg(
#             avg_counting_hedging_np_corp = ('counting_hedging_corp', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_corp_count_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict:
#                 agg_dict[key] = {}
#             agg_dict[key]['avg_counting_hedging_np_corp'] = row['avg_counting_hedging_np_corp']
        
        
#         ### Individual contributors
#         ## Hedging
#         temp_ind = input_treated[
#             (input_treated['contributor.type'] == 'I') & 
#             (input_treated['later_than_special'] != 1)
#         ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
#             total_amount=('amount', 'sum')
#         ).reset_index()
        
#         temp_ind = temp_ind.pivot_table(
#             index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
#             columns='party',
#             values='total_amount',
#             fill_value=0
#         ).reset_index()
        
#         if 100.0 in temp_ind.columns:
#             temp_ind = temp_ind.rename(columns={100.0: 'total_amount_dem'})
#         else:
#             temp_ind['total_amount_dem'] = 0
            
#         if 200.0 in temp_ind.columns:
#             temp_ind = temp_ind.rename(columns={200.0: 'total_amount_rep'})
#         else:
#             temp_ind['total_amount_rep'] = 0
        
#         temp_ind['hedging'] = abs(temp_ind['total_amount_dem'] - temp_ind['total_amount_rep'])
        
#         temp_ind_result = temp_ind.groupby(['district', 'cycle']).agg(
#             hedging_money_general_np_ind=('hedging', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_ind_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict:
#                 agg_dict[key] = {}
#             agg_dict[key]['hedging_money_general_np_ind'] = row['hedging_money_general_np_ind']
        
#         ## Avg_count
#         temp_ind_count = input_treated[
#             (input_treated['contributor.type'] == 'I') & 
#             (input_treated['later_than_special'] != 1)
#         ].groupby(['district', 'cycle', 'party']).agg(
#             counting_hedging_ind=('bonica.rid', 'nunique')
#         ).reset_index()

#         temp_ind_count_result = temp_ind_count.groupby(['district', 'cycle']).agg(
#             avg_counting_hedging_np_ind = ('counting_hedging_ind', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_ind_count_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict:
#                 agg_dict[key] = {}
#             agg_dict[key]['avg_counting_hedging_np_ind'] = row['avg_counting_hedging_np_ind']
        
        
#         ### Small individual contributors
#         ## Hedging
#         temp_smallind = input_treated[
#             (input_treated['contributor.type'] == 'I') & 
#             (input_treated['later_than_special'] != 1) &
#             (input_treated['amount'] < 200)
#         ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
#             total_amount=('amount', 'sum')
#         ).reset_index()
        
#         temp_smallind = temp_smallind.pivot_table(
#             index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
#             columns='party',
#             values='total_amount',
#             fill_value=0
#         ).reset_index()
        
#         if 100.0 in temp_smallind.columns:
#             temp_smallind = temp_smallind.rename(columns={100.0: 'total_amount_dem'})
#         else:
#             temp_smallind['total_amount_dem'] = 0
            
#         if 200.0 in temp_smallind.columns:
#             temp_smallind = temp_smallind.rename(columns={200.0: 'total_amount_rep'})
#         else:
#             temp_smallind['total_amount_rep'] = 0
        
#         temp_smallind['hedging'] = abs(temp_smallind['total_amount_dem'] - temp_smallind['total_amount_rep'])
        
#         temp_smallind_result = temp_smallind.groupby(['district', 'cycle']).agg(
#             hedging_money_general_np_smallind=('hedging', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_smallind_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict:
#                 agg_dict[key] = {}
#             agg_dict[key]['hedging_money_general_np_smallind'] = row['hedging_money_general_np_smallind']
        
#         ## Avg_count
#         temp_smallind_count = input_treated[
#             (input_treated['contributor.type'] == 'I') & 
#             (input_treated['later_than_special'] != 1) &
#             (input_treated['amount'] < 200)
#         ].groupby(['district', 'cycle', 'party']).agg(
#             counting_hedging_smallind=('bonica.rid', 'nunique')
#         ).reset_index()

#         temp_smallind_count_result = temp_smallind_count.groupby(['district', 'cycle']).agg(
#             avg_counting_hedging_np_smallind = ('counting_hedging_smallind', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_smallind_count_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict:
#                 agg_dict[key] = {}
#             agg_dict[key]['avg_counting_hedging_np_smallind'] = row['avg_counting_hedging_np_smallind']

        
#         # Update result_df with calculated values for treated rows
#         print("  Updating result_df with _gen_np values for treated rows...")
#         for (district, cycle), values in agg_dict.items():
#             mask = (result_df['district'] == district) & (result_df['cycle'] == cycle)
#             for col, val in values.items():
#                 if col in result_df.columns:
#                     result_df.loc[mask, col] = val
    
#     # Process untreated rows (treat_3 == 0 or NaN) - use election.type == 'G'
#     print("  Processing untreated rows with election.type == 'G'...")
    
#     # Get all district-cycle combinations
#     all_districts = result_df[['district', 'cycle']].copy()
    
#     # Merge with treatment info
#     all_districts = pd.merge(
#         all_districts,
#         output_7_df[['district', 'cycle', 'treat_3']],
#         on=['district', 'cycle'],
#         how='left'
#     )
    
#     # Select untreated rows (treat_3 == 0 or NaN)
#     untreated_districts = all_districts[(all_districts['treat_3'] == 0) | (all_districts['treat_3'].isna())][['district', 'cycle']]
    
#     if not untreated_districts.empty:
#         # Merge with input data
#         input_untreated = pd.merge(
#             input_df,
#             untreated_districts,
#             on=['district', 'cycle'],
#             how='inner'
#         )
        
#         # Filter for general elections only
#         input_untreated = input_untreated[input_untreated['election.type'] == 'G']
        
#         # Create aggregations for untreated rows
#         agg_dict_untreated = {}
        
#         ### Corporate contributors - general only
        
#         ## Hedging
#         temp_corp_unt = input_untreated[
#             (input_untreated['contributor.type'] == 'C') & 
#             (input_untreated['later_than_special'] != 1)
#         ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
#             total_amount=('amount', 'sum')
#         ).reset_index()
        
#         temp_corp_unt = temp_corp_unt.pivot_table(
#             index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
#             columns='party',
#             values='total_amount',
#             fill_value=0
#         ).reset_index()
        
#         if 100.0 in temp_corp_unt.columns:
#             temp_corp_unt = temp_corp_unt.rename(columns={100.0: 'total_amount_dem'})
#         else:
#             temp_corp_unt['total_amount_dem'] = 0
            
#         if 200.0 in temp_corp_unt.columns:
#             temp_corp_unt = temp_corp_unt.rename(columns={200.0: 'total_amount_rep'})
#         else:
#             temp_corp_unt['total_amount_rep'] = 0
        
#         temp_corp_unt['hedging'] = abs(temp_corp_unt['total_amount_dem'] - temp_corp_unt['total_amount_rep'])
        
#         temp_corp_unt_result = temp_corp_unt.groupby(['district', 'cycle']).agg(
#             hedging_money_general_np_corp=('hedging', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_corp_unt_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict_untreated:
#                 agg_dict_untreated[key] = {}
#             agg_dict_untreated[key]['hedging_money_general_np_corp'] = row['hedging_money_general_np_corp']
        
#         ## Avg_count
#         temp_corp_unt_count = input_untreated[
#             (input_untreated['contributor.type'] == 'C') & 
#             (input_untreated['later_than_special'] != 1)
#         ].groupby(['district', 'cycle', 'party']).agg(
#             counting_hedging_corp=('bonica.rid', 'nunique')
#         ).reset_index()

#         temp_corp_unt_count_result = temp_corp_unt_count.groupby(['district', 'cycle']).agg(
#             avg_counting_hedging_np_corp = ('counting_hedging_corp', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_corp_unt_count_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict_untreated:
#                 agg_dict_untreated[key] = {}
#             agg_dict_untreated[key]['avg_counting_hedging_np_corp'] = row['avg_counting_hedging_np_corp']
        
        
#         ### Individual contributors - general only
        
#         ## Hedging
#         temp_ind_unt = input_untreated[
#             (input_untreated['contributor.type'] == 'I') & 
#             (input_untreated['later_than_special'] != 1)
#         ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
#             total_amount=('amount', 'sum')
#         ).reset_index()
        
#         temp_ind_unt = temp_ind_unt.pivot_table(
#             index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
#             columns='party',
#             values='total_amount',
#             fill_value=0
#         ).reset_index()
        
#         if 100.0 in temp_ind_unt.columns:
#             temp_ind_unt = temp_ind_unt.rename(columns={100.0: 'total_amount_dem'})
#         else:
#             temp_ind_unt['total_amount_dem'] = 0
            
#         if 200.0 in temp_ind_unt.columns:
#             temp_ind_unt = temp_ind_unt.rename(columns={200.0: 'total_amount_rep'})
#         else:
#             temp_ind_unt['total_amount_rep'] = 0
        
#         temp_ind_unt['hedging'] = abs(temp_ind_unt['total_amount_dem'] - temp_ind_unt['total_amount_rep'])
        
#         temp_ind_unt_result = temp_ind_unt.groupby(['district', 'cycle']).agg(
#             hedging_money_general_np_ind=('hedging', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_ind_unt_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict_untreated:
#                 agg_dict_untreated[key] = {}
#             agg_dict_untreated[key]['hedging_money_general_np_ind'] = row['hedging_money_general_np_ind']
        
#         ## Avg_count
#         temp_ind_unt_count = input_untreated[
#             (input_untreated['contributor.type'] == 'I') & 
#             (input_untreated['later_than_special'] != 1)
#         ].groupby(['district', 'cycle', 'party']).agg(
#             counting_hedging_ind=('bonica.rid', 'nunique')
#         ).reset_index()

#         temp_ind_unt_count_result = temp_ind_unt_count.groupby(['district', 'cycle']).agg(
#             avg_counting_hedging_np_ind = ('counting_hedging_ind', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_ind_unt_count_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict_untreated:
#                 agg_dict_untreated[key] = {}
#             agg_dict_untreated[key]['avg_counting_hedging_np_ind'] = row['avg_counting_hedging_np_ind']
        
        
#         ### Small individual contributors - general only
        
#         ## Hedging
#         temp_smallind_unt = input_untreated[
#             (input_untreated['contributor.type'] == 'I') & 
#             (input_untreated['later_than_special'] != 1) &
#             (input_untreated['amount'] < 200)
#         ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
#             total_amount=('amount', 'sum')
#         ).reset_index()
        
#         temp_smallind_unt = temp_smallind_unt.pivot_table(
#             index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
#             columns='party',
#             values='total_amount',
#             fill_value=0
#         ).reset_index()
        
#         if 100.0 in temp_smallind_unt.columns:
#             temp_smallind_unt = temp_smallind_unt.rename(columns={100.0: 'total_amount_dem'})
#         else:
#             temp_smallind_unt['total_amount_dem'] = 0
            
#         if 200.0 in temp_smallind_unt.columns:
#             temp_smallind_unt = temp_smallind_unt.rename(columns={200.0: 'total_amount_rep'})
#         else:
#             temp_smallind_unt['total_amount_rep'] = 0
        
#         temp_smallind_unt['hedging'] = abs(temp_smallind_unt['total_amount_dem'] - temp_smallind_unt['total_amount_rep'])
        
#         temp_smallind_unt_result = temp_smallind_unt.groupby(['district', 'cycle']).agg(
#             hedging_money_general_np_smallind=('hedging', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_smallind_unt_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict_untreated:
#                 agg_dict_untreated[key] = {}
#             agg_dict_untreated[key]['hedging_money_general_np_smallind'] = row['hedging_money_general_np_smallind']
        
#         ## Avg_count
#         temp_smallind_unt_count = input_untreated[
#             (input_untreated['contributor.type'] == 'I') & 
#             (input_untreated['later_than_special'] != 1) &
#             (input_untreated['amount'] < 200)
#         ].groupby(['district', 'cycle', 'party']).agg(
#             counting_hedging_smallind=('bonica.rid', 'nunique')
#         ).reset_index()

#         temp_smallind_unt_count_result = temp_smallind_unt_count.groupby(['district', 'cycle']).agg(
#             avg_counting_hedging_np_smallind = ('counting_hedging_smallind', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_smallind_unt_count_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict_untreated:
#                 agg_dict_untreated[key] = {}
#             agg_dict_untreated[key]['avg_counting_hedging_np_smallind'] = row['avg_counting_hedging_np_smallind']
        
#         # Update result_df with calculated values for untreated rows
#         print("  Updating result_df with _gen_np values for untreated rows...")
#         for (district, cycle), values in agg_dict_untreated.items():
#             mask = (result_df['district'] == district) & (result_df['cycle'] == cycle)
#             for col, val in values.items():
#                 if col in result_df.columns:
#                     result_df.loc[mask, col] = val
    
#     print("Finished adding _gen_np variables for OUTPUT_8_ext")
#     return result_df
    


#%%


def add_gen_np_variables(input_df, output_7_df, ext_df, output_name, single_death_districts, multiple_death_districts):
    """
    Add _gen_np suffix variables with _1, _2, _3 versions to _ext datasets.
    
    For rows where treat_X == 1: select contributions with election.type != 'P' between death_date and special_elections_date
    For other rows: keep election.type == 'G'
    
    Parameters:
    -----------
    input_df : DataFrame
        The raw contribution data (OUTPUT_1)
    output_7_df : DataFrame
        The treatment data with treat_1, treat_2, treat_3, death_date, special_elections_date
    ext_df : DataFrame
        The extended output dataset (OUTPUT_2_ext, OUTPUT_3_ext, etc.)
    output_name : str
        Name for the output (e.g., 'OUTPUT_2', 'OUTPUT_3', 'OUTPUT_4_1')
    single_death_districts : list
        List of districts with single death
    multiple_death_districts : list
        List of districts with multiple deaths
    
    Returns:
    --------
    DataFrame
        DataFrame with additional _gen_np_1, _gen_np_2, _gen_np_3 variables
    """
    
    print(f"Adding _gen_np variables with _1, _2, _3 suffixes for {output_name}_ext...")
    
    # Start with the existing ext_df
    result_df = ext_df.copy()
    
    # Determine filter type based on output_name
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
    
    # Apply type and amount filters to input_df
    input_df_filtered = input_df.copy()
    if filter_type:
        input_df_filtered = input_df_filtered[input_df_filtered['contributor.type'] == filter_type]
    if amount_filter:
        input_df_filtered = input_df_filtered[input_df_filtered['amount'] < amount_filter]
    
    # Define base gen_np variables
    gen_np_vars_base = [
        f'total_amount_gen_np{suffix}',
        f'tran_count_gen_np{suffix}',
        f'total_amount_gen_np_without_LTS1{suffix}',
        f'tran_count_gen_np_without_LTS1{suffix}',
        f'total_amount_dem_gen_np{suffix}',
        f'tran_count_dem_gen_np{suffix}',
        f'total_amount_dem_gen_np_without_LTS1{suffix}',
        f'tran_count_dem_gen_np_without_LTS1{suffix}',
        f'total_amount_rep_gen_np{suffix}',
        f'tran_count_rep_gen_np{suffix}',
        f'total_amount_rep_gen_np_without_LTS1{suffix}',
        f'tran_count_rep_gen_np_without_LTS1{suffix}'
    ]
    
    # Initialize _1, _2, _3 versions with NaN
    for treat_num in [1, 2, 3]:
        for var in gen_np_vars_base:
            result_df[f'{var}_{treat_num}'] = np.nan
    
    # Process each treatment
    for treat_num in [1, 2, 3]:
        print(f"  Processing treat_{treat_num} for gen_np variables...")
        
        # Get district-cycles where treatment == 1
        treated_districts = output_7_df[output_7_df[f'treat_{treat_num}'] == 1][['district', 'cycle', 'death_date', 'special_elections_date']]
        
        if treated_districts.empty:
            print(f"  No treated districts for treat_{treat_num}")
            continue
        
        # For treat_1 with multiple death districts, only keep the first instance per district
        if treat_num == 1:
            multiple_death_treated = treated_districts[treated_districts['district'].isin(multiple_death_districts)]
            
            if not multiple_death_treated.empty:
                first_occurrences = multiple_death_treated.groupby('district')['cycle'].idxmin()
                multiple_death_treated_first = multiple_death_treated.loc[first_occurrences]
                
                single_death_treated = treated_districts[treated_districts['district'].isin(single_death_districts)]
                
                treated_districts = pd.concat([single_death_treated, multiple_death_treated_first], ignore_index=True)
        
        # Merge filtered input_df with treated districts
        input_treated = pd.merge(
            input_df_filtered.drop(columns=['death_date', 'spec_election_date'], errors='ignore'),
            treated_districts,
            on=['district', 'cycle'],
            how='inner'
        )
        
        # Filter for contributions with election.type != 'P' between death_date and special_elections_date
        input_treated = input_treated[
            (input_treated['date'] > input_treated['death_date']) &
            (input_treated['date'] < input_treated['special_elections_date']) &
            (input_treated['election.type'] != 'P')
        ]
        
        # Create aggregations for treated rows
        treat_suffix = f'_{treat_num}'
        agg_dict = {}
        
        # 1: All contributions (no primary) - includes both G and S
        agg_1 = input_treated.groupby(['district', 'cycle']).agg(
            **{f'total_amount_gen_np{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_gen_np{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        agg_dict.update(agg_1.set_index(['district', 'cycle']).to_dict('index'))
        
        # 2: Before special election - includes both G and S
        agg_2 = input_treated[input_treated['later_than_special'] != 1].groupby(['district', 'cycle']).agg(
            **{f'total_amount_gen_np_without_LTS1{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_gen_np_without_LTS1{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_2.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 3: Dem general
        agg_3 = input_treated[input_treated['party'] == 100].groupby(['district', 'cycle']).agg(
            **{f'total_amount_dem_gen_np{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_dem_gen_np{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_3.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 4: Dem general, before special
        agg_4 = input_treated[(input_treated['party'] == 100) & (input_treated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
            **{f'total_amount_dem_gen_np_without_LTS1{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_dem_gen_np_without_LTS1{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_4.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 5: Rep general
        agg_5 = input_treated[input_treated['party'] == 200].groupby(['district', 'cycle']).agg(
            **{f'total_amount_rep_gen_np{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_rep_gen_np{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_5.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 6: Rep general, before special
        agg_6 = input_treated[(input_treated['party'] == 200) & (input_treated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
            **{f'total_amount_rep_gen_np_without_LTS1{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_rep_gen_np_without_LTS1{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_6.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # Update result_df with calculated values for treated rows
        print(f"  Updating result_df with _gen_np{treat_suffix} values for treated rows...")
        for (district, cycle), values in agg_dict.items():
            mask = (result_df['district'] == district) & (result_df['cycle'] == cycle)
            for col, val in values.items():
                if col in result_df.columns:
                    result_df.loc[mask, col] = val
    
    # # Process untreated rows (all treatments use election.type == 'G')
    # # We need to process this for EACH treatment separately since treat_1 has different treated rows than treat_2 and treat_3
    # print("  Processing untreated rows with election.type == 'G'...")
    
    # all_districts = result_df[['district', 'cycle']].copy()
    
    # # Merge with treatment info for treat_3 (most comprehensive)
    # all_districts = pd.merge(
    #     all_districts,
    #     output_7_df[['district', 'cycle', 'treat_3']],
    #     on=['district', 'cycle'],
    #     how='left'
    # )
    
    # # Select untreated rows (treat_3 == 0 or NaN)
    # untreated_districts = all_districts[(all_districts['treat_3'] == 0) | (all_districts['treat_3'].isna())][['district', 'cycle']]
    
    # if not untreated_districts.empty:
    #     # Merge with input data
    #     input_untreated = pd.merge(
    #         input_df_filtered,
    #         untreated_districts,
    #         on=['district', 'cycle'],
    #         how='inner'
    #     )
        
    #     # Filter for general elections only
    #     input_untreated = input_untreated[input_untreated['election.type'] == 'G']
        
    #     # Create aggregations for untreated rows
    #     agg_dict_untreated = {}
        
    #     # 1: All general election contributions
    #     agg_gen_all = input_untreated.groupby(['district', 'cycle']).agg(
    #         **{f'total_amount_gen_np{suffix}': ('amount', 'sum'),
    #            f'tran_count_gen_np{suffix}': ('transaction.id', 'count')}
    #     ).reset_index()
    #     agg_dict_untreated.update(agg_gen_all.set_index(['district', 'cycle']).to_dict('index'))
        
    #     # 2: General elections, before special election
    #     agg_gen_lts = input_untreated[input_untreated['later_than_special'] != 1].groupby(['district', 'cycle']).agg(
    #         **{f'total_amount_gen_np_without_LTS1{suffix}': ('amount', 'sum'),
    #            f'tran_count_gen_np_without_LTS1{suffix}': ('transaction.id', 'count')}
    #     ).reset_index()
    #     for idx, row in agg_gen_lts.iterrows():
    #         key = (row['district'], row['cycle'])
    #         if key not in agg_dict_untreated:
    #             agg_dict_untreated[key] = {}
    #         agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
    #     # Party-specific aggregations
    #     agg_dem = input_untreated[input_untreated['party'] == 100].groupby(['district', 'cycle']).agg(
    #         **{f'total_amount_dem_gen_np{suffix}': ('amount', 'sum'),
    #            f'tran_count_dem_gen_np{suffix}': ('transaction.id', 'count')}
    #     ).reset_index()
    #     for idx, row in agg_dem.iterrows():
    #         key = (row['district'], row['cycle'])
    #         if key not in agg_dict_untreated:
    #             agg_dict_untreated[key] = {}
    #         agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
    #     agg_dem_lts = input_untreated[(input_untreated['party'] == 100) & (input_untreated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
    #         **{f'total_amount_dem_gen_np_without_LTS1{suffix}': ('amount', 'sum'),
    #            f'tran_count_dem_gen_np_without_LTS1{suffix}': ('transaction.id', 'count')}
    #     ).reset_index()
    #     for idx, row in agg_dem_lts.iterrows():
    #         key = (row['district'], row['cycle'])
    #         if key not in agg_dict_untreated:
    #             agg_dict_untreated[key] = {}
    #         agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
    #     agg_rep = input_untreated[input_untreated['party'] == 200].groupby(['district', 'cycle']).agg(
    #         **{f'total_amount_rep_gen_np{suffix}': ('amount', 'sum'),
    #            f'tran_count_rep_gen_np{suffix}': ('transaction.id', 'count')}
    #     ).reset_index()
    #     for idx, row in agg_rep.iterrows():
    #         key = (row['district'], row['cycle'])
    #         if key not in agg_dict_untreated:
    #             agg_dict_untreated[key] = {}
    #         agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
    #     agg_rep_lts = input_untreated[(input_untreated['party'] == 200) & (input_untreated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
    #         **{f'total_amount_rep_gen_np_without_LTS1{suffix}': ('amount', 'sum'),
    #            f'tran_count_rep_gen_np_without_LTS1{suffix}': ('transaction.id', 'count')}
    #     ).reset_index()
    #     for idx, row in agg_rep_lts.iterrows():
    #         key = (row['district'], row['cycle'])
    #         if key not in agg_dict_untreated:
    #             agg_dict_untreated[key] = {}
    #         agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
    #     # Update result_df with calculated values for untreated rows (copy to all _1, _2, _3)
    #     print("  Updating result_df with _gen_np values for untreated rows...")
    #     for (district, cycle), values in agg_dict_untreated.items():
    #         mask = (result_df['district'] == district) & (result_df['cycle'] == cycle)
    #         for col, val in values.items():
    #             # Update all three treatment versions with the same untreated values
    #             for treat_num in [1, 2, 3]:
    #                 col_with_suffix = f'{col}_{treat_num}'
    #                 if col_with_suffix in result_df.columns:
    #                     result_df.loc[mask, col_with_suffix] = val
    
    # print(f"Finished adding _gen_np variables with _1, _2, _3 suffixes for {output_name}_ext")
    # return result_df
    
    # Process untreated rows (all treatments use election.type == 'G')
    # We need to process this for EACH treatment separately since treat_1 has different treated rows than treat_2 and treat_3
    print("  Processing untreated rows with election.type == 'G'...")
    
    for treat_num in [1, 2, 3]:
        print(f"  Processing untreated rows for treat_{treat_num}...")
        
        all_districts = result_df[['district', 'cycle']].copy()
        
        # Merge with ALL treatment info at once
        all_districts = pd.merge(
            all_districts,
            output_7_df[['district', 'cycle', 'treat_1', 'treat_2', 'treat_3']],
            on=['district', 'cycle'],
            how='left'
        )
        
        if treat_num == 1:
            # For treat_1: untreated means:
            # 1. treat_1 == 0 (never treated)
            # 2. OR (treat_1 == 1 AND treat_3 == 0) - post first death but no death this cycle
            # 3. OR (district in multiple_death_districts AND treat_1 == 1 AND treat_3 == 1 AND this is NOT the first death)
            
            # Get the first death for each district (already processed in treated section)
            first_deaths = output_7_df[output_7_df['treat_1'] == 1].copy()
            if not first_deaths.empty and len(multiple_death_districts) > 0:
                first_deaths_multiple = first_deaths[first_deaths['district'].isin(multiple_death_districts)]
                if not first_deaths_multiple.empty:
                    first_death_cycles = first_deaths_multiple.groupby('district')['cycle'].min().reset_index()
                    first_death_cycles.columns = ['district', 'first_death_cycle']
                    
                    # Merge to identify which rows are first deaths
                    all_districts = pd.merge(
                        all_districts,
                        first_death_cycles,
                        on='district',
                        how='left'
                    )
                    
                    # Create a flag for multiple-death districts
                    all_districts['is_multiple_death'] = all_districts['district'].isin(multiple_death_districts)
                    
                    # Untreated for treat_1 includes:
                    # - Never treated (treat_1 == 0)
                    # - Post-treatment non-death cycles (treat_1 == 1, treat_3 == 0)
                    # - Subsequent deaths ONLY in multiple-death districts (is_multiple_death & treat_3 == 1 & cycle != first_death_cycle)
                    untreated_districts = all_districts[
                        (all_districts['treat_1'] == 0) | 
                        ((all_districts['treat_1'] == 1) & (all_districts['treat_3'] == 0)) |
                        ((all_districts['is_multiple_death'] == True) & 
                         (all_districts['treat_1'] == 1) & 
                         (all_districts['treat_3'] == 1) & 
                         (all_districts['cycle'] != all_districts['first_death_cycle']))
                    ][['district', 'cycle']]
                else:
                    # No multiple death districts, simpler logic
                    untreated_districts = all_districts[
                        (all_districts['treat_1'] == 0) | 
                        ((all_districts['treat_1'] == 1) & (all_districts['treat_3'] == 0))
                    ][['district', 'cycle']]
            else:
                # No multiple death districts, simpler logic
                untreated_districts = all_districts[
                    (all_districts['treat_1'] == 0) | 
                    ((all_districts['treat_1'] == 1) & (all_districts['treat_3'] == 0))
                ][['district', 'cycle']]
            
        else:
            # For treat_2 and treat_3: untreated means treat_3 == 0 (no death in this cycle)
            # treat_2 and treat_3 have identical logic
            untreated_districts = all_districts[
                (all_districts['treat_3'] == 0) | (all_districts['treat_3'].isna())
            ][['district', 'cycle']]
            
        if untreated_districts.empty:
            print(f"  No untreated districts for treat_{treat_num}")
            continue
        
        # Merge with input data
        input_untreated = pd.merge(
            input_df_filtered,
            untreated_districts,
            on=['district', 'cycle'],
            how='inner'
        )
        
        # Filter for general elections only
        input_untreated = input_untreated[input_untreated['election.type'] == 'G']
        
        # Create aggregations for untreated rows
        agg_dict_untreated = {}
        
        # 1: All general election contributions
        agg_gen_all = input_untreated.groupby(['district', 'cycle']).agg(
            **{f'total_amount_gen_np{suffix}': ('amount', 'sum'),
               f'tran_count_gen_np{suffix}': ('transaction.id', 'count')}
        ).reset_index()
        agg_dict_untreated.update(agg_gen_all.set_index(['district', 'cycle']).to_dict('index'))
        
        # 2: General elections, before special election
        agg_gen_lts = input_untreated[input_untreated['later_than_special'] != 1].groupby(['district', 'cycle']).agg(
            **{f'total_amount_gen_np_without_LTS1{suffix}': ('amount', 'sum'),
               f'tran_count_gen_np_without_LTS1{suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_gen_lts.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict_untreated:
                agg_dict_untreated[key] = {}
            agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # Party-specific aggregations
        agg_dem = input_untreated[input_untreated['party'] == 100].groupby(['district', 'cycle']).agg(
            **{f'total_amount_dem_gen_np{suffix}': ('amount', 'sum'),
               f'tran_count_dem_gen_np{suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_dem.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict_untreated:
                agg_dict_untreated[key] = {}
            agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        agg_dem_lts = input_untreated[(input_untreated['party'] == 100) & (input_untreated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
            **{f'total_amount_dem_gen_np_without_LTS1{suffix}': ('amount', 'sum'),
               f'tran_count_dem_gen_np_without_LTS1{suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_dem_lts.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict_untreated:
                agg_dict_untreated[key] = {}
            agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        agg_rep = input_untreated[input_untreated['party'] == 200].groupby(['district', 'cycle']).agg(
            **{f'total_amount_rep_gen_np{suffix}': ('amount', 'sum'),
               f'tran_count_rep_gen_np{suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_rep.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict_untreated:
                agg_dict_untreated[key] = {}
            agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        agg_rep_lts = input_untreated[(input_untreated['party'] == 200) & (input_untreated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
            **{f'total_amount_rep_gen_np_without_LTS1{suffix}': ('amount', 'sum'),
               f'tran_count_rep_gen_np_without_LTS1{suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_rep_lts.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict_untreated:
                agg_dict_untreated[key] = {}
            agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # Update result_df with calculated values for untreated rows for THIS treatment
        print(f"  Updating result_df with _gen_np_{treat_num} values for untreated rows...")
        treat_suffix = f'_{treat_num}'
        for (district, cycle), values in agg_dict_untreated.items():
            mask = (result_df['district'] == district) & (result_df['cycle'] == cycle)
            for col, val in values.items():
                col_with_suffix = f'{col}{treat_suffix}'
                if col_with_suffix in result_df.columns:
                    result_df.loc[mask, col_with_suffix] = val    
                    
    print(f"Finished adding _gen_np variables with _1, _2, _3 suffixes for {output_name}_ext")
    return result_df


def add_gen_np_variables_output8(input_df, output_7_df, output8_ext, single_death_districts, multiple_death_districts):
    """
    Add _gen_np suffix variables with _1, _2, _3 versions to OUTPUT_8_ext dataset.
    
    For rows where treat_X == 1: select contributions with election.type != 'P' between death_date and special_elections_date
    For other rows: keep election.type == 'G'
    
    Parameters:
    -----------
    input_df : DataFrame
        The raw contribution data (OUTPUT_1)
    output_7_df : DataFrame
        The treatment data with treat_1, treat_2, treat_3, death_date, special_elections_date
    output8_ext : DataFrame
        The OUTPUT_8_ext dataset
    single_death_districts : list
        List of districts with single death
    multiple_death_districts : list
        List of districts with multiple deaths
    
    Returns:
    --------
    DataFrame
        DataFrame with additional _gen_np_1, _gen_np_2, _gen_np_3 variables
    """
    
    print("Adding _gen_np variables with _1, _2, _3 suffixes for OUTPUT_8_ext...")
    
    # Start with the existing output8_ext
    result_df = output8_ext.copy()
    
    # Define the base _gen_np variables
    gen_np_vars_base = [
        'hedging_money_general_np_corp',
        'avg_counting_hedging_np_corp',
        'hedging_money_general_np_ind',
        'avg_counting_hedging_np_ind',
        'hedging_money_general_np_smallind',
        'avg_counting_hedging_np_smallind',
    ]
    
    # Initialize _1, _2, _3 versions with NaN
    for treat_num in [1, 2, 3]:
        for var in gen_np_vars_base:
            result_df[f'{var}_{treat_num}'] = np.nan
    
    # Process each treatment
    for treat_num in [1, 2, 3]:
        print(f"  Processing treat_{treat_num} for OUTPUT_8 gen_np variables...")
        
        # Get district-cycles where treatment == 1
        treated_districts = output_7_df[output_7_df[f'treat_{treat_num}'] == 1][['district', 'cycle', 'death_date', 'special_elections_date']]
        
        if treated_districts.empty:
            print(f"  No treated districts for treat_{treat_num}")
            continue
        
        # For treat_1 with multiple death districts, only keep the first instance per district
        if treat_num == 1:
            multiple_death_treated = treated_districts[treated_districts['district'].isin(multiple_death_districts)]
            
            if not multiple_death_treated.empty:
                first_occurrences = multiple_death_treated.groupby('district')['cycle'].idxmin()
                multiple_death_treated_first = multiple_death_treated.loc[first_occurrences]
                
                single_death_treated = treated_districts[treated_districts['district'].isin(single_death_districts)]
                
                treated_districts = pd.concat([single_death_treated, multiple_death_treated_first], ignore_index=True)
        
        # Merge input_df with treated districts
        input_treated = pd.merge(
            input_df.drop(columns=['death_date', 'spec_election_date'], errors='ignore'),
            treated_districts,
            on=['district', 'cycle'],
            how='inner'
        )
        
        # Filter for contributions with election.type != 'P' between death_date and special_elections_date
        input_treated = input_treated[
            (input_treated['date'] > input_treated['death_date']) &
            (input_treated['date'] < input_treated['special_elections_date']) &
            (input_treated['election.type'] != 'P')
        ]
        
        treat_suffix = f'_{treat_num}'
        agg_dict = {}
        
        ### Corporate contributors
        
        ## Hedging
        temp_corp = input_treated[
            (input_treated['contributor.type'] == 'C') & 
            (input_treated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
            total_amount=('amount', 'sum')
        ).reset_index()
        
        temp_corp = temp_corp.pivot_table(
            index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
            columns='party',
            values='total_amount',
            fill_value=0
        ).reset_index()
        
        if 100.0 in temp_corp.columns:
            temp_corp = temp_corp.rename(columns={100.0: 'total_amount_dem'})
        else:
            temp_corp['total_amount_dem'] = 0
            
        if 200.0 in temp_corp.columns:
            temp_corp = temp_corp.rename(columns={200.0: 'total_amount_rep'})
        else:
            temp_corp['total_amount_rep'] = 0
        
        temp_corp['hedging'] = abs(temp_corp['total_amount_dem'] - temp_corp['total_amount_rep'])
        
        temp_corp_result = temp_corp.groupby(['district', 'cycle']).agg(
            **{f'hedging_money_general_np_corp{treat_suffix}': ('hedging', 'mean')}
        ).reset_index()
        
        for idx, row in temp_corp_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key][f'hedging_money_general_np_corp{treat_suffix}'] = row[f'hedging_money_general_np_corp{treat_suffix}']
            
           
        ## Avg_count
        temp_corp_count = input_treated[
            (input_treated['contributor.type'] == 'C') & 
            (input_treated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'party']).agg(
            counting_hedging_corp=('bonica.rid', 'nunique')
        ).reset_index()

        temp_corp_count_result = temp_corp_count.groupby(['district', 'cycle']).agg(
            **{f'avg_counting_hedging_np_corp{treat_suffix}': ('counting_hedging_corp', 'mean')}
        ).reset_index()
        
        for idx, row in temp_corp_count_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key][f'avg_counting_hedging_np_corp{treat_suffix}'] = row[f'avg_counting_hedging_np_corp{treat_suffix}']
        
        
        ### Individual contributors
        ## Hedging
        temp_ind = input_treated[
            (input_treated['contributor.type'] == 'I') & 
            (input_treated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
            total_amount=('amount', 'sum')
        ).reset_index()
        
        temp_ind = temp_ind.pivot_table(
            index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
            columns='party',
            values='total_amount',
            fill_value=0
        ).reset_index()
        
        if 100.0 in temp_ind.columns:
            temp_ind = temp_ind.rename(columns={100.0: 'total_amount_dem'})
        else:
            temp_ind['total_amount_dem'] = 0
            
        if 200.0 in temp_ind.columns:
            temp_ind = temp_ind.rename(columns={200.0: 'total_amount_rep'})
        else:
            temp_ind['total_amount_rep'] = 0
        
        temp_ind['hedging'] = abs(temp_ind['total_amount_dem'] - temp_ind['total_amount_rep'])
        
        temp_ind_result = temp_ind.groupby(['district', 'cycle']).agg(
            **{f'hedging_money_general_np_ind{treat_suffix}': ('hedging', 'mean')}
        ).reset_index()
        
        for idx, row in temp_ind_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key][f'hedging_money_general_np_ind{treat_suffix}'] = row[f'hedging_money_general_np_ind{treat_suffix}']
        
        ## Avg_count
        temp_ind_count = input_treated[
            (input_treated['contributor.type'] == 'I') & 
            (input_treated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'party']).agg(
            counting_hedging_ind=('bonica.rid', 'nunique')
        ).reset_index()

        temp_ind_count_result = temp_ind_count.groupby(['district', 'cycle']).agg(
            **{f'avg_counting_hedging_np_ind{treat_suffix}': ('counting_hedging_ind', 'mean')}
        ).reset_index()
        
        for idx, row in temp_ind_count_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key][f'avg_counting_hedging_np_ind{treat_suffix}'] = row[f'avg_counting_hedging_np_ind{treat_suffix}']
        
        
        ### Small individual contributors
        ## Hedging
        temp_smallind = input_treated[
            (input_treated['contributor.type'] == 'I') & 
            (input_treated['later_than_special'] != 1) &
            (input_treated['amount'] < 200)
        ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
            total_amount=('amount', 'sum')
        ).reset_index()
        
        temp_smallind = temp_smallind.pivot_table(
            index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
            columns='party',
            values='total_amount',
            fill_value=0
        ).reset_index()
        
        if 100.0 in temp_smallind.columns:
            temp_smallind = temp_smallind.rename(columns={100.0: 'total_amount_dem'})
        else:
            temp_smallind['total_amount_dem'] = 0
            
        if 200.0 in temp_smallind.columns:
            temp_smallind = temp_smallind.rename(columns={200.0: 'total_amount_rep'})
        else:
            temp_smallind['total_amount_rep'] = 0
        
        temp_smallind['hedging'] = abs(temp_smallind['total_amount_dem'] - temp_smallind['total_amount_rep'])
        
        temp_smallind_result = temp_smallind.groupby(['district', 'cycle']).agg(
            **{f'hedging_money_general_np_smallind{treat_suffix}': ('hedging', 'mean')}
        ).reset_index()
        
        for idx, row in temp_smallind_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key][f'hedging_money_general_np_smallind{treat_suffix}'] = row[f'hedging_money_general_np_smallind{treat_suffix}']
        
        ## Avg_count
        temp_smallind_count = input_treated[
            (input_treated['contributor.type'] == 'I') & 
            (input_treated['later_than_special'] != 1) &
            (input_treated['amount'] < 200)
        ].groupby(['district', 'cycle', 'party']).agg(
            counting_hedging_smallind=('bonica.rid', 'nunique')
        ).reset_index()

        temp_smallind_count_result = temp_smallind_count.groupby(['district', 'cycle']).agg(
            **{f'avg_counting_hedging_np_smallind{treat_suffix}': ('counting_hedging_smallind', 'mean')}
        ).reset_index()
        
        for idx, row in temp_smallind_count_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key][f'avg_counting_hedging_np_smallind{treat_suffix}'] = row[f'avg_counting_hedging_np_smallind{treat_suffix}']

        
        # Update result_df with calculated values for treated rows
        print(f"  Updating result_df with _gen_np{treat_suffix} values for treated rows...")
        for (district, cycle), values in agg_dict.items():
            mask = (result_df['district'] == district) & (result_df['cycle'] == cycle)
            for col, val in values.items():
                if col in result_df.columns:
                    result_df.loc[mask, col] = val
    
    # # Process untreated rows (all treatments use election.type == 'G')
    # print("  Processing untreated rows with election.type == 'G'...")
    
    # all_districts = result_df[['district', 'cycle']].copy()
    
    # # Merge with treatment info
    # all_districts = pd.merge(
    #     all_districts,
    #     output_7_df[['district', 'cycle', 'treat_3']],
    #     on=['district', 'cycle'],
    #     how='left'
    # )
    
    # # Select untreated rows (treat_3 == 0 or NaN)
    # untreated_districts = all_districts[(all_districts['treat_3'] == 0) | (all_districts['treat_3'].isna())][['district', 'cycle']]
    
    # if not untreated_districts.empty:
    #     # Merge with input data
    #     input_untreated = pd.merge(
    #         input_df,
    #         untreated_districts,
    #         on=['district', 'cycle'],
    #         how='inner'
    #     )
        
    #     # Filter for general elections only
    #     input_untreated = input_untreated[input_untreated['election.type'] == 'G']
        
    #     # Create aggregations for untreated rows
    #     agg_dict_untreated = {}
        
    #     ### Corporate contributors - general only
        
    #     ## Hedging
    #     temp_corp_unt = input_untreated[
    #         (input_untreated['contributor.type'] == 'C') & 
    #         (input_untreated['later_than_special'] != 1)
    #     ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
    #         total_amount=('amount', 'sum')
    #     ).reset_index()
        
    #     temp_corp_unt = temp_corp_unt.pivot_table(
    #         index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
    #         columns='party',
    #         values='total_amount',
    #         fill_value=0
    #     ).reset_index()
        
    #     if 100.0 in temp_corp_unt.columns:
    #         temp_corp_unt = temp_corp_unt.rename(columns={100.0: 'total_amount_dem'})
    #     else:
    #         temp_corp_unt['total_amount_dem'] = 0
            
    #     if 200.0 in temp_corp_unt.columns:
    #         temp_corp_unt = temp_corp_unt.rename(columns={200.0: 'total_amount_rep'})
    #     else:
    #         temp_corp_unt['total_amount_rep'] = 0
        
    #     temp_corp_unt['hedging'] = abs(temp_corp_unt['total_amount_dem'] - temp_corp_unt['total_amount_rep'])
        
    #     temp_corp_unt_result = temp_corp_unt.groupby(['district', 'cycle']).agg(
    #         hedging_money_general_np_corp=('hedging', 'mean')
    #     ).reset_index()
        
    #     for idx, row in temp_corp_unt_result.iterrows():
    #         key = (row['district'], row['cycle'])
    #         if key not in agg_dict_untreated:
    #             agg_dict_untreated[key] = {}
    #         agg_dict_untreated[key]['hedging_money_general_np_corp'] = row['hedging_money_general_np_corp']
        
    #     ## Avg_count
    #     temp_corp_unt_count = input_untreated[
    #         (input_untreated['contributor.type'] == 'C') & 
    #         (input_untreated['later_than_special'] != 1)
    #     ].groupby(['district', 'cycle', 'party']).agg(
    #         counting_hedging_corp=('bonica.rid', 'nunique')
    #     ).reset_index()

    #     temp_corp_unt_count_result = temp_corp_unt_count.groupby(['district', 'cycle']).agg(
    #         avg_counting_hedging_np_corp=('counting_hedging_corp', 'mean')
    #     ).reset_index()
        
    #     for idx, row in temp_corp_unt_count_result.iterrows():
    #         key = (row['district'], row['cycle'])
    #         if key not in agg_dict_untreated:
    #             agg_dict_untreated[key] = {}
    #         agg_dict_untreated[key]['avg_counting_hedging_np_corp'] = row['avg_counting_hedging_np_corp']
        
        
    #     ### Individual contributors - general only
        
    #     ## Hedging
    #     temp_ind_unt = input_untreated[
    #         (input_untreated['contributor.type'] == 'I') & 
    #         (input_untreated['later_than_special'] != 1)
    #     ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
    #         total_amount=('amount', 'sum')
    #     ).reset_index()
        
    #     temp_ind_unt = temp_ind_unt.pivot_table(
    #         index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
    #         columns='party',
    #         values='total_amount',
    #         fill_value=0
    #     ).reset_index()
        
    #     if 100.0 in temp_ind_unt.columns:
    #         temp_ind_unt = temp_ind_unt.rename(columns={100.0: 'total_amount_dem'})
    #     else:
    #         temp_ind_unt['total_amount_dem'] = 0
            
    #     if 200.0 in temp_ind_unt.columns:
    #         temp_ind_unt = temp_ind_unt.rename(columns={200.0: 'total_amount_rep'})
    #     else:
    #         temp_ind_unt['total_amount_rep'] = 0
        
    #     temp_ind_unt['hedging'] = abs(temp_ind_unt['total_amount_dem'] - temp_ind_unt['total_amount_rep'])
        
    #     temp_ind_unt_result = temp_ind_unt.groupby(['district', 'cycle']).agg(
    #         hedging_money_general_np_ind=('hedging', 'mean')
    #     ).reset_index()
        
    #     for idx, row in temp_ind_unt_result.iterrows():
    #         key = (row['district'], row['cycle'])
    #         if key not in agg_dict_untreated:
    #             agg_dict_untreated[key] = {}
    #         agg_dict_untreated[key]['hedging_money_general_np_ind'] = row['hedging_money_general_np_ind']
        
    #     ## Avg_count
    #     temp_ind_unt_count = input_untreated[
    #         (input_untreated['contributor.type'] == 'I') & 
    #         (input_untreated['later_than_special'] != 1)
    #     ].groupby(['district', 'cycle', 'party']).agg(
    #         counting_hedging_ind=('bonica.rid', 'nunique')
    #     ).reset_index()

    #     temp_ind_unt_count_result = temp_ind_unt_count.groupby(['district', 'cycle']).agg(
    #         avg_counting_hedging_np_ind=('counting_hedging_ind', 'mean')
    #     ).reset_index()
        
    #     for idx, row in temp_ind_unt_count_result.iterrows():
    #         key = (row['district'], row['cycle'])
    #         if key not in agg_dict_untreated:
    #             agg_dict_untreated[key] = {}
    #         agg_dict_untreated[key]['avg_counting_hedging_np_ind'] = row['avg_counting_hedging_np_ind']
        
        
    #     ### Small individual contributors - general only
        
    #     ## Hedging
    #     temp_smallind_unt = input_untreated[
    #         (input_untreated['contributor.type'] == 'I') & 
    #         (input_untreated['later_than_special'] != 1) &
    #         (input_untreated['amount'] < 200)
    #     ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
    #         total_amount=('amount', 'sum')
    #     ).reset_index()
        
    #     temp_smallind_unt = temp_smallind_unt.pivot_table(
    #         index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
    #         columns='party',
    #         values='total_amount',
    #         fill_value=0
    #     ).reset_index()
        
    #     if 100.0 in temp_smallind_unt.columns:
    #         temp_smallind_unt = temp_smallind_unt.rename(columns={100.0: 'total_amount_dem'})
    #     else:
    #         temp_smallind_unt['total_amount_dem'] = 0
            
    #     if 200.0 in temp_smallind_unt.columns:
    #         temp_smallind_unt = temp_smallind_unt.rename(columns={200.0: 'total_amount_rep'})
    #     else:
    #         temp_smallind_unt['total_amount_rep'] = 0
        
    #     temp_smallind_unt['hedging'] = abs(temp_smallind_unt['total_amount_dem'] - temp_smallind_unt['total_amount_rep'])
        
    #     temp_smallind_unt_result = temp_smallind_unt.groupby(['district', 'cycle']).agg(
    #         hedging_money_general_np_smallind=('hedging', 'mean')
    #     ).reset_index()
        
    #     for idx, row in temp_smallind_unt_result.iterrows():
    #         key = (row['district'], row['cycle'])
    #         if key not in agg_dict_untreated:
    #             agg_dict_untreated[key] = {}
    #         agg_dict_untreated[key]['hedging_money_general_np_smallind'] = row['hedging_money_general_np_smallind']
        
    #     ## Avg_count
    #     temp_smallind_unt_count = input_untreated[
    #         (input_untreated['contributor.type'] == 'I') & 
    #         (input_untreated['later_than_special'] != 1) &
    #         (input_untreated['amount'] < 200)
    #     ].groupby(['district', 'cycle', 'party']).agg(
    #         counting_hedging_smallind=('bonica.rid', 'nunique')
    #     ).reset_index()

    #     temp_smallind_unt_count_result = temp_smallind_unt_count.groupby(['district', 'cycle']).agg(
    #         avg_counting_hedging_np_smallind=('counting_hedging_smallind', 'mean')
    #     ).reset_index()
        
    #     for idx, row in temp_smallind_unt_count_result.iterrows():
    #         key = (row['district'], row['cycle'])
    #         if key not in agg_dict_untreated:
    #             agg_dict_untreated[key] = {}
    #         agg_dict_untreated[key]['avg_counting_hedging_np_smallind'] = row['avg_counting_hedging_np_smallind']
        
    #     # Update result_df with calculated values for untreated rows (copy to all _1, _2, _3)
    #     print("  Updating result_df with _gen_np values for untreated rows...")
    #     for (district, cycle), values in agg_dict_untreated.items():
    #         mask = (result_df['district'] == district) & (result_df['cycle'] == cycle)
    #         for col, val in values.items():
    #             # Update all three treatment versions with the same untreated values
    #             for treat_num in [1, 2, 3]:
    #                 col_with_suffix = f'{col}_{treat_num}'
    #                 if col_with_suffix in result_df.columns:
    #                     result_df.loc[mask, col_with_suffix] = val
    
    # print("Finished adding _gen_np variables with _1, _2, _3 suffixes for OUTPUT_8_ext")
    # return result_df
    
    
    # Process untreated rows (all treatments use election.type == 'G')
    # We need to process this for EACH treatment separately since treat_1 has different treated rows than treat_2 and treat_3
    print("  Processing untreated rows with election.type == 'G'...")
    
    for treat_num in [1, 2, 3]:
        print(f"  Processing untreated rows for treat_{treat_num}...")
        
        all_districts = result_df[['district', 'cycle']].copy()
        
        # Merge with ALL treatment info at once
        all_districts = pd.merge(
            all_districts,
            output_7_df[['district', 'cycle', 'treat_1', 'treat_2', 'treat_3']],
            on=['district', 'cycle'],
            how='left'
        )
        
        if treat_num == 1:
            # For treat_1: untreated means:
            # 1. treat_1 == 0 (never treated)
            # 2. OR (treat_1 == 1 AND treat_3 == 0) - post first death but no death this cycle
            # 3. OR (district in multiple_death_districts AND treat_1 == 1 AND treat_3 == 1 AND this is NOT the first death)
            
            # Get the first death for each district (already processed in treated section)
            first_deaths = output_7_df[output_7_df['treat_1'] == 1].copy()
            if not first_deaths.empty and len(multiple_death_districts) > 0:
                first_deaths_multiple = first_deaths[first_deaths['district'].isin(multiple_death_districts)]
                if not first_deaths_multiple.empty:
                    first_death_cycles = first_deaths_multiple.groupby('district')['cycle'].min().reset_index()
                    first_death_cycles.columns = ['district', 'first_death_cycle']
                    
                    # Merge to identify which rows are first deaths
                    all_districts = pd.merge(
                        all_districts,
                        first_death_cycles,
                        on='district',
                        how='left'
                    )
                    
                    # Create a flag for multiple-death districts
                    all_districts['is_multiple_death'] = all_districts['district'].isin(multiple_death_districts)
                    
                    # Untreated for treat_1 includes:
                    # - Never treated (treat_1 == 0)
                    # - Post-treatment non-death cycles (treat_1 == 1, treat_3 == 0)
                    # - Subsequent deaths ONLY in multiple-death districts (is_multiple_death & treat_3 == 1 & cycle != first_death_cycle)
                    untreated_districts = all_districts[
                        (all_districts['treat_1'] == 0) | 
                        ((all_districts['treat_1'] == 1) & (all_districts['treat_3'] == 0)) |
                        ((all_districts['is_multiple_death'] == True) & 
                         (all_districts['treat_1'] == 1) & 
                         (all_districts['treat_3'] == 1) & 
                         (all_districts['cycle'] != all_districts['first_death_cycle']))
                    ][['district', 'cycle']]
                else:
                    # No multiple death districts, simpler logic
                    untreated_districts = all_districts[
                        (all_districts['treat_1'] == 0) | 
                        ((all_districts['treat_1'] == 1) & (all_districts['treat_3'] == 0))
                    ][['district', 'cycle']]
            else:
                # No multiple death districts, simpler logic
                untreated_districts = all_districts[
                    (all_districts['treat_1'] == 0) | 
                    ((all_districts['treat_1'] == 1) & (all_districts['treat_3'] == 0))
                ][['district', 'cycle']]
            
        else:
            # For treat_2 and treat_3: untreated means treat_3 == 0 (no death in this cycle)
            # treat_2 and treat_3 have identical logic
            untreated_districts = all_districts[
                (all_districts['treat_3'] == 0) | (all_districts['treat_3'].isna())
            ][['district', 'cycle']]
            
        if untreated_districts.empty:
            print(f"  No untreated districts for treat_{treat_num}")
            continue

                
        # Merge with input data
        input_untreated = pd.merge(
            input_df,
            untreated_districts,
            on=['district', 'cycle'],
            how='inner'
        )
        
        # Filter for general elections only
        input_untreated = input_untreated[input_untreated['election.type'] == 'G']
        
        # Create aggregations for untreated rows
        agg_dict_untreated = {}
        
        ### Corporate contributors - general only
        
        ## Hedging
        temp_corp_unt = input_untreated[
            (input_untreated['contributor.type'] == 'C') & 
            (input_untreated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
            total_amount=('amount', 'sum')
        ).reset_index()
        
        temp_corp_unt = temp_corp_unt.pivot_table(
            index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
            columns='party',
            values='total_amount',
            fill_value=0
        ).reset_index()
        
        if 100.0 in temp_corp_unt.columns:
            temp_corp_unt = temp_corp_unt.rename(columns={100.0: 'total_amount_dem'})
        else:
            temp_corp_unt['total_amount_dem'] = 0
            
        if 200.0 in temp_corp_unt.columns:
            temp_corp_unt = temp_corp_unt.rename(columns={200.0: 'total_amount_rep'})
        else:
            temp_corp_unt['total_amount_rep'] = 0
        
        temp_corp_unt['hedging'] = abs(temp_corp_unt['total_amount_dem'] - temp_corp_unt['total_amount_rep'])
        
        temp_corp_unt_result = temp_corp_unt.groupby(['district', 'cycle']).agg(
            hedging_money_general_np_corp=('hedging', 'mean')
        ).reset_index()
        
        for idx, row in temp_corp_unt_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict_untreated:
                agg_dict_untreated[key] = {}
            agg_dict_untreated[key]['hedging_money_general_np_corp'] = row['hedging_money_general_np_corp']
        
        ## Avg_count
        temp_corp_unt_count = input_untreated[
            (input_untreated['contributor.type'] == 'C') & 
            (input_untreated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'party']).agg(
            counting_hedging_corp=('bonica.rid', 'nunique')
        ).reset_index()

        temp_corp_unt_count_result = temp_corp_unt_count.groupby(['district', 'cycle']).agg(
            avg_counting_hedging_np_corp=('counting_hedging_corp', 'mean')
        ).reset_index()
        
        for idx, row in temp_corp_unt_count_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict_untreated:
                agg_dict_untreated[key] = {}
            agg_dict_untreated[key]['avg_counting_hedging_np_corp'] = row['avg_counting_hedging_np_corp']
        
        
        ### Individual contributors - general only
        
        ## Hedging
        temp_ind_unt = input_untreated[
            (input_untreated['contributor.type'] == 'I') & 
            (input_untreated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
            total_amount=('amount', 'sum')
        ).reset_index()
        
        temp_ind_unt = temp_ind_unt.pivot_table(
            index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
            columns='party',
            values='total_amount',
            fill_value=0
        ).reset_index()
        
        if 100.0 in temp_ind_unt.columns:
            temp_ind_unt = temp_ind_unt.rename(columns={100.0: 'total_amount_dem'})
        else:
            temp_ind_unt['total_amount_dem'] = 0
            
        if 200.0 in temp_ind_unt.columns:
            temp_ind_unt = temp_ind_unt.rename(columns={200.0: 'total_amount_rep'})
        else:
            temp_ind_unt['total_amount_rep'] = 0
        
        temp_ind_unt['hedging'] = abs(temp_ind_unt['total_amount_dem'] - temp_ind_unt['total_amount_rep'])
        
        temp_ind_unt_result = temp_ind_unt.groupby(['district', 'cycle']).agg(
            hedging_money_general_np_ind=('hedging', 'mean')
        ).reset_index()
        
        for idx, row in temp_ind_unt_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict_untreated:
                agg_dict_untreated[key] = {}
            agg_dict_untreated[key]['hedging_money_general_np_ind'] = row['hedging_money_general_np_ind']
        
        ## Avg_count
        temp_ind_unt_count = input_untreated[
            (input_untreated['contributor.type'] == 'I') & 
            (input_untreated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'party']).agg(
            counting_hedging_ind=('bonica.rid', 'nunique')
        ).reset_index()

        temp_ind_unt_count_result = temp_ind_unt_count.groupby(['district', 'cycle']).agg(
            avg_counting_hedging_np_ind=('counting_hedging_ind', 'mean')
        ).reset_index()
        
        for idx, row in temp_ind_unt_count_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict_untreated:
                agg_dict_untreated[key] = {}
            agg_dict_untreated[key]['avg_counting_hedging_np_ind'] = row['avg_counting_hedging_np_ind']
        
        
        ### Small individual contributors - general only
        
        ## Hedging
        temp_smallind_unt = input_untreated[
            (input_untreated['contributor.type'] == 'I') & 
            (input_untreated['later_than_special'] != 1) &
            (input_untreated['amount'] < 200)
        ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
            total_amount=('amount', 'sum')
        ).reset_index()
        
        temp_smallind_unt = temp_smallind_unt.pivot_table(
            index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
            columns='party',
            values='total_amount',
            fill_value=0
        ).reset_index()
        
        if 100.0 in temp_smallind_unt.columns:
            temp_smallind_unt = temp_smallind_unt.rename(columns={100.0: 'total_amount_dem'})
        else:
            temp_smallind_unt['total_amount_dem'] = 0
            
        if 200.0 in temp_smallind_unt.columns:
            temp_smallind_unt = temp_smallind_unt.rename(columns={200.0: 'total_amount_rep'})
        else:
            temp_smallind_unt['total_amount_rep'] = 0
        
        temp_smallind_unt['hedging'] = abs(temp_smallind_unt['total_amount_dem'] - temp_smallind_unt['total_amount_rep'])
        
        temp_smallind_unt_result = temp_smallind_unt.groupby(['district', 'cycle']).agg(
            hedging_money_general_np_smallind=('hedging', 'mean')
        ).reset_index()
        
        for idx, row in temp_smallind_unt_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict_untreated:
                agg_dict_untreated[key] = {}
            agg_dict_untreated[key]['hedging_money_general_np_smallind'] = row['hedging_money_general_np_smallind']
        
        ## Avg_count
        temp_smallind_unt_count = input_untreated[
            (input_untreated['contributor.type'] == 'I') & 
            (input_untreated['later_than_special'] != 1) &
            (input_untreated['amount'] < 200)
        ].groupby(['district', 'cycle', 'party']).agg(
            counting_hedging_smallind=('bonica.rid', 'nunique')
        ).reset_index()

        temp_smallind_unt_count_result = temp_smallind_unt_count.groupby(['district', 'cycle']).agg(
            avg_counting_hedging_np_smallind=('counting_hedging_smallind', 'mean')
        ).reset_index()
        
        for idx, row in temp_smallind_unt_count_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict_untreated:
                agg_dict_untreated[key] = {}
            agg_dict_untreated[key]['avg_counting_hedging_np_smallind'] = row['avg_counting_hedging_np_smallind']
        
        # Update result_df with calculated values for untreated rows for THIS treatment
        print(f"  Updating result_df with _gen_np_{treat_num} values for untreated rows...")
        treat_suffix = f'_{treat_num}'
        for (district, cycle), values in agg_dict_untreated.items():
            mask = (result_df['district'] == district) & (result_df['cycle'] == cycle)
            for col, val in values.items():
                col_with_suffix = f'{col}{treat_suffix}'
                if col_with_suffix in result_df.columns:
                    result_df.loc[mask, col_with_suffix] = val
    
    print("Finished adding _gen_np variables with _1, _2, _3 suffixes for OUTPUT_8_ext")
    return result_df    
    
    


#%%
# For 'gen' variables, we also create 'gen_np_spec' (general elections non-primary and special) variables, 
#  -> we keep contributions going towards special elections in treated district-cycles, 
#  -> whereas in untreated ones we select only the ones going towards general elections

# def add_gen_np_spec_variables(input_df, output_7_df, ext_df, output_name):
#     """
#     Add _gen_np_spec suffix variables to _ext datasets.
    
#     For rows where treat_3 == 1: select contributions with election.type == 'S' between death_date and special_elections_date
#     For other rows: keep election.type == 'G'
    
#     Parameters:
#     -----------
#     input_df : DataFrame
#         The raw contribution data (OUTPUT_1)
#     output_7_df : DataFrame
#         The treatment data with treat_3, death_date, special_elections_date
#     ext_df : DataFrame
#         The extended output dataset (OUTPUT_2_ext, OUTPUT_3_ext, etc.)
#     output_name : str
#         Name for the output (e.g., 'OUTPUT_2', 'OUTPUT_3', 'OUTPUT_4_1')
    
#     Returns:
#     --------
#     DataFrame
#         DataFrame with additional _gen_np_spec variables
#     """
    
#     print(f"Adding _gen_np_spec variables for {output_name}_ext...")
    
#     # Start with the existing ext_df
#     result_df = ext_df.copy()
    
#     # Determine filter type based on output_name
#     if output_name == 'OUTPUT_2':
#         filter_type = None
#         amount_filter = None
#         suffix = ''
#     elif output_name == 'OUTPUT_3':
#         filter_type = 'C'
#         amount_filter = None
#         suffix = '_corp'
#     elif output_name == 'OUTPUT_4_1':
#         filter_type = 'I'
#         amount_filter = None
#         suffix = '_ind'
#     elif output_name == 'OUTPUT_4_2':
#         filter_type = 'I'
#         amount_filter = 200
#         suffix = '_smallind'
    
#     # Apply type and amount filters to input_df
#     input_df_filtered = input_df.copy()
#     if filter_type:
#         input_df_filtered = input_df_filtered[input_df_filtered['contributor.type'] == filter_type]
#     if amount_filter:
#         input_df_filtered = input_df_filtered[input_df_filtered['amount'] < amount_filter]
    
#     # Initialize _gen_np_spec columns
#     gen_np_spec_vars = [
#         f'total_amount_gen_np_spec{suffix}',
#         f'tran_count_gen_np_spec{suffix}',
#         f'total_amount_gen_np_spec_without_LTS1{suffix}',
#         f'tran_count_gen_np_spec_without_LTS1{suffix}',
#         f'total_amount_dem_gen_np_spec{suffix}',
#         f'tran_count_dem_gen_np_spec{suffix}',
#         f'total_amount_dem_gen_np_spec_without_LTS1{suffix}',
#         f'tran_count_dem_gen_np_spec_without_LTS1{suffix}',
#         f'total_amount_rep_gen_np_spec{suffix}',
#         f'tran_count_rep_gen_np_spec{suffix}',
#         f'total_amount_rep_gen_np_spec_without_LTS1{suffix}',
#         f'tran_count_rep_gen_np_spec_without_LTS1{suffix}'
#     ]
    
#     # Initialize _gen_np_spec columns with NaN
#     for var in gen_np_spec_vars:
#         result_df[var] = np.nan
    
#     # Process treated rows (treat_3 == 1)
#     treated_districts = output_7_df[output_7_df['treat_3'] == 1][['district', 'cycle', 'death_date', 'special_elections_date']]
    
#     if not treated_districts.empty:
#         print(f"  Processing {len(treated_districts)} treated district-cycles...")
        
#         # Merge filtered input_df with treated districts
#         input_treated = pd.merge(
#             input_df_filtered.drop(columns=['death_date', 'spec_election_date'], errors='ignore'),
#             treated_districts,
#             on=['district', 'cycle'],
#             how='inner'
#         )
        
#         # Filter for contributions with election.type == 'S' between death_date and special_elections_date
#         input_treated = input_treated[
#             (input_treated['date'] > input_treated['death_date']) &
#             (input_treated['date'] < input_treated['special_elections_date']) &
#             (input_treated['election.type'] == 'S')
#         ]
        
#         # Create aggregations for treated rows
#         agg_dict = {}
        
#         # 1: All special contributions
#         agg_1 = input_treated.groupby(['district', 'cycle']).agg(
#             **{f'total_amount_gen_np_spec{suffix}': ('amount', 'sum'),
#                f'tran_count_gen_np_spec{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         agg_dict.update(agg_1.set_index(['district', 'cycle']).to_dict('index'))
        
#         # 2: Before special election
#         agg_2 = input_treated[input_treated['later_than_special'] != 1].groupby(['district', 'cycle']).agg(
#             **{f'total_amount_gen_np_spec_without_LTS1{suffix}': ('amount', 'sum'),
#                f'tran_count_gen_np_spec_without_LTS1{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         for idx, row in agg_2.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict:
#                 agg_dict[key] = {}
#             agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
#         # 3: Dem special
#         agg_3 = input_treated[input_treated['party'] == 100].groupby(['district', 'cycle']).agg(
#             **{f'total_amount_dem_gen_np_spec{suffix}': ('amount', 'sum'),
#                f'tran_count_dem_gen_np_spec{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         for idx, row in agg_3.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict:
#                 agg_dict[key] = {}
#             agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
#         # 4: Dem special, before special
#         agg_4 = input_treated[(input_treated['party'] == 100) & (input_treated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
#             **{f'total_amount_dem_gen_np_spec_without_LTS1{suffix}': ('amount', 'sum'),
#                f'tran_count_dem_gen_np_spec_without_LTS1{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         for idx, row in agg_4.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict:
#                 agg_dict[key] = {}
#             agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
#         # 5: Rep special
#         agg_5 = input_treated[input_treated['party'] == 200].groupby(['district', 'cycle']).agg(
#             **{f'total_amount_rep_gen_np_spec{suffix}': ('amount', 'sum'),
#                f'tran_count_rep_gen_np_spec{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         for idx, row in agg_5.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict:
#                 agg_dict[key] = {}
#             agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
#         # 6: Rep special, before special
#         agg_6 = input_treated[(input_treated['party'] == 200) & (input_treated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
#             **{f'total_amount_rep_gen_np_spec_without_LTS1{suffix}': ('amount', 'sum'),
#                f'tran_count_rep_gen_np_spec_without_LTS1{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         for idx, row in agg_6.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict:
#                 agg_dict[key] = {}
#             agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
#         # Update result_df with calculated values for treated rows
#         print("  Updating result_df with _gen_np_spec values for treated rows...")
#         for (district, cycle), values in agg_dict.items():
#             mask = (result_df['district'] == district) & (result_df['cycle'] == cycle)
#             for col, val in values.items():
#                 if col in result_df.columns:
#                     result_df.loc[mask, col] = val
    
#     # Process untreated rows (treat_3 == 0 or NaN) - use election.type == 'G'
#     print("  Processing untreated rows with election.type == 'G'...")
    
#     # Get all district-cycle combinations
#     all_districts = result_df[['district', 'cycle']].copy()
    
#     # Merge with treatment info
#     all_districts = pd.merge(
#         all_districts,
#         output_7_df[['district', 'cycle', 'treat_3']],
#         on=['district', 'cycle'],
#         how='left'
#     )
    
#     # Select untreated rows (treat_3 == 0 or NaN)
#     untreated_districts = all_districts[(all_districts['treat_3'] == 0) | (all_districts['treat_3'].isna())][['district', 'cycle']]
    
#     if not untreated_districts.empty:
#         # Merge with input data
#         input_untreated = pd.merge(
#             input_df_filtered,
#             untreated_districts,
#             on=['district', 'cycle'],
#             how='inner'
#         )
        
#         # Filter for general elections only
#         input_untreated = input_untreated[input_untreated['election.type'] == 'G']
        
#         # Create aggregations for untreated rows (only for general elections)
#         agg_dict_untreated = {}
        
#         # 1: All general election contributions
#         agg_gen_all = input_untreated.groupby(['district', 'cycle']).agg(
#             **{f'total_amount_gen_np_spec{suffix}': ('amount', 'sum'),
#                f'tran_count_gen_np_spec{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         agg_dict_untreated.update(agg_gen_all.set_index(['district', 'cycle']).to_dict('index'))
        
#         # 2: General elections, before special election (with LTS1 filter)
#         agg_gen_lts = input_untreated[input_untreated['later_than_special'] != 1].groupby(['district', 'cycle']).agg(
#             **{f'total_amount_gen_np_spec_without_LTS1{suffix}': ('amount', 'sum'),
#                f'tran_count_gen_np_spec_without_LTS1{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         for idx, row in agg_gen_lts.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict_untreated:
#                 agg_dict_untreated[key] = {}
#             agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
#         # Party-specific aggregations
#         agg_dem = input_untreated[input_untreated['party'] == 100].groupby(['district', 'cycle']).agg(
#             **{f'total_amount_dem_gen_np_spec{suffix}': ('amount', 'sum'),
#                f'tran_count_dem_gen_np_spec{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         for idx, row in agg_dem.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict_untreated:
#                 agg_dict_untreated[key] = {}
#             agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
#         agg_dem_lts = input_untreated[(input_untreated['party'] == 100) & (input_untreated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
#             **{f'total_amount_dem_gen_np_spec_without_LTS1{suffix}': ('amount', 'sum'),
#                f'tran_count_dem_gen_np_spec_without_LTS1{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         for idx, row in agg_dem_lts.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict_untreated:
#                 agg_dict_untreated[key] = {}
#             agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
#         agg_rep = input_untreated[input_untreated['party'] == 200].groupby(['district', 'cycle']).agg(
#             **{f'total_amount_rep_gen_np_spec{suffix}': ('amount', 'sum'),
#                f'tran_count_rep_gen_np_spec{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         for idx, row in agg_rep.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict_untreated:
#                 agg_dict_untreated[key] = {}
#             agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
#         agg_rep_lts = input_untreated[(input_untreated['party'] == 200) & (input_untreated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
#             **{f'total_amount_rep_gen_np_spec_without_LTS1{suffix}': ('amount', 'sum'),
#                f'tran_count_rep_gen_np_spec_without_LTS1{suffix}': ('transaction.id', 'count')}
#         ).reset_index()
#         for idx, row in agg_rep_lts.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict_untreated:
#                 agg_dict_untreated[key] = {}
#             agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
#         # Update result_df with calculated values for untreated rows
#         print("  Updating result_df with _gen_np_spec values for untreated rows...")
#         for (district, cycle), values in agg_dict_untreated.items():
#             mask = (result_df['district'] == district) & (result_df['cycle'] == cycle)
#             for col, val in values.items():
#                 if col in result_df.columns:
#                     result_df.loc[mask, col] = val
    
#     print(f"Finished adding _gen_np_spec variables for {output_name}_ext")
#     return result_df


# def add_gen_np_spec_variables_output8(input_df, output_7_df, output8_ext):
#     """
#     Add _gen_np_spec suffix variables to OUTPUT_8_ext dataset.
    
#     For rows where treat_3 == 1: select contributions with election.type == 'S' between death_date and special_elections_date
#     For other rows: keep election.type == 'G'
    
#     Parameters:
#     -----------
#     input_df : DataFrame
#         The raw contribution data (OUTPUT_1)
#     output_7_df : DataFrame
#         The treatment data with treat_3, death_date, special_elections_date
#     output8_ext : DataFrame
#         The OUTPUT_8_ext dataset
    
#     Returns:
#     --------
#     DataFrame
#         DataFrame with additional _gen_np_spec variables
#     """
    
#     print("Adding _gen_np_spec variables for OUTPUT_8_ext...")
    
#     # Start with the existing output8_ext
#     result_df = output8_ext.copy()
    
#     # Define the _gen_np_spec variables we want to create (only "general" variables)
#     gen_np_spec_vars = [
#         'hedging_money_general_np_spec_corp',
#         'avg_counting_hedging_np_spec_corp',
        
#         'hedging_money_general_np_spec_ind',
#         'avg_counting_hedging_np_spec_ind',
        
#         'hedging_money_general_np_spec_smallind',
#         'avg_counting_hedging_np_spec_smallind',
#     ]
    
#     # Initialize _gen_np_spec columns with NaN
#     for var in gen_np_spec_vars:
#         result_df[var] = np.nan
    
#     # Process treated rows (treat_3 == 1)
#     treated_districts = output_7_df[output_7_df['treat_3'] == 1][['district', 'cycle', 'death_date', 'special_elections_date']]
    
#     if not treated_districts.empty:
#         print(f"  Processing {len(treated_districts)} treated district-cycles...")
        
#         # Merge input_df with treated districts
#         input_treated = pd.merge(
#             input_df.drop(columns=['death_date', 'spec_election_date'], errors='ignore'),
#             treated_districts,
#             on=['district', 'cycle'],
#             how='inner'
#         )
        
#         # Filter for contributions with election.type == 'S' between death_date and special_elections_date
#         input_treated = input_treated[
#             (input_treated['date'] > input_treated['death_date']) &
#             (input_treated['date'] < input_treated['special_elections_date']) &
#             (input_treated['election.type'] == 'S')
#         ]
        
#         # Create aggregations for treated rows
#         agg_dict = {}
        
#         ### Corporate contributors
        
#         ## Hedging
#         temp_corp = input_treated[
#             (input_treated['contributor.type'] == 'C') & 
#             (input_treated['later_than_special'] != 1)
#         ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
#             total_amount=('amount', 'sum')
#         ).reset_index()
        
#         temp_corp = temp_corp.pivot_table(
#             index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
#             columns='party',
#             values='total_amount',
#             fill_value=0
#         ).reset_index()
        
#         if 100.0 in temp_corp.columns:
#             temp_corp = temp_corp.rename(columns={100.0: 'total_amount_dem'})
#         else:
#             temp_corp['total_amount_dem'] = 0
            
#         if 200.0 in temp_corp.columns:
#             temp_corp = temp_corp.rename(columns={200.0: 'total_amount_rep'})
#         else:
#             temp_corp['total_amount_rep'] = 0
        
#         temp_corp['hedging'] = abs(temp_corp['total_amount_dem'] - temp_corp['total_amount_rep'])
        
#         temp_corp_result = temp_corp.groupby(['district', 'cycle']).agg(
#             hedging_money_general_np_spec_corp=('hedging', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_corp_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict:
#                 agg_dict[key] = {}
#             agg_dict[key]['hedging_money_general_np_spec_corp'] = row['hedging_money_general_np_spec_corp']
            
           
#         ## Avg_count
#         temp_corp_count = input_treated[
#             (input_treated['contributor.type'] == 'C') & 
#             (input_treated['later_than_special'] != 1)
#         ].groupby(['district', 'cycle', 'party']).agg(
#             counting_hedging_corp=('bonica.rid', 'nunique')
#         ).reset_index()

#         temp_corp_count_result = temp_corp_count.groupby(['district', 'cycle']).agg(
#             avg_counting_hedging_np_spec_corp = ('counting_hedging_corp', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_corp_count_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict:
#                 agg_dict[key] = {}
#             agg_dict[key]['avg_counting_hedging_np_spec_corp'] = row['avg_counting_hedging_np_spec_corp']
        
        
#         ### Individual contributors
#         ## Hedging
#         temp_ind = input_treated[
#             (input_treated['contributor.type'] == 'I') & 
#             (input_treated['later_than_special'] != 1)
#         ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
#             total_amount=('amount', 'sum')
#         ).reset_index()
        
#         temp_ind = temp_ind.pivot_table(
#             index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
#             columns='party',
#             values='total_amount',
#             fill_value=0
#         ).reset_index()
        
#         if 100.0 in temp_ind.columns:
#             temp_ind = temp_ind.rename(columns={100.0: 'total_amount_dem'})
#         else:
#             temp_ind['total_amount_dem'] = 0
            
#         if 200.0 in temp_ind.columns:
#             temp_ind = temp_ind.rename(columns={200.0: 'total_amount_rep'})
#         else:
#             temp_ind['total_amount_rep'] = 0
        
#         temp_ind['hedging'] = abs(temp_ind['total_amount_dem'] - temp_ind['total_amount_rep'])
        
#         temp_ind_result = temp_ind.groupby(['district', 'cycle']).agg(
#             hedging_money_general_np_spec_ind=('hedging', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_ind_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict:
#                 agg_dict[key] = {}
#             agg_dict[key]['hedging_money_general_np_spec_ind'] = row['hedging_money_general_np_spec_ind']
        
#         ## Avg_count
#         temp_ind_count = input_treated[
#             (input_treated['contributor.type'] == 'I') & 
#             (input_treated['later_than_special'] != 1)
#         ].groupby(['district', 'cycle', 'party']).agg(
#             counting_hedging_ind=('bonica.rid', 'nunique')
#         ).reset_index()

#         temp_ind_count_result = temp_ind_count.groupby(['district', 'cycle']).agg(
#             avg_counting_hedging_np_spec_ind = ('counting_hedging_ind', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_ind_count_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict:
#                 agg_dict[key] = {}
#             agg_dict[key]['avg_counting_hedging_np_spec_ind'] = row['avg_counting_hedging_np_spec_ind']
        
        
#         ### Small individual contributors
#         ## Hedging
#         temp_smallind = input_treated[
#             (input_treated['contributor.type'] == 'I') & 
#             (input_treated['later_than_special'] != 1) &
#             (input_treated['amount'] < 200)
#         ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
#             total_amount=('amount', 'sum')
#         ).reset_index()
        
#         temp_smallind = temp_smallind.pivot_table(
#             index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
#             columns='party',
#             values='total_amount',
#             fill_value=0
#         ).reset_index()
        
#         if 100.0 in temp_smallind.columns:
#             temp_smallind = temp_smallind.rename(columns={100.0: 'total_amount_dem'})
#         else:
#             temp_smallind['total_amount_dem'] = 0
            
#         if 200.0 in temp_smallind.columns:
#             temp_smallind = temp_smallind.rename(columns={200.0: 'total_amount_rep'})
#         else:
#             temp_smallind['total_amount_rep'] = 0
        
#         temp_smallind['hedging'] = abs(temp_smallind['total_amount_dem'] - temp_smallind['total_amount_rep'])
        
#         temp_smallind_result = temp_smallind.groupby(['district', 'cycle']).agg(
#             hedging_money_general_np_spec_smallind=('hedging', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_smallind_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict:
#                 agg_dict[key] = {}
#             agg_dict[key]['hedging_money_general_np_spec_smallind'] = row['hedging_money_general_np_spec_smallind']
        
#         ## Avg_count
#         temp_smallind_count = input_treated[
#             (input_treated['contributor.type'] == 'I') & 
#             (input_treated['later_than_special'] != 1) &
#             (input_treated['amount'] < 200)
#         ].groupby(['district', 'cycle', 'party']).agg(
#             counting_hedging_smallind=('bonica.rid', 'nunique')
#         ).reset_index()

#         temp_smallind_count_result = temp_smallind_count.groupby(['district', 'cycle']).agg(
#             avg_counting_hedging_np_spec_smallind = ('counting_hedging_smallind', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_smallind_count_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict:
#                 agg_dict[key] = {}
#             agg_dict[key]['avg_counting_hedging_np_spec_smallind'] = row['avg_counting_hedging_np_spec_smallind']

        
#         # Update result_df with calculated values for treated rows
#         print("  Updating result_df with _gen_np_spec values for treated rows...")
#         for (district, cycle), values in agg_dict.items():
#             mask = (result_df['district'] == district) & (result_df['cycle'] == cycle)
#             for col, val in values.items():
#                 if col in result_df.columns:
#                     result_df.loc[mask, col] = val
    
#     # Process untreated rows (treat_3 == 0 or NaN) - use election.type == 'G'
#     print("  Processing untreated rows with election.type == 'G'...")
    
#     # Get all district-cycle combinations
#     all_districts = result_df[['district', 'cycle']].copy()
    
#     # Merge with treatment info
#     all_districts = pd.merge(
#         all_districts,
#         output_7_df[['district', 'cycle', 'treat_3']],
#         on=['district', 'cycle'],
#         how='left'
#     )
    
#     # Select untreated rows (treat_3 == 0 or NaN)
#     untreated_districts = all_districts[(all_districts['treat_3'] == 0) | (all_districts['treat_3'].isna())][['district', 'cycle']]
    
#     if not untreated_districts.empty:
#         # Merge with input data
#         input_untreated = pd.merge(
#             input_df,
#             untreated_districts,
#             on=['district', 'cycle'],
#             how='inner'
#         )
        
#         # Filter for general elections only
#         input_untreated = input_untreated[input_untreated['election.type'] == 'G']
        
#         # Create aggregations for untreated rows
#         agg_dict_untreated = {}
        
#         ### Corporate contributors - general only
        
#         ## Hedging
#         temp_corp_unt = input_untreated[
#             (input_untreated['contributor.type'] == 'C') & 
#             (input_untreated['later_than_special'] != 1)
#         ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
#             total_amount=('amount', 'sum')
#         ).reset_index()
        
#         temp_corp_unt = temp_corp_unt.pivot_table(
#             index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
#             columns='party',
#             values='total_amount',
#             fill_value=0
#         ).reset_index()
        
#         if 100.0 in temp_corp_unt.columns:
#             temp_corp_unt = temp_corp_unt.rename(columns={100.0: 'total_amount_dem'})
#         else:
#             temp_corp_unt['total_amount_dem'] = 0
            
#         if 200.0 in temp_corp_unt.columns:
#             temp_corp_unt = temp_corp_unt.rename(columns={200.0: 'total_amount_rep'})
#         else:
#             temp_corp_unt['total_amount_rep'] = 0
        
#         temp_corp_unt['hedging'] = abs(temp_corp_unt['total_amount_dem'] - temp_corp_unt['total_amount_rep'])
        
#         temp_corp_unt_result = temp_corp_unt.groupby(['district', 'cycle']).agg(
#             hedging_money_general_np_spec_corp=('hedging', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_corp_unt_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict_untreated:
#                 agg_dict_untreated[key] = {}
#             agg_dict_untreated[key]['hedging_money_general_np_spec_corp'] = row['hedging_money_general_np_spec_corp']
        
#         ## Avg_count
#         temp_corp_unt_count = input_untreated[
#             (input_untreated['contributor.type'] == 'C') & 
#             (input_untreated['later_than_special'] != 1)
#         ].groupby(['district', 'cycle', 'party']).agg(
#             counting_hedging_corp=('bonica.rid', 'nunique')
#         ).reset_index()

#         temp_corp_unt_count_result = temp_corp_unt_count.groupby(['district', 'cycle']).agg(
#             avg_counting_hedging_np_spec_corp = ('counting_hedging_corp', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_corp_unt_count_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict_untreated:
#                 agg_dict_untreated[key] = {}
#             agg_dict_untreated[key]['avg_counting_hedging_np_spec_corp'] = row['avg_counting_hedging_np_spec_corp']
        
        
#         ### Individual contributors - general only
         
#         ## Hedging
#         temp_ind_unt = input_untreated[
#             (input_untreated['contributor.type'] == 'I') & 
#             (input_untreated['later_than_special'] != 1)
#         ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
#             total_amount=('amount', 'sum')
#         ).reset_index()
        
#         temp_ind_unt = temp_ind_unt.pivot_table(
#             index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
#             columns='party',
#             values='total_amount',
#             fill_value=0
#         ).reset_index()
        
#         if 100.0 in temp_ind_unt.columns:
#             temp_ind_unt = temp_ind_unt.rename(columns={100.0: 'total_amount_dem'})
#         else:
#             temp_ind_unt['total_amount_dem'] = 0
            
#         if 200.0 in temp_ind_unt.columns:
#             temp_ind_unt = temp_ind_unt.rename(columns={200.0: 'total_amount_rep'})
#         else:
#             temp_ind_unt['total_amount_rep'] = 0
        
#         temp_ind_unt['hedging'] = abs(temp_ind_unt['total_amount_dem'] - temp_ind_unt['total_amount_rep'])
        
#         temp_ind_unt_result = temp_ind_unt.groupby(['district', 'cycle']).agg(
#             hedging_money_general_np_spec_ind=('hedging', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_ind_unt_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict_untreated:
#                 agg_dict_untreated[key] = {}
#             agg_dict_untreated[key]['hedging_money_general_np_spec_ind'] = row['hedging_money_general_np_spec_ind']
        
#         ## Avg_count
#         temp_ind_unt_count = input_untreated[
#             (input_untreated['contributor.type'] == 'I') & 
#             (input_untreated['later_than_special'] != 1)
#         ].groupby(['district', 'cycle', 'party']).agg(
#             counting_hedging_ind=('bonica.rid', 'nunique')
#         ).reset_index()

#         temp_ind_unt_count_result = temp_ind_unt_count.groupby(['district', 'cycle']).agg(
#             avg_counting_hedging_np_spec_ind = ('counting_hedging_ind', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_ind_unt_count_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict_untreated:
#                 agg_dict_untreated[key] = {}
#             agg_dict_untreated[key]['avg_counting_hedging_np_spec_ind'] = row['avg_counting_hedging_np_spec_ind']
        
        
#         ### Small individual contributors - general only
        
#         ## Hedging
#         temp_smallind_unt = input_untreated[
#             (input_untreated['contributor.type'] == 'I') & 
#             (input_untreated['later_than_special'] != 1) &
#             (input_untreated['amount'] < 200)
#         ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
#             total_amount=('amount', 'sum')
#         ).reset_index()
        
#         temp_smallind_unt = temp_smallind_unt.pivot_table(
#             index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
#             columns='party',
#             values='total_amount',
#             fill_value=0
#         ).reset_index()
        
#         if 100.0 in temp_smallind_unt.columns:
#             temp_smallind_unt = temp_smallind_unt.rename(columns={100.0: 'total_amount_dem'})
#         else:
#             temp_smallind_unt['total_amount_dem'] = 0
            
#         if 200.0 in temp_smallind_unt.columns:
#             temp_smallind_unt = temp_smallind_unt.rename(columns={200.0: 'total_amount_rep'})
#         else:
#             temp_smallind_unt['total_amount_rep'] = 0
        
#         temp_smallind_unt['hedging'] = abs(temp_smallind_unt['total_amount_dem'] - temp_smallind_unt['total_amount_rep'])
        
#         temp_smallind_unt_result = temp_smallind_unt.groupby(['district', 'cycle']).agg(
#             hedging_money_general_np_spec_smallind=('hedging', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_smallind_unt_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict_untreated:
#                 agg_dict_untreated[key] = {}
#             agg_dict_untreated[key]['hedging_money_general_np_spec_smallind'] = row['hedging_money_general_np_spec_smallind']
        
#         ## Avg_count
#         temp_smallind_unt_count = input_untreated[
#             (input_untreated['contributor.type'] == 'I') & 
#             (input_untreated['later_than_special'] != 1) &
#             (input_untreated['amount'] < 200)
#         ].groupby(['district', 'cycle', 'party']).agg(
#             counting_hedging_smallind=('bonica.rid', 'nunique')
#         ).reset_index()

#         temp_smallind_unt_count_result = temp_smallind_unt_count.groupby(['district', 'cycle']).agg(
#             avg_counting_hedging_np_spec_smallind = ('counting_hedging_smallind', 'mean'),
#         ).reset_index()
        
#         for idx, row in temp_smallind_unt_count_result.iterrows():
#             key = (row['district'], row['cycle'])
#             if key not in agg_dict_untreated:
#                 agg_dict_untreated[key] = {}
#             agg_dict_untreated[key]['avg_counting_hedging_np_spec_smallind'] = row['avg_counting_hedging_np_spec_smallind']
        
#         # Update result_df with calculated values for untreated rows
#         print("  Updating result_df with _gen_np_spec values for untreated rows...")
#         for (district, cycle), values in agg_dict_untreated.items():
#             mask = (result_df['district'] == district) & (result_df['cycle'] == cycle)
#             for col, val in values.items():
#                 if col in result_df.columns:
#                     result_df.loc[mask, col] = val
    
#     print("Finished adding _gen_np_spec variables for OUTPUT_8_ext")
#     return result_df


#%%

def add_gen_np_spec_variables(input_df, output_7_df, ext_df, output_name, single_death_districts, multiple_death_districts):
    """
    Add _gen_np_spec suffix variables with _1, _2, _3 versions to _ext datasets.
    
    For rows where treat_X == 1: select contributions with election.type == 'S' between death_date and special_elections_date
    For other rows: keep election.type == 'G'
    
    Parameters:
    -----------
    input_df : DataFrame
        The raw contribution data (OUTPUT_1)
    output_7_df : DataFrame
        The treatment data with treat_1, treat_2, treat_3, death_date, special_elections_date
    ext_df : DataFrame
        The extended output dataset (OUTPUT_2_ext, OUTPUT_3_ext, etc.)
    output_name : str
        Name for the output (e.g., 'OUTPUT_2', 'OUTPUT_3', 'OUTPUT_4_1')
    single_death_districts : list
        List of districts with single death
    multiple_death_districts : list
        List of districts with multiple deaths
    
    Returns:
    --------
    DataFrame
        DataFrame with additional _gen_np_spec_1, _gen_np_spec_2, _gen_np_spec_3 variables
    """
    
    print(f"Adding _gen_np_spec variables with _1, _2, _3 suffixes for {output_name}_ext...")
    
    # Start with the existing ext_df
    result_df = ext_df.copy()
    
    # Determine filter type based on output_name
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
    
    # Apply type and amount filters to input_df
    input_df_filtered = input_df.copy()
    if filter_type:
        input_df_filtered = input_df_filtered[input_df_filtered['contributor.type'] == filter_type]
    if amount_filter:
        input_df_filtered = input_df_filtered[input_df_filtered['amount'] < amount_filter]
    
    # Define base gen_np_spec variables
    gen_np_spec_vars_base = [
        f'total_amount_gen_np_spec{suffix}',
        f'tran_count_gen_np_spec{suffix}',
        f'total_amount_gen_np_spec_without_LTS1{suffix}',
        f'tran_count_gen_np_spec_without_LTS1{suffix}',
        f'total_amount_dem_gen_np_spec{suffix}',
        f'tran_count_dem_gen_np_spec{suffix}',
        f'total_amount_dem_gen_np_spec_without_LTS1{suffix}',
        f'tran_count_dem_gen_np_spec_without_LTS1{suffix}',
        f'total_amount_rep_gen_np_spec{suffix}',
        f'tran_count_rep_gen_np_spec{suffix}',
        f'total_amount_rep_gen_np_spec_without_LTS1{suffix}',
        f'tran_count_rep_gen_np_spec_without_LTS1{suffix}'
    ]
    
    # Initialize _1, _2, _3 versions with NaN
    for treat_num in [1, 2, 3]:
        for var in gen_np_spec_vars_base:
            result_df[f'{var}_{treat_num}'] = np.nan
    
    # Process each treatment
    for treat_num in [1, 2, 3]:
        print(f"  Processing treat_{treat_num} for gen_np_spec variables...")
        
        # Get district-cycles where treatment == 1
        treated_districts = output_7_df[output_7_df[f'treat_{treat_num}'] == 1][['district', 'cycle', 'death_date', 'special_elections_date']]
        
        if treated_districts.empty:
            print(f"  No treated districts for treat_{treat_num}")
            continue
        
        # For treat_1 with multiple death districts, only keep the first instance per district
        if treat_num == 1:
            multiple_death_treated = treated_districts[treated_districts['district'].isin(multiple_death_districts)]
            
            if not multiple_death_treated.empty:
                first_occurrences = multiple_death_treated.groupby('district')['cycle'].idxmin()
                multiple_death_treated_first = multiple_death_treated.loc[first_occurrences]
                
                single_death_treated = treated_districts[treated_districts['district'].isin(single_death_districts)]
                
                treated_districts = pd.concat([single_death_treated, multiple_death_treated_first], ignore_index=True)
        
        # Merge filtered input_df with treated districts
        input_treated = pd.merge(
            input_df_filtered.drop(columns=['death_date', 'spec_election_date'], errors='ignore'),
            treated_districts,
            on=['district', 'cycle'],
            how='inner'
        )
        
        # Filter for contributions with election.type == 'S' between death_date and special_elections_date
        input_treated = input_treated[
            (input_treated['date'] > input_treated['death_date']) &
            (input_treated['date'] < input_treated['special_elections_date']) &
            (input_treated['election.type'] == 'S')
        ]
        
        # Create aggregations for treated rows
        treat_suffix = f'_{treat_num}'
        agg_dict = {}
        
        # 1: All special contributions
        agg_1 = input_treated.groupby(['district', 'cycle']).agg(
            **{f'total_amount_gen_np_spec{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_gen_np_spec{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        agg_dict.update(agg_1.set_index(['district', 'cycle']).to_dict('index'))
        
        # 2: Before special election
        agg_2 = input_treated[input_treated['later_than_special'] != 1].groupby(['district', 'cycle']).agg(
            **{f'total_amount_gen_np_spec_without_LTS1{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_gen_np_spec_without_LTS1{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_2.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 3: Dem special
        agg_3 = input_treated[input_treated['party'] == 100].groupby(['district', 'cycle']).agg(
            **{f'total_amount_dem_gen_np_spec{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_dem_gen_np_spec{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_3.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 4: Dem special, before special
        agg_4 = input_treated[(input_treated['party'] == 100) & (input_treated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
            **{f'total_amount_dem_gen_np_spec_without_LTS1{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_dem_gen_np_spec_without_LTS1{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_4.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 5: Rep special
        agg_5 = input_treated[input_treated['party'] == 200].groupby(['district', 'cycle']).agg(
            **{f'total_amount_rep_gen_np_spec{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_rep_gen_np_spec{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_5.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # 6: Rep special, before special
        agg_6 = input_treated[(input_treated['party'] == 200) & (input_treated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
            **{f'total_amount_rep_gen_np_spec_without_LTS1{suffix}{treat_suffix}': ('amount', 'sum'),
               f'tran_count_rep_gen_np_spec_without_LTS1{suffix}{treat_suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_6.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # Update result_df with calculated values for treated rows
        print(f"  Updating result_df with _gen_np_spec{treat_suffix} values for treated rows...")
        for (district, cycle), values in agg_dict.items():
            mask = (result_df['district'] == district) & (result_df['cycle'] == cycle)
            for col, val in values.items():
                if col in result_df.columns:
                    result_df.loc[mask, col] = val
    
    # # Process untreated rows (all treatments use election.type == 'G')
    # print("  Processing untreated rows with election.type == 'G'...")
    
    # all_districts = result_df[['district', 'cycle']].copy()
    
    # # Merge with treatment info
    # all_districts = pd.merge(
    #     all_districts,
    #     output_7_df[['district', 'cycle', 'treat_3']],
    #     on=['district', 'cycle'],
    #     how='left'
    # )
    
    # # Select untreated rows (treat_3 == 0 or NaN)
    # untreated_districts = all_districts[(all_districts['treat_3'] == 0) | (all_districts['treat_3'].isna())][['district', 'cycle']]
    
    # if not untreated_districts.empty:
    #     # Merge with input data
    #     input_untreated = pd.merge(
    #         input_df_filtered,
    #         untreated_districts,
    #         on=['district', 'cycle'],
    #         how='inner'
    #     )
        
    #     # Filter for general elections only
    #     input_untreated = input_untreated[input_untreated['election.type'] == 'G']
        
    #     # Create aggregations for untreated rows (only for general elections)
    #     agg_dict_untreated = {}
        
    #     # 1: All general election contributions
    #     agg_gen_all = input_untreated.groupby(['district', 'cycle']).agg(
    #         **{f'total_amount_gen_np_spec{suffix}': ('amount', 'sum'),
    #            f'tran_count_gen_np_spec{suffix}': ('transaction.id', 'count')}
    #     ).reset_index()
    #     agg_dict_untreated.update(agg_gen_all.set_index(['district', 'cycle']).to_dict('index'))
        
    #     # 2: General elections, before special election (with LTS1 filter)
    #     agg_gen_lts = input_untreated[input_untreated['later_than_special'] != 1].groupby(['district', 'cycle']).agg(
    #         **{f'total_amount_gen_np_spec_without_LTS1{suffix}': ('amount', 'sum'),
    #            f'tran_count_gen_np_spec_without_LTS1{suffix}': ('transaction.id', 'count')}
    #     ).reset_index()
    #     for idx, row in agg_gen_lts.iterrows():
    #         key = (row['district'], row['cycle'])
    #         if key not in agg_dict_untreated:
    #             agg_dict_untreated[key] = {}
    #         agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
    #     # Party-specific aggregations
    #     agg_dem = input_untreated[input_untreated['party'] == 100].groupby(['district', 'cycle']).agg(
    #         **{f'total_amount_dem_gen_np_spec{suffix}': ('amount', 'sum'),
    #            f'tran_count_dem_gen_np_spec{suffix}': ('transaction.id', 'count')}
    #     ).reset_index()
    #     for idx, row in agg_dem.iterrows():
    #         key = (row['district'], row['cycle'])
    #         if key not in agg_dict_untreated:
    #             agg_dict_untreated[key] = {}
    #         agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
    #     agg_dem_lts = input_untreated[(input_untreated['party'] == 100) & (input_untreated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
    #         **{f'total_amount_dem_gen_np_spec_without_LTS1{suffix}': ('amount', 'sum'),
    #            f'tran_count_dem_gen_np_spec_without_LTS1{suffix}': ('transaction.id', 'count')}
    #     ).reset_index()
    #     for idx, row in agg_dem_lts.iterrows():
    #         key = (row['district'], row['cycle'])
    #         if key not in agg_dict_untreated:
    #             agg_dict_untreated[key] = {}
    #         agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
    #     agg_rep = input_untreated[input_untreated['party'] == 200].groupby(['district', 'cycle']).agg(
    #         **{f'total_amount_rep_gen_np_spec{suffix}': ('amount', 'sum'),
    #            f'tran_count_rep_gen_np_spec{suffix}': ('transaction.id', 'count')}
    #     ).reset_index()
    #     for idx, row in agg_rep.iterrows():
    #         key = (row['district'], row['cycle'])
    #         if key not in agg_dict_untreated:
    #             agg_dict_untreated[key] = {}
    #         agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
    #     agg_rep_lts = input_untreated[(input_untreated['party'] == 200) & (input_untreated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
    #         **{f'total_amount_rep_gen_np_spec_without_LTS1{suffix}': ('amount', 'sum'),
    #            f'tran_count_rep_gen_np_spec_without_LTS1{suffix}': ('transaction.id', 'count')}
    #     ).reset_index()
    #     for idx, row in agg_rep_lts.iterrows():
    #         key = (row['district'], row['cycle'])
    #         if key not in agg_dict_untreated:
    #             agg_dict_untreated[key] = {}
    #         agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
    #     # Update result_df with calculated values for untreated rows (copy to all _1, _2, _3)
    #     print("  Updating result_df with _gen_np_spec values for untreated rows...")
    #     for (district, cycle), values in agg_dict_untreated.items():
    #         mask = (result_df['district'] == district) & (result_df['cycle'] == cycle)
    #         for col, val in values.items():
    #             # Update all three treatment versions with the same untreated values
    #             for treat_num in [1, 2, 3]:
    #                 col_with_suffix = f'{col}_{treat_num}'
    #                 if col_with_suffix in result_df.columns:
    #                     result_df.loc[mask, col_with_suffix] = val
    
    # print(f"Finished adding _gen_np_spec variables with _1, _2, _3 suffixes for {output_name}_ext")
    # return result_df
    
    
    # Process untreated rows (all treatments use election.type == 'G')
    # We need to process this for EACH treatment separately since treat_1 has different treated rows than treat_2 and treat_3
    print("  Processing untreated rows with election.type == 'G'...")
    
    for treat_num in [1, 2, 3]:
        print(f"  Processing untreated rows for treat_{treat_num}...")
        
        all_districts = result_df[['district', 'cycle']].copy()
        
        # Merge with ALL treatment info at once
        all_districts = pd.merge(
            all_districts,
            output_7_df[['district', 'cycle', 'treat_1', 'treat_2', 'treat_3']],
            on=['district', 'cycle'],
            how='left'
        )
        
        if treat_num == 1:
            # For treat_1: untreated means:
            # 1. treat_1 == 0 (never treated)
            # 2. OR (treat_1 == 1 AND treat_3 == 0) - post first death but no death this cycle
            # 3. OR (district in multiple_death_districts AND treat_1 == 1 AND treat_3 == 1 AND this is NOT the first death)
            
            # Get the first death for each district (already processed in treated section)
            first_deaths = output_7_df[output_7_df['treat_1'] == 1].copy()
            if not first_deaths.empty and len(multiple_death_districts) > 0:
                first_deaths_multiple = first_deaths[first_deaths['district'].isin(multiple_death_districts)]
                if not first_deaths_multiple.empty:
                    first_death_cycles = first_deaths_multiple.groupby('district')['cycle'].min().reset_index()
                    first_death_cycles.columns = ['district', 'first_death_cycle']
                    
                    # Merge to identify which rows are first deaths
                    all_districts = pd.merge(
                        all_districts,
                        first_death_cycles,
                        on='district',
                        how='left'
                    )
                    
                    # Create a flag for multiple-death districts
                    all_districts['is_multiple_death'] = all_districts['district'].isin(multiple_death_districts)
                    
                    # Untreated for treat_1 includes:
                    # - Never treated (treat_1 == 0)
                    # - Post-treatment non-death cycles (treat_1 == 1, treat_3 == 0)
                    # - Subsequent deaths ONLY in multiple-death districts (is_multiple_death & treat_3 == 1 & cycle != first_death_cycle)
                    untreated_districts = all_districts[
                        (all_districts['treat_1'] == 0) | 
                        ((all_districts['treat_1'] == 1) & (all_districts['treat_3'] == 0)) |
                        ((all_districts['is_multiple_death'] == True) & 
                         (all_districts['treat_1'] == 1) & 
                         (all_districts['treat_3'] == 1) & 
                         (all_districts['cycle'] != all_districts['first_death_cycle']))
                    ][['district', 'cycle']]
                else:
                    # No multiple death districts, simpler logic
                    untreated_districts = all_districts[
                        (all_districts['treat_1'] == 0) | 
                        ((all_districts['treat_1'] == 1) & (all_districts['treat_3'] == 0))
                    ][['district', 'cycle']]
            else:
                # No multiple death districts, simpler logic
                untreated_districts = all_districts[
                    (all_districts['treat_1'] == 0) | 
                    ((all_districts['treat_1'] == 1) & (all_districts['treat_3'] == 0))
                ][['district', 'cycle']]
            
        else:
            # For treat_2 and treat_3: untreated means treat_3 == 0 (no death in this cycle)
            # treat_2 and treat_3 have identical logic
            untreated_districts = all_districts[
                (all_districts['treat_3'] == 0) | (all_districts['treat_3'].isna())
            ][['district', 'cycle']]
            
        if untreated_districts.empty:
            print(f"  No untreated districts for treat_{treat_num}")
            continue
        
        # Merge with input data
        input_untreated = pd.merge(
            input_df_filtered,
            untreated_districts,
            on=['district', 'cycle'],
            how='inner'
        )
        
        # Filter for general elections only
        input_untreated = input_untreated[input_untreated['election.type'] == 'G']
        
        # Create aggregations for untreated rows (only for general elections)
        agg_dict_untreated = {}
        
        # 1: All general election contributions
        agg_gen_all = input_untreated.groupby(['district', 'cycle']).agg(
            **{f'total_amount_gen_np_spec{suffix}': ('amount', 'sum'),
               f'tran_count_gen_np_spec{suffix}': ('transaction.id', 'count')}
        ).reset_index()
        agg_dict_untreated.update(agg_gen_all.set_index(['district', 'cycle']).to_dict('index'))
        
        # 2: General elections, before special election (with LTS1 filter)
        agg_gen_lts = input_untreated[input_untreated['later_than_special'] != 1].groupby(['district', 'cycle']).agg(
            **{f'total_amount_gen_np_spec_without_LTS1{suffix}': ('amount', 'sum'),
               f'tran_count_gen_np_spec_without_LTS1{suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_gen_lts.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict_untreated:
                agg_dict_untreated[key] = {}
            agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # Party-specific aggregations
        agg_dem = input_untreated[input_untreated['party'] == 100].groupby(['district', 'cycle']).agg(
            **{f'total_amount_dem_gen_np_spec{suffix}': ('amount', 'sum'),
               f'tran_count_dem_gen_np_spec{suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_dem.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict_untreated:
                agg_dict_untreated[key] = {}
            agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        agg_dem_lts = input_untreated[(input_untreated['party'] == 100) & (input_untreated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
            **{f'total_amount_dem_gen_np_spec_without_LTS1{suffix}': ('amount', 'sum'),
               f'tran_count_dem_gen_np_spec_without_LTS1{suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_dem_lts.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict_untreated:
                agg_dict_untreated[key] = {}
            agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        agg_rep = input_untreated[input_untreated['party'] == 200].groupby(['district', 'cycle']).agg(
            **{f'total_amount_rep_gen_np_spec{suffix}': ('amount', 'sum'),
               f'tran_count_rep_gen_np_spec{suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_rep.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict_untreated:
                agg_dict_untreated[key] = {}
            agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        agg_rep_lts = input_untreated[(input_untreated['party'] == 200) & (input_untreated['later_than_special'] != 1)].groupby(['district', 'cycle']).agg(
            **{f'total_amount_rep_gen_np_spec_without_LTS1{suffix}': ('amount', 'sum'),
               f'tran_count_rep_gen_np_spec_without_LTS1{suffix}': ('transaction.id', 'count')}
        ).reset_index()
        for idx, row in agg_rep_lts.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict_untreated:
                agg_dict_untreated[key] = {}
            agg_dict_untreated[key].update({k: v for k, v in row.items() if k not in ['district', 'cycle']})
        
        # Update result_df with calculated values for untreated rows for THIS treatment
        print(f"  Updating result_df with _gen_np_spec_{treat_num} values for untreated rows...")
        treat_suffix = f'_{treat_num}'
        for (district, cycle), values in agg_dict_untreated.items():
            mask = (result_df['district'] == district) & (result_df['cycle'] == cycle)
            for col, val in values.items():
                col_with_suffix = f'{col}{treat_suffix}'
                if col_with_suffix in result_df.columns:
                    result_df.loc[mask, col_with_suffix] = val

    return result_df
    print(f"Finished adding _gen_np_spec variables with _1, _2, _3 suffixes for {output_name}_ext")



def add_gen_np_spec_variables_output8(input_df, output_7_df, output8_ext, single_death_districts, multiple_death_districts):
    """
    Add _gen_np_spec suffix variables with _1, _2, _3 versions to OUTPUT_8_ext dataset.
    
    For rows where treat_X == 1: select contributions with election.type == 'S' between death_date and special_elections_date
    For other rows: keep election.type == 'G'
    
    Parameters:
    -----------
    input_df : DataFrame
        The raw contribution data (OUTPUT_1)
    output_7_df : DataFrame
        The treatment data with treat_1, treat_2, treat_3, death_date, special_elections_date
    output8_ext : DataFrame
        The OUTPUT_8_ext dataset
    single_death_districts : list
        List of districts with single death
    multiple_death_districts : list
        List of districts with multiple deaths
    
    Returns:
    --------
    DataFrame
        DataFrame with additional _gen_np_spec_1, _gen_np_spec_2, _gen_np_spec_3 variables
    """
    
    print("Adding _gen_np_spec variables with _1, _2, _3 suffixes for OUTPUT_8_ext...")
    
    # Start with the existing output8_ext
    result_df = output8_ext.copy()
    
    # Define the base _gen_np_spec variables
    gen_np_spec_vars_base = [
        'hedging_money_general_np_spec_corp',
        'avg_counting_hedging_np_spec_corp',
        'hedging_money_general_np_spec_ind',
        'avg_counting_hedging_np_spec_ind',
        'hedging_money_general_np_spec_smallind',
        'avg_counting_hedging_np_spec_smallind',
    ]
    
    # Initialize _1, _2, _3 versions with NaN
    for treat_num in [1, 2, 3]:
        for var in gen_np_spec_vars_base:
            result_df[f'{var}_{treat_num}'] = np.nan
    
    # Process each treatment
    for treat_num in [1, 2, 3]:
        print(f"  Processing treat_{treat_num} for OUTPUT_8 gen_np_spec variables...")
        
        # Get district-cycles where treatment == 1
        treated_districts = output_7_df[output_7_df[f'treat_{treat_num}'] == 1][['district', 'cycle', 'death_date', 'special_elections_date']]
        
        if treated_districts.empty:
            print(f"  No treated districts for treat_{treat_num}")
            continue
        
        # For treat_1 with multiple death districts, only keep the first instance per district
        if treat_num == 1:
            multiple_death_treated = treated_districts[treated_districts['district'].isin(multiple_death_districts)]
            
            if not multiple_death_treated.empty:
                first_occurrences = multiple_death_treated.groupby('district')['cycle'].idxmin()
                multiple_death_treated_first = multiple_death_treated.loc[first_occurrences]
                
                single_death_treated = treated_districts[treated_districts['district'].isin(single_death_districts)]
                
                treated_districts = pd.concat([single_death_treated, multiple_death_treated_first], ignore_index=True)
        
        # Merge input_df with treated districts
        input_treated = pd.merge(
            input_df.drop(columns=['death_date', 'spec_election_date'], errors='ignore'),
            treated_districts,
            on=['district', 'cycle'],
            how='inner'
        )
        
        # Filter for contributions with election.type == 'S' between death_date and special_elections_date
        input_treated = input_treated[
            (input_treated['date'] > input_treated['death_date']) &
            (input_treated['date'] < input_treated['special_elections_date']) &
            (input_treated['election.type'] == 'S')
        ]
        
        treat_suffix = f'_{treat_num}'
        agg_dict = {}
        
        ### Corporate contributors
        
        ## Hedging
        temp_corp = input_treated[
            (input_treated['contributor.type'] == 'C') & 
            (input_treated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
            total_amount=('amount', 'sum')
        ).reset_index()
        
        temp_corp = temp_corp.pivot_table(
            index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
            columns='party',
            values='total_amount',
            fill_value=0
        ).reset_index()
        
        if 100.0 in temp_corp.columns:
            temp_corp = temp_corp.rename(columns={100.0: 'total_amount_dem'})
        else:
            temp_corp['total_amount_dem'] = 0
            
        if 200.0 in temp_corp.columns:
            temp_corp = temp_corp.rename(columns={200.0: 'total_amount_rep'})
        else:
            temp_corp['total_amount_rep'] = 0
        
        temp_corp['hedging'] = abs(temp_corp['total_amount_dem'] - temp_corp['total_amount_rep'])
        
        temp_corp_result = temp_corp.groupby(['district', 'cycle']).agg(
            **{f'hedging_money_general_np_spec_corp{treat_suffix}': ('hedging', 'mean')}
        ).reset_index()
        
        for idx, row in temp_corp_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key][f'hedging_money_general_np_spec_corp{treat_suffix}'] = row[f'hedging_money_general_np_spec_corp{treat_suffix}']
            
           
        ## Avg_count
        temp_corp_count = input_treated[
            (input_treated['contributor.type'] == 'C') & 
            (input_treated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'party']).agg(
            counting_hedging_corp=('bonica.rid', 'nunique')
        ).reset_index()

        temp_corp_count_result = temp_corp_count.groupby(['district', 'cycle']).agg(
            **{f'avg_counting_hedging_np_spec_corp{treat_suffix}': ('counting_hedging_corp', 'mean')}
        ).reset_index()
        
        for idx, row in temp_corp_count_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key][f'avg_counting_hedging_np_spec_corp{treat_suffix}'] = row[f'avg_counting_hedging_np_spec_corp{treat_suffix}']
        
        
        ### Individual contributors
        ## Hedging
        temp_ind = input_treated[
            (input_treated['contributor.type'] == 'I') & 
            (input_treated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
            total_amount=('amount', 'sum')
        ).reset_index()
        
        temp_ind = temp_ind.pivot_table(
            index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
            columns='party',
            values='total_amount',
            fill_value=0
        ).reset_index()
        
        if 100.0 in temp_ind.columns:
            temp_ind = temp_ind.rename(columns={100.0: 'total_amount_dem'})
        else:
            temp_ind['total_amount_dem'] = 0
            
        if 200.0 in temp_ind.columns:
            temp_ind = temp_ind.rename(columns={200.0: 'total_amount_rep'})
        else:
            temp_ind['total_amount_rep'] = 0
        
        temp_ind['hedging'] = abs(temp_ind['total_amount_dem'] - temp_ind['total_amount_rep'])
        
        temp_ind_result = temp_ind.groupby(['district', 'cycle']).agg(
            **{f'hedging_money_general_np_spec_ind{treat_suffix}': ('hedging', 'mean')}
        ).reset_index()
        
        for idx, row in temp_ind_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key][f'hedging_money_general_np_spec_ind{treat_suffix}'] = row[f'hedging_money_general_np_spec_ind{treat_suffix}']
        
        ## Avg_count
        temp_ind_count = input_treated[
            (input_treated['contributor.type'] == 'I') & 
            (input_treated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'party']).agg(
            counting_hedging_ind=('bonica.rid', 'nunique')
        ).reset_index()

        temp_ind_count_result = temp_ind_count.groupby(['district', 'cycle']).agg(
            **{f'avg_counting_hedging_np_spec_ind{treat_suffix}': ('counting_hedging_ind', 'mean')}
        ).reset_index()
        
        for idx, row in temp_ind_count_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key][f'avg_counting_hedging_np_spec_ind{treat_suffix}'] = row[f'avg_counting_hedging_np_spec_ind{treat_suffix}']
        
        
        ### Small individual contributors
        ## Hedging
        temp_smallind = input_treated[
            (input_treated['contributor.type'] == 'I') & 
            (input_treated['later_than_special'] != 1) &
            (input_treated['amount'] < 200)
        ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
            total_amount=('amount', 'sum')
        ).reset_index()
        
        temp_smallind = temp_smallind.pivot_table(
            index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
            columns='party',
            values='total_amount',
            fill_value=0
        ).reset_index()
        
        if 100.0 in temp_smallind.columns:
            temp_smallind = temp_smallind.rename(columns={100.0: 'total_amount_dem'})
        else:
            temp_smallind['total_amount_dem'] = 0
            
        if 200.0 in temp_smallind.columns:
            temp_smallind = temp_smallind.rename(columns={200.0: 'total_amount_rep'})
        else:
            temp_smallind['total_amount_rep'] = 0
        
        temp_smallind['hedging'] = abs(temp_smallind['total_amount_dem'] - temp_smallind['total_amount_rep'])
        
        temp_smallind_result = temp_smallind.groupby(['district', 'cycle']).agg(
            **{f'hedging_money_general_np_spec_smallind{treat_suffix}': ('hedging', 'mean')}
        ).reset_index()
        
        for idx, row in temp_smallind_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key][f'hedging_money_general_np_spec_smallind{treat_suffix}'] = row[f'hedging_money_general_np_spec_smallind{treat_suffix}']
        
        ## Avg_count
        temp_smallind_count = input_treated[
            (input_treated['contributor.type'] == 'I') & 
            (input_treated['later_than_special'] != 1) &
            (input_treated['amount'] < 200)
        ].groupby(['district', 'cycle', 'party']).agg(
            counting_hedging_smallind=('bonica.rid', 'nunique')
        ).reset_index()

        temp_smallind_count_result = temp_smallind_count.groupby(['district', 'cycle']).agg(
            **{f'avg_counting_hedging_np_spec_smallind{treat_suffix}': ('counting_hedging_smallind', 'mean')}
        ).reset_index()
        
        for idx, row in temp_smallind_count_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict:
                agg_dict[key] = {}
            agg_dict[key][f'avg_counting_hedging_np_spec_smallind{treat_suffix}'] = row[f'avg_counting_hedging_np_spec_smallind{treat_suffix}']

        
        # Update result_df with calculated values for treated rows
        print(f"  Updating result_df with _gen_np_spec{treat_suffix} values for treated rows...")
        for (district, cycle), values in agg_dict.items():
            mask = (result_df['district'] == district) & (result_df['cycle'] == cycle)
            for col, val in values.items():
                if col in result_df.columns:
                    result_df.loc[mask, col] = val
    
    # # Process untreated rows (all treatments use election.type == 'G')
    # print("  Processing untreated rows with election.type == 'G'...")
    
    # all_districts = result_df[['district', 'cycle']].copy()
    
    # # Merge with treatment info
    # all_districts = pd.merge(
    #     all_districts,
    #     output_7_df[['district', 'cycle', 'treat_3']],
    #     on=['district', 'cycle'],
    #     how='left'
    # )
    
    # # Select untreated rows (treat_3 == 0 or NaN)
    # untreated_districts = all_districts[(all_districts['treat_3'] == 0) | (all_districts['treat_3'].isna())][['district', 'cycle']]
    
    # if not untreated_districts.empty:
    #     # Merge with input data
    #     input_untreated = pd.merge(
    #         input_df,
    #         untreated_districts,
    #         on=['district', 'cycle'],
    #         how='inner'
    #     )
        
    #     # Filter for general elections only
    #     input_untreated = input_untreated[input_untreated['election.type'] == 'G']
        
    #     # Create aggregations for untreated rows
    #     agg_dict_untreated = {}
        
    #     ### Corporate contributors - general only
        
    #     ## Hedging
    #     temp_corp_unt = input_untreated[
    #         (input_untreated['contributor.type'] == 'C') & 
    #         (input_untreated['later_than_special'] != 1)
    #     ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
    #         total_amount=('amount', 'sum')
    #     ).reset_index()
        
    #     temp_corp_unt = temp_corp_unt.pivot_table(
    #         index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
    #         columns='party',
    #         values='total_amount',
    #         fill_value=0
    #     ).reset_index()
        
    #     if 100.0 in temp_corp_unt.columns:
    #         temp_corp_unt = temp_corp_unt.rename(columns={100.0: 'total_amount_dem'})
    #     else:
    #         temp_corp_unt['total_amount_dem'] = 0
            
    #     if 200.0 in temp_corp_unt.columns:
    #         temp_corp_unt = temp_corp_unt.rename(columns={200.0: 'total_amount_rep'})
    #     else:
    #         temp_corp_unt['total_amount_rep'] = 0
        
    #     temp_corp_unt['hedging'] = abs(temp_corp_unt['total_amount_dem'] - temp_corp_unt['total_amount_rep'])
        
    #     temp_corp_unt_result = temp_corp_unt.groupby(['district', 'cycle']).agg(
    #         hedging_money_general_np_spec_corp=('hedging', 'mean')
    #     ).reset_index()
        
    #     for idx, row in temp_corp_unt_result.iterrows():
    #         key = (row['district'], row['cycle'])
    #         if key not in agg_dict_untreated:
    #             agg_dict_untreated[key] = {}
    #         agg_dict_untreated[key]['hedging_money_general_np_spec_corp'] = row['hedging_money_general_np_spec_corp']
        
    #     ## Avg_count
    #     temp_corp_unt_count = input_untreated[
    #         (input_untreated['contributor.type'] == 'C') & 
    #         (input_untreated['later_than_special'] != 1)
    #     ].groupby(['district', 'cycle', 'party']).agg(
    #         counting_hedging_corp=('bonica.rid', 'nunique')
    #     ).reset_index()

    #     temp_corp_unt_count_result = temp_corp_unt_count.groupby(['district', 'cycle']).agg(
    #         avg_counting_hedging_np_spec_corp=('counting_hedging_corp', 'mean')
    #     ).reset_index()
        
    #     for idx, row in temp_corp_unt_count_result.iterrows():
    #         key = (row['district'], row['cycle'])
    #         if key not in agg_dict_untreated:
    #             agg_dict_untreated[key] = {}
    #         agg_dict_untreated[key]['avg_counting_hedging_np_spec_corp'] = row['avg_counting_hedging_np_spec_corp']
        
        
    #     ### Individual contributors - general only
        
    #     ## Hedging
    #     temp_ind_unt = input_untreated[
    #         (input_untreated['contributor.type'] == 'I') & 
    #         (input_untreated['later_than_special'] != 1)
    #     ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
    #         total_amount=('amount', 'sum')
    #     ).reset_index()
        
    #     temp_ind_unt = temp_ind_unt.pivot_table(
    #         index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
    #         columns='party',
    #         values='total_amount',
    #         fill_value=0
    #     ).reset_index()
        
    #     if 100.0 in temp_ind_unt.columns:
    #         temp_ind_unt = temp_ind_unt.rename(columns={100.0: 'total_amount_dem'})
    #     else:
    #         temp_ind_unt['total_amount_dem'] = 0
            
    #     if 200.0 in temp_ind_unt.columns:
    #         temp_ind_unt = temp_ind_unt.rename(columns={200.0: 'total_amount_rep'})
    #     else:
    #         temp_ind_unt['total_amount_rep'] = 0
        
    #     temp_ind_unt['hedging'] = abs(temp_ind_unt['total_amount_dem'] - temp_ind_unt['total_amount_rep'])
        
    #     temp_ind_unt_result = temp_ind_unt.groupby(['district', 'cycle']).agg(
    #         hedging_money_general_np_spec_ind=('hedging', 'mean')
    #     ).reset_index()
        
    #     for idx, row in temp_ind_unt_result.iterrows():
    #         key = (row['district'], row['cycle'])
    #         if key not in agg_dict_untreated:
    #             agg_dict_untreated[key] = {}
    #         agg_dict_untreated[key]['hedging_money_general_np_spec_ind'] = row['hedging_money_general_np_spec_ind']
        
    #     ## Avg_count
    #     temp_ind_unt_count = input_untreated[
    #         (input_untreated['contributor.type'] == 'I') & 
    #         (input_untreated['later_than_special'] != 1)
    #     ].groupby(['district', 'cycle', 'party']).agg(
    #         counting_hedging_ind=('bonica.rid', 'nunique')
    #     ).reset_index()

    #     temp_ind_unt_count_result = temp_ind_unt_count.groupby(['district', 'cycle']).agg(
    #         avg_counting_hedging_np_spec_ind=('counting_hedging_ind', 'mean')
    #     ).reset_index()
        
    #     for idx, row in temp_ind_unt_count_result.iterrows():
    #         key = (row['district'], row['cycle'])
    #         if key not in agg_dict_untreated:
    #             agg_dict_untreated[key] = {}
    #         agg_dict_untreated[key]['avg_counting_hedging_np_spec_ind'] = row['avg_counting_hedging_np_spec_ind']
        
        
    #     ### Small individual contributors - general only
        
    #     ## Hedging
    #     temp_smallind_unt = input_untreated[
    #         (input_untreated['contributor.type'] == 'I') & 
    #         (input_untreated['later_than_special'] != 1) &
    #         (input_untreated['amount'] < 200)
    #     ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
    #         total_amount=('amount', 'sum')
    #     ).reset_index()
        
    #     temp_smallind_unt = temp_smallind_unt.pivot_table(
    #         index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
    #         columns='party',
    #         values='total_amount',
    #         fill_value=0
    #     ).reset_index()
        
    #     if 100.0 in temp_smallind_unt.columns:
    #         temp_smallind_unt = temp_smallind_unt.rename(columns={100.0: 'total_amount_dem'})
    #     else:
    #         temp_smallind_unt['total_amount_dem'] = 0
            
    #     if 200.0 in temp_smallind_unt.columns:
    #         temp_smallind_unt = temp_smallind_unt.rename(columns={200.0: 'total_amount_rep'})
    #     else:
    #         temp_smallind_unt['total_amount_rep'] = 0
        
    #     temp_smallind_unt['hedging'] = abs(temp_smallind_unt['total_amount_dem'] - temp_smallind_unt['total_amount_rep'])
        
    #     temp_smallind_unt_result = temp_smallind_unt.groupby(['district', 'cycle']).agg(
    #         hedging_money_general_np_spec_smallind=('hedging', 'mean')
    #     ).reset_index()
        
    #     for idx, row in temp_smallind_unt_result.iterrows():
    #         key = (row['district'], row['cycle'])
    #         if key not in agg_dict_untreated:
    #             agg_dict_untreated[key] = {}
    #         agg_dict_untreated[key]['hedging_money_general_np_spec_smallind'] = row['hedging_money_general_np_spec_smallind']
        
    #     ## Avg_count
    #     temp_smallind_unt_count = input_untreated[
    #         (input_untreated['contributor.type'] == 'I') & 
    #         (input_untreated['later_than_special'] != 1) &
    #         (input_untreated['amount'] < 200)
    #     ].groupby(['district', 'cycle', 'party']).agg(
    #         counting_hedging_smallind=('bonica.rid', 'nunique')
    #     ).reset_index()

    #     temp_smallind_unt_count_result = temp_smallind_unt_count.groupby(['district', 'cycle']).agg(
    #         avg_counting_hedging_np_spec_smallind=('counting_hedging_smallind', 'mean')
    #     ).reset_index()
        
    #     for idx, row in temp_smallind_unt_count_result.iterrows():
    #         key = (row['district'], row['cycle'])
    #         if key not in agg_dict_untreated:
    #             agg_dict_untreated[key] = {}
    #         agg_dict_untreated[key]['avg_counting_hedging_np_spec_smallind'] = row['avg_counting_hedging_np_spec_smallind']
        
    #     # Update result_df with calculated values for untreated rows (copy to all _1, _2, _3)
    #     print("  Updating result_df with _gen_np_spec values for untreated rows...")
    #     for (district, cycle), values in agg_dict_untreated.items():
    #         mask = (result_df['district'] == district) & (result_df['cycle'] == cycle)
    #         for col, val in values.items():
    #             # Update all three treatment versions with the same untreated values
    #             for treat_num in [1, 2, 3]:
    #                 col_with_suffix = f'{col}_{treat_num}'
    #                 if col_with_suffix in result_df.columns:
    #                     result_df.loc[mask, col_with_suffix] = val
    
    # print("Finished adding _gen_np_spec variables with _1, _2, _3 suffixes for OUTPUT_8_ext")
    # return result_df


    # Process untreated rows (all treatments use election.type == 'G')
    # We need to process this for EACH treatment separately since treat_1 has different treated rows than treat_2 and treat_3
    print("  Processing untreated rows with election.type == 'G'...")
    
    for treat_num in [1, 2, 3]:
        print(f"  Processing untreated rows for treat_{treat_num}...")
        
        all_districts = result_df[['district', 'cycle']].copy()
        
        # Merge with ALL treatment info at once
        all_districts = pd.merge(
            all_districts,
            output_7_df[['district', 'cycle', 'treat_1', 'treat_2', 'treat_3']],
            on=['district', 'cycle'],
            how='left'
        )
        
        if treat_num == 1:
            # For treat_1: untreated means:
            # 1. treat_1 == 0 (never treated)
            # 2. OR (treat_1 == 1 AND treat_3 == 0) - post first death but no death this cycle
            # 3. OR (district in multiple_death_districts AND treat_1 == 1 AND treat_3 == 1 AND this is NOT the first death)
            
            # Get the first death for each district (already processed in treated section)
            first_deaths = output_7_df[output_7_df['treat_1'] == 1].copy()
            if not first_deaths.empty and len(multiple_death_districts) > 0:
                first_deaths_multiple = first_deaths[first_deaths['district'].isin(multiple_death_districts)]
                if not first_deaths_multiple.empty:
                    first_death_cycles = first_deaths_multiple.groupby('district')['cycle'].min().reset_index()
                    first_death_cycles.columns = ['district', 'first_death_cycle']
                    
                    # Merge to identify which rows are first deaths
                    all_districts = pd.merge(
                        all_districts,
                        first_death_cycles,
                        on='district',
                        how='left'
                    )
                    
                    # Create a flag for multiple-death districts
                    all_districts['is_multiple_death'] = all_districts['district'].isin(multiple_death_districts)
                    
                    # Untreated for treat_1 includes:
                    # - Never treated (treat_1 == 0)
                    # - Post-treatment non-death cycles (treat_1 == 1, treat_3 == 0)
                    # - Subsequent deaths ONLY in multiple-death districts (is_multiple_death & treat_3 == 1 & cycle != first_death_cycle)
                    untreated_districts = all_districts[
                        (all_districts['treat_1'] == 0) | 
                        ((all_districts['treat_1'] == 1) & (all_districts['treat_3'] == 0)) |
                        ((all_districts['is_multiple_death'] == True) & 
                         (all_districts['treat_1'] == 1) & 
                         (all_districts['treat_3'] == 1) & 
                         (all_districts['cycle'] != all_districts['first_death_cycle']))
                    ][['district', 'cycle']]
                else:
                    # No multiple death districts, simpler logic
                    untreated_districts = all_districts[
                        (all_districts['treat_1'] == 0) | 
                        ((all_districts['treat_1'] == 1) & (all_districts['treat_3'] == 0))
                    ][['district', 'cycle']]
            else:
                # No multiple death districts, simpler logic
                untreated_districts = all_districts[
                    (all_districts['treat_1'] == 0) | 
                    ((all_districts['treat_1'] == 1) & (all_districts['treat_3'] == 0))
                ][['district', 'cycle']]
            
        else:
            # For treat_2 and treat_3: untreated means treat_3 == 0 (no death in this cycle)
            # treat_2 and treat_3 have identical logic
            untreated_districts = all_districts[
                (all_districts['treat_3'] == 0) | (all_districts['treat_3'].isna())
            ][['district', 'cycle']]
            
        if untreated_districts.empty:
            print(f"  No untreated districts for treat_{treat_num}")
            continue
        
        # Merge with input data
        input_untreated = pd.merge(
            input_df,
            untreated_districts,
            on=['district', 'cycle'],
            how='inner'
        )
        
        # Filter for general elections only
        input_untreated = input_untreated[input_untreated['election.type'] == 'G']
        
        # Create aggregations for untreated rows
        agg_dict_untreated = {}
        
        ### Corporate contributors - general only
        
        ## Hedging
        temp_corp_unt = input_untreated[
            (input_untreated['contributor.type'] == 'C') & 
            (input_untreated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
            total_amount=('amount', 'sum')
        ).reset_index()
        
        temp_corp_unt = temp_corp_unt.pivot_table(
            index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
            columns='party',
            values='total_amount',
            fill_value=0
        ).reset_index()
        
        if 100.0 in temp_corp_unt.columns:
            temp_corp_unt = temp_corp_unt.rename(columns={100.0: 'total_amount_dem'})
        else:
            temp_corp_unt['total_amount_dem'] = 0
            
        if 200.0 in temp_corp_unt.columns:
            temp_corp_unt = temp_corp_unt.rename(columns={200.0: 'total_amount_rep'})
        else:
            temp_corp_unt['total_amount_rep'] = 0
        
        temp_corp_unt['hedging'] = abs(temp_corp_unt['total_amount_dem'] - temp_corp_unt['total_amount_rep'])
        
        temp_corp_unt_result = temp_corp_unt.groupby(['district', 'cycle']).agg(
            hedging_money_general_np_spec_corp=('hedging', 'mean')
        ).reset_index()
        
        for idx, row in temp_corp_unt_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict_untreated:
                agg_dict_untreated[key] = {}
            agg_dict_untreated[key]['hedging_money_general_np_spec_corp'] = row['hedging_money_general_np_spec_corp']
        
        ## Avg_count
        temp_corp_unt_count = input_untreated[
            (input_untreated['contributor.type'] == 'C') & 
            (input_untreated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'party']).agg(
            counting_hedging_corp=('bonica.rid', 'nunique')
        ).reset_index()

        temp_corp_unt_count_result = temp_corp_unt_count.groupby(['district', 'cycle']).agg(
            avg_counting_hedging_np_spec_corp=('counting_hedging_corp', 'mean')
        ).reset_index()
        
        for idx, row in temp_corp_unt_count_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict_untreated:
                agg_dict_untreated[key] = {}
            agg_dict_untreated[key]['avg_counting_hedging_np_spec_corp'] = row['avg_counting_hedging_np_spec_corp']
        
        
        ### Individual contributors - general only
        
        ## Hedging
        temp_ind_unt = input_untreated[
            (input_untreated['contributor.type'] == 'I') & 
            (input_untreated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
            total_amount=('amount', 'sum')
        ).reset_index()
        
        temp_ind_unt = temp_ind_unt.pivot_table(
            index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
            columns='party',
            values='total_amount',
            fill_value=0
        ).reset_index()
        
        if 100.0 in temp_ind_unt.columns:
            temp_ind_unt = temp_ind_unt.rename(columns={100.0: 'total_amount_dem'})
        else:
            temp_ind_unt['total_amount_dem'] = 0
            
        if 200.0 in temp_ind_unt.columns:
            temp_ind_unt = temp_ind_unt.rename(columns={200.0: 'total_amount_rep'})
        else:
            temp_ind_unt['total_amount_rep'] = 0
        
        temp_ind_unt['hedging'] = abs(temp_ind_unt['total_amount_dem'] - temp_ind_unt['total_amount_rep'])
        
        temp_ind_unt_result = temp_ind_unt.groupby(['district', 'cycle']).agg(
            hedging_money_general_np_spec_ind=('hedging', 'mean')
        ).reset_index()
        
        for idx, row in temp_ind_unt_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict_untreated:
                agg_dict_untreated[key] = {}
            agg_dict_untreated[key]['hedging_money_general_np_spec_ind'] = row['hedging_money_general_np_spec_ind']
        
        ## Avg_count
        temp_ind_unt_count = input_untreated[
            (input_untreated['contributor.type'] == 'I') & 
            (input_untreated['later_than_special'] != 1)
        ].groupby(['district', 'cycle', 'party']).agg(
            counting_hedging_ind=('bonica.rid', 'nunique')
        ).reset_index()

        temp_ind_unt_count_result = temp_ind_unt_count.groupby(['district', 'cycle']).agg(
            avg_counting_hedging_np_spec_ind=('counting_hedging_ind', 'mean')
        ).reset_index()
        
        for idx, row in temp_ind_unt_count_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict_untreated:
                agg_dict_untreated[key] = {}
            agg_dict_untreated[key]['avg_counting_hedging_np_spec_ind'] = row['avg_counting_hedging_np_spec_ind']
        
        
        ### Small individual contributors - general only
        
        ## Hedging
        temp_smallind_unt = input_untreated[
            (input_untreated['contributor.type'] == 'I') & 
            (input_untreated['later_than_special'] != 1) &
            (input_untreated['amount'] < 200)
        ].groupby(['district', 'cycle', 'bonica.cid', 'contributor.name', 'party']).agg(
            total_amount=('amount', 'sum')
        ).reset_index()
        
        temp_smallind_unt = temp_smallind_unt.pivot_table(
            index=['district', 'cycle', 'bonica.cid', 'contributor.name'],
            columns='party',
            values='total_amount',
            fill_value=0
        ).reset_index()
        
        if 100.0 in temp_smallind_unt.columns:
            temp_smallind_unt = temp_smallind_unt.rename(columns={100.0: 'total_amount_dem'})
        else:
            temp_smallind_unt['total_amount_dem'] = 0
            
        if 200.0 in temp_smallind_unt.columns:
            temp_smallind_unt = temp_smallind_unt.rename(columns={200.0: 'total_amount_rep'})
        else:
            temp_smallind_unt['total_amount_rep'] = 0
        
        temp_smallind_unt['hedging'] = abs(temp_smallind_unt['total_amount_dem'] - temp_smallind_unt['total_amount_rep'])
        
        temp_smallind_unt_result = temp_smallind_unt.groupby(['district', 'cycle']).agg(
            hedging_money_general_np_spec_smallind=('hedging', 'mean')
        ).reset_index()
        
        for idx, row in temp_smallind_unt_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict_untreated:
                agg_dict_untreated[key] = {}
            agg_dict_untreated[key]['hedging_money_general_np_spec_smallind'] = row['hedging_money_general_np_spec_smallind']
        
        ## Avg_count
        temp_smallind_unt_count = input_untreated[
            (input_untreated['contributor.type'] == 'I') & 
            (input_untreated['later_than_special'] != 1) &
            (input_untreated['amount'] < 200)
        ].groupby(['district', 'cycle', 'party']).agg(
            counting_hedging_smallind=('bonica.rid', 'nunique')
        ).reset_index()

        temp_smallind_unt_count_result = temp_smallind_unt_count.groupby(['district', 'cycle']).agg(
            avg_counting_hedging_np_spec_smallind=('counting_hedging_smallind', 'mean')
        ).reset_index()
        
        for idx, row in temp_smallind_unt_count_result.iterrows():
            key = (row['district'], row['cycle'])
            if key not in agg_dict_untreated:
                agg_dict_untreated[key] = {}
            agg_dict_untreated[key]['avg_counting_hedging_np_spec_smallind'] = row['avg_counting_hedging_np_spec_smallind']
        
        # Update result_df with calculated values for untreated rows for THIS treatment
        print(f"  Updating result_df with _gen_np_spec_{treat_num} values for untreated rows...")
        treat_suffix = f'_{treat_num}'
        for (district, cycle), values in agg_dict_untreated.items():
            mask = (result_df['district'] == district) & (result_df['cycle'] == cycle)
            for col, val in values.items():
                col_with_suffix = f'{col}{treat_suffix}'
                if col_with_suffix in result_df.columns:
                    result_df.loc[mask, col_with_suffix] = val
    
    print("Finished adding _gen_np_spec variables with _1, _2, _3 suffixes for OUTPUT_8_ext")
    return result_df














