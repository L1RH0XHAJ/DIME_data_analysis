#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 18 16:53:37 2025

@author: lirhoxhaj
"""

## PURPOSE OF FILE: Converting HTML code of Wikipedia page that has special elections date to a csv format, and cleaning the data properly

#%%

### LIBRARIES

import os
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import re
from datetime import datetime


#%%

### SETUP

# These lines will get the location of this file '\code\main.py'. Please ensure file is saved in folder \code. 

# This line does not work in interactive environment (e.g., Jupyter Notebook or interpreters like IDLE)
# code_folder = os.path.dirname(os.path.abspath(__file__))

# Get the directory where the current script is located
# code_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

# If all fails define working folder manually and run the lines here:
code_folder = r"C:\Users\lhoxhaj\OneDrive - Imperial College London\Desktop\RA\Tommaso\Contributions_Paper\working_folder_lir\code"
code_folder = "/Users/lirhoxhaj/Library/CloudStorage/OneDrive-ImperialCollegeLondon/Desktop/RA/Tommaso/Contributions_Paper/working_folder_lir/code"

# This is your working folder where folders '\code' and '\data' are saved
parent_folder = os.path.dirname(code_folder)

data_folder = os.path.join(parent_folder, "data")

print("Parent folder:", parent_folder, "\n")
print("Code folder:", code_folder, "\n")
print("Data folder:", data_folder, "\n")

#%%

### FUNCTIONALITY

## Extracting data from Wikipedia webpage

def extract_districts_with_years(html_file):
    try:
        with open(html_file, "r", encoding="utf-8") as file:
            soup = BeautifulSoup(file, "html.parser")
    except Exception as e:
        print(f"Error reading file: {e}")
        return None
    
    main_content = soup.find("div", {"id": "bodyContent"})
    if not main_content:
        print("Warning: Main content area not found.")
        return None
    
    data = []
    current_state = None
    # Retrieving data for each state (<h2> heading in HTML)
    for tag in main_content.find_all(["h2", "ul"]):
        if tag.name == "h2" and tag.has_attr("id"):
            current_state = tag["id"].replace("_", " ").strip()
        elif tag.name == "ul":
            for li in tag.find_all("li"):
                text = li.get_text(strip=True)
                # Check for either numbered districts or At-large districts
                if (("district" in text.lower() or "at-large" in text.lower()) and 
                    re.search(r"\b(?:17|18|19|20)\d{2}\b", text)):
                    
                    # Match either numbered district or At-large
                    district_match = re.search(r"([0-9]+(?:st|nd|rd|th) district|At-large)", text, re.IGNORECASE)
                    year_matches = re.findall(r"\b(?:17|18|19|20)\d{2}\b", text)
                    
                    if district_match:
                        # See if there is a year that is after 1980
                        years_int = list(map(int, year_matches))
                        year_after_1980 = int(any(y > 1980 for y in years_int))
                        
                        # See if this district was discontinued, or assign special
                        discontinued_after_1980 = 0
                        if "present" in text.lower():
                            # If text contains "present", district is still active, so set to 0
                            discontinued_after_1980 = 0
                        elif year_after_1980 and len(years_int) >= 2:
                            if years_int[-1] > 1980 and years_int[-2] > 1980:
                                discontinued_after_1980 = "Special case"
                            elif years_int[-1] > 1980 and years_int[-2] <= 1980:
                                discontinued_after_1980 = 1
                                
                        # See if district was created after created_after_1980
                        created_after_1980 = 0
                        if year_after_1980 and discontinued_after_1980 == 0:
                            created_after_1980 = 1
                        
                        data.append({
                            "state": current_state,
                            "district": district_match.group(1),
                            "years": ", ".join(year_matches),
                            "full_text": text,
                            "year_after_1980": year_after_1980,
                            "discontinued_after_1980": discontinued_after_1980,
                            "created_after_1980": created_after_1980
                        })
    
    # Error display
    if not data:
        print("Warning: No district data found in HTML.")
        return None
    
    return pd.DataFrame(data)


# Note: HTML file was copied by source of Wiki page here: https://en.wikipedia.org/wiki/List_of_United_States_congressional_districts
html_file = os.path.join(data_folder, "new_districts.html")
df = extract_districts_with_years(html_file)

# Dropping another heading that is getting picked because of the 'At-large' condition
df = df[df['state'] != 'Extremes']

print("Value counts for year_after_1980")
print(df['year_after_1980'].value_counts())
print()
print("Value counts for discontinued_after_1980 != 0")
print(df[df['discontinued_after_1980'] != 0]['discontinued_after_1980'].value_counts())
print()
print("Value counts for created_after_1980")
print(df[df['created_after_1980'] == 1]['created_after_1980'].value_counts())


#%%

## Manually changing values

# List of US states and their codes (retrieved from Internet)
us_states = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY"
}

us_states_list = []
nr = 0
for state in us_states.keys():
    # nr += 1
    # print(nr, state)
    us_states_list.append(state)

# Checking retrieved states
us_states_retrieved = []
nr = 0
for state in df['state'].unique():
    # nr += 1
    # print(nr, state)
    us_states_retrieved.append(state)


missing_states = set(us_states_list) - set(us_states_retrieved)
print("Count of all US states:", len(us_states_list))
print("Count of retrieved US states:", len(us_states_retrieved))
print("States missing from the retrieved HTML data", missing_states)

# Manually checked states, no creation of new districts or discontinuation of old ones!


# Creating state code variable
df['code'] = np.nan
for state, code in us_states.items():
    df.loc[df['state'] == state, 'code'] = code

# Creating district_number variable

print("Unique values for var district:\n", df['district'].unique()) # consistent format

df['district_number'] = np.nan
# Dealing with 'At-large'
df['at-large_dummy'] = 0
df.loc[df['district'] == 'At-large', 'at-large_dummy'] = 1
district_numbers = df['district'].str.replace('At-large', '1', regex=False)
# Dealing with the rest
district_numbers = district_numbers.str.extract(r'(\d+)').astype(int) # get number
df['district_number'] = district_numbers[0].apply(lambda x: f"{x:02d}") # two-digit string

# Overwriting for final district code
df['district'] = df['code'] + df['district_number'].astype(str)

# NOTE: For discontinued years, the Wikipedia page records the year when the district was discontinued from Congress, not when it held its last elections
#       For the sake of our research, we retain only the year when the district became 'obsolete' per the Wikipedia description, meaning no more elections were held.
df['obsolete'] = df['full_text'].str.contains('obsolete', case=False).astype(int)
df.loc[df['obsolete'] == 1, 'obsolete_year'] = df.loc[df['obsolete'] == 1, 'full_text'].str.extract(r'\(.*?(\d{4}).*?\)', expand=False)

df.to_csv(os.path.join(data_folder, "new_districts.csv"), index = False)

#%%

# Filtering data for merge with the outputs later

df_2 = df[df['year_after_1980'] == 1][
    ['district', 'years', 'full_text', 'year_after_1980','discontinued_after_1980', 'created_after_1980', 'at-large_dummy', 'obsolete_year']
    ]

df_2['discontinued_year'] = np.nan
df_2['created_year'] = np.nan
for idx in df_2.index:
    # Condition for discountinued districts
    if df_2.loc[idx, 'discontinued_after_1980'] == 1:
        # If there is an obsolete_year already, we write this as our value for discontinued year
        if not pd.isna(df_2.loc[idx, 'obsolete_year']):
            df_2.loc[idx, 'discontinued_year'] = df_2.loc[idx, 'obsolete_year']
        # Else, we get the last value from the list of years in the 'years' variable
        else:
            years_value = df_2.loc[idx, 'years']
            if isinstance(years_value, str):
                years_list = [year.strip() for year in years_value.split(',')]
                if years_list:  # Check if the list is not empty
                    last_year = years_list[-1]
                    df_2.loc[idx, 'discontinued_year'] = last_year
    # Dealing with special cases
    elif df_2.loc[idx, 'discontinued_after_1980'] == 'Special case':
        # Repeating same logic for obsolete
        if not pd.isna(df_2.loc[idx, 'obsolete_year']):
            df_2.loc[idx, 'discontinued_year'] = df_2.loc[idx, 'obsolete_year']
        else:
            years_value = df_2.loc[idx, 'years']
            if isinstance(years_value, str):
                years_list = [year.strip() for year in years_value.split(',')]
                if years_list:  # Check if the list is not empty
                    last_year = years_list[-1]
                    year_prior_last_year = years_list[-2]
                    df_2.loc[idx, 'discontinued_year'] = last_year
                    df_2.loc[idx, 'created_year'] = year_prior_last_year
    # Condition for created districts
    elif df_2.loc[idx, 'created_after_1980'] == 1:
        years_value = df_2.loc[idx, 'years']
        if isinstance(years_value, str):
            years_list = [year.strip() for year in years_value.split(',')]
            if years_list:  # Check if the list is not empty
                last_year = years_list[-1]
                df_2.loc[idx, 'created_year'] = last_year
    # Error
    else:
        print("Warning: Row is unique, it neither discontinued_after_1980 == 1 nor created_after_1980 == 1 nor is it a special case!")                

df_2['discontinued_year'] = pd.to_numeric(df_2['discontinued_year'], errors='coerce')
df_2['created_year'] = pd.to_numeric(df_2['created_year'], errors='coerce')
# We lag created_year by -1 to match actual data from Bonica's DIME to the Wikipedia data
df_2['created_year'] = df_2['created_year'] - 1


# There are some districts that have changed between at-large and 01 after 1980, this is recorded as having two 01 in the data (e.g. MT01)
duplicate_districts = df_2[(df_2.duplicated(subset=['district'], keep=False)) & 
                           (df_2['full_text'].str.contains('present', case=False))]['district'].unique()
rows_to_drop = df_2['district'].isin(duplicate_districts) 
# Drop the identified rows
df_2 = df_2[~rows_to_drop]


#%%

# Saving data
df_2 = df_2[['district', 'created_year', 'discontinued_year']]
df_2.to_csv(os.path.join(data_folder, "new_districts_filtered.csv"), index = False)








