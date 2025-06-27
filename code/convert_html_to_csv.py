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
code_folder = "/Users/lirhoxhaj/Library/CloudStorage/OneDrive-ImperialCollegeLondon/Desktop/RA/Tommaso/Contributions_Paper/working_folder_lir/code"
code_folder = r"C:\Users\lhoxhaj\OneDrive - Imperial College London\Desktop\RA\Tommaso\Contributions_Paper\working_folder_lir\code"

# This is your working folder where folders '\code' and '\data' are saved
parent_folder = os.path.dirname(code_folder)

data_folder = os.path.join(parent_folder, "data")

print("Parent folder:", parent_folder, "\n")
print("Code folder:", code_folder, "\n")
print("Data folder:", data_folder, "\n")

#%%

### FUNCTIONALITY

## Extracting data from Wikipedia webpage

def extract_table_to_df(html_file):
    # Load the HTML file
    with open(html_file, "r", encoding="utf-8") as file:
        soup = BeautifulSoup(file, "html.parser")

    # Find all tables in the document
    tables = soup.find_all("table", {"class": "wikitable"})

    if not tables:
        print("No tables found in the HTML file.")
        return None

    # Extract relevant table (assuming it's the first one)
    table = tables[0]

    # Extract headers
    headers = [header.text.strip() for header in table.find_all("th")]

    # Extract rows
    rows = []
    for row in table.find_all("tr")[1:]:  # Skip header row
        cols = row.find_all("td")
        row_data = [col.text.strip() for col in cols]
        if row_data:
            rows.append(row_data)

    # Create DataFrame
    df = pd.DataFrame(rows, columns=headers)
    
    return df

# Note: HTML file was copied by source of Wiki page here: https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives
html_file = os.path.join(data_folder, "special_elections.html")
df = extract_table_to_df(html_file)

df = df.rename(columns={'Date[b](linked to election article)':'Date',
                        'Con­gress[a]':'Congress'})

# Converting string Date variable to numerical year var
df["year"] = df["Date"].apply(lambda x: re.search(r'\d{4}', str(x)).group(0) if isinstance(x, str) and re.search(r'\d{4}', x) else None)
df['year'] = pd.to_numeric(df['year'], errors='coerce')  # will convert None values to NaN
# Selecting only years we care about
df = df.iloc[761:]

# Dropping missing value of year (two elections in Hawaii 2003/03, only need last date)
df = df.dropna(subset=['year'])

# Removing 2025 values since no winner determined
df = df[df['year'] != 2025]

# df = df[~df['Cause of vacancy'].str.contains('resign', case=False, na=False)]
# df = df[~df['Cause of vacancy'].str.contains('expel', case=False, na=False)]
# df = df[~df['Cause of vacancy'].str.contains('annulled', case=False, na=False)]

#%%

## Manually replacing values

# Adding some data
df.loc[df['Original'] == "Patsy Mink (D)", 'Date'] = "January 4, 2003"
df.loc[df['Original'] == "Don Young (R)", 'District'] = "AK 1"

# Fixing some rows
df.loc[df['Cause of vacancy'] == "Julian Dixon (D) died December 8, 2000, during the previous Congress", 'Original'] = "Julian Dixon (D)"
df.loc[df['Cause of vacancy'] == "Representative-elect Jack Swigert (R) died December 27, 1982", 'Original'] = "Jack Swigert (D)"
df.loc[df['Cause of vacancy'] == "Bob Matsui (D) died January 1, 2005, before the end of previous Congress", 'Original'] = "Bob Matsui (D)"
df.loc[df['Cause of vacancy'] == "Representative-elect Luke Letlow (R) died December 29, 2020", 'Original'] = "Luke Letlow (R)"
df.loc[df['Cause of vacancy'] == "Donald McEachin (D) died November 28, 2022, during the previous Congress", 'Original'] = "Donald McEachin (D)"
df.loc[df['Cause of vacancy'] == "Newt Gingrich (R) resigned January 3, 1999, at the end of the previous Congress", 'Original'] = "Newt Gingrich (R)"
df.loc[df['Cause of vacancy'] == "Jesse Jackson Jr. (D) resigned November 21, 2012, during the previous Congress[9]", 'Original'] = "Jesse Jackson Jr. (D)"
df.loc[df['Cause of vacancy'] == "Tim Scott (R) resigned January 2, 2013, before the end of the previous Congress", 'Original'] = "Tim Scott (R)"
df.loc[df['Cause of vacancy'] == "Results of 2018 election were annulled; seat was declared vacant[11][12]", 'Original'] = "Robert Pittenger (R)" # information retrieved from Wiki page and internet

# Changing names
df.loc[df['Original'] == "Bill Nichols (D)", 'Original'] = "William F. Nichols"

# Dealing with districts
df['district_1'] = df['District'].str.extract('([A-Za-z]+)')
df['district_2'] = df['District'].str[-2:]
df['district_2'] = pd.to_numeric(df['district_2'], errors='coerce')  # will convert None values to NaN
df['district_2'] = df['district_2'].apply(lambda x: f'{int(x):02d}' if pd.notnull(x) else x)

df['district_new'] = df['district_1'] + df['district_2'].astype(str)

# Renaming again (we use this later)
df = df.drop(columns=['District', 'district_1', 'district_2'], axis = 1)
df = df.rename(columns = {"district_new": "district"})
# df.to_csv(os.path.join(data_folder, "election_dates_special.csv"), index = False)



#%%

## Cleaning and adding data to already collected deaths

deaths = pd.read_csv(data_folder + "/Deaths.csv", encoding='latin-1')
deaths = deaths.rename(columns = {"death_district":"district"})

deaths['death_date'] = pd.to_datetime(deaths['death_date'])
deaths['death_year'] = deaths['death_date'].dt.year
deaths['year'] = deaths['death_year'] # this is for special elections merging later!

# Manually changing district values to correct one (based on internet searchers and Wiki page for special elections)
deaths.loc[deaths['death_member'] == "John Duncan Sr.", 'district'] = "TN02"
deaths.loc[deaths['death_member'] == "Larkin I. Smith", 'district'] = "MS05"
deaths.loc[deaths['death_member'] == "Stewart McKinney", 'district'] = "CT04"
deaths.loc[deaths['death_member'] == "William R. Cotter", 'district'] = "CT01"
deaths.loc[deaths['death_member'] == "Bill Emerson", 'district'] = "MO08"
deaths.loc[deaths['death_member'] == "Alan Nunnelee", 'district'] = "MS01"
deaths.loc[deaths['death_member'] == "Jim Hagedorn", 'district'] = "MN01"
deaths.loc[deaths['death_member'] == "Elijah Cummings", 'district'] = "MD07"


# Manually changing special election year values
deaths.loc[deaths['death_member'] == "Clement J. Zablocki", 'year'] = "1984"
deaths.loc[deaths['death_member'] == "Walter Capps", 'year'] = "1998"
deaths.loc[deaths['death_member'] == "William R. Cotter", 'year'] = "1982"
deaths.loc[deaths['death_member'] == "Julia Carson", 'year'] = "2008"
deaths.loc[deaths['death_member'] == "Bill Young", 'year'] = "2014"
deaths.loc[deaths['death_member'] == "Elijah Cummings", 'year'] = "2020"
deaths.loc[deaths['death_member'] == "Alcee Hastings", 'year'] = "2022"
deaths.loc[deaths['death_member'] == "Julian Dixon", 'year'] = "2001"
deaths.loc[deaths['death_member'] == "Donald McEachin", 'year'] = "2023"
deaths.loc[deaths['death_member'] == "William F. Nichols", 'year'] = "1989"


deaths['year'] = pd.to_numeric(deaths['year'], errors='coerce') # will convert None values to NaN


# Merging deaths and election_dates_special, and understanding what is going on

deaths_merged = pd.merge(
    deaths, 
    df, 
    how='outer', 
    on=['year', 'district'])

# Note: deaths data has some inconsistencies (John Duncan Sr. was rep in Tennessee TN02, not TX02 which would be Texas!), 
#     or because the years of death and the year of special election are different (e.g. Clement Zablocki)

# For NAN values of variable death_member, make values of death_member equal to Original for first name and last name, and then attach the letter in parantheses to the variable death_party_member

mask = deaths_merged['death_member'].isna()
deaths_merged.loc[mask, 'death_member'] = deaths_merged.loc[mask, 'Original'].str.extract(r'([A-Za-z]+ [A-Za-z]+)')[0]
deaths_merged.loc[mask, 'death_party_member'] = deaths_merged.loc[mask, 'Original'].str.extract(r'\(([A-Za-z])\)')[0]


# Adding special election dates for deaths in deaths.csv but not in Wiki
deaths_merged.loc[deaths_merged['death_member'] == "Harold L. Runnels", 'Date'] = "November 4, 1980"
deaths_merged.loc[deaths_merged['death_member'] == "George M. O'Brien", 'Date'] = "December 16, 1986"
deaths_merged.loc[deaths_merged['death_member'] == "John E. Grotberg", 'Date'] = "November 15, 1986"
deaths_merged.loc[deaths_merged['death_member'] == "William F. Nichols", 'Date'] = "April 4, 1989"
deaths_merged.loc[deaths_merged['death_member'] == "Dean Gallo", 'Date'] = "November 8, 1994"
deaths_merged.loc[deaths_merged['death_member'] == "Bruce Vento", 'Date'] = "November 7, 2000"
deaths_merged.loc[deaths_merged['death_member'] == "Herb Bateman", 'Date'] = "November 7, 2000"
deaths_merged.loc[deaths_merged['death_member'] == "Nathan Deal", 'Date'] = "June 8, 2010"

# Adding death dates (not in Wiki)
deaths_merged.loc[deaths_merged['death_member'] == "Gladys Spellman", 'death_date'] = "February 24, 1981"
deaths_merged.loc[deaths_merged['death_member'] == "Jack Swigert", 'death_date'] = "December 27, 1982"
deaths_merged.loc[deaths_merged['death_member'] == "Luke Letlow", 'death_date'] = "December 29, 2020"
deaths_merged.loc[deaths_merged['death_member'] == "Donald Payne", 'death_date'] = "April 24, 2024"
deaths_merged.loc[deaths_merged['death_member'] == "Sheila Jackson", 'death_date'] = "July 19, 2024"

# Changing districts
# NOTE: this is being manually changed, Wyoming has one district, sometimes written as WY-AL, in the contributions data known as WY01
#       These codes were found using the recipients data
deaths_merged.loc[deaths_merged['death_member'] == "Dick Cheney", 'district'] = "WY01"
deaths_merged.loc[deaths_merged['death_member'] == "Bill Janklow", 'district'] = "SD01"
deaths_merged.loc[deaths_merged['death_member'] == "Ryan Zinke", 'district'] = "MT01"
deaths_merged.loc[deaths_merged['death_member'] == "Filemon Vela", 'district'] = "TX34"

# Adding other death / resignation dates and districts not captured in Deaths.xlsx
death_dates = {
    "Leo Ryan": {"death_date": "1978-11-18", "original_district": "CA11"},
    "William A. Steiger": {"death_date": "1978-12-04", "original_district": "WI06"},
    "Harold L. Runnels": {"death_date": "1980-08-05", "original_district": "NM02"},
    "John M. Slack Jr.": {"death_date": "1980-03-17", "original_district": "WV03"},
    "Gladys Spellman": {"death_date": "1988-06-19", "original_district": "MD05"},
    "Tennyson Guyer": {"death_date": "1981-04-12", "original_district": "OH04"},
    "William R. Cotter": {"death_date": "1981-09-08", "original_district": "CT01"},
    "Adam Benjamin Jr.": {"death_date": "1982-09-07", "original_district": "IN01"},
    "John M. Ashbrook": {"death_date": "1982-04-24", "original_district": "OH17"},
    "Phillip Burton": {"death_date": "1983-04-10", "original_district": "CA05"},
    "Benjamin S. Rosenthal": {"death_date": "1983-01-04", "original_district": "NY07"},
    "Carl D. Perkins": {"death_date": "1984-08-03", "original_district": "KY07"},
    "Clement J. Zablocki": {"death_date": "1983-12-03", "original_district": "WI04"},
    "Gillis W. Long": {"death_date": "1985-01-20", "original_district": "LA08"},
    "George M. O'Brien": {"death_date": "1986-07-17", "original_district": "IL04"},
    "John E. Grotberg": {"death_date": "1986-11-15", "original_district": "IL14"},
    "Joseph P. Addabbo": {"death_date": "1986-04-10", "original_district": "NY06"},
    "Sala Burton": {"death_date": "1987-02-01", "original_district": "CA05"},
    "Stewart McKinney": {"death_date": "1987-05-07", "original_district": "CT04"},
    "C. Melvin Price": {"death_date": "1988-04-22", "original_district": "IL21"},
    "John Duncan Sr.": {"death_date": "1988-06-21", "original_district": "TN02"},
    "Dan Daniel": {"death_date": "1988-01-23", "original_district": "VA05"},
    "William F. Nichols": {"death_date": "1988-12-13", "original_district": "AL03"},
    "Claude Pepper": {"death_date": "1989-05-30", "original_district": "FL18"},
    "Larkin I. Smith": {"death_date": "1989-08-13", "original_district": "MS05"},
    "Mickey Leland": {"death_date": "1989-08-07", "original_district": "TX18"},
    "Silvio Conte": {"death_date": "1991-02-08", "original_district": "MA01"},
    "Walter B. Jones Sr.": {"death_date": "1992-09-15", "original_district": "NC01"},
    "Theodore S. Weiss": {"death_date": "1992-09-14", "original_district": "NY17"},
    "Dean Gallo": {"death_date": "1994-11-06", "original_district": "NJ11"},
    "Frank Tejeda": {"death_date": "1997-01-30", "original_district": "TX28"},
    "Steven Schiff": {"death_date": "1998-03-25", "original_district": "NM01"},
    "George Brown Jr.": {"death_date": "1999-07-15", "original_district": "CA42"},
    "Bruce Vento": {"death_date": "2000-10-10", "original_district": "MN04"},
    "Herb Bateman": {"death_date": "2000-09-11", "original_district": "VA01"},
    "Nathan Deal": {"death_date": "2010-03-21", "original_district": "GA09"}
}   
    
for name, data in death_dates.items():
    mask = deaths_merged['death_member'].str.contains(name, case=False, na=False)
    deaths_merged.loc[mask, 'death_date'] = pd.to_datetime(data['death_date'])
    deaths_merged.loc[mask, 'original_district'] = data['original_district']




# # Manually adding resignation dates (don't need all of them for our analysis later)
# # First, define a function to extract dates from text
# def extract_date(text):
#     if pd.isna(text):
#         return pd.NaT
    
#     # Pattern to match dates like "April 24, 2024"
#     pattern = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}'
    
#     match = re.search(pattern, text)
#     if match:
#         date_str = match.group(0)
#         try:
#             return pd.to_datetime(date_str)
#         except:
#             return pd.NaT
#     return pd.NaT

# # Apply the function to extract dates
# deaths_merged.loc[:, 'death_date'] = deaths_merged['Cause of vacancy'].apply(extract_date)

# Manually replacing the rest


# Creating new cause of vacancy instead of long description
deaths_merged['cause_vacancy'] = pd.NA
deaths_merged.loc[deaths_merged['Cause of vacancy'].str.contains('resign|expell', case=False, na=False), 'cause_vacancy'] = 'Resigned'
deaths_merged.loc[deaths_merged['Cause of vacancy'].str.contains('died|death|deceased|coma', case=False, na=False), 'cause_vacancy'] = 'Death'
deaths_merged.loc[deaths_merged['death_member'] == "Robert Pittenger", 'cause_vacancy'] = "Resigned"
deaths_merged.loc[deaths_merged['Cause of vacancy'].isna(), 'cause_vacancy'] = "Death" # any missing ones from the first deaths data



#%%

# Adding missing deaths_unexpected, age of death, and death party member data

deaths_merged.loc[deaths_merged['Original'] == "William A. Steiger (R)", 'death_date'] = "1978-12-04"

# Checking before
deaths_merged_2 = deaths_merged[deaths_merged['cause_vacancy']=='Death'][['death_member', 'death_date', 'Original', 'death_party_member', 'death_cause', 'death_unexpected', 'death_age']]
print(deaths_merged_2.isna().sum())

death_dates_2 = {
    'Leo Ryan': {'death_unexpected': 1, 'death_cause': 'Assassinated during the Jonestown massacre in Guyana', 'death_age': 53},
    'William A': {'death_unexpected': 1, 'death_cause': 'Complications following heart surgery', 'death_age': 40},
    'Gladys Spellman': {'death_unexpected': 0, 'death_cause': 'Heart attack in 1980, death after coma in 1988', 'death_age': 70},
    'Jack Swigert': {'death_unexpected': 0, 'death_cause': 'Bone marrow cancer', 'death_age': 51},
    'Luke Letlow': {'death_unexpected': 1, 'death_cause': 'Covid-19 complications', 'death_age': 41},
    'Donald Payne': {'death_unexpected': 1, 'death_cause': 'Colon cancer', 'death_age': 77},
    'Sheila Jackson': {'death_unexpected': 0, 'death_cause': 'Pancreatic cancer', 'death_age': 74},    
    }

for name, data in death_dates_2.items():
    print(name, data['death_unexpected'], data['death_cause'], data['death_age'])
    deaths_merged.loc[deaths_merged['death_member'] == name, 'death_unexpected'] = data['death_unexpected']
    deaths_merged.loc[deaths_merged['death_member'] == name, 'death_cause'] = data['death_cause']
    deaths_merged.loc[deaths_merged['death_member'] == name, 'death_age'] = data['death_age']    
    
# Checking after
deaths_merged_2 = deaths_merged[deaths_merged['cause_vacancy']=='Death'][['death_member', 'death_date', 'Original', 'death_party_member', 'death_cause', 'death_unexpected', 'death_age']]
print(deaths_merged_2.isna().sum())



#%%

# Creating death_cycle variable to show when they died
deaths_merged['Date'] = pd.to_datetime(deaths_merged['Date'], format='%B %d, %Y', errors='coerce')
deaths_merged['death_date'] = pd.to_datetime(deaths_merged['death_date'], errors = 'coerce') # need to use errors = 'coerce' to include incorrect dates
# deaths_merged['death_year'] = deaths_merged['death_date'].dt.year # overwriting death_year with new data

# Creating a new dataset from election_dates.csv that has the respective year for each election cycle
election_dates_df = pd.read_csv(os.path.join(data_folder, 'election_dates.csv'), encoding = 'latin-1') # manually created data for election dates and election cycles

year_to_cycle_df = pd.DataFrame()
for cycle in sorted(election_dates_df['cycle'].unique()):
    # For each election year and the year before, assign the cycle
    years = [cycle-1, cycle]
    temp_df = pd.DataFrame({
        'year': years,
        'cycle': [cycle, cycle]  # Both years belong to the same election cycle
    })
    year_to_cycle_df = pd.concat([year_to_cycle_df, temp_df])
year_to_cycle_df = year_to_cycle_df.sort_values('year').reset_index(drop=True)

# Adding these data to our special elections data, now we have a election cycle variable: 'cycle' as well
special_elections_final = pd.merge(
    deaths_merged,
    year_to_cycle_df,
    how = 'left',
    on = 'year'
    )
    

#%%
# Fixing dates


# Final renaming

# NOTE: resign_date and death_date are almost always identical, but the former refers to both resignations and deaths, whereas the latter only to deaths (latter will be used for treatment vars)
special_elections_final = special_elections_final.rename(columns = {
    'death_date': 'resign_date',
    'death_year': 'resign_year',
    'death_member': 'spec_member',
    'death_party_member': 'spec_party',
    'year': 'spec_election_year',
    'Date': 'spec_election_date',
    'Congress': 'spec_election_Congress',
    'Original': 'spec_election_Original_candidate',
    'Cause of vacancy': 'spec_election_Cause_of_vacancy',
    'Winner': 'spec_Winner',
    'cycle': 'spec_cycle'
    })
special_elections_final = special_elections_final.sort_values(by = ['spec_election_year', 'spec_election_date', 'district'])

death_mask = special_elections_final['cause_vacancy'] == 'Death'
special_elections_final.loc[death_mask, 'death_date'] = special_elections_final.loc[death_mask, 'resign_date']



columns_list = ['district', 'spec_election_year', 'spec_cycle', 'spec_member', 'spec_party', 'resign_date', 'death_date', 'spec_election_date', 'cause_vacancy']
cols = columns_list + [
    col for col in special_elections_final.columns if col not in columns_list]
special_elections_final = special_elections_final[cols]


special_elections_final.to_csv(os.path.join(data_folder, "special_elections_final.csv"), index = False)



