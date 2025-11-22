# DIME_data_analysis

Research procedure, code functionality, and folder directory description

(Last update: 22.11.2025)

# Motivation

This project is work faciliated for the research of [Matteo Broso](https://github.com/Mbroso21) and [Dr. Tommaso Valletti](https://profiles.imperial.ac.uk/t.valletti). We merge various datasets from the extensive [DIME database](https://data.stanford.edu/dime) created by Dr Adam Bonica at Stanford University, which consist of contributions going to different political candidates (recipients) in the United States. We particularly focus on contributions going to candidates of the US House of Representatives. 

The authors hypothesise that in more competitive elections, firms and individuals tend to exert more influence via campaign contributions. The main goal of the authors' research paper is to understand how contribution patterns may differ between candidates in contested (i.e., more competitive) election districts and more one-sided ones. The authors utilise the plausibly exogenous variation in political competition induced by an incumbent’s death in the US House of Representatives elections. 

# Structure and functionality

## Folder structure and hierarchy

Files are grouped in two folders: \code and \data. \code contains all scripts used to process and handle the data for the research, while \data consists of raw data coming from different sources and a subfolder \OUTPUTS_FINAL where processed outputs are saved.

## Dependencies

### Data
INPUT_1 and INPUT_2 may be found in the DIME website. Please refer to the DIME codebook (see Codebook (DIME) - Version 4.0 (pdf)) for more detailed variable description and other references for other datasets and download the following data in the \data folder:

- contribDB_1980.csv to contribDB_2024.csv;
- dime_recipients_1979_2024.csv;

and also save the following datasets and files in the same folder: 

- 1976-2024-house.csv; 
- Deaths.csv;
- election_dates.csv;
- new_districts.html;
- special_elections.html.

### Libraries
Install/import the following libraries to run the Python scripts:

File paths and directories
- os
- inspect
- Path

Reading and manipulating data
- pandas
- numpy
- re
- datetime

Data visualization (optional)
- matplotlib.pyplot
- seaborn

Document parsing
- BeautifulSoup


## Code

Our main working directory (referred to as \parent in the code) consists of two folders: \code and \data. Main scripts are stored under:

- \code\convert_html_to_csv.py
- \code\convert_html_to_csv_2.py
- \code\main.py
- \code\outputs.py
- \code\run.py

## Running the code 

Step 0: Make sure all libraries and data/files from the 'Dependencies' section are installed / downloaded and stored correctly in the right folder.
Step 1: Run the first two scripts, which convert important recipient and district data from special_elections.html and new_district.html, respectively.
Step 2: Run main.py, which creates the contribution-level data and a dictionary of variables and their description (OUTPUT_1.csv and OUTPUT_1_dict.csv) by merging together the DIME contributions and recipients data (contribDB_1980 to contribDB_2024 and dime_recipients_1979_2024) with our own dataset for candidates/recipients deaths (special_elections_final.csv) and dates of general elections in each year.
Step 3: Run outputs.py, which to generate uses the outputs from main.py and creates district-cycle-level data for a set of important variables we use in our analysis (see OUTPUT_1_final_collapsed.csv and OUTPUT_1_final_collapsed_dict.csv).

The latter script, run.py, runs all of these scripts in the given order: convert_html_to_csv.py, convert_html_to_csv_2.py, main.py, and outputs.py. 

### convert_html_to_csv.py and convert_html_to_csv_2.py

The purpose of the first two scripts is to convert the HTML source code, namely data\special_elections.html and data\new_districts.html, of two separate Wikipedia websites, which store information of all special elections in US House of Representatives history and all congressional districts in US history, into readable csv files: \data\special_elections_final.csv and \data\new_districts_filtered.csv. 
The first script scrapes data for resignations and deaths of all incumbents who's vacancy lead to a special election in their district at that election cycle, and manually add any deaths that were missed. We also add voting election outcomes for treated cases (i.e. incumbent deaths only) to calculate margin of victory for the most recent election incumbent ran in before they passed away, and that of the winner in the following special elections organised to replace deceased incumbent (note: a runoff dummy indicates whether district was won in runoff or not). 

The second script scrapes data from another Wikipedia website (\data\new_district.html) which has the creation and discontinuation year for each district in the United States and converts this information in csv format (\data\new_district_processed.csv). This is especially important in generating the real_data variable later for the OUTPUT_1_final_collapsed data, which helps us understand whether the district existed in that year, or if we are imputing zeros for those years to balance the panel data. The final output from this script is the new_districts_filtered_all.csv, which in outputs.py is used to create the universe of districts. Using information from Wikipedia, we also add an additional dummy for non-voting delegations in the US House of Representatives (American Samoa, District of Columbia, Guam, Northern Mariana Islands, Puerto Rico, United States Virgin Islands).

### main.py

On the other hand, our main script \code\main.py merges four main inputs into one final database:

1. INPUT_1: The contributions data coming from DIME (see \data\contribDB_1980.csv to \data\contribDB_1980.csv);
2. INPUT_2: The recipients data coming from DIME (see \data\dime_recipients_1979_2024.csv);
3. INPUT_3: Our self-constructed dataset of deaths and resignations, using special_elections.csv (refer to \main\convert_html_to_csv.py to understand how data was processed);
4. INPUT_4: A manually created general elections dataset for each year (cycle) from 1980 to 2024 (see \data\election_dates.csv);

After merging and filtering these datasets for relevant and valid values, we produces one final cleaned contribution-level dataset (\data\OUTPUTS_FINAL\OUTPUT_1.csv), two filtered datasets for contributions coming from individuals and corporations respectively (\data\OUTPUTS_FINAL\OUTPUT_1_ind.csv and \data\OUTPUTS_FINAL\OUTPUT_1_corp.csv), and a dictionary (\data\OUTPUTS_FINAL\OUTPUT_1_dict.csv) containing variable names, their description, external source, dataset of origin in our folder, and any relevant, external URL linked to the variable.

Please refer to the dictionary of the contribution-level data, OUTPUT_1_dict.csv, for a more detailed description of every variable.

### outputs.py

This script processes new_districts_filtered_filtered_all.csv, which contains information for all US districts that were either created and/or discontinued during our period of interest (i.e., 1980 to 2024). outputs.py convert this data into new_districts_filtered_universe.csv and new_districts_filtered_universe_party.csv, two files that are a Cartesian product of unique values of district and cycle, and district, cycle, and party, respectively. We use these manually-created datasets to balance the panel of data coming from OUTPUT_1 and generate relevant variables. Because of inconsistencies in district naming in the raw data (non-existent districts like NY84), we compute a variable titled 'real_data' that indicates whether the district-cycle is real or not (i.e., whether the district has existed in that election cycle). It is important to note that because we are dealing with contribution, our definition of district existence is if there were elections being held at that district in that election cycle. This was the most consistent definition, which allowed us to keep a constant number of 435 voting districts across all election cycles when keeping rows `real_data == 1` and `territorial == 1`. This is the actual number of voting district in the US House of Representatives 

1976-2024-house.csv is a dataset downloaded from the MIT Election Data and Science Lab data on US senate election from 1976 to 2020, which we use to estimate general election results and related measures for each district-cycle. 

The main purpose of outputs.py is to process this data into a series of insightful variables that are grouped by district and election cycle (see OUTPUT_1_final_collapsed.csv). The panel district-cycle-level dataset, see OUTPUT_1_final_collapsed.csv, is then balanced when merged with OUTPUT_0 and OUTPUT_0_2. Similar to main.py, outputs.py creates a dictionary OUTPUT_1_final_collapsed_dict.csv with an additional column output_relation, which we use to categorise variables into groups that serve a common purpose or come from a common source (note that variables belonging to the same output are grouped in a way so that they contain a similar type of information). Please refer to the dictionary of the district-cycle-level data, OUTPUT_1_final_collapsed_dict.csv, for a more detailed description of every variable.

In addition to OUTPUT_1_final_collapsed.csv and OUTPUT_1_final_collapsed_dict.csv, we create a list of _ext_ datasets (suffix for 'extended'), where from a subset of original variables we create new variables that apply a condition based on the treatment (either treat_1, treat_2, or treat_3). Each variable has a suffix - {varname}_1, {varname}_2, or {varname}_3, respectively - that show this. All variables are merged in OUTPUT_1_final_collapsed_ext.csv and the rules we apply for the variables are explained in OUTPUT_1_final_collapsed_dict_ext.csv. 


# References

1. Bonica, Adam. 2024. Database on Ideology, Money in Politics, and Elections: Public version 4.0 [Computer file]. Stanford, CA: Stanford University Libraries. https://data.stanford.edu/dime.

2. MIT Election Data and Science Lab. (2017). U.S. Senate statewide 1976–2020 (Version V7) [Data set]. Harvard Dataverse. https://doi.org/10.7910/DVN/PEJ5QU

3. Wikipedia contributors. (n.d.). List of United States congressional districts. Wikipedia. Retrieved April 9, 2025, from https://en.wikipedia.org/wiki/List_of_United_States_congressional_districts

4. Wikipedia contributors. (n.d.). List of special elections to the United States House of Representatives. Wikipedia. Retrieved April 9, 2025, from https://en.wikipedia.org/wiki/List_of_special_elections_to_the_United_States_House_of_Representatives
