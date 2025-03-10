# DIME_data_analysis

(Work under progress)

(Last update: March 9th, 2025)

## Motivation

This project is work faciliated for the research of [Matteo Broso](https://github.com/Mbroso21) and Dr. Tommaso Valletti. We merge various datasets from the extensive [DIME database](https://data.stanford.edu/dime) created by Dr Adam Bonica at Stanford University, which consist of contributions going to different political candidates (recipients) in the United States. We particularly focus on contributions going to candidates of the US House of Representatives. 

The authors hypothesise that in more competitive elections, firms and individuals tend to exert more influence via campaign contributions. The main goal of the authors' research paper is to understand how contribution patterns may differ between candidates in contested (i.e., more competitive) election districts and more one-sided ones. The authors utilise exogenous variation coming from the death of an incumbent to omit bias coming from any endogenous variation and truly analyse the impact of this phenomenon. 

## Structure and functionality

Our main working directory (`\parent`) consists of two folders: `\code` and `\data`. Main scripts are stored under `\code\convert_html_to_csv.py` and `\code\main.py`. The purpose of the former script is to convert the HTML source code (`data\special_elections.html`) of the Wikipedia website that stores information of all special elections in US House of Representatives history (`data\special_elections_Wikipedia.pdf`) into a readable csv file (`\data\special_elections_final.csv`). 

On the other hand, the latter script merges four main inputs into one final database:

1. The contributions data coming from DIME (see [contribDB_1980.csv.gz to contribDB_2024.csv.gz](https://data.stanford.edu/dime)), omitted from repo because of size;
2. The recipients data coming from DIME (see [dime_recipients_1979_2024.csv.gz](https://data.stanford.edu/dime)), omitted from repo because of size;
3. Our self-constructed dataset of deaths and resignations, using special_elections.csv (see `\main\convert_html_to_csv.py`);
4. A manually created general elections dataset for each year (cycle) from 1980 to 2024 (see `\data\election_dates.csv`);

, and produces one final cleaned contribution-level dataset (`output_1`) and several other insightful district and cycle-level outputs (omitted from repo).

## Dependencies

### Data

Input 1 and 2 may be found in the [DIME website](https://data.stanford.edu/dime). Please refer to the DIME codebook (see [Codebook (DIME) - Version 4.0 (pdf)](https://data.stanford.edu/dime)) for more detailed variable description.

### Libraries

Install/import the following libraries for:

- file path and directories
  - *os*
  - *inspect*
  - *Path*
- reading and manipulating data
  - *pandas*
  - *numpy*
  - *re*
  - *datetime*
- data visualization
  - *matplotlib.pyplot* 
  - *seaborn* 
- Document parsing
  - *BeautifulSoup*

## References

Bonica, Adam. 2024. Database on Ideology, Money in Politics, and Elections: Public version 4.0 [Computer file]. Stanford, CA: Stanford University Libraries. [https://data.stanford.edu/dime](https://data.stanford.edu/dime).
