#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 18 16:53:37 2025

@author: lirhoxhaj
"""

## PURPOSE OF FILE: Running all Python scripts by order

#%%

### LIBRARIES

import os
import inspect
import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
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

### MODULES
import convert_html_to_csv
import convert_html_to_csv_2
import main
import outputs

#%%

### FUNCTIONALITY

print("=" * 80)
print("STEP 1: Running convert_html_to_csv")
print("=" * 80)
# Run the entire script
convert_html_to_csv.run_module('convert_html_to_csv', run_name='__main__')

print("\n" + "=" * 80)
print("STEP 2: Running convert_html_to_csv_2")
print("=" * 80)
convert_html_to_csv_2.run_module('convert_html_to_csv_2', run_name='__main__')

print("\n" + "=" * 80)
print("STEP 3: Running main")
print("=" * 80)
main.run_module('main', run_name='__main__')

print("\n" + "=" * 80)
print("STEP 4: Running outputs")
print("=" * 80)
outputs.run_module('outputs', run_name='__main__')

print("\n" + "=" * 80)
print("All scripts completed successfully!")
print("=" * 80)




