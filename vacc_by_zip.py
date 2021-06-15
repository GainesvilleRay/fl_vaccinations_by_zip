# built-in libraries
import csv
import datetime
import os
import os.path
import re
import sqlite3
from sqlite3 import Error

# installed with pip
from pandas import pandas as pd
import numpy as np
import xlrd

# create a csv file that groups shots by zip code of residence
# for each county where shot was administered.

def make_zip_list():

    """
    Get a list of all Florida zips from a csv file that has them by row
    return: list of zips
    """

    with open('flzips.csv', newline='') as f:
        reader = csv.reader(f)
        data = list(reader)

    zip_list = [item for sublist in data for item in sublist]
    return zip_list

def grouped_vaccinations(filename, zip_list):
    
    """
    Group the vaccinations administered in a given county for all zip codes in Florida,
    and append to csv file.
    param filename: file that includes given county
    param zip_list: list of all Florida zip codes
    return: nothing
    """
    
    # make list of counties contained in filename
    cols = ['county']
    df_counties = pd.read_csv(filename, usecols=[0], names=['county'], skiprows=1, encoding="ISO-8859-1")
    counties_grouped = df_counties.groupby("county").count().index.tolist()
        
    # read csv of individual vaccinations for one or more counties into a dataframe
    cols = ['county', 'zip', 'gender', 'date_first', 'complete', 'date_complete', 'age', 'status', ]
    dtypes = {
        'county' : str,
        'zip' : str,
        'gender' : str,
        'date_first' : str,
        'complete' : str,
        'date_complete' : str,
        'age' : str,
        'status' : str
        }
    date_cols = ['date_first', 'date_complete']
    df = pd.read_csv(
        filename,
        usecols=[0, 1, 3, 4, 6, 7, 8, 9],
        names=cols,
        dtype=dtypes,
        parse_dates=date_cols,
        skiprows=1,
        encoding="ISO-8859-1")

    # filter for individual counties and group by zip
    for county in counties_grouped:
        county_df = df[df.county == county] # dataframe for individual county
        df_local_zips = county_df[county_df['zip'].isin(zip_list)] # filter for Florida zips only

        # how many folks vaccinated in given county for each Florida zip
        zip_count = df_local_zips.groupby("zip")["county"].count()
        df_zipcount = pd.DataFrame(zip_count).reset_index('zip')
        df_zipcount.rename(columns = {'county':'total_shots'}, inplace = True)
        df_zipcount['county'] = county
        # append to csv
        df_zipcount.to_csv('grouped_vaccinations_by_zip.csv', mode='a', header= False, index=False)
     
def make_zcta_file():
  
    """
    Create a csv file that consolidates zip-code counts across all counties and adds in demographic data
    PARAM
    """
    # make df for grouped vaccinations
    df = pd.read_csv('grouped_vaccinations_by_zip.csv', names=['zip','total_shots','county'])
    df_grouped = df.groupby('zip').sum('total')

    # make df for zcta data
    zctafile = "FL_ZCTA_variables_2019_r1.xlsx"
    cols = ['zip', 'pop2019', 'poverty_pct', 'asian_pct', 'hispanic_pct', 'white_pct', 'black_pct']
    usecols = [0, 1, 2, 6, 7, 8, 9]

    df_zcta = pd.read_excel(zctafile, names=cols, usecols=usecols)
    df_zcta = df_zcta.set_index('zip')

    # merge the two df's
    df_zcta = pd.concat([df_grouped['total_shots'], df_zcta], axis=1).reset_index()
    df_zcta['vacc_rate'] = df_zcta['total_shots']/df_zcta['pop2019']

    # write to csv
    df_zcta.to_csv('zcta_grouped.csv', mode='w', header= True, index=False)
    
def main():
    
    zip_list = make_zip_list() # get list of all Florida zip codes
    dir_csv = r'C:\Users\DRay1\Projects\\vacc_by_zip\\datafiles\\'
    filenames = ([name for name in os.listdir(dir_csv) if os.path.isfile(os.path.join(dir_csv, name))])
    for filename in filenames:
        filename = dir_csv + filename
        grouped_vaccinations(filename, zip_list)
        
    make_zcta_file()

if __name__ == '__main__':
    main()
