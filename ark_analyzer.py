#########################################################################
# Basic tracking of holdings in ARK funds - https://ark-funds.com/
# ebharucha: 1/4/2020
########################################################################

# Import dependencies 
import sqlite3
import numpy as np
import pandas as pd
import requests
import datetime
import re
import os
import glob
import shutil
import warnings
warnings.filterwarnings("ignore")

# Function to initiatize values for ARK funds
def arkIntialize ():
    ark = {
        'ARKK' : {
        'name': 'ARK Innovation ETF',
        'details': 'https://ark-funds.com/arkk',
        'URL': 'https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv'
        },
        'ARKQ' : {
            'name': 'ARK Autonomous Tech. & Robotics ETF',
            'details': 'https://ark-funds.com/arkq',
            'URL': 'https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_AUTONOMOUS_TECHNOLOGY_&_ROBOTICS_ETF_ARKQ_HOLDINGS.csv'
        },
        'ARKW' : {
            'name': 'ARK Next Generation Internet ETF',
            'details': 'https://ark-funds.com/arkw',
            'URL': 'https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_NEXT_GENERATION_INTERNET_ETF_ARKW_HOLDINGS.csv'
        },
        'ARKG' : {
            'name': 'Genomic Revolution ETF',
            'details': 'https://ark-funds.com/arkg',
            'URL': 'https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_GENOMIC_REVOLUTION_MULTISECTOR_ETF_ARKG_HOLDINGS.csv'
        },
        'ARKF' : {
            'name': 'Fintech Innovation ETF',
            'details': 'https://ark-funds.com/arkf',
            'URL': 'https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_FINTECH_INNOVATION_ETF_ARKF_HOLDINGS.csv'
        },
        'PRNT' : {
            'name': 'The 3D Printing ETF',
            'details': 'https://ark-funds.com/prnt',
            'URL': 'https://ark-funds.com/wp-content/fundsiteliterature/csv/THE_3D_PRINTING_ETF_PRNT_HOLDINGS.csv'
        }
    }
    return (ark)

# Function to download fund data
def getFundData(fund, URL):
    csv = requests.get(URL)
    url_content = csv.content
    with open(f'{DATADIR}/{fund}_{today}.csv', 'wb') as file:
        file.write(url_content)
    df = pd.read_csv(f'{DATADIR}/{fund}_{today}.csv')
    df.dropna(inplace=True)
    return (df)

# Function to consoldiate data across funds & sort by market cap
def consolidateFundData(df_):
    df_ticker = df_.groupby('ticker')
    tickers = []
    companies = []
    shares = []
    market_value = []
    count = []
    for ticker in df_ticker.groups.keys():
        info = df_ticker.get_group(ticker).sum()
        tickers.append(ticker)
        companies.append(info["company"])
        shares.append(int(info["shares"]))
        market_value.append(info["market value($)"])             
        count.append(info['count'])
    date = re.sub('/', '-', df_.date.unique()[0])
    df_consolidated = pd.DataFrame()
    df_consolidated['ticker'] = tickers
    df_consolidated['company'] = companies
    df_consolidated[f'shares_{date}'] = shares
    df_consolidated['market value($)'] = market_value
    df_consolidated[f'num_funds'] = count
    df_consolidated.sort_values('market value($)', ascending=False, inplace=True)
    return (df_consolidated)


# Function to merge prior & new data
def mergeFundData(df_ark_prior, df_consolidated):
    df = df_ark_prior.merge(df_consolidated, left_on=['ticker'], right_on=['ticker'], how='outer')
    company = []
    print (df.columns)
    for x, y in zip(df.company_x, df.company_y):
        if (type(x) == float):
            company.append(y)
        else:
            company.append(x)
    df['company'] = company
    cols = ['ticker', 'company']
    for col in df.columns:
        if re.findall('shares_.*', col):
            cols.append(col)
    cols += ['market value($)_y', 'num_funds_y']
    print (f'----------{cols}----------')
    df = df[cols]
    shares = [col for col in cols if 'shares_' in col]
    dates = [share.split('_')[1] for share in shares]
    # df[f'shares_delta_{dates[-2]}_To_{dates[-1]}'] = df[shares[-1]] - df[shares[-2]]
    # cols_ = cols[:2] + shares + [f'shares_delta_{dates[-2]}_To_{dates[-1]}'] + cols[-2:]
    cols_ = cols[:2] + shares + cols[-2:]
    print (f'----------{cols}----------')
    df = df[cols_]
    # shares_ = ["_".join(col.split('_')[:-1]) for col in cols if 'shares_' in col]
    # print (f'----------{shares_}----------')
    # for idx, col in enumerate(shares):
    #     df.rename(columns={col: shares_[idx]}, inplace=True)
    # cols = cols[:2] + ['Price'] + shares_ + [f'SharesDelta_{dates[-2]}_To_{dates[-1]}'] + cols[-2:]
    # df = df[cols]
    return (df)

def storeDB(db, table, df, ifexists):
    conn = sqlite3.connect(db)
    df.to_sql(table, con=conn, if_exists=ifexists)
    if (table == "ark"):
        query = f'DELETE FROM {table} WHERE rowid NOT IN (SELECT MIN(rowid) FROM {table} GROUP BY date,fund,ticker)'
        try:
            pd.read_sql_query(query, con=conn)
            print ('Deleting duplicate Individual Fund data rows')
        except Exception as e:
            print(e)
    elif (table == "arkConsolidated"):
        query = f'DELETE FROM {table} WHERE rowid NOT IN (SELECT MIN(rowid) FROM {table} GROUP BY ticker)'
        try:
            pd.read_sql_query(query, con=conn)
            print ('Deleting duplicate Individual Fund data rows')
        except Exception as e:
            print(e)
    conn.commit()
    conn.close()

def loadDFFromDB(db, table):
    conn = sqlite3.connect(db)
    try:
        df = pd.read_sql_query(f'select * from {table}', con=conn)
        df.drop(columns=['index'], inplace=True)
    except Exception as e:
        print (e)
    conn.close()
    return (df)
    

#####################################
# Specify directory to store data
today = str(datetime.date.today())
DATADIR = './data'
UPDATEDIR = f'{DATADIR}/update'
UPDATEFLAG = f'update_{today}'
DB = 'ark.db'
IND_FUND_TABLE = 'ark'
CONS_FUND_TABLE = 'arkConsolidated'

# Main function
def main():
    # Remove existing .csv files in DATADIR
    filelist=glob.glob(f'{DATADIR}/*.csv')
    for file in filelist:
        try:
            os.remove(file)
            print (f'Deleted {file}')
        except:
            print (f'No removal')

    # Create DATADIR & UPDATEDIR if required
    if not os.path.exists(DATADIR):
        os.makedirs(DATADIR)
    if not os.path.exists(UPDATEDIR):
        os.makedirs(UPDATEDIR)

    # Load prior Consolidated Fund data from DB
    df_ark_prior = loadDFFromDB(f'{DATADIR}/{DB}', f'{CONS_FUND_TABLE}')

    # Initialzie values for ARK funds
    ark = arkIntialize()

    if os.path.isfile(f'{UPDATEDIR}/{UPDATEFLAG}'):
        df = df_ark_prior
    else: 
        # Download fund data
        df = {}
        df_ = pd.DataFrame()
        for fund in ark.keys():
            df[fund] = getFundData(fund, ark[fund]['URL'])
            df_ = pd.concat([df_, df[fund]])
        df_['count'] = [1]*df_.shape[0]
        df_.reset_index(inplace=True)
        df_.drop(columns=['index'], inplace=True)

        # Store Individual Fund data in DB
        storeDB(f'{DATADIR}/{DB}', f'{IND_FUND_TABLE}', df_, 'append')

        # Create update flag
        with open(f'{UPDATEDIR}/{UPDATEFLAG}', 'w') as f:
            f.write(today)

        # Consoldiate data across funds & sort by market cap
        ark_consolidated = 'ARK.xlsx'
        df_consolidated = consolidateFundData(df_)
        # df_consolidated.to_excel(f'{DATADIR}/{ark_consolidated}', index=False)

        # Store new Consolidated Fund data in DB
        # storeDB(f'{DATADIR}/{DB}', f'{CONS_FUND_TABLE}', df_consolidated, 'replace')

        # Merge prior & new data
        try:
            df_ark_prior
            df = mergeFundData(df_ark_prior, df_consolidated)
        except Exception as e:
            print(e)

        # Store overall Consolidated Fund data in DB
        storeDB(f'{DATADIR}/{DB}', f'{CONS_FUND_TABLE}', df, 'replace')
        df.to_excel(f'{DATADIR}/{ark_consolidated}', index=False)

    return(ark, df)

# main()
