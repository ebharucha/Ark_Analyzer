#########################################################################
# Basic tracking of holdings in ARK funds - https://ark-funds.com/
# ebharucha: 1/4/2020
########################################################################

# Import dependencies
import numpy as np
import pandas as pd
import requests
import datetime
import re
import os
import shutil

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
    df_consolidated['Ticker'] = tickers
    df_consolidated['Company'] = companies
    df_consolidated[f'Shares_{date}'] = shares
    df_consolidated['Market Value($)'] = market_value
    df_consolidated[f'NumFunds'] = count
    df_consolidated.sort_values('Market Value($)', ascending=False, inplace=True)
    return (df_consolidated)

# Function to merge prior & new data
def mergeFundData(df_ark_prior, df_consolidated):
    df = df_ark_prior.merge(df_consolidated, left_on=['Ticker'], right_on=['Ticker'], how='outer')
    company = []
    for x, y in zip(df.Company_x, df.Company_y):
        if (type(x) == float):
            company.append(y)
        else:
            company.append(x)
    df['Company'] = company
    cols = ['Ticker', 'Company']
    for col in df.columns:
        if re.findall('Shares_.*', col):
            cols.append(col)
    cols += ['Market Value($)', 'NumFunds']
    df = df[cols]
    shares = [col for col in cols if 'Shares_' in col]
    dates = [share.split('_')[1] for share in shares]
    df[f'SharesDelta_{dates[-2]}_To_{dates[-1]}'] = df[shares[-1]] - df[shares[-2]]
    cols_ = cols[:2] + shares + [f'SharesDelta_{dates[-2]}_To_{dates[-1]}'] + cols[-2:]
    df = df[cols_]
    shares_ = ["_".join(col.split('_')[:-1]) for col in cols if 'Shares_' in col]
    for idx, col in enumerate(shares):
        df.rename(columns={col:shares_[idx]}, inplace=True)
    # cols = cols[:2] + shares_ + [f'SharesDelta_{dates[-2]}_To_{dates[-1]}'] + cols[-2:]
    # print (cols)
    # df = df[cols]
    return (df)

#####################################
# Specify directory to store data
DATADIR = './data'
if not os.path.exists(DATADIR):
    os.makedirs(DATADIR)

today = str(datetime.date.today())

# Backup & load prior data
ark_prior = 'ARK.xlsx'
if os.path.isfile(f'{DATADIR}/{ark_prior}'):
    shutil.copyfile(f'{DATADIR}/{ark_prior}', f'{DATADIR}/{ark_prior}.bak')  # Make a backup copy   
    df_ark_prior = pd.read_excel(f'{DATADIR}/{ark_prior}')
    df_ark_prior = df_ark_prior.iloc[:,:-2]

# Initialzie values for ARK funds
ark = arkIntialize()

# Download fund data
df = {}
df_ = pd.DataFrame()
for fund in ark.keys():
    df[fund] = getFundData(fund, ark[fund]['URL'])
    df_ = pd.concat([df_, df[fund]])
df_['count'] = [1]*df_.shape[0]
df_.reset_index(inplace=True)
df_.drop(columns=['index'], inplace=True)

# Consoldiate data across funds & sort by market cap
df_consolidated = consolidateFundData(df_)
df_consolidated.to_excel(f'{DATADIR}/{ark_prior}', index=False)

# Merge prior & new data
try:
    df_ark_prior
    df = mergeFundData(df_ark_prior, df_consolidated)
except NameError:
    print ('df_ark_prior not defined')