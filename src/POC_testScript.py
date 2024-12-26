import json
import pandas as pd
import numpy as np

from libs.Meter.EmuMeterClass import EmuMeter

startTime = 1718056800
stopTime = 1735081200
solarOwnershipHaus1 = 0
solarOwnershipHaus2 = 1
solarOwnershipAllg = 0

def getHost(name):
    with open('src/POC_meters.secret', 'r') as file:
        #print(json.load(file)[name])
        # load IP-address of a testhose from secret json file.
        # testHost.secret example content: {"host01": "XXX.XXX.XXX.XXX"}
        return str(json.load(file)[name])

print("reading meters...")
try:
    meterdataHome1 = pd.read_pickle(f"cache/Home1_{startTime}_{stopTime}.secret")
except:
    home1Meter = EmuMeter(getHost('Haus1'))
    meterdataHome1 = home1Meter.read(startTime,stopTime)
    meterdataHome1.to_pickle(f"cache/Home1_{startTime}_{stopTime}.secret")
print("Home1 done (1/4)")

try:
    meterdataHome2 = pd.read_pickle(f"cache/Home2_{startTime}_{stopTime}.secret")
except:
    home2Meter = EmuMeter(getHost("Haus2"))
    meterdataHome2 = home2Meter.read(startTime,stopTime)
    meterdataHome2.to_pickle(f"cache/Home2_{startTime}_{stopTime}.secret")
print("Home2 done (2/4)")

try:
    meterdataAllg = pd.read_pickle(f"cache/Allg_{startTime}_{stopTime}.secret")
except:
    allgMeter = EmuMeter(getHost("Allg"))
    meterdataAllg = allgMeter.read(startTime,stopTime)
    meterdataAllg.to_pickle(f"cache/Allg_{startTime}_{stopTime}.secret")
print("Allg done (3/4)")

try:
    meterdataSolar = pd.read_pickle(f"cache/Solar_{startTime}_{stopTime}.secret")
except:
    solarMeter = EmuMeter(getHost("Solar"))
    meterdataSolar = solarMeter.read(startTime,stopTime)
    meterdataSolar.to_pickle(f"cache/Solar_{startTime}_{stopTime}.secret")
print("Solar done (4/4)")

meterdataHome1.rename(columns={'Energy_Import_Wh': 'Home1_Usage_Wh', 
                               'Energy_Export_Wh': 'Home1_Prod_Wh'}, inplace=True)
meterdataHome2.rename(columns={'Energy_Import_Wh': 'Home2_Usage_Wh', 
                               'Energy_Export_Wh': 'Home2_Prod_Wh'}, inplace=True)
meterdataAllg.rename(columns={'Energy_Import_Wh': 'Allg_Usage_Wh', 
                              'Energy_Export_Wh': 'Allg_Prod_Wh'}, inplace=True)
meterdataSolar.rename(columns={'Energy_Import_Wh': 'Solar_Usage_Wh', 
                               'Energy_Export_Wh': 'Solar_Prod_Wh'}, inplace=True)

meterData = pd.merge(meterdataHome1,meterdataHome2,on='Timestamp',how='outer')
meterData = pd.merge(meterData,meterdataAllg,on='Timestamp',how='outer')
meterData = pd.merge(meterData,meterdataSolar,on='Timestamp',how='outer')

meterData = meterData.diff()

print('divide solar energy up for each user...')
meterData['Home1_Usage_Wh'] += (meterData['Solar_Usage_Wh'] * solarOwnershipHaus1)
meterData['Home1_Prod_Wh'] += (meterData['Solar_Prod_Wh'] * solarOwnershipHaus1)

meterData['Home2_Usage_Wh'] += (meterData['Solar_Usage_Wh'] * solarOwnershipHaus2)
meterData['Home2_Prod_Wh'] += (meterData['Solar_Prod_Wh'] * solarOwnershipHaus2)

meterData['Allg_Usage_Wh'] += (meterData['Solar_Usage_Wh'] * solarOwnershipAllg)
meterData['Allg_Prod_Wh'] += (meterData['Solar_Prod_Wh'] * solarOwnershipAllg)

meterData.drop(['Solar_Usage_Wh','Solar_Prod_Wh'], axis= 1, inplace=True)

print('eigenverbrauch pro benutzer...')
for i, row in meterData.iterrows():
    if i%100 == 0:
        print(f'Calculating eigenverbrauch {i} of {len(meterData)}...')

    for user in ['Home1', 'Home2', 'Allg']:

        if meterData.loc[i, f'{user}_Usage_Wh'] < meterData.loc[i, f'{user}_Prod_Wh']:
            meterData.loc[i, f'{user}_eigenV_Wh'] = meterData.loc[i, f'{user}_Usage_Wh']
        else:
            meterData.loc[i, f'{user}_eigenV_Wh'] = meterData.loc[i, f'{user}_Prod_Wh']

        meterData.loc[i, f'{user}_Usage_Wh'] -= meterData.loc[i, f'{user}_eigenV_Wh']
        meterData.loc[i, f'{user}_Prod_Wh'] -= meterData.loc[i, f'{user}_eigenV_Wh']

meterData['total_Usage_Wh'] = meterData['Home1_Usage_Wh'] + meterData['Home2_Usage_Wh'] + meterData['Allg_Usage_Wh']
meterData['total_Prod_Wh'] = meterData['Home1_Prod_Wh'] + meterData['Home2_Prod_Wh'] + meterData['Allg_Prod_Wh']

print('calc ovnership fractions...')
for i, row in meterData.iterrows():
    if i%100 == 0:
        print(f'Calculating ovnership fraction {i} of {len(meterData)}...')

    for user in ['Home1', 'Home2', 'Allg']:
        #calc prod and usage fraction
        if meterData.loc[i, 'total_Usage_Wh'] != 0:
            meterData.loc[i, f'{user}_Usage_frac'] = meterData.loc[i, f'{user}_Usage_Wh'] / meterData.loc[i, 'total_Usage_Wh']
        else:
            meterData.loc[i, f'{user}_Usage_frac'] = 0

        if meterData.loc[i, 'total_Prod_Wh'] != 0:
            meterData.loc[i, f'{user}_Prod_frac'] = meterData.loc[i, f'{user}_Prod_Wh'] / meterData.loc[i, 'total_Prod_Wh']
        else:
            meterData.loc[i, f'{user}_Prod_frac'] = 0

    if meterData.loc[i, 'total_Usage_Wh'] > meterData.loc[i, 'total_Prod_Wh']:
        # buying energy
        meterData.loc[i, 'total_eigenV_Wh'] = meterData.loc[i, 'total_Prod_Wh']
    else:
        # selling energy
        meterData.loc[i, 'total_eigenV_Wh'] = meterData.loc[i, 'total_Usage_Wh']

    meterData.loc[i, 'total_Usage_Wh'] -= meterData.loc[i, 'total_eigenV_Wh']
    meterData.loc[i, 'total_Prod_Wh'] -= meterData.loc[i, 'total_eigenV_Wh']
    

    for user in ['Home1', 'Home2', 'Allg']:
        meterData.loc[i, f'{user}_bought_Wh'] = meterData.loc[i, 'total_Usage_Wh'] * meterData.loc[i, f'{user}_Usage_frac']
        meterData.loc[i, f'{user}_sold_Wh'] = meterData.loc[i, 'total_Prod_Wh'] * meterData.loc[i, f'{user}_Prod_frac']
        meterData.loc[i, f'{user}_verbandUsage_Wh'] = meterData.loc[i, 'total_eigenV_Wh'] * meterData.loc[i, f'{user}_Usage_frac']
        meterData.loc[i, f'{user}_verbandProd_Wh'] = meterData.loc[i, 'total_eigenV_Wh'] * meterData.loc[i, f'{user}_Prod_frac']
        

meterData.sort_index(axis=1, inplace=True)

#print(meterData.sum())
#print(meterData.loc[150])

for i, row in meterData.iterrows():
    if i%100 == 0:
        print(f'Testing entry {i} of {len(meterData)}...')

    fracTotal = meterData.loc[i, 'Home1_Usage_frac'] + meterData.loc[i, 'Home2_Usage_frac'] + meterData.loc[i, 'Allg_Usage_frac']
    if fracTotal < 0.999 and fracTotal > 1.001:
        print(f'usage fraction on index {i} not adding up: {fracTotal}')

    fracTotal = meterData.loc[i, 'Home1_Prod_frac'] + meterData.loc[i, 'Home2_Prod_frac'] + meterData.loc[i, 'Allg_Prod_frac']
    if fracTotal < 0.999 and fracTotal > 1.001:
        print(f'production fraction on index {i} not adding up: {fracTotal}')

    verbandUsage = meterData.loc[i, 'Home1_verbandUsage_Wh'] + meterData.loc[i, 'Home2_verbandUsage_Wh'] + meterData.loc[i, 'Allg_verbandUsage_Wh'] 
    verbandProd = meterData.loc[i, 'Home1_verbandProd_Wh'] + meterData.loc[i, 'Home2_verbandProd_Wh'] + meterData.loc[i, 'Allg_verbandProd_Wh']
    if (verbandUsage - verbandProd) > 0.001 and (verbandUsage - verbandProd) < -0.001:
        print(f'verband Eigenverbrauch not adding up at index {i}: {(verbandUsage - verbandProd)}')

    for user in ['Home1', 'Home2', 'Allg']:
        error = meterData.loc[i, f'{user}_sold_Wh'] + meterData.loc[i, f'{user}_verbandProd_Wh'] - meterData.loc[i, f'{user}_Prod_Wh']
        if error < -0.001 and error > 0.001:
            print(f'production not even on index {i} with {error}')
            
        error = meterData.loc[i, f'{user}_bought_Wh'] + meterData.loc[i, f'{user}_verbandUsage_Wh'] - meterData.loc[i, f'{user}_Usage_Wh']
        if error < -0.001 and error > 0.001:
            print(f'usage not even on index {i} with {error}')

print('===============================')
print('Results:')   
for user in ['Home1', 'Home2', 'Allg']:
    print(f' {user}:')
    data = int(meterData[f'{user}_bought_Wh'].sum()/1000)
    print(f'       EW Bezug: {data} kWh')
    data = int(meterData[f'{user}_sold_Wh'].sum()/1000)
    print(f'     EW Verkauf: {data} kWh')
    data = int(meterData[f'{user}_verbandUsage_Wh'].sum()/1000)
    print(f'      ZEV Bezug: {data} kWh')
    data = int(meterData[f'{user}_verbandProd_Wh'].sum()/1000)
    print(f'     ZEV Einsp.: {data} kWh')
    data = int(meterData[f'{user}_eigenV_Wh'].sum()/1000)
    print(f'  Eigenverbauch: {data} kWh')
    print('')

#print(meterData.loc[150])