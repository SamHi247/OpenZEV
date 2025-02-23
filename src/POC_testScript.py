import json
import pandas as pd
import threading
from tqdm import tqdm
from libs.Meter.EmuMeterClass import EmuMeter
import sys

def readOutMeterThread(name, host, startTime, stopTime, results):
    print(f'Start loading {name} data...')
    try:
        # try to read from cache
        data = pd.read_pickle(f"cache/{name}_{startTime}_{stopTime}.secret")
    except:
        # download from meter and save to cache
        meter = EmuMeter(host)
        data = meter.read(startTime,stopTime)
        data.to_pickle(f"cache/{name}_{startTime}_{stopTime}.secret")

    results[name] = data
    print(f'Loading of {name} data done.')

def getEnergyData(startTime, stopTime):
    meters = {}
    results = {}
    threads = []

    with open('src/POC_meters.secret', 'r') as file:
        #print(json.load(file)[name])
        # load IP-address of a testhose from secret json file.
        # testHost.secret example content: {"host01": "XXX.XXX.XXX.XXX"}
        meters = json.load(file)

    # start downloads as threads
    for key in meters.keys():
        thread = threading.Thread(target=readOutMeterThread, args=(key, meters[key], startTime, stopTime, results))
        thread.daemon = True
        threads.append(thread)
        thread.start()

    # wait for threads to be done
    try:
        threadsAlive = True
        while threadsAlive:
            threadsAlive = False
            for thread in threads:
                threadsAlive = threadsAlive or thread.is_alive()
                thread.join(1)
    except KeyboardInterrupt:
        sys.exit(1)

    print('Data complete...')

    # rename columns
    for key in meters.keys():
        results[key].rename(columns={'Energy_Import_Wh': f'{key}_meterIn', 
                                    'Energy_Export_Wh': f'{key}_meterOut'}, inplace=True)
    
    # merge data in to one DataFrame
    meterData = pd.DataFrame()
    for key in meters.keys():
        try:
            meterData = pd.merge(meterData,results[key],on='Timestamp',how='outer')
        except:
            meterData = results[key]
    
    # calculate diff
    meterData.set_index('Timestamp', inplace=True)
    meterData = meterData.diff()
    meterData.reset_index(inplace=True)

    return meterData

def calculate(energyDF, solarKey, ewKey, consumerKeys, ownershipKey):
    for i in tqdm(range(len(energyDF)), bar_format='{l_bar}{bar:20}{r_bar}', 
                  desc ="Calculating energy data", smoothing = 0.1, mininterval = 0.5):
        
        # solar energy aufteilen
        for key in consumerKeys:
            if ownershipKey[key] > 0:
                energyDF.loc[i,f'{key}_meterIn'] += (energyDF.loc[i,f'{solarKey}_meterIn'] * ownershipKey[key])
                energyDF.loc[i,f'{key}_meterIn'] -= (energyDF.loc[i,f'{solarKey}_meterOut'] * ownershipKey[key])

                if energyDF.loc[i, f'{key}_meterIn'] < 0:
                    energyDF.loc[i, f'{key}_meterOut'] += (energyDF.loc[i, f'{key}_meterIn'] * -1)
                    energyDF.loc[i, f'{key}_meterIn'] = 0

        # calculate energy fraction
        totalConsumedEnergy = 0
        totalProducedEnergy = 0
        for key in consumerKeys:
            totalConsumedEnergy += energyDF.loc[i,f'{key}_meterIn']
            totalProducedEnergy += energyDF.loc[i,f'{key}_meterOut']

        '''if energyDF.loc[i,f'{ewKey}_meterOut'] > totalProducedEnergy:
            #eigenUsage = -energyDF.loc[i,f'{ewKey}_meterOut']
            diff = energyDF.loc[i,f'{ewKey}_meterOut'] - totalProducedEnergy
            print(f'Alarm: {diff}')'''

        for key in consumerKeys:
            if totalConsumedEnergy != 0:
                energyDF.loc[i,f'{key}_usedFrac'] = energyDF.loc[i,f'{key}_meterIn'] / totalConsumedEnergy
            else:
                energyDF.loc[i,f'{key}_usedFrac'] = 1/len(consumerKeys)

            if totalProducedEnergy != 0:
                energyDF.loc[i,f'{key}_prodFrac'] = energyDF.loc[i,f'{key}_meterOut'] / totalProducedEnergy
            else:
                energyDF.loc[i,f'{key}_prodFrac'] = ownershipKey[key]

            # split up energy     
            energyDF.loc[i,f'{key}_boughtEn'] = energyDF.loc[i,f'{ewKey}_meterIn'] * energyDF.loc[i,f'{key}_usedFrac']
            energyDF.loc[i,f'{key}_soldEn'] = energyDF.loc[i,f'{ewKey}_meterOut'] * energyDF.loc[i,f'{key}_prodFrac']

            energyDF.loc[i,f'{key}_zevIn'] = energyDF.loc[i,f'{key}_meterIn'] - energyDF.loc[i,f'{key}_boughtEn']
            energyDF.loc[i,f'{key}_zevOut'] = energyDF.loc[i,f'{key}_meterOut'] - energyDF.loc[i,f'{key}_soldEn']

    return energyDF

def displayResults(energyDF, consumerKeys):
    # print individual user stats
    print(energyDF)

    for user in consumerKeys:
        print(f' {user}:')
        data = int(energyDF[f'{user}_boughtEn'].sum()/1000)
        print(f'       EW Bezug: {data} kWh')
        data = int(energyDF[f'{user}_soldEn'].sum()/1000)
        print(f'     EW Verkauf: {data} kWh')
        data = int(energyDF[f'{user}_zevIn'].sum()/1000)
        print(f'      ZEV Bezug: {data} kWh')
        data = int(energyDF[f'{user}_zevOut'].sum()/1000)
        print(f'     ZEV Einsp.: {data} kWh')
        print('')

    # print overall stats
    print(' Stats:')
    data = 0
    for user in consumerKeys:
        data += int(energyDF[f'{user}_boughtEn'].sum()/1000)
    print(f'       EW Bezug: {data} kWh')
    data = 0
    for user in consumerKeys:
        data += int(energyDF[f'{user}_soldEn'].sum()/1000)
    print(f'      EW Einsp.: {data} kWh')
    for user in consumerKeys:
        print(f'          {user}:')
        data = int(energyDF[f'{user}_boughtEn'].sum()/1000) + int(energyDF[f'{user}_zevIn'].sum()/1000)
        print(f'               used: {data} kWh')
        data = int(energyDF[f'{user}_soldEn'].sum()/1000) + int(energyDF[f'{user}_zevOut'].sum()/1000)
        print(f'            prodced: {data} kWh')

if __name__ == '__main__':
    #Juli
    #START_TIME = 1719784800
    #STOP_TIME = 1722463200

    #10.Juni-24.Dez
    #START_TIME = 1718056800
    #STOP_TIME = 1735081200

    #1.Sept - 24.Dez
    #START_TIME = 1725062400
    #STOP_TIME = 1735081200

    #1.Sept - 31.Dez
    #START_TIME = 1725141600
    #STOP_TIME = 1735686000

    #25.Mai - 31.Dez
    START_TIME = 1716588000
    STOP_TIME = 1735686000

    OW_KEY = {"Home1":0,
              "Home2":1,
              "Allg":0}

    data = getEnergyData(START_TIME, STOP_TIME)
    print('========================================================================================')
    data = calculate(data,'Solar', 'EW', ['Home1', 'Home2', 'Allg'],OW_KEY)
    print('========================================================================================')
    displayResults(data,['Home1', 'Home2', 'Allg'])