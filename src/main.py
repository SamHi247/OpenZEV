import pandas as pd
import threading
import sys
import logging
import json
from typing import List
from libs.Meter.EmuMeterClass import EmuMeter
from libs.Meter.meterClass import Meter

def readOutMeterThread(
    meter: Meter,
    start_epoch_time, 
    stop_epoch_time, 
    results,
    ):
    """
    Reads meter data for a given meter within a specified time range, using cache if available.

    Attempts to load meter data from a local cache file. If the cache is missing or invalid,
    downloads the data from the meter device, saves it to cache, and updates the results dictionary.

    Args:
        meter (Meter): Meter object to read data from.
        start_epoch_time (int): Start time in epoch seconds.
        stop_epoch_time (int): Stop time in epoch seconds.
        results (dict): Dictionary to store the resulting DataFrame, keyed by meter name.

    Returns:
        None. The results dictionary is updated in place with the meter data as a pandas DataFrame.
    """

    try:
        # try to read from cache
        data = pd.read_pickle(f"cache/{meter.name}_{start_epoch_time}_{stop_epoch_time}.secret")
    except Exception:
        # download from meter and save to cache
        data = meter.read(start_epoch_time, stop_epoch_time)
        data.to_pickle(f"cache/{meter.name}_{start_epoch_time}_{stop_epoch_time}.secret")

    results[meter.name] = data

def getEnergyData(
    start_epoch_time: int,
    stop_epoch_time: int,
    meter_list: List[Meter],
    ):
    """
    Collects and combines energy data from multiple meters over a specified time range.

    Spawns threads to read data from each meter (using cache if available), renames columns for 
    clarity, merges all meter data into a single DataFrame, and calculates the difference between 
    consecutive readings.

    Args:
        start_epoch_time (int): Start time in epoch seconds.
        stop_epoch_time (int): Stop time in epoch seconds.
        meters (List[Meter]): List of Meter objects.

    Returns:
        pd.DataFrame: Combined DataFrame containing energy data from all meters, with columns 
        renamed and differences calculated.
    """

    results = {}
    threads = []

    # start downloads as threads
    for meter in meter_list:
        thread = threading.Thread(
            target=readOutMeterThread,
            args=(
                meter,
                start_epoch_time, 
                stop_epoch_time, 
                results,
            ),
        )
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

    # rename columns
    for meter in meter_list:
        results[meter.name].rename(
            columns={
                "Energy_Import_Wh": f"{meter.name}_meterIn_Wh",
                "Energy_Export_Wh": f"{meter.name}_meterOut_Wh",
            },
            inplace=True,
        )

    # merge data in to one DataFrame
    meterData = pd.DataFrame()
    for meter in meter_list:
        try:
            meterData = pd.merge(meterData, 
                                    results[meter.name], 
                                    on="Timestamp", 
                                    how="outer",
                                    )
        except Exception:
            meterData = results[meter.name]

    # calculate diff
    meterData.set_index("Timestamp", inplace=True)
    meterData = meterData.diff()
    meterData.reset_index(inplace=True)

    return meterData

def calculate(energyDF: pd.DataFrame, userMeter_list: List[Meter], ewMeter: Meter):
    """
    Calculates energy distribution, consumption, and production for each user meter.

    This function computes total production and consumption, energy sold and bought via the energy bus (EW),
    internal energy exchanges between users, and meter error. It adds new columns to the DataFrame for
    each user's bought/sold energy, internal energy transactions, and overall statistics.

    Args:
        energyDF (pd.DataFrame): DataFrame containing energy readings for all meters.
        userMeter_list (List[Meter]): List of user Meter objects (excluding EW).
        ewMeter (Meter): Meter object representing the energy bus (EW).

    Returns:
        pd.DataFrame: The input DataFrame with additional columns for energy calculations and statistics.
    """
    # calculation derived from here: 
    # https://www.bulletin.ch/de/news-detail/gerecht-abrechnen-bei-eigenverbrauch.html
    
    # calc total production and consumption
    energyDF["totalProd_Wh"] = 0  
    energyDF["totalUse_Wh"] = 0
    for meter in userMeter_list:
        energyDF["totalProd_Wh"] += energyDF[f"{meter.name}_meterOut_Wh"]
        energyDF["totalUse_Wh"] += energyDF[f"{meter.name}_meterIn_Wh"]

    energyDF["eigenV_Wh"] = energyDF["totalProd_Wh"] - energyDF[f"{ewMeter.name}_meterOut_Wh"]
    for meter in userMeter_list:
        # calc energy sold and bought
        energyDF[f"{meter.name}_EnSold_Wh"] = energyDF[f"{ewMeter.name}_meterOut_Wh"] * (
            energyDF[f"{meter.name}_meterOut_Wh"] / energyDF["totalProd_Wh"])
        energyDF[f"{meter.name}_EnBought_Wh"] = energyDF[f"{ewMeter.name}_meterIn_Wh"] * (
            energyDF[f"{meter.name}_meterIn_Wh"] / energyDF["totalUse_Wh"])

        # calc eigenverbrauch enregy (can be combined with energy sold)
        energyDF[f"{meter.name}_EnSoldInt_Wh"] = energyDF["eigenV_Wh"] * (
            energyDF[f"{meter.name}_meterOut_Wh"] / energyDF["totalProd_Wh"])

    # calc eigenverbrauch energy
    for meter in userMeter_list:
        energyDF[f"{meter.name}_EnBoughtInt_Wh"] = (
            energyDF[f"{meter.name}_meterIn_Wh"] - energyDF[f"{meter.name}_EnBought_Wh"])
        internalFremdProduktion = 0
        for meter2 in userMeter_list:
            if not (meter.name == meter2.name):
                internalFremdProduktion += energyDF[f"{meter2.name}_EnSoldInt_Wh"]
        for meter2 in userMeter_list:
            if not (meter.name == meter2.name):
                energyDF[f"{meter2.name}_2_{meter.name}_EnBoughtInt_Wh"] = (
                    energyDF[f"{meter2.name}_EnSoldInt_Wh"] * (
                        energyDF[f"{meter.name}_EnBoughtInt_Wh"] / internalFremdProduktion))

    # calculate meter error    
    energyDF["EnergyIn_Wh"] = energyDF["totalProd_Wh"] + energyDF[f"{ewMeter.name}_meterIn_Wh"]
    energyDF["EnergyOut_Wh"] = energyDF["totalUse_Wh"] + energyDF[f"{ewMeter.name}_meterOut_Wh"]
    energyDF["EnergyError_Wh"] = energyDF["EnergyIn_Wh"] - energyDF["EnergyOut_Wh"]
    
    return energyDF

def displayResults(energyDF, consumerKeys):
    # print individual user stats

    for user in consumerKeys:
        print(f" {user}:")
        data = energyDF[f"{user}_EnBought_Wh"].sum() / 1000
        if data > 0: 
            print(f"       EW Bezug: {data:.3f} kWh")
        data = energyDF[f"{user}_EnSold_Wh"].sum() / 1000
        if data > 0: 
            print(f"     EW Verkauf: {data:.3f} kWh")
        data = energyDF[f"{user}_EnBoughtInt_Wh"].sum() / 1000
        if data > 0: 
            print(f"      ZEV Bezug: {data:.3f} kWh")
        for user2 in consumerKeys:
            if not (user == user2):
                data = energyDF[f"{user2}_2_{user}_EnBoughtInt_Wh"].sum() / 1000
                if data > 0: 
                    print(f"         - von {user2}: {data:.3f} kWh")
        data = energyDF[f"{user}_EnSoldInt_Wh"].sum() / 1000
        if data > 0: 
            print(f"     ZEV Einsp.: {data:.3f} kWh")
        print("")

    # print overall stats
    print(" Stats:")
    data = 0
    for user in consumerKeys:
        data += energyDF[f"{user}_EnBought_Wh"].sum() / 1000
    if data > 0: 
        print(f"       EW Bezug: {data:.3f} kWh")
    data = 0
    for user in consumerKeys:
        data += energyDF[f"{user}_EnSold_Wh"].sum() / 1000
    if data > 0: 
        print(f"      EW Einsp.: {data:.3f} kWh")
    data = energyDF["EnergyError_Wh"].sum() / 1000
    if data > 0: 
        print(f"    Meter Error: {data:.3f} kWh")
    for user in consumerKeys:
        print(f"          {user}:")
        data = energyDF[f"{user}_EnBought_Wh"].sum() / 1000 + energyDF[f"{user}_EnBoughtInt_Wh"].sum() / 1000
        if data > 0: 
            print(f"               used: {data:.3f} kWh")
        data = energyDF[f"{user}_EnSold_Wh"].sum() / 1000 + energyDF[f"{user}_EnSoldInt_Wh"].sum() / 1000
        if data > 0: 
            print(f"            prodced: {data:.3f} kWh")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    with open("src/POC_meters.secret", "r") as file:
        # print(json.load(file)[name])
        # load IP-address of a testhose from secret json file.
        # testHost.secret example content: {"host01": "XXX.XXX.XXX.XXX"}
        meters_info = json.load(file)

    meter_list = []
    userMeter_list = []
    for meter in meters_info.keys():
        newMeter = EmuMeter(meters_info[meter], meter)
        meter_list.append(newMeter)
        if meter != "EW":
            userMeter_list.append(newMeter)
        else:
            ewMeter = newMeter
        
    data = getEnergyData(1756677600, 1756764000, meter_list)

    print(
        "========================================================================================"
    )
    
    data = calculate(data, userMeter_list, ewMeter)
    print(
        "========================================================================================"
    )
    displayResults(data, ["Home1", "Home2", "Allg", "Solar"])

    data.to_csv("output.csv", index=False, sep=';')
