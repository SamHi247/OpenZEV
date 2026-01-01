import pandas as pd
import threading
import logging
import json
import datetime
from typing import List
from libs.Meter.EmuMeterClass import EmuMeter
from libs.Meter.meterClass import Meter

def readOutMeterThread(
    meter: Meter,
    start_epoch_time: int, 
    stop_epoch_time: int, 
    results: dict,
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
        meter.log.info(" Data read from cache.")
    except OSError:
        # download from meter and save to cache
        data = meter.read(start_epoch_time, stop_epoch_time)
        data.to_pickle(f"cache/{meter.name}_{start_epoch_time}_{stop_epoch_time}.secret")
        meter.log.info(" Data cached.")

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
    if start_epoch_time >= stop_epoch_time:
        raise ValueError("The start-time has to be earlier than the stop-time.")
    if meter_list.__len__() == 0:
        raise MemoryError("At least one meter has to be given.")

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
        log.debug(f" Thread to read meter \"{meter.name}\" started.")

    # wait for threads to be done
    try:
        threadsAlive = True
        while threadsAlive:
            threadsAlive = False
            for thread in threads:
                threadsAlive = threadsAlive or thread.is_alive()
                thread.join(1)
    except KeyboardInterrupt:
        log.warning(" Meter read has been canceled")
        return pd.DataFrame()

    # merge data in to one DataFrame
    meterData = pd.DataFrame()
    for meter in meter_list:
        try:
            meterData = pd.merge(meterData, 
                                    results[meter.name], 
                                    on="Timestamp", 
                                    how="outer",
                                    )
        except KeyError:
            meterData = results[meter.name]
        log.debug(f" Data from meter \"{meter.name}\" merged.")

    # calculate diff
    meterData.set_index("Timestamp", inplace=True)
    meterData = meterData.diff()
    meterData.reset_index(inplace=True)

    return meterData

def combineMeters(energyDF: pd.DataFrame, importMeter: str, exportMeter: str):

    # production < consumtion
    energyDF.loc[energyDF[f"{exportMeter}_Export_Wh"] < energyDF[f"{importMeter}_Import_Wh"], "temp_Export_Wh"] = 0
    energyDF.loc[energyDF[f"{exportMeter}_Export_Wh"] < energyDF[f"{importMeter}_Import_Wh"], "temp_Import_Wh"] = energyDF[f"{importMeter}_Import_Wh"] - energyDF[f"{exportMeter}_Export_Wh"]

    # production > consumtion
    energyDF.loc[energyDF[f"{exportMeter}_Export_Wh"] > energyDF[f"{importMeter}_Import_Wh"], "temp_Export_Wh"] = energyDF[f"{exportMeter}_Export_Wh"] - energyDF[f"{importMeter}_Import_Wh"]
    energyDF.loc[energyDF[f"{exportMeter}_Export_Wh"] > energyDF[f"{importMeter}_Import_Wh"], "temp_Import_Wh"] = 0

    energyDF[f"{importMeter}_Import_Wh"] = energyDF["temp_Import_Wh"]
    energyDF[f"{importMeter}_Export_Wh"] = energyDF["temp_Export_Wh"]

    energyDF[f"{exportMeter}_Import_Wh"] = 0
    energyDF[f"{exportMeter}_Export_Wh"] = 0

    return energyDF

def calculate(energyDF: pd.DataFrame, userMeter_list: List[str]):
    """
    Calculates energy distribution, consumption, and production for each user meter.

    This function computes total production and consumption, energy sold and bought via the energy provider (EW),
    internal energy exchanges between users, and meter error. It adds new columns to the DataFrame for
    each user's bought/sold energy, internal energy transactions, and overall statistics.

    Args:
        energyDF (pd.DataFrame): DataFrame containing energy readings for all meters.
        userMeter_list (List[str]): List of user Meter names (excluding EW).

    Returns:
        pd.DataFrame: The input DataFrame with additional columns for energy calculations and statistics.
    """
    # calculation derived from here: 
    # https://www.bulletin.ch/de/news-detail/gerecht-abrechnen-bei-eigenverbrauch.html
    
    # calc total production and consumption
    energyDF["totalProd_Wh"] = 0  
    energyDF["totalUse_Wh"] = 0
    for meter in userMeter_list:
        energyDF["totalProd_Wh"] += energyDF[f"{meter}_Export_Wh"]
        energyDF["totalUse_Wh"] += energyDF[f"{meter}_Import_Wh"]

    energyDF["eigenV_Wh"] = energyDF["totalProd_Wh"] - energyDF["ewMeter_Export_Wh"]
    for meter in userMeter_list:
        # calc energy sold and bought
        energyDF[f"{meter}_EnSold_Wh"] = energyDF["ewMeter_Export_Wh"] * (
            energyDF[f"{meter}_Export_Wh"] / energyDF["totalProd_Wh"])
        energyDF[f"{meter}_EnBought_Wh"] = energyDF["ewMeter_Import_Wh"] * (
            energyDF[f"{meter}_Import_Wh"] / energyDF["totalUse_Wh"])

        # calc eigenverbrauch enregy (can be combined with energy sold)
        energyDF[f"{meter}_EnSoldInt_Wh"] = energyDF["eigenV_Wh"] * (
            energyDF[f"{meter}_Export_Wh"] / energyDF["totalProd_Wh"])

    # calc eigenverbrauch energy
    for meter in userMeter_list:
        energyDF[f"{meter}_EnBoughtInt_Wh"] = (
            energyDF[f"{meter}_Import_Wh"] - energyDF[f"{meter}_EnBought_Wh"])
        internalFremdProduktion = 0
        for meter2 in userMeter_list:
            if not (meter == meter2):
                internalFremdProduktion += energyDF[f"{meter2}_EnSoldInt_Wh"]
        for meter2 in userMeter_list:
            if not (meter == meter2):
                energyDF[f"{meter2}_2_{meter}_EnBoughtInt_Wh"] = (
                    energyDF[f"{meter2}_EnSoldInt_Wh"] * (
                        energyDF[f"{meter}_EnBoughtInt_Wh"] / internalFremdProduktion))

    # calculate meter error    
    energyDF["EnergyIn_Wh"] = energyDF["totalProd_Wh"] + energyDF["ewMeter_Import_Wh"]
    energyDF["EnergyOut_Wh"] = energyDF["totalUse_Wh"] + energyDF["ewMeter_Export_Wh"]
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

def menu(options: dict, question: str):
    print(
        "========================================================================================"
    )
    print(question)
    for key in options.keys():
        print(f"   {key}: {options[key]}")
    sel = input("Please enter selection: ")
    print(
        "========================================================================================"
    )
    try:
        return options[sel]
    except KeyError:
        return sel

def meterConfig(confData: dict):
    zaehlerMenu = {
        "1" : "Verbraucher-Zähler auflisten",
        "2" : "Verbraucher-Zähler entfernen",
        "3" : "Verbraucher-Zähler hinzufügen/editieren",
        "5" : "EW-Zähler anzeigen",
        "6" : "EW-Zähler Adresse setzten",
        "9" : "zurück",
    }
    while True:
        answer = menu(zaehlerMenu, "Zähler/ Bitte wähle eine Aktion")
        if answer == "zurück":
            break
        elif answer == "Verbraucher-Zähler auflisten":
            print("Verbraucher-Zähler:")
            try:
                for meter in confData["meters"].keys():
                    print(f" -    Name: {meter}")
                    adr = confData["meters"][meter]
                    print(f"   Adresse: {adr}")
            except KeyError:
                print("Es gibt noch keine Zähler")
        elif answer == "Verbraucher-Zähler entfernen":
            meterName = input("Zähler-Name der entfernt werden soll: ")
            try:
                confData["meters"].pop(meterName)
            except KeyError:
                print(f"Es existiert kein Zähler mit dem Name \"{meterName}\".")
        elif answer == "Verbraucher-Zähler hinzufügen/editieren":
            meterName = input("Zähler-Name: ")
            meterAdr = input("Zähler-Adresse: ")
            confData["meters"][meterName] = meterAdr
        elif answer == "EW-Zähler anzeigen":
            print("EW-Zähler:")
            try:
                adr = confData["ewMeter"]
                print(f" - Adresse: {adr}")
            except KeyError:
                print("Adresse wurde noch nicht gesetzt")
        elif answer == "EW-Zähler Adresse setzten":
            meterAdr = input("EW-Zähler-Adresse: ")
            confData["ewMeter"] = meterAdr
        else:
            print(f"Auswahl \"{answer}\" ist ungültig")

    return confData

def readMeters(confData: dict):
    print("Tip: Vom eingegebenen Datum wird immer Mitternacht angenommen. Für 1 Jahr")
    print("     wäre das Start-, und Enddatum also jehweils dasselbe, ausser dem Jahr")
    startTime = datetime.datetime.strptime(input("Bitte Startdatum der Auslesung im Format \"1.1.1970\" eingeben: "),"%d.%m.%Y")
    stopTime = datetime.datetime.strptime(input("Bitte Enddatum der Auslesung im Format \"1.1.1971\" eingeben: "),"%d.%m.%Y")
    
    meter_list = []
    for meter in confData["meters"].keys():
        newMeter = EmuMeter(confData["meters"][meter], meter)
        meter_list.append(newMeter)
    meter_list.append(EmuMeter(confData["ewMeter"], "ewMeter"))
        
    return getEnergyData(datetime.datetime.timestamp(startTime), datetime.datetime.timestamp(stopTime), meter_list)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    log = logging.getLogger("Main")

    mainMenu = {
        "1" : "Zähler konfigurieren",
        "2" : "Zähler auslesen",
        "3" : "Zähler kombinieren",
        "4" : "Abrechnen",
        "9" : "beenden",
    }

    try:
        with open("src/confData.secret", "r") as file:
                confData = json.load(file)
    except Exception:
        confData = {
            "meters" : {},
            "ewMeter" : "",
        }

    while True:
        answer = menu(mainMenu, "Bitte wähle eine Aktion")
        if answer == "beenden":
            with open("src/confData.secret", 'w', encoding='utf-8') as f:
                json.dump(confData, f, ensure_ascii=False, indent=4)
            break
        elif answer == "Zähler konfigurieren":
            confData = meterConfig(confData)
        elif answer == "Zähler auslesen":
            data = readMeters(confData)
        elif answer == "Zähler kombinieren":
            meter1 = input("Verbrauchszähler der kombiniert werden soll: ")
            meter2 = input("Produktionszähler der kombiniert werden soll: ")
            try:
                data = combineMeters(data, meter1, meter2)
            except KeyError:
                print(f"Einer der Zählernamen \"{meter1}\" oder \"{meter2}\" ist ungültig.")
        elif answer == "Abrechnen":
            data = calculate(data, confData["meters"].keys())
            displayResults(data, confData["meters"].keys())
            data.to_csv("output.csv", index=False, sep=';')
        else:
            print(f"Auswahl \"{answer}\" ist ungültig")
