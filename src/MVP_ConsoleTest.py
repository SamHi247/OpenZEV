import pandas as pd
import threading
import sys
import logging
import datetime
from tqdm import tqdm
from libs.Meter.EmuMeterClass import EmuMeter


def readOutMeterThread(name, host, startTime, stopTime, results):
    meter = EmuMeter(host, name)
    data = meter.read(startTime, stopTime)

    results[name] = data

def getEnergyData(startTime, stopTime):
    meters = {
        "Whg1" : input("Bitte die IP-Adresse des Whg1 Zählers eingeben: "),
        "Whg2" : input("Bitte die IP-Adresse des Whg2 Zählers eingeben: "),
        "Allg" : input("Bitte die IP-Adresse des Allg Zählers eingeben: "),
        "Solar" : input("Bitte die IP-Adresse des Solar Zählers eingeben: "),
        "EW": input("Bitte die IP-Adresse des EW Zählers eingeben: ")
    }
    results = {}
    threads = []


        # start downloads as threads
    for key in meters.keys():
        thread = threading.Thread(
            target=readOutMeterThread,
            args=(key, meters[key], startTime, stopTime, results),
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
    for key in meters.keys():
        results[key].rename(
            columns={
                "Energy_Import_Wh": f"{key}_meterIn",
                "Energy_Export_Wh": f"{key}_meterOut",
            },
            inplace=True,
        )

    # merge data in to one DataFrame
    meterData = pd.DataFrame()
    for key in meters.keys():
        try:
            meterData = pd.merge(meterData, results[key], on="Timestamp", how="outer")
        except Exception:
            meterData = results[key]

    # calculate diff
    meterData.set_index("Timestamp", inplace=True)
    meterData = meterData.diff()
    meterData.reset_index(inplace=True)

    return meterData

def calculate(energyDF, solarKey, ewKey, consumerKeys, ownershipKey):
    # calculation derived from here: https://www.bulletin.ch/de/news-detail/gerecht-abrechnen-bei-eigenverbrauch.html
    for i in tqdm(
        range(len(energyDF)),
        bar_format="{l_bar}{bar:20}{r_bar}",
        desc="Calculating energy data",
        smoothing=0.1,
        mininterval=0.5,
    ):
        totalConsumedEnergy = 0
        totalProducedEnergy = 0

        for key in consumerKeys:
            # split solar energy
            if ownershipKey[key] > 0:
                energyDF.loc[i, f"{key}_meterIn"] += (
                    energyDF.loc[i, f"{solarKey}_meterIn"] * ownershipKey[key]
                )
                energyDF.loc[i, f"{key}_meterIn"] -= (
                    energyDF.loc[i, f"{solarKey}_meterOut"] * ownershipKey[key]
                )

                if energyDF.loc[i, f"{key}_meterIn"] < 0:
                    energyDF.loc[i, f"{key}_meterOut"] += (
                        energyDF.loc[i, f"{key}_meterIn"] * -1
                    )
                    energyDF.loc[i, f"{key}_meterIn"] = 0

            # calculate total energy consumtion and production
            totalConsumedEnergy += energyDF.loc[i, f"{key}_meterIn"]
            totalProducedEnergy += energyDF.loc[i, f"{key}_meterOut"]

        # calc sold and bought energy fractions on energy bus
        if totalConsumedEnergy != 0:
            boughtEnergyFraction = (
                energyDF.loc[i, f"{ewKey}_meterIn"] / totalConsumedEnergy
            )
        else:
            boughtEnergyFraction = 1 / len(consumerKeys)

        if totalProducedEnergy != 0:
            soldEnergyFraction = (
                energyDF.loc[i, f"{ewKey}_meterOut"] / totalProducedEnergy
            )
        else:
            soldEnergyFraction = 0

        # split up energy
        for key in consumerKeys:
            # used energy
            energyDF.loc[i, f"{key}_boughtEn"] = (
                energyDF.loc[i, f"{key}_meterIn"] * boughtEnergyFraction
            )
            energyDF.loc[i, f"{key}_zevIn"] = energyDF.loc[i, f"{key}_meterIn"] * (
                1 - boughtEnergyFraction
            )
            # produced energy
            energyDF.loc[i, f"{key}_soldEn"] = (
                energyDF.loc[i, f"{key}_meterOut"] * soldEnergyFraction
            )
            energyDF.loc[i, f"{key}_zevOut"] = energyDF.loc[i, f"{key}_meterOut"] * (
                1 - soldEnergyFraction
            )

    return energyDF

def displayResults(energyDF, consumerKeys):
    # print individual user stats

    for user in consumerKeys:
        print(f" {user}:")
        data = int(energyDF[f"{user}_boughtEn"].sum() / 1000)
        print(f"       EW Bezug: {data} kWh")
        data = int(energyDF[f"{user}_soldEn"].sum() / 1000)
        print(f"     EW Verkauf: {data} kWh")
        data = int(energyDF[f"{user}_zevIn"].sum() / 1000)
        print(f"      ZEV Bezug: {data} kWh")
        data = int(energyDF[f"{user}_zevOut"].sum() / 1000)
        print(f"     ZEV Einsp.: {data} kWh")
        print("")

    # print overall stats
    print(" Stats:")
    data = 0
    for user in consumerKeys:
        data += int(energyDF[f"{user}_boughtEn"].sum() / 1000)
    print(f"       EW Bezug: {data} kWh")
    data = 0
    for user in consumerKeys:
        data += int(energyDF[f"{user}_soldEn"].sum() / 1000)
    print(f"      EW Einsp.: {data} kWh")
    for user in consumerKeys:
        print(f"          {user}:")
        data = int(energyDF[f"{user}_boughtEn"].sum() / 1000) + int(
            energyDF[f"{user}_zevIn"].sum() / 1000
        )
        print(f"               used: {data} kWh")
        data = int(energyDF[f"{user}_soldEn"].sum() / 1000) + int(
            energyDF[f"{user}_zevOut"].sum() / 1000
        )
        print(f"            prodced: {data} kWh")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print(
        "========================================================================================"
    )
    print("Der Eigentumsanteil der Whg1 und Whg2 muss in der Summe 1 ergeben!")
    whg1Ownership = float(input("Bitte Eigentumsanteil Whg1 eingeben (0-1): "))
    whg2Ownership = float(input("Bitte Eigentumsanteil Whg2 eingeben (0-1): "))
    OW_KEY = {"Whg1": whg1Ownership, "Whg2": whg2Ownership, "Allg": 0}

    startTime = datetime.datetime.strptime(input("Bitte Startdatum der Abrechung im Format 01/01/1970 eingeben: "),"%d/%m/%Y")
    stopTime = datetime.datetime.strptime(input("Bitte Enddatum der Abrechung im Format 01/01/1970 eingeben: "),"%d/%m/%Y")

    data = getEnergyData(datetime.datetime.timestamp(startTime), datetime.datetime.timestamp(stopTime))
    print(
        "========================================================================================"
    )
    data = calculate(data, "Solar", "EW", ["Whg1", "Whg2", "Allg"], OW_KEY)
    print(
        "========================================================================================"
    )
    displayResults(data, ["Whg1", "Whg2", "Allg"])