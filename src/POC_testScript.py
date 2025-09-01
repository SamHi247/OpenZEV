import json
import pandas as pd
from tqdm import tqdm
from libs.Calculator.EnergyCalClass import EnergyCal
import logging





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
                energyDF.loc[i, f"{key}_meterIn_Wh"] += (
                    energyDF.loc[i, f"{solarKey}_meterIn_Wh"] * ownershipKey[key]
                )
                energyDF.loc[i, f"{key}_meterIn_Wh"] -= (
                    energyDF.loc[i, f"{solarKey}_meterOut_Wh"] * ownershipKey[key]
                )

                if energyDF.loc[i, f"{key}_meterIn_Wh"] < 0:
                    energyDF.loc[i, f"{key}_meterOut_Wh"] += (
                        energyDF.loc[i, f"{key}_meterIn_Wh"] * -1
                    )
                    energyDF.loc[i, f"{key}_meterIn_Wh"] = 0

            # calculate total energy consumtion and production
            totalConsumedEnergy += energyDF.loc[i, f"{key}_meterIn_Wh"]
            totalProducedEnergy += energyDF.loc[i, f"{key}_meterOut_Wh"]

        # calc sold and bought energy fractions on energy bus
        if totalConsumedEnergy != 0:
            boughtEnergyFraction = (
                energyDF.loc[i, f"{ewKey}_meterIn_Wh"] / totalConsumedEnergy
            )
        else:
            boughtEnergyFraction = 1 / len(consumerKeys)

        if totalProducedEnergy != 0:
            soldEnergyFraction = (
                energyDF.loc[i, f"{ewKey}_meterOut_Wh"] / totalProducedEnergy
            )
        else:
            soldEnergyFraction = 0

        # split up energy
        for key in consumerKeys:
            # used energy
            energyDF.loc[i, f"{key}_boughtEn"] = (
                energyDF.loc[i, f"{key}_meterIn_Wh"] * boughtEnergyFraction
            )
            energyDF.loc[i, f"{key}_zevIn"] = energyDF.loc[i, f"{key}_meterIn_Wh"] * (
                1 - boughtEnergyFraction
            )
            # produced energy
            energyDF.loc[i, f"{key}_soldEn"] = (
                energyDF.loc[i, f"{key}_meterOut_Wh"] * soldEnergyFraction
            )
            energyDF.loc[i, f"{key}_zevOut"] = energyDF.loc[i, f"{key}_meterOut_Wh"] * (
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
    calculator = EnergyCal()

    # Juli
    # START_TIME = 1719784800
    # STOP_TIME = 1722463200

    # 10.Juni-24.Dez
    # START_TIME = 1718056800
    # STOP_TIME = 1735081200

    # 1.Sept - 24.Dez
    # START_TIME = 1725062400
    # STOP_TIME = 1735081200

    #1.Sept - 31.Dez
    #START_TIME = 1725141600
    #STOP_TIME = 1735686000

    #25.Mai - 31.Dez
    #START_TIME = 1716588000
    #STOP_TIME = 1735686000
    
    # 01.Jab - 31.März
    START_TIME = 1735686000
    STOP_TIME = 1743372000

    OW_KEY = {"Home1": 0, "Home2": 1, "Allg": 0}

    with open("src/POC_meters.secret", "r") as file:
        # print(json.load(file)[name])
        # load IP-address of a testhose from secret json file.
        # testHost.secret example content: {"host01": "XXX.XXX.XXX.XXX"}
        meters = json.load(file)

    data = calculator.getEnergyData(START_TIME, STOP_TIME, meters)
    print(
        "========================================================================================"
    )
    data = calculate(data, "Solar", "EW", ["Home1", "Home2", "Allg"], OW_KEY)
    print(
        "========================================================================================"
    )
    displayResults(data, ["Home1", "Home2", "Allg"])
