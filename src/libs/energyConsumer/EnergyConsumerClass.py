import libs.Meter.meterClass as Meter

import pandas as pd
from datetime import timedelta


class EnergyConsumer:
    meter = None
    productionOwnershipKey = None
    energyDataCache = pd.DataFrame

    def __init__(self, meter: Meter):
        self.meter = meter
        self.energyDataCache = pd.DataFrame(
            columns=["Timestamp", "Energy_Import_Wh", "Energy_Export_Wh"]
        )

    def getEnergyData(self, startEpochTime: int, stopEpochTime: int):
        """complete cached data if necessray and return data

        Args:
            startEpochTime (int): startTime in Epoch
            stopEpochTime (int): stopTime in Epoch

        Returns:
            pd.DataFrame: requested data as pandas DataFrame with the following columns:
                - "Timestamp"
                - "Energy_Import_Wh"
                - "Energy_Export_Wh"
        """
        missingDataSlots = self.findMissingDataSlots(startEpochTime, stopEpochTime)

        for slot in missingDataSlots:
            newData = self.meter.read(slot[0], slot[1])
            self.energyDataCache = pd.merge(
                self.energyDataCache,
                newData,
                "outer",
                on=["Timestamp", "Energy_Import_Wh", "Energy_Export_Wh"],
            )

        returnData = self.energyDataCache.set_index("Timestamp")
        returnData = returnData.loc[
            pd.to_datetime(startEpochTime, unit="s") : pd.to_datetime(
                stopEpochTime, unit="s"
            )
        ]
        returnData = returnData.reset_index()

        return returnData

    def findMissingDataSlots(self, startEpochTime: int, stopEpochTime: int):
        """Search for missing data in cached data and get the beginning and end of these
        missing slots

        Args:
            startEpochTime (int): startTime in Epoch
            stopEpochTime (int): stopTime in Epoch

        Raises:
            ValueError: if the start time is after the stop time

        Returns:
            int[[]]: array of start/stop arrays for each slot
        """
        if startEpochTime >= stopEpochTime:
            raise ValueError(
                f"The startTime of {startEpochTime} is anfter the stopTime of {stopEpochTime}."
            )

        if self.energyDataCache.shape[0] > 2:
            # get existing data from cache
            cachedDataInSlot = self.energyDataCache.set_index("Timestamp")
            cachedDataInSlot = cachedDataInSlot.loc[
                pd.to_datetime(startEpochTime, unit="s") : pd.to_datetime(
                    stopEpochTime, unit="s"
                )
            ]
            cachedDataInSlot = cachedDataInSlot.reset_index()

            # calculate time differences and note the ones larger than 20 min
            timeDeltas = cachedDataInSlot["Timestamp"].diff()[1:]
            gaps = timeDeltas[timeDeltas > timedelta(minutes=20)]

            if not gaps.shape[0] == 0:
                # format data for return
                missingSlots = []
                for i, g in gaps.items():
                    missingSlots.append(
                        [
                            cachedDataInSlot["Timestamp"].iloc[i - 1].timestamp(),
                            cachedDataInSlot["Timestamp"].iloc[i].timestamp(),
                        ]
                    )
        else:
            missingSlots = [[startEpochTime, stopEpochTime]]

        return missingSlots
