import pandas as pd
import threading
import sys
from libs.Meter.EmuMeterClass import EmuMeter

class EnergyCal():
    def __init__(self):
        pass

    def calculateData():
        pass

    def readOutMeterThread(
        self, 
        name, 
        host, 
        start_epoch_time, 
        stop_epoch_time, 
        results,
        ):
        """
        Reads meter data for a given meter within a specified time range, using cache if available.

        Attempts to load meter data from a local cache file. If the cache is missing or invalid,
        downloads the data from the meter device, saves it to cache, and updates the results dictionary.

        Args:
            name (str): Name of the meter.
            host (str): Host address of the meter.
            start_epoch_time (int): Start time in epoch seconds.
            stop_epoch_time (int): Stop time in epoch seconds.
            results (dict): Dictionary to store the resulting DataFrame, keyed by meter name.

        Returns:
            None. The results dictionary is updated in place with the meter data as a pandas DataFrame.
        """

        try:
            # try to read from cache
            data = pd.read_pickle(f"cache/{name}_{start_epoch_time}_{stop_epoch_time}.secret")
        except Exception:
            # download from meter and save to cache
            meter = EmuMeter(host, name)
            data = meter.read(start_epoch_time, stop_epoch_time)
            data.to_pickle(f"cache/{name}_{start_epoch_time}_{stop_epoch_time}.secret")

        results[name] = data

    def getEnergyData(
        self, 
        start_epoch_time: int,
        stop_epoch_time: int,
        meters: dict,
        ):
        """
        Collects and combines energy data from multiple meters over a specified time range.

        Spawns threads to read data from each meter (using cache if available), renames columns for clarity,
        merges all meter data into a single DataFrame, and calculates the difference between consecutive readings.

        Args:
            start_epoch_time (int): Start time in epoch seconds.
            stop_epoch_time (int): Stop time in epoch seconds.
            meters (dict): Dictionary of meters in the format {"name": "host", ...}.

        Returns:
            pd.DataFrame: Combined DataFrame containing energy data from all meters, with columns renamed and differences calculated.
        """

        results = {}
        threads = []

        # start downloads as threads
        for key in meters.keys():
            thread = threading.Thread(
                target=self.readOutMeterThread,
                args=(
                    key, 
                    meters[key], 
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
        for key in meters.keys():
            results[key].rename(
                columns={
                    "Energy_Import_Wh": f"{key}_meterIn_Wh",
                    "Energy_Export_Wh": f"{key}_meterOut_Wh",
                },
                inplace=True,
            )

        # merge data in to one DataFrame
        meterData = pd.DataFrame()
        for key in meters.keys():
            try:
                meterData = pd.merge(meterData, 
                                     results[key], 
                                     on="Timestamp", 
                                     how="outer",
                                     )
            except Exception:
                meterData = results[key]

        # calculate diff
        meterData.set_index("Timestamp", inplace=True)
        meterData = meterData.diff()
        meterData.reset_index(inplace=True)

        return meterData
    
