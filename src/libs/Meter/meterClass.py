import pandas as pd
import logging


class Meter:
    def __init__(self, meter_type: str, name: str, invert: bool = False):
        """Connect to a power meter

        Args:
            meterTpe (str): Name/Type of meter
            invert (bool, optional): set "True" if Import an export on this meter are reversed
        """
        self.meter_type = meter_type
        self.invert_energy_direction = invert

        self.log = logging.getLogger(f"{meter_type} | {name}")

    def read(self, start_epoch_time: int, stop_epoch_time: int) -> pd.DataFrame:
        """Read all entries in a range of epoch time. No size limit, exept what is available on the meter.

        Args:
            startEpochTime (int): startTime in epoch
            stopEpochTime (int): stopTime in epoch

        Returns:
            pd.DataFrame: requested data as pandas DataFrame with the following columns:
                - "Timestamp"
                - "Energy_Import_Wh"
                - "Energy_Export_Wh"
        """
        self.log.warning("This read function belongs to the base class. Please override this function in the meter specific implementation.")
        raise NotImplementedError()