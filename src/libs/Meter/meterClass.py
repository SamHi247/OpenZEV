import pandas as pd


class Meter:
    def __init__(self, meterType: str, invert: bool = False):
        """Connect to a power meter

        Args:
            meterTpe (str): Name/Type of meter
            invert (bool, optional): set "True" if Import an export on this meter are reversed
        """
        self.meterType = meterType
        self.invertEnergyDirection = invert

    def read(self, startEpochTime: int, stopEpochTime: int) -> pd.DataFrame:
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
        raise NotImplementedError()


class TcpMeter(Meter):
    def __init__(self, meterType: str, host: str, invert: bool = False):
        """Connect to a TCP power meter

        Args:
            meterTpe (str): Name/Type of meter
            host (str): hostname like IP-address of power meter
            invert (bool, optional): set "True" if Import an export on this meter are reversed
        """
        super().__init__(meterType, invert)
        self.hostName = host
