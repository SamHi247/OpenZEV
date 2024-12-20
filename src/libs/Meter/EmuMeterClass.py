import pandas as pd
import math

from libs.Meter.meterClass import TcpMeter

class EmuMeter(TcpMeter):
    LOG_INTERVAL = 15 * 60
    MAX_READBLOCK_SIZE = 3000

    currentIndex = None
    currentTime = None
    readBlockSize = None

    def __init__(self, host: str, invert: bool = False, readBlockSize: int = MAX_READBLOCK_SIZE):
        """Connect to a EMU Pro II power meter and set up all relevant Variables

        Args:
            host (str): hostname like IP-address of power meter
            invert (bool, optional): set "True" if Import an export on this meter are reversed
            readBlockSize (int, optional): Size of request blocks sent to meter. Max is 3000. Defaults to 3000.

        Raises:
            ValueError: if more than 3000 entries can be requested

        """
        if readBlockSize < 1 and readBlockSize > self.MAX_READBLOCK_SIZE:
            raise ValueError(f"A readBlockSize value of {readBlockSize} is not allowed. (0 < readBlockSize <= 3000")

        super().__init__("EMU Pro II",host,invert)

        self.readBlockSize = readBlockSize
        self.LOG_INTERVAL = 15 * 60

        # get last log entry from meter
        url = "http://" + host + "/data/?last=1"
        currentReading = pd.read_csv(url,delimiter=';')

        # set up variables
        currentReading['Timestamp'] = pd.to_datetime(currentReading['Timestamp'])
        self.currentTime = currentReading['Timestamp'][0].timestamp()
        self.currentIndex = currentReading['Index'][0]

    def readSingleBlock(self, startIndex: int, stopIndex: int):
        """Just read Entries from, to a specific index. The meter it self can't
        deliver more than 3000 entries at once and there are no negative idexes.

        Args:
            startIndex (int): first meter internal index do be read
            stopIndex (int): last miter internal index to be read

        Raises:
            ValueError: if more than 3000 entries are requested
            ValueError: if a negative index is requested

        Returns:
            pd.DataFrame: Dataframe of requested data
        """
        if stopIndex < 0 or startIndex < 0:
            raise ValueError(f"Negative Indexes are not allowed: start={startIndex}, stop={stopIndex}")
        if (stopIndex - startIndex) > self.MAX_READBLOCK_SIZE:
            raise ValueError(f"It is impossible to read more than 3000 entrys at once. ({(stopIndex - startIndex)})")
        
        # get data from meter
        url = "http://" + self.hostName + "/data/?from=" + str(startIndex) + "&to=" + str(stopIndex)
        rawMeterData = pd.read_csv(url,delimiter=';')

        # clean up data
        columnsToRemove = [ 'Index',
                            'Status',
                            'Serial',
                            'Active Energy Import L123 T2 [Wh]',
                            'Active Energy Export L123 T2 [Wh]',
                            'Reactive Energy Import L123 T1 [varh]',
                            'Reactive Energy Import L123 T2 [varh]',
                            'Reactive Energy Export L123 T1 [varh]',
                            'Reactive Energy Export L123 T2 [varh]',
                            'Active Power L123 [W]',
                            'Active Power L1 [W]',
                            'Active Power L2 [W]',
                            'Active Power L3 [W]',
                            'Current L123 [mA]',
                            'Current L1 [mA]',
                            'Current L2 [mA]',
                            'Current L3 [mA]',
                            'Current N [mA]',
                            'Voltage L1-N [1/10 V]',
                            'Voltage L2-N [1/10 V]',
                            'Voltage L3-N [1/10 V]',
                            'Powerfactor L1 [1/100]',
                            'Powerfactor L2 [1/100]',
                            'Powerfactor L3 [1/100]',
                            'Frequency [1/10 Hz]']
        data = rawMeterData.drop(columnsToRemove, axis= 1)
        data['Timestamp'] = pd.to_datetime(data['Timestamp'])
        
        if not (self.invertEnergyDirection):
            colMap = {"Timestamp": "Timestamp", 
                      "Active Energy Import L123 T1 [Wh]": "Energy_Import_Wh", 
                      "Active Energy Export L123 T1 [Wh]": "Energy_Export_Wh"}
        else:
            colMap = {"Timestamp": "Timestamp", 
                      "Active Energy Import L123 T1 [Wh]": "Energy_Export_Wh", 
                      "Active Energy Export L123 T1 [Wh]": "Energy_Import_Wh"}
    
        data.rename(columns=colMap)

        return data
    
    def calcIndex(self, startEpochTime: int, stopEpochTime: int):
        """calculate meter log index with a time range. Index overflow in the
        meter is not yet implemented.

        Args:
            startEpochTime (int): startTime in epoch
            stopEpochTime (int): stopTime in epoch

        Returns:
            startIndex (int): meter index of last entry before or at startTime
            stopIndex (int): meter index of last entry bevoe or at stopTime
        """
        timeToStart = self.currentTime - startEpochTime
        timeToStop = self.currentTime - stopEpochTime

        indexToStart = int(math.ceil(timeToStart / self.LOG_INTERVAL))
        indexToStop = int(math.ceil(timeToStop / self.LOG_INTERVAL))
  
        startIndex = self.currentIndex - indexToStart
        stopIndex = self.currentIndex - indexToStop

        return int(startIndex), int(stopIndex)
    
    def splitIndexRange(self, startIndex: int, stopIndex: int):
        """splits a range of idexes in to specified blocks

        Args:
            startIndex (int): first index
            stopIndex (int): last index

        Returns:
            int[[]]: array of start/stop arrays for each split
        """
        lenIndex = stopIndex - startIndex
        numOfSimpleReads = math.ceil(lenIndex / self.readBlockSize)

        blocks2Read = []
        for i in range(numOfSimpleReads):
            if not i >= (numOfSimpleReads - 1): 
                # add full block
                blocks2Read.append([(i * self.readBlockSize) + startIndex, ((i + 1) * self.readBlockSize) + startIndex - 1])
            else:
                # add remaining
                blocks2Read.append([(i * self.readBlockSize) + startIndex, stopIndex])

        return blocks2Read
    
    def read(self, startEpochTime: int, stopEpochTime: int):
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
        startIndex, stopIndex = self.calcIndex(startEpochTime, stopEpochTime)
        blocks2Read = self.splitIndexRange(startIndex, stopIndex)
        data = pd.DataFrame()
        for block in blocks2Read:
            newData = self.readSingleBlock(block[0], block[1])
            data = pd.concat([data, newData])

        return data