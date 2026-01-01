import pandas as pd
import math
from libs.Meter.meterClass import Meter

class EmuMeter(Meter):
    LOG_INTERVAL = 15 * 60
    MAX_READBLOCK_SIZE = 3000

    current_index = None
    current_time = None
    read_block_size = None

    def __init__(
        self,
        host: str,
        name: str,
        invert: bool = False,
        read_block_size: int = MAX_READBLOCK_SIZE,
    ):
        """Connect to a EMU Pro II power meter and set up all relevant Variables

        Args:
            host (str): hostname like IP-address of power meter
            invert (bool, optional): set "True" if Import an export on this meter are reversed
            read_block_size (int, optional): Size of request blocks sent to meter. Max is 3000. Defaults to 3000.

        Raises:
            ValueError: if more than 3000 entries can be requested

        """
        super().__init__("EMU Pro II", name, invert)

        self.log.info(f" Init meter with host address \"{host}\".")

        if read_block_size < 1 and read_block_size > self.MAX_READBLOCK_SIZE:
            raise ValueError(
                f"A readBlockSize value of {read_block_size} is not allowed. (0 < readBlockSize <= 3000"
            )

        if read_block_size != self.MAX_READBLOCK_SIZE:
            self.log.debug(f" Set read_block_size to {read_block_size}.")
        self.read_block_size = read_block_size

        # get last log entry from meter
        self.log.debug(" Loading newest meter datapoint and setup")
        self.host_name = host
        url = "http://" + self.host_name + "/data/?last=1"
        current_reading = pd.read_csv(url, delimiter=";")

        # set up variables
        current_reading["Timestamp"] = pd.to_datetime(current_reading["Timestamp"])
        self.current_time = current_reading["Timestamp"][0].timestamp()
        self.current_index = current_reading["Index"][0]

        self.log.debug("Meter setup complete.")

    def read_single_block(self, start_index: int, stop_index: int):
        """Just read Entries from, to a specific index. The meter it self can't
        deliver more than 3000 entries at once and there are no negative idexes.

        Args:
            start_index (int): first meter internal index do be read
            stop_index (int): last miter internal index to be read

        Raises:
            ValueError: if more than 3000 entries are requested
            ValueError: if a negative index is requested

        Returns:
            pd.DataFrame: Dataframe of requested data
        """
        if stop_index < 0 or start_index < 0:
            raise ValueError(
                f"Negative Indexes are not allowed: start={start_index}, stop={stop_index}"
            )
        if (stop_index - start_index) > self.MAX_READBLOCK_SIZE:
            raise ValueError(
                f"It is impossible to read more than 3000 entrys at once. ({(stop_index - start_index)})"
            )
        
        self.log.debug(f" Reading block from {start_index} to {stop_index}.")

        # get data from meter
        url = "".join([
            "http://",
            self.host_name,
            "/data/?from=",
            str(start_index),
            "&to=",
            str(stop_index),
        ])
        raw_meter_data = pd.read_csv(url, delimiter=";")

        # clean up data
        columns_to_remove = [
            "Index",
            "Status",
            "Serial",
            "Active Energy Import L123 T2 [Wh]",
            "Active Energy Export L123 T2 [Wh]",
            "Reactive Energy Import L123 T1 [varh]",
            "Reactive Energy Import L123 T2 [varh]",
            "Reactive Energy Export L123 T1 [varh]",
            "Reactive Energy Export L123 T2 [varh]",
            "Active Power L123 [W]",
            "Active Power L1 [W]",
            "Active Power L2 [W]",
            "Active Power L3 [W]",
            "Current L123 [mA]",
            "Current L1 [mA]",
            "Current L2 [mA]",
            "Current L3 [mA]",
            "Current N [mA]",
            "Voltage L1-N [1/10 V]",
            "Voltage L2-N [1/10 V]",
            "Voltage L3-N [1/10 V]",
            "Powerfactor L1 [1/100]",
            "Powerfactor L2 [1/100]",
            "Powerfactor L3 [1/100]",
            "Frequency [1/10 Hz]",
        ]
        data = raw_meter_data.drop(columns_to_remove, axis=1)
        data["Timestamp"] = pd.to_datetime(data["Timestamp"])

        if not (self.invert_energy_direction):
            col_map = {
                "Timestamp": "Timestamp",
                "Active Energy Import L123 T1 [Wh]": f"{self.name}_Import_Wh",
                "Active Energy Export L123 T1 [Wh]": f"{self.name}_Export_Wh",
            }
        else:
            col_map = {
                "Timestamp": "Timestamp",
                "Active Energy Import L123 T1 [Wh]": f"{self.name}_Export_Wh",
                "Active Energy Export L123 T1 [Wh]": f"{self.name}_Import_Wh",
            }

        data.rename(columns=col_map, inplace=True)

        return data

    def calc_index(self, start_epoch_time: int, stop_epoch_time: int):
        """calculate meter log index with a time range. Index overflow in the
        meter is not yet implemented.

        Args:
            start_epoch_time (int): startTime in epoch
            stop_epoch_time (int): stopTime in epoch

        Returns:
            start_index (int): meter index of last entry before or at startTime
            stop_index (int): meter index of last entry bevoe or at stopTime
        """
        self.log.debug(f" Calculating index for time range {start_epoch_time} to {stop_epoch_time}.")

        time_to_start = self.current_time - start_epoch_time
        time_to_stop = self.current_time - stop_epoch_time

        index_to_start = int(math.ceil(time_to_start / self.LOG_INTERVAL))
        index_to_stop = int(math.ceil(time_to_stop / self.LOG_INTERVAL))

        start_index = self.current_index - index_to_start
        stop_index = self.current_index - index_to_stop

        if (start_index < 0) or (stop_index < 0):
            raise NotImplementedError(
                "An index overflow has occurred. Handling of this overflow has not been implemented yet"
            )

        return int(start_index), int(stop_index)

    def split_index_range(self, start_index: int, stop_index: int):
        """splits a range of idexes in to specified blocks

        Args:
            start_index (int): first index
            stop_index (int): last index

        Returns:
            [[start_index, stop_index]] (int): array of start/stop arrays for each split
        """
        len_index = stop_index - start_index
        num_of_simple_reads = math.ceil(len_index / self.read_block_size)

        self.log.debug(f" Splitting index range {start_index} to {stop_index} in {num_of_simple_reads} blocks.")

        blocks_to_read = []
        for i in range(num_of_simple_reads):
            if not i >= (num_of_simple_reads - 1):
                # add full block
                blocks_to_read.append(
                    [
                        (i * self.read_block_size) + start_index,
                        ((i + 1) * self.read_block_size) + start_index - 1,
                    ]
                )
            else:
                # add remaining
                blocks_to_read.append(
                    [(i * self.read_block_size) + start_index, stop_index]
                )

        return blocks_to_read

    def read(self, start_epoch_time: int, stop_epoch_time: int):
        """Read all entries in a range of epoch time. No size limit, exept what is available on the meter.

        Args:
            start_epoch_time (int): startTime in epoch
            stop_epoch_time (int): stopTime in epoch
            log_name (str, optional): if a name is given, the process will be logged in the console

        Returns:
            pd.DataFrame: requested data as pandas DataFrame with the following columns:
                - "Timestamp"
                - f"{self.name}_Import_Wh"
                - f"{self.name}_Export_Wh"
        """
        start_index, stop_index = self.calc_index(start_epoch_time, stop_epoch_time)
        blocks_to_read = self.split_index_range(start_index, stop_index)
        data = pd.DataFrame()

        count = 0
        for block in blocks_to_read:
            self.log.info(f" Reading block {count} of {len(blocks_to_read)}")

            new_data = self.read_single_block(block[0], block[1])
            data = pd.concat([data, new_data])
            count += 1

        self.log.info(" Reading complete.")

        return data
