import pytest
import pandas as pd

from libs.energyConsumer.EnergyConsumerClass import EnergyConsumer
from libs.Meter.meterClass import Meter


def test_missingSlots():
    consumer = EnergyConsumer(Meter("test"))
    consumer.energyDataCache = pd.DataFrame(
        [
            ["01.01.2000T00:00:00", 1, 1],
            ["01.01.2000T00:15:00", 2, 2],
            ["01.01.2000T00:45:00", 4, 4],
            ["01.01.2000T01:00:00", 5, 5],
            ["01.01.2000T01:30:00", 7, 7],
            ["01.01.2000T01:45:00", 8, 8],
        ],
        columns=["Timestamp", "Energy_Import_Wh", "Energy_Export_Wh"],
    )
    consumer.energyDataCache["Timestamp"] = pd.to_datetime(
        consumer.energyDataCache["Timestamp"]
    )

    missingSlots = consumer.findMissingDataSlots(946684800, 946691100)

    assert missingSlots == [[946685700, 946687500], [946688400, 946690200]]


def test_noDataMissingSlots():
    consumer = EnergyConsumer(Meter("test"))
    missingSlots = consumer.findMissingDataSlots(946684800, 946691100)
    assert missingSlots == [[946684800, 946691100]]


def test_invalidArgs():
    consumer = EnergyConsumer(Meter("test"))

    with pytest.raises(ValueError) as excinfo:
        consumer.findMissingDataSlots(2, 1)
    assert "The startTime of 2 is anfter the stopTime of 1." == str(excinfo.value)


def test_noDataInSlot():
    consumer = EnergyConsumer(Meter("test"))
    consumer.energyDataCache = pd.DataFrame(
        [
            ["01.01.2000T02:00:00", 1, 1],
            ["01.01.2000T02:15:00", 2, 2],
            ["01.01.2000T02:45:00", 4, 4],
            ["01.01.2000T03:00:00", 5, 5],
            ["01.01.2000T03:30:00", 7, 7],
            ["01.01.2000T03:45:00", 8, 8],
        ],
        columns=["Timestamp", "Energy_Import_Wh", "Energy_Export_Wh"],
    )
    consumer.energyDataCache["Timestamp"] = pd.to_datetime(
        consumer.energyDataCache["Timestamp"]
    )

    missingSlots = consumer.findMissingDataSlots(946684800, 946691100)

    assert missingSlots == [[946684800, 946691100]]
