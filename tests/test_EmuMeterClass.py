import json
import time
from libs.Meter.EmuMeterClass import EmuMeter
from libs.Meter.meterClass import Meter


def getHost():
    try:
        with open("src/libs/Meter/testHost.secret", "r") as file:
            # load IP-address of a testhose from secret json file.
            # testHost.secret example content: {"host01": "XXX.XXX.XXX.XXX"}
            host = json.load(file)["host01"]
    except Exception:
        host = "NA"

    return host


if not getHost() == "NA":
    meter = EmuMeter(getHost(), readBlockSize=4)
else:
    meter = Meter("testMeter")


def test_meterConnection():
    # test if timestamp of meter is close to system time
    timeToLastRecord = meter.currentTime - int(time.time())
    assert timeToLastRecord <= 30 * 60


def test_indexSplit():
    indexRange = meter.splitIndexRange(10, 20)
    assert indexRange == [[10, 13], [14, 17], [18, 20]]


def test_numberOfReturnedEntries():
    # a timeslot of 1h should contain 4 entries
    now = time.time()
    past = now - (60 * 60)
    pastIndex, nowIndex = meter.calcIndex(past, now)
    assert nowIndex - pastIndex == 4


def test_newestEntry():
    # if stoptime is curent time, stopindex should be equal to init index
    # test can fail in edge case where initMeter and setting of now time are
    # executed right at XX:00, XX:15, XX:30 or XX:45
    now = time.time()
    past = now - (60 * 60)
    _, nowIndex = meter.calcIndex(past, now)
    assert nowIndex == meter.currentIndex


def test_readLen():
    stopTime = time.time()
    startTime = stopTime - (4 * 60 * 60)
    data = meter.read(startTime, stopTime)
    assert 17 == data.shape[0]


def test_correctTime():
    stopTime = time.time()
    startTime = stopTime - (4 * 60 * 60)
    data = meter.read(startTime, stopTime)
    startTimeSmaller = startTime >= data["Timestamp"].iloc[0].timestamp()
    stopTimeSmaller = stopTime >= data["Timestamp"].iloc[-1].timestamp()
    startTimeIn15 = (startTime - data["Timestamp"].iloc[0].timestamp()) <= (15 * 60)
    stopTimeIn15 = (stopTime - data["Timestamp"].iloc[-1].timestamp()) <= (15 * 60)
    print(
        f"1:{startTimeSmaller},2:{stopTimeSmaller},3:{startTimeIn15},4:{stopTimeIn15}"
    )
    assert startTimeSmaller and stopTimeSmaller and startTimeIn15 and stopTimeIn15
