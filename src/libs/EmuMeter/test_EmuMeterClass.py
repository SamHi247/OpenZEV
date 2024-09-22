import json
import pytest
import time
from EmuMeterClass import EmuMeter

def initMeter():
    with open('src/libs/EmuMeter/testHost.secret', 'r') as file:
        # load IP-address of a testhose from secret json file.
        # testHost.secret example content: {"host01": "XXX.XXX.XXX.XXX"}
        host = json.load(file)["host01"]

    return EmuMeter(host)

class TestMeterInit:
    meter = initMeter()

    def test_recordValidity(self):
        # test if timestamp of meter is close to system time
        timeToLastRecord = self.meter.currentTime - int(time.time())
        assert timeToLastRecord <= 30 * 60

class TestSimpleRead:
    meter = initMeter()

    # Exeptions
    def test_largeMeterRead(self):
        with pytest.raises(ValueError):
            self.meter.simpleRead(0,3001)

    def test_negativeMeterRead01(self):
        with pytest.raises(ValueError):
            self.meter.simpleRead(-1,1)

    def test_negativeMeterRead02(self):
        with pytest.raises(ValueError):
            self.meter.simpleRead(1,-1)

    # implement valid reads

class TestIndexCalc:
    meter = initMeter()

    def test_numberOfReturnedEntries(self):
        # a timeslot of 1h should contain 4 entries
        now = time.time()
        past = now - (60*60)
        pastIndex, nowIndex = self.meter.calcIndex(past,now)
        assert nowIndex - pastIndex == 4

    def test_newestEntry(self):
        # if stoptime is curent time, stopindex should be equal to init index
        # test can fail in edge case where initMeter and setting of now time are
        # executed right at XX:00, XX:15, XX:30 or XX:45
        now = time.time()
        past = now - (60 * 60)
        _, nowIndex = self.meter.calcIndex(past,now)
        assert nowIndex == self.meter.currentIndex
        
class TestSplitIndexRange:
    meter = initMeter()

    def test_split(self):
        indexRange = self.meter.splitIndexRange(10,20,4)
        assert indexRange == [[10,13],[14,17],[18,20]]

class TestLargeRead:
    meter = initMeter()

    def test_readLen(self):
        stopTime = time.time()
        startTime = stopTime - (4 * 60 * 60)
        data = self.meter.largeRead(startTime, stopTime, 4)
        assert 17 == data.shape[0]

    def test_correctIndex(self):
        stopTime = time.time()
        startTime = stopTime - (4 * 60 * 60)
        data = self.meter.largeRead(startTime, stopTime, 4)
        expFirstIndex, expLastIndex = self.meter.calcIndex(startTime, stopTime)
        assert [data['Index'].iloc[0],data['Index'].iloc[-1]] == [expFirstIndex,expLastIndex]

    def test_correctTime(self):
        stopTime = time.time()
        startTime = stopTime - (4 * 60 * 60)
        data = self.meter.largeRead(startTime, stopTime, 4)
        startTimeSmaller = (startTime >= data['Timestamp'].iloc[0].timestamp())
        stopTimeSmaller = (stopTime >= data['Timestamp'].iloc[-1].timestamp())
        startTimeIn15 = ((startTime - data['Timestamp'].iloc[0].timestamp()) <= (15 * 60))
        stopTimeIn15 = ((stopTime - data['Timestamp'].iloc[-1].timestamp()) <= (15 * 60))
        print(f"1:{startTimeSmaller},2:{stopTimeSmaller},3:{startTimeIn15},4:{stopTimeIn15}")
        assert startTimeSmaller and stopTimeSmaller and startTimeIn15 and stopTimeIn15
