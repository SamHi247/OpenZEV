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
        