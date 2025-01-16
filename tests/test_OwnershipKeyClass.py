import pytest
from libs.energyConsumer.OwnershipKeyClass import OwnershipKey
from libs.energyConsumer.EnergyConsumerClass import EnergyConsumer

testConsumer1 = EnergyConsumer(None)
testConsumer2 = EnergyConsumer(None)


def test_keyValidationGood():
    goodOwKey = OwnershipKey()
    goodOwKey.owners.append(testConsumer1)
    goodOwKey.shares.append(0.5)
    goodOwKey.owners.append(testConsumer2)
    goodOwKey.shares.append(0.5)

    assert goodOwKey.validateKey()


def test_keyValidationUniquenes():
    uniqueOwKey = OwnershipKey()
    uniqueOwKey.owners.append(testConsumer1)
    uniqueOwKey.shares.append(0.1)
    uniqueOwKey.owners.append(testConsumer2)
    uniqueOwKey.shares.append(0.4)
    uniqueOwKey.owners.append(testConsumer1)
    uniqueOwKey.shares.append(0.5)

    with pytest.raises(Warning) as excinfo:
        uniqueOwKey.validateKey()
    assert str(excinfo.value) == "Ownerkeys are not unique"


OwnershipKey.shares


def test_keyValidationArrayLen():
    arraylenOwKey = OwnershipKey()
    arraylenOwKey.owners.append(testConsumer1)
    arraylenOwKey.shares.append(0.5)
    arraylenOwKey.owners.append(testConsumer2)

    with pytest.raises(Warning) as excinfo:
        arraylenOwKey.validateKey()
    assert str(excinfo.value) == "key len = 2 does not match share len = 1."


def test_keyValidationSharelimit():
    sharelimOwKey = OwnershipKey()
    sharelimOwKey.owners.append(testConsumer1)
    sharelimOwKey.shares.append(0.5)
    sharelimOwKey.owners.append(testConsumer2)
    sharelimOwKey.shares.append(0.6)

    with pytest.raises(Warning) as excinfo:
        sharelimOwKey.validateKey()
    assert str(excinfo.value) == "Sum of shares exeeds 1: 1.1"


def test_instancing():
    sharelimOwKey1 = OwnershipKey()
    sharelimOwKey1.owners.append(testConsumer1)
    sharelimOwKey1.shares.append(0.5)

    sharelimOwKey2 = OwnershipKey()
    sharelimOwKey2.owners.append(testConsumer2)
    sharelimOwKey2.shares.append(0.5)

    assert sharelimOwKey1.owners[0] == testConsumer1


def test_addingKey():
    addingKey = OwnershipKey()
    addingKey.addKey(testConsumer1, 0.5)
    addingKey.addKey(testConsumer2, 0.5)

    assert addingKey.owners == [testConsumer1, testConsumer2] and addingKey.shares == [
        0.5,
        0.5,
    ]


def test_addingKeyValidity():
    addingKey = OwnershipKey()
    addingKey.addKey(testConsumer1, 0.5)
    addingKey.shares.append(0.6)

    with pytest.raises(Warning) as excinfo:
        addingKey.addKey(testConsumer2, 0.5)
    assert "Adding of owner failed:" in str(excinfo.value)


def test_duplicateAvoidance():
    addingKey = OwnershipKey()
    addingKey.addKey(testConsumer1, 0.5)
    addingKey.addKey(testConsumer2, 0.5)
    addingKey.addKey(testConsumer1, 0.4)

    assert addingKey.owners == [testConsumer1, testConsumer2] and addingKey.shares == [
        0.4,
        0.5,
    ]


def test_removingKey():
    addingKey = OwnershipKey()
    addingKey.addKey(testConsumer1, 0.5)
    addingKey.addKey(testConsumer2, 0.5)
    addingKey.removeKey(testConsumer1)

    assert addingKey.owners == [testConsumer2] and addingKey.shares == [0.5]


def test_removingNonexKey():
    addingKey = OwnershipKey()
    addingKey.addKey(testConsumer1, 0.5)

    with pytest.raises(Warning) as excinfo:
        addingKey.removeKey(testConsumer2)
    assert "Removing of owner failed:" in str(excinfo.value)
