from libs.energyConsumer.EnergyConsumerClass import EnergyConsumer


class OwnershipKey:
    owners = []
    shares = []

    def __init__(self):
        self.owners = []
        self.shares = []

    def addKey(self, owner: EnergyConsumer, share: float):
        """add a new owner tho the keymap. if the owner already exists,
        the old value gets overwritten.

        Args:
            owner (EnergyConsumer): owner of this share
            share (float): fraction owned by owner from 0 to 1

        Raises:
            Warning: if there was a problem adding the key
        """
        try:
            if owner in self.owners:
                self.shares[self.owners.index(owner)] = share
            else:
                self.owners.append(owner)
                self.shares.append(share)
            self.validateKey()
        except Exception as e:
            raise Warning(f"Adding of owner failed: {e}")

    def removeKey(self, owner: EnergyConsumer):
        """remove a existing owner from the keymap

        Args:
            owner (EnergyConsumer): owner to remove

        Raises:
            Warning: if there was a problem removing the owner
        """
        try:
            index = self.owners.index(owner)
            self.owners.pop(index)
            self.shares.pop(index)
            self.validateKey()
        except Exception as e:
            raise Warning(f"Removing of owner failed: {e}")

    def validateKey(self):
        """check if the keymap is valid

        Raises:
            Warning: if the keymap is not valid:
                - unequal number of shares and owners
                - duplicate owners
                - sum of shares greater than 1

        Returns:
            boolean: True if valid
        """
        if not len(self.shares) == len(self.owners):
            raise Warning(
                f"key len = {len(self.owners)} does not match share len = {len(self.shares)}."
            )

        if not len(self.owners) == len(set(self.owners)):
            raise Warning("Ownerkeys are not unique")

        sum = 0
        for share in self.shares:
            sum += share
        if sum > 1:
            raise Warning(f"Sum of shares exeeds 1: {sum}")

        return True
