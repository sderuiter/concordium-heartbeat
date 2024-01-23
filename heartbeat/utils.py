# ruff: noqa: F403, F405, E402, E501, E722
from enum import Enum
from sharingiscaring.GRPCClient.CCD_Types import *


class Queue(Enum):
    """
    Type of queue to store messages in to send to MongoDB.
    Names correspond to the collection names.
    """

    block_per_day = -2
    block_heights = -1
    blocks = 0
    transactions = 1
    involved_all = 2
    involved_transfer = 3
    involved_contract = 4
    instances = 5
    modules = 6
    updated_modules = 7
    logged_events = 8
    token_addresses_to_redo_accounting = 9
    provenance_contracts_to_add = 10
    impacted_addresses = 11
    special_events = 12


class Utils:
    def get_sum_amount_from_scheduled_transfer(self, schedule: list[CCD_NewRelease]):
        sum = 0
        for release in schedule:
            sum += release.amount
        return sum

    def address_to_str(self, address: CCD_Address) -> str:
        if address.contract:
            return address.contract.to_str()
        else:
            return address.account
