# ruff: noqa: F403, F405, E402, E501, E722
from rich import print
from rich.progress import track


from ccdefundamentals.GRPCClient.CCD_Types import *
from ccdefundamentals.tooter import Tooter
from ccdefundamentals.mongodb import (
    MongoDB,
    Collections,
    MongoMotor,
    MongoImpactedAddress,
    AccountStatementEntryType,
    MongoTypeAccountReward,
    MongoTypePayday,
)

from pymongo import ReplaceOne
from env import *
from rich.console import Console
import urllib3

urllib3.disable_warnings()

console = Console()

tooter: Tooter = Tooter(
    ENVIRONMENT, BRANCH, NOTIFIER_API_TOKEN, API_TOKEN, FASTMAIL_TOKEN
)
mongodb = MongoDB(
    {
        "MONGODB_PASSWORD": MONGODB_PASSWORD,
    },
    tooter,
)

# fmt: off
motormongo = MongoMotor(
    {
        "MONGODB_PASSWORD": MONGODB_PASSWORD,
    },
    tooter,
)
# fmt: on


class EntryTypeEnum(Enum):
    AMOUNT_DECRYPTED = "Amount Decrypted"
    AMOUNT_ENCRYPTED = "Amount Encrypted"
    BAKER_REWARD = "Baker Reward"
    FINALIZATION_REWARD = "Finalization Reward"
    FOUNDATION_REWARD = "Foundation Reward"
    TRANSACTION_FEE = "Transaction Fee"
    TRANSACTION_FEE_REWARD = "Transaction Fee Reward"
    TRANSFER_IN = "Transfer In"
    TRANSFER_OUT = "Transfer Out"


net = "mainnet"
db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet

# REMOVE in HEARTBEAT
# db_to_use[Collections.impacted_addresses].delete_many({"effect_type": "Account Reward"})
result = list(
    db_to_use[Collections.impacted_addresses]
    .find({"_id": {"$type": "objectId"}})
    .limit(20)
)
# result = db_to_use[Collections.impacted_addresses].aggregate(
#     [{"$match": {"$expr": {"$eq": [{"$type": "$type_id"}, "objectId"]}}}]
# )

# REMOVE in HEARTBEAT


def address_to_str(address: CCD_Address) -> str:
    if address.contract:
        return address.contract.to_str()
    else:
        return address.account


paydays = db_to_use[Collections.paydays].find({})
paydays = {x["date"]: MongoTypePayday(**x) for x in paydays}

int_result = db_to_use[Collections.paydays_rewards].find(
    # {"account_transaction.effects.contract_update_issued": {"$exists": True}}
    {"account_id": {"$exists": True}}
    # {"block_info.height": {"$gt": 8_387_000, "$lte": 8_391_276}},
    # {"block_info.height": {"$gt": 6_000_000, "$lte": 6282951}},
    # {}
)


account_rewards = [MongoTypeAccountReward(**x) for x in int_result]

impacted_addresses_queue = []


def get_sum_amount_from_scheduled_transfer(schedule: list[CCD_NewRelease]):
    sum = 0
    for release in schedule:
        sum += release.amount
    return sum


def file_a_balance_movement(
    block_height: int,
    impacted_addresses_in_tx: dict[str, MongoImpactedAddress],
    impacted_address: str,
    balance_movement_to_add: AccountStatementEntryType,
):
    if impacted_addresses_in_tx.get(impacted_address):
        impacted_address_as_class: MongoImpactedAddress = impacted_addresses_in_tx[
            impacted_address
        ]
        bm: AccountStatementEntryType = impacted_address_as_class.balance_movement
        field_set = list(balance_movement_to_add.model_fields_set)[0]
        if field_set == "transfer_in":
            if not bm.transfer_in:
                bm.transfer_in = []
            bm.transfer_in.extend(balance_movement_to_add.transfer_in)
        elif field_set == "transfer_out":
            if not bm.transfer_out:
                bm.transfer_out = []
            bm.transfer_out.extend(balance_movement_to_add.transfer_out)
        elif field_set == "amount_encrypted":
            bm.amount_encrypted = balance_movement_to_add.amount_encrypted
        elif field_set == "amount_decrypted":
            bm.amount_decrypted = balance_movement_to_add.amount_decrypted
        elif field_set == "baker_reward":
            bm.baker_reward = balance_movement_to_add.baker_reward
        elif field_set == "finalization_reward":
            bm.finalization_reward = balance_movement_to_add.finalization_reward
        elif field_set == "foundation_reward":
            bm.foundation_reward = balance_movement_to_add.foundation_reward
        elif field_set == "transaction_fee_reward":
            bm.transaction_fee_reward = balance_movement_to_add.transaction_fee_reward

        impacted_address_as_class.balance_movement = bm
    else:
        impacted_address_as_class = MongoImpactedAddress(
            **{
                "_id": f"{block_height}-{impacted_address[:29]}",
                "impacted_address": impacted_address,
                "impacted_address_canonical": impacted_address[:29],
                "effect_type": "Account Reward",
                "balance_movement": balance_movement_to_add,
                "block_height": block_height,
            }
        )
        impacted_addresses_in_tx[impacted_address] = impacted_address_as_class


# def file_balance_movements(
#     tx: CCD_BlockItemSummary,
#     impacted_addresses_in_tx: dict[MongoImpactedAddress],
#     amount: microCCD,
#     sender: str,
#     receiver: str,
# ):
#     # first add to sander balance_movement
#     balance_movement = AccountStatementEntryType(
#         transfer_out=[
#             AccountStatementTransferType(
#                 amount=amount,
#                 counterparty=receiver[:29] if len(receiver) > 20 else receiver,
#             )
#         ]
#     )
#     file_a_balance_movement(
#         tx,
#         impacted_addresses_in_tx,
#         sender,
#         balance_movement,
#     )

#     # then to the receiver balance_movement
#     balance_movement = AccountStatementEntryType(
#         transfer_in=[
#             AccountStatementTransferType(
#                 amount=amount,
#                 counterparty=sender[:29] if len(sender) > 20 else sender,
#             )
#         ]
#     )
#     file_a_balance_movement(
#         tx,
#         impacted_addresses_in_tx,
#         receiver,
#         balance_movement,
#     )


print(f"Doing {len(account_rewards):,.0f} account_rewards...")
for ar in track(account_rewards):
    impacted_addresses_in_tx: dict[str:MongoImpactedAddress] = {}
    if ar.reward:
        balance_movement = AccountStatementEntryType(
            transaction_fee_reward=ar.reward.transaction_fees,
            baker_reward=ar.reward.baker_reward,
            finalization_reward=ar.reward.finalization_reward,
        )
        file_a_balance_movement(
            paydays[ar.date].height_for_last_block + 1,
            impacted_addresses_in_tx,
            ar.account_id,
            balance_movement,
        )

    # now this tx is done, so add impacted_addresses to queue
    for ia in impacted_addresses_in_tx.values():
        ia: MongoImpactedAddress
        repl_dict = ia.model_dump(exclude_none=True)
        if "id" in repl_dict:
            del repl_dict["id"]

        impacted_addresses_queue.append(
            ReplaceOne(
                {"_id": ia.id},
                repl_dict,
                upsert=True,
            )
        )
    if len(impacted_addresses_queue) > 1000:
        result = db_to_use[Collections.impacted_addresses].bulk_write(
            impacted_addresses_queue
        )
        impacted_addresses_queue = []

# clear out the queue
result = db_to_use[Collections.impacted_addresses].bulk_write(impacted_addresses_queue)
impacted_addresses_queue = []

# elif (
#     (tx.account_transaction.effects.module_deployed)
#     or (tx.account_transaction.effects.contract_initialized)
#     or (tx.account_transaction.effects.baker_added)
#     or (tx.account_transaction.effects.baker_removed)
#     or (tx.account_transaction.effects.baker_stake_updated)
#     or (tx.account_transaction.effects.baker_restake_earnings_updated)
#     or (tx.account_transaction.effects.baker_keys_updated)
#     or (tx.account_transaction.effects.encrypted_amount_transferred)
#     or (tx.account_transaction.effects.credential_keys_updated)
#     or (tx.account_transaction.effects.credentials_updated)
#     or (tx.account_transaction.effects.data_registered)
#     or (tx.account_transaction.effects.baker_configured)
#     or (tx.account_transaction.effects.delegation_configured)
# ):
#     pass


# print(impacted_addresses_in_tx)
