# ruff: noqa: F403, F405, E402, E501, E722
from rich import print
from rich.progress import track
from bson.objectid import ObjectId


from sharingiscaring.GRPCClient.CCD_Types import *
from sharingiscaring.tooter import Tooter
from sharingiscaring.mongodb import (
    MongoDB,
    Collections,
    MongoMotor,
    MongoImpactedAddress,
    AccountStatementEntryType,
    AccountStatementTransferType,
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

motormongo = MongoMotor(
    {
        "MONGODB_PASSWORD": MONGODB_PASSWORD,
    },
    tooter,
)


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


net = "testnet"
db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet

# REMOVE in HEARTBEAT
# db_to_use[Collections.impacted_addresses].delete_many({})

# REMOVE in HEARTBEAT


def address_to_str(address: CCD_Address) -> str:
    if address.contract:
        return address.contract.to_str()
    else:
        return address.account


int_result = (
    db_to_use[Collections.transactions].find(
        # {"_id": "1949de1d02e4650626ca564e530b2fd8979bb69f6851ede562df8fa1378434ee"}
        {"account_transaction.effects.contract_update_issued": {"$exists": True}}
        # {"type.contents": {"$not": {"$eq": "data_registered"}}}
        # {"account_creation": {"$exists": True}}
        # {"block_info.height": {"$gte": 8_300_000, "$lte": 8_434_000}},
        # {"block_info.height": {"$gt": 6_000_000, "$lte": 6282951}},
        # {}
    )
    # .sort("block_info.height", -1)
)


tx_result = [CCD_BlockItemSummary(**x) for x in int_result]

impacted_addresses_queue = []


def get_sum_amount_from_scheduled_transfer(schedule: list[CCD_NewRelease]):
    sum = 0
    for release in schedule:
        sum += release.amount
    return sum


def file_a_balance_movement(
    tx: CCD_BlockItemSummary,
    impacted_addresses_in_tx: dict[MongoImpactedAddress],
    impacted_address: str,
    balance_movement_to_add: AccountStatementEntryType,
):
    if impacted_addresses_in_tx.get(impacted_address):
        impacted_address_as_class: MongoImpactedAddress = impacted_addresses_in_tx[
            impacted_address
        ]
        bm: AccountStatementEntryType = impacted_address_as_class.balance_movement
        if not bm:
            bm = AccountStatementEntryType()
        if balance_movement_to_add:
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
                bm.transaction_fee_reward = (
                    balance_movement_to_add.transaction_fee_reward
                )

            impacted_address_as_class.balance_movement = bm
    else:
        impacted_address_as_class = MongoImpactedAddress(
            **{
                "_id": f"{tx.hash}-{impacted_address[:29]}",
                # "_id": ObjectId(),
                "tx_hash": tx.hash,
                "impacted_address": impacted_address,
                "impacted_address_canonical": impacted_address[:29],
                "effect_type": tx.type.contents,
                "balance_movement": balance_movement_to_add,
                "block_height": tx.block_info.height,
            }
        )
        impacted_addresses_in_tx[impacted_address] = impacted_address_as_class


def file_balance_movements(
    tx: CCD_BlockItemSummary,
    impacted_addresses_in_tx: dict[MongoImpactedAddress],
    amount: microCCD,
    sender: str,
    receiver: str,
):
    # first add to sender balance_movement
    if amount > 0:
        balance_movement = AccountStatementEntryType(
            transfer_out=[
                AccountStatementTransferType(
                    amount=amount,
                    counterparty=receiver[:29] if len(receiver) > 20 else receiver,
                )
            ]
        )
    else:
        balance_movement = None

    file_a_balance_movement(
        tx,
        impacted_addresses_in_tx,
        sender,
        balance_movement,
    )

    # then to the receiver balance_movement
    if amount > 0:
        balance_movement = AccountStatementEntryType(
            transfer_in=[
                AccountStatementTransferType(
                    amount=amount,
                    counterparty=sender[:29] if len(sender) > 20 else sender,
                )
            ]
        )
    else:
        balance_movement = None

    file_a_balance_movement(
        tx,
        impacted_addresses_in_tx,
        receiver,
        balance_movement,
    )


print(f"Doing {len(tx_result):,.0f} transactions...")
for tx in track(tx_result):
    impacted_addresses_in_tx: dict[str:MongoImpactedAddress] = {}
    if tx.account_creation:
        balance_movement = None
        file_a_balance_movement(
            tx,
            impacted_addresses_in_tx,
            tx.account_creation.address,
            balance_movement,
        )
    if tx.account_transaction:
        # Always store the fee for the sender
        balance_movement = AccountStatementEntryType(
            transaction_fee=tx.account_transaction.cost
        )
        file_a_balance_movement(
            tx,
            impacted_addresses_in_tx,
            tx.account_transaction.sender,
            balance_movement,
        )

        # Next, for the below effect types, we need to store additional
        # balance movements.
        if tx.account_transaction.effects.contract_initialized:
            if tx.account_transaction.effects.contract_initialized.amount > 0:
                file_balance_movements(
                    tx,
                    impacted_addresses_in_tx,
                    tx.account_transaction.effects.contract_initialized.amount,
                    tx.account_transaction.sender,
                    tx.account_transaction.effects.contract_initialized.address.to_str(),
                )
            else:
                balance_movement = None
                file_a_balance_movement(
                    tx,
                    impacted_addresses_in_tx,
                    tx.account_transaction.effects.contract_initialized.address.to_str(),
                    balance_movement,
                )

        if tx.account_transaction.effects.contract_update_issued:
            for effect in tx.account_transaction.effects.contract_update_issued.effects:
                if effect.updated:
                    instigator_str = address_to_str(effect.updated.instigator)
                    # if effect.updated.amount > 0:
                    file_balance_movements(
                        tx,
                        impacted_addresses_in_tx,
                        effect.updated.amount,
                        instigator_str,
                        effect.updated.address.to_str(),
                    )

                elif effect.transferred:
                    file_balance_movements(
                        tx,
                        impacted_addresses_in_tx,
                        effect.transferred.amount,
                        effect.transferred.sender.to_str(),
                        effect.transferred.receiver,
                    )

                elif effect.interrupted:
                    balance_movement = None
                    file_a_balance_movement(
                        tx,
                        impacted_addresses_in_tx,
                        effect.interrupted.address.to_str(),
                        balance_movement,
                    )

                elif effect.resumed:
                    balance_movement = None
                    file_a_balance_movement(
                        tx,
                        impacted_addresses_in_tx,
                        effect.resumed.address.to_str(),
                        balance_movement,
                    )

        elif tx.account_transaction.effects.account_transfer:
            file_balance_movements(
                tx,
                impacted_addresses_in_tx,
                tx.account_transaction.effects.account_transfer.amount,
                tx.account_transaction.sender,
                tx.account_transaction.effects.account_transfer.receiver,
            )

        elif tx.account_transaction.effects.transferred_with_schedule:
            file_balance_movements(
                tx,
                impacted_addresses_in_tx,
                get_sum_amount_from_scheduled_transfer(
                    tx.account_transaction.effects.transferred_with_schedule.amount
                ),
                tx.account_transaction.sender,
                tx.account_transaction.effects.transferred_with_schedule.receiver,
            )

        elif tx.account_transaction.effects.transferred_to_encrypted:
            balance_movement = AccountStatementEntryType(
                amount_encrypted=tx.account_transaction.effects.transferred_to_encrypted.amount
            )
            file_a_balance_movement(
                tx,
                impacted_addresses_in_tx,
                tx.account_transaction.sender,
                balance_movement,
            )

        elif tx.account_transaction.effects.transferred_to_public:
            balance_movement = AccountStatementEntryType(
                amount_decrypted=tx.account_transaction.effects.transferred_to_public.amount
            )
            file_a_balance_movement(
                tx,
                impacted_addresses_in_tx,
                tx.account_transaction.sender,
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
