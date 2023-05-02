from sharingiscaring.mongodb import (
    MongoDB,
    Collections,
    MongoTypeModule,
    MongoTypeInstance,
    MongoTypeInvolvedContract,
)
from sharingiscaring.GRPCClient import GRPCClient
from sharingiscaring.cis import (
    CIS,
    StandardIdentifiers,
    mintEvent,
    burnEvent,
    transferEvent,
    tokenMetadataEvent,
)
from sharingiscaring.enums import NET
from sharingiscaring.tooter import Tooter
from pymongo import ASCENDING, DESCENDING, ReplaceOne
from pymongo.collection import Collection
from sharingiscaring.GRPCClient.CCD_Types import *
from rich import print
from env import *

tooter: Tooter = Tooter(
    ENVIRONMENT, BRANCH, NOTIFIER_API_TOKEN, API_TOKEN, FASTMAIL_TOKEN
)
MONGODB_PASSWORD = os.environ.get("MONGODB_PASSWORD", MONGODB_PASSWORD_LOCAL)
grpcclient = GRPCClient()
mongodb = MongoDB(
    {
        "MONGODB_PASSWORD": MONGODB_PASSWORD,
    },
    tooter,
)


def save_mint(
    db_to_use: dict[Collections, Collection],
    instance: MongoTypeInstance,
    result: mintEvent,
):
    # steps
    # 1. save newly minted token to collection tokens_token_addresses
    # 2. Create/update token holder's account in tokens_accounts

    # Step 1
    token_address = f"{instance.id}-{result.token_id}"
    d = {
        "_id": token_address,
        "contract": instance.id,
        "token_id": result.token_id,
        "token_amount": str(result.token_amount),
    }
    _ = db_to_use[Collections.tokens_token_addresses].bulk_write(
        [
            ReplaceOne(
                {"_id": token_address},
                replacement=d,
                upsert=True,
            )
        ]
    )

    # Step 2
    # lookup existing account
    d = db_to_use[Collections.tokens_accounts].find_one({"_id": result.to_address})
    # if this account doesn't have tokens, create empty dict.
    if not d:
        d = {"_id": result.to_address, "tokens": {}}  # keyed on token_address

    current_tokens = d["tokens"]
    current_tokens[token_address] = {
        "token_address": token_address,
        "contract": instance.id,
        "token_id": result.token_id,
        "token_amount": str(result.token_amount),
    }
    d.update({"tokens": current_tokens})

    _ = db_to_use[Collections.tokens_accounts].bulk_write(
        [
            ReplaceOne(
                {"_id": result.to_address},
                replacement=d,
                upsert=True,
            )
        ]
    )


def save_metadata(
    db_to_use: dict[Collections, Collection],
    instance: MongoTypeInstance,
    result: tokenMetadataEvent,
):
    # steps
    # 1. find or create token in/from collection tokens_token_addresses
    # 2. update metadata

    # Step 1
    token_address = f"{instance.id}-{result.token_id}"
    d = db_to_use[Collections.tokens_token_addresses].find_one({"_id": token_address})
    if not d:
        print(f"Huh? {token_address} isn't minted yet, but is now updating metadata?")
        d = {
            "_id": token_address,
            "contract": instance.id,
            "token_id": result.token_id,
        }
    d.update(
        {
            "metadata_url": result.metadata.url,
        }
    )
    _ = db_to_use[Collections.tokens_token_addresses].bulk_write(
        [
            ReplaceOne(
                {"_id": token_address},
                replacement=d,
                upsert=True,
            )
        ]
    )


def save_transfer(
    db_to_use: dict[Collections, Collection],
    instance: MongoTypeInstance,
    result: transferEvent,
):
    # steps
    # 1. update from address account
    # 2. Create/update to account

    token_address = f"{instance.id}-{result.token_id}"

    # Step 1
    # lookup existing account
    d = db_to_use[Collections.tokens_accounts].find_one({"_id": result.from_address})
    if not d:
        d = {"_id": result.from_address, "tokens": {}}  # keyed on token_address

    current_tokens = d["tokens"]
    if token_address in current_tokens.keys():
        current_token = current_tokens[token_address]
        # lower the current amount of the token
        current_token["token_amount"] = str(
            (int(current_token["token_amount"]) - result.token_amount)
        )
        current_tokens[token_address] = current_token
    else:
        print(
            f"{result.from_address} is transferring a token at {token_address} that is doesn't own?"
        )

    d.update({"tokens": current_tokens})

    _ = db_to_use[Collections.tokens_accounts].bulk_write(
        [
            ReplaceOne(
                {"_id": result.from_address},
                replacement=d,
                upsert=True,
            )
        ]
    )

    # Step 2
    # lookup existing account
    d = db_to_use[Collections.tokens_accounts].find_one({"_id": result.to_address})
    # if this account doesn't have tokens, create empty dict.
    if not d:
        d = {"_id": result.to_address, "tokens": {}}  # keyed on token_address

    current_tokens = d["tokens"]
    if token_address in current_tokens.keys():
        current_token = current_tokens[token_address]
        # lower the current amount of the token
        current_token["token_amount"] = str(
            (int(current_token["token_amount"]) + result.token_amount)
        )
        current_tokens[token_address] = current_token
    else:
        current_tokens[token_address] = {
            "token_address": token_address,
            "contract": instance.id,
            "token_id": result.token_id,
            "token_amount": str(result.token_amount),
        }
    d.update({"tokens": current_tokens})

    _ = db_to_use[Collections.tokens_accounts].bulk_write(
        [
            ReplaceOne(
                {"_id": result.to_address},
                replacement=d,
                upsert=True,
            )
        ]
    )


def save_burn(
    db_to_use: dict[Collections, Collection],
    instance: MongoTypeInstance,
    result: burnEvent,
):
    # steps
    # 1. lower token amount for from_ address

    token_address = f"{instance.id}-{result.token_id}"

    # Step 1
    # lookup existing account
    d = db_to_use[Collections.tokens_accounts].find_one({"_id": result.from_address})
    if not d:
        d = {"_id": result.from_address, "tokens": {}}  # keyed on token_address

    current_tokens = d["tokens"]
    if token_address in current_tokens.keys():
        current_token = current_tokens[token_address]
        # lower the current amount of the token
        current_token["token_amount"] = str(
            (int(current_token["token_amount"]) - result.token_amount)
        )
        current_tokens[token_address] = current_token
    else:
        print(
            f"{result.from_address} is burning a token at {token_address} that is doesn't own?"
        )

    d.update({"tokens": current_tokens})

    _ = db_to_use[Collections.tokens_accounts].bulk_write(
        [
            ReplaceOne(
                {"_id": result.from_address},
                replacement=d,
                upsert=True,
            )
        ]
    )


def process_event(cis: CIS, db_to_use, instance, event):
    tag_, result = cis.process_log_events(event)
    if result:
        if tag_ == 255:
            save_transfer(db_to_use, instance, result)
        if tag_ == 254:
            save_mint(db_to_use, instance, result)
        if tag_ == 253:
            save_burn(db_to_use, instance, result)
        if tag_ == 252:
            # operatorUpdateEvent
            pass
        if tag_ == 251:
            save_metadata(db_to_use, instance, result)

    else:
        print(f"{instance.id} gave error with tag {tag_} for event {event}.")


net = "testnet"
db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet

# REMOVE in HEARTBEAT
db_to_use[Collections.tokens_accounts].delete_many({})
db_to_use[Collections.tokens_token_addresses].delete_many({})
# REMOVE in HEARTBEAT


result = [MongoTypeInstance(**x) for x in db_to_use[Collections.instances].find()]


for index, instance in enumerate(result):
    if instance.v1:
        entrypoint = instance.v1.name[5:] + ".supports"
        if entrypoint in instance.v1.methods:
            index = int(instance.id.split(",")[0][1:])
            subindex = int(instance.id.split(",")[1][:-1])
            cis = CIS(grpcclient, index, subindex, entrypoint, NET(net))
            supports_cis_2 = cis.supports_standard(StandardIdentifiers.CIS_2)
            if supports_cis_2:
                print(
                    f"{instance.v1.name}: {instance.id}: Supports CIS-2 = {supports_cis_2}"
                )

            # now look up transactions with this contract
            result = [
                MongoTypeInvolvedContract(**x)
                for x in db_to_use[Collections.involved_contracts].find(
                    {"contract": instance.id}
                )
            ]
            tx_hashes = [x.id.split("-")[0] for x in result]
            int_result = (
                db_to_use[Collections.transactions]
                .find({"_id": {"$in": tx_hashes}})
                .sort("block_info.height", ASCENDING)
            )

            tx_result = [CCD_BlockItemSummary(**x) for x in int_result]
            for tx in tx_result:
                if tx.account_transaction.effects.contract_initialized:
                    for (
                        event
                    ) in tx.account_transaction.effects.contract_initialized.events:
                        process_event(cis, db_to_use, instance, event)

                if tx.account_transaction.effects.contract_update_issued:
                    for (
                        effect
                    ) in tx.account_transaction.effects.contract_update_issued.effects:
                        if effect:
                            if effect.interrupted:
                                for event in effect.interrupted.events:
                                    process_event(cis, db_to_use, instance, event)
                            if effect.updated:
                                for event in effect.updated.events:
                                    process_event(cis, db_to_use, instance, event)

            pass

# heights = [x["block_info"]["height"] for x in result]

# d = {"_id": "special_purpose_block_request", "heights": heights}
# _ = mongodb.mainnet[Collections.helpers].bulk_write(
#     [
#         ReplaceOne(
#             {"_id": "special_purpose_block_request"},
#             replacement=d,
#             upsert=True,
#         )
#     ]
# )
pass
