from sharingiscaring.mongodb import (
    MongoDB,
    Collections,
    MongoTypeModule,
    MongoTypeInstance,
    MongoTypeInvolvedContract,
)
from sharingiscaring.GRPCClient import GRPCClient
from sharingiscaring.GRPCClient.CCD_Types import *

from sharingiscaring.cis import (
    CIS,
    StandardIdentifiers,
    mintEvent,
    burnEvent,
    transferEvent,
    updateOperatorEvent,
    tokenMetadataEvent,
    LoggedEvents,
    MongoTypeTokenAddress,
    MongoTypeLoggedEvent,
    MongoTypeTokenHolderAddress,
    MongoTypeTokenForAddress,
)
from sharingiscaring.enums import NET
from sharingiscaring.tooter import Tooter
from pymongo import ASCENDING, DESCENDING, ReplaceOne
from pymongo.collection import Collection
from pymongo.database import Database
from sharingiscaring.GRPCClient.CCD_Types import *
from rich import print
from env import *
from rich.console import Console
from rich.progress import track

console = Console()

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


def execute_save(collection: Collection, replacement, _id: str):
    repl_dict = replacement.dict()
    if "id" in repl_dict:
        del repl_dict["id"]

    # sort tokens and token_holders
    if "tokens" in repl_dict:
        sorted_tokens = list(repl_dict["tokens"].keys())
        sorted_tokens.sort()
        tokens_sorted = {i: repl_dict["tokens"][i] for i in sorted_tokens}
        repl_dict["tokens"] = tokens_sorted

    if "token_holders" in repl_dict:
        sorted_holders = list(repl_dict["token_holders"].keys())
        sorted_holders.sort()
        token_holders_sorted = {
            i: repl_dict["token_holders"][i] for i in sorted_holders
        }
        repl_dict["token_holders"] = token_holders_sorted

    _ = collection.bulk_write(
        [
            ReplaceOne(
                {"_id": _id},
                replacement=repl_dict,
                upsert=True,
            )
        ]
    )


def restore_state_for_token_address(
    db_to_use: dict[Collections, Collection],
    token_address: str,
):
    d: dict = db_to_use[Collections.tokens_token_addresses].find_one(
        {"_id": token_address}
    )

    d.update(
        {
            "token_amount": str(int(0)),  # mongo limitation on int size
            "token_holders": {},  # {CCD_AccountAddress, str(token_amount)}
            "last_height_processed": 0,
        }
    )

    d = MongoTypeTokenAddress(**d)
    execute_save(db_to_use[Collections.tokens_token_addresses], d, token_address)


def copy_token_holders_state_to_address_and_save(
    token_address_info: MongoTypeTokenAddress, address: str
):
    token_address = token_address_info.id
    d = db_to_use[Collections.tokens_accounts].find_one({"_id": address})
    # if this account doesn't have tokens, create empty dict.
    if not d:
        d = MongoTypeTokenHolderAddress(
            **{
                "_id": address,
                "tokens": {},
            }
        )  # keyed on token_address
    else:
        d = MongoTypeTokenHolderAddress(**d)

    token_to_save = MongoTypeTokenForAddress(
        **{
            "token_address": token_address,
            "contract": token_address_info.contract,
            "token_id": token_address_info.token_id,
            "token_amount": str(token_address_info.token_holders.get(address, 0)),
        }
    )

    d.tokens[token_address] = token_to_save

    if token_to_save.token_amount == str(0):
        del d.tokens[token_address]

    execute_save(db_to_use[Collections.tokens_accounts], d, address)


def save_mint(
    db_to_use: dict[Collections, Collection],
    instance_address: str,
    result: mintEvent,
    height: int,
):
    token_address = f"{instance_address}-{result.token_id}"
    d = db_to_use[Collections.tokens_token_addresses].find_one({"_id": token_address})
    if not d:
        d = MongoTypeTokenAddress(
            **{
                "_id": token_address,
                "contract": instance_address,
                "token_id": result.token_id,
                "token_amount": str(int(0)),  # mongo limitation on int size
                "token_holders": {},  # {CCD_AccountAddress, str(token_amount)}
                "last_height_processed": height,
            }
        )
    else:
        d = MongoTypeTokenAddress(**d)

    token_holders: dict[CCD_AccountAddress, str] = d.token_holders
    token_holders[result.to_address] = str(
        int(token_holders.get(result.to_address, "0")) + result.token_amount
    )
    d.token_amount = str((int(d.token_amount) + result.token_amount))
    d.token_holders = token_holders

    execute_save(db_to_use[Collections.tokens_token_addresses], d, token_address)
    copy_token_holders_state_to_address_and_save(d, result.to_address)


def save_metadata(
    db_to_use: dict[Collections, Collection],
    instance_address: str,
    result: tokenMetadataEvent,
    height: int,
):
    token_address = f"{instance_address}-{result.token_id}"
    d = db_to_use[Collections.tokens_token_addresses].find_one({"_id": token_address})
    if not d:
        d = MongoTypeTokenAddress(
            **{
                "_id": token_address,
                "contract": instance_address,
                "token_id": result.token_id,
                "last_height_processed": height,
            }
        )
    else:
        d = MongoTypeTokenAddress(**d)

    d.metadata_url = result.metadata.url

    execute_save(db_to_use[Collections.tokens_token_addresses], d, token_address)


def save_operator(
    db_to_use: dict[Collections, Collection],
    instance_address: str,
    result: tokenMetadataEvent,
    height: int,
):
    token_address = f"{instance_address}-{result.token_id}"
    d = db_to_use[Collections.tokens_token_addresses].find_one({"_id": token_address})
    if not d:
        d = MongoTypeTokenAddress(
            **{
                "_id": token_address,
                "contract": instance_address,
                "token_id": result.token_id,
                "last_height_processed": height,
            }
        )
    else:
        d = MongoTypeTokenAddress(**d)

    d.metadata_url = result.metadata.url

    execute_save(db_to_use[Collections.tokens_token_addresses], d, token_address)


def save_transfer(
    db_to_use: dict[Collections, Collection],
    instance_address: str,
    result: transferEvent,
    height: int,
):
    token_address = f"{instance_address}-{result.token_id}"
    d = db_to_use[Collections.tokens_token_addresses].find_one({"_id": token_address})
    if not d:
        return None

    d = MongoTypeTokenAddress(**d)

    try:
        token_holders: dict[CCD_AccountAddress, str] = d.token_holders
    except:
        console.log(f"{result.tag}: {token_address} | {d} has no field token_holders?")
        Exception(
            console.log(
                f"{result.tag}: {token_address} | {d} has no field token_holders?"
            )
        )

    token_holders[result.to_address] = str(
        int(token_holders.get(result.to_address, "0")) + result.token_amount
    )
    try:
        token_holders[result.from_address] = str(
            int(token_holders.get(result.from_address, None)) - result.token_amount
        )
        if int(token_holders[result.from_address]) >= 0:
            d.token_holders = token_holders
            d.last_height_processed = height
            execute_save(
                db_to_use[Collections.tokens_token_addresses], d, token_address
            )

            copy_token_holders_state_to_address_and_save(d, result.from_address)
            copy_token_holders_state_to_address_and_save(d, result.to_address)

    except:
        if result.token_amount > 0:
            console.log(
                f"{result.tag}: {result.from_address} is not listed as token holder for {token_address}?"
            )


def save_burn(
    db_to_use: dict[Collections, Collection],
    instance_address: str,
    result: burnEvent,
    height: int,
):
    token_address = f"{instance_address}-{result.token_id}"

    d = MongoTypeTokenAddress(
        **db_to_use[Collections.tokens_token_addresses].find_one({"_id": token_address})
    )

    token_holders: dict[CCD_AccountAddress, str] = d.token_holders
    try:
        token_holders[result.from_address] = str(
            int(token_holders.get(result.from_address, "0")) - result.token_amount
        )
        if token_holders[result.from_address] == str(0):
            del token_holders[result.from_address]

        d.token_amount = str((int(d.token_amount) - result.token_amount))
        d.token_holders = token_holders
        d.last_height_processed = height

        if int(d.token_amount) >= 0:
            execute_save(
                db_to_use[Collections.tokens_token_addresses], d, token_address
            )
            copy_token_holders_state_to_address_and_save(d, result.from_address)

    except:
        console.log(
            f"{result.tag}: {result.from_address} is not listed as token holder for {token_address}?"
        )
        # exit


def save_logged_event(
    db_to_use,
    tag_: int,
    result: Union[
        mintEvent, burnEvent, transferEvent, updateOperatorEvent, tokenMetadataEvent
    ],
    instance_address: str,
    event: str,
    height: int,
    tx_hash: str,
    tx_index: int,
    ordering: int,
    _id_postfix: str,
):
    if tag_ in [255, 254, 253, 252, 251]:
        if tag_ == 252:
            token_address = f"{instance_address}-operator"
        else:
            token_address = f"{instance_address}-{result.token_id}"
        _id = f"{height}-{token_address}-{event}-{_id_postfix}"
        if result:
            result_dict = result.dict()
        else:
            result_dict = {}
        if "token_amount" in result_dict:
            result_dict["token_amount"] = str(result_dict["token_amount"])

        d = {
            "_id": _id,
            "logged_event": event,
            "result": result_dict,
            "tag": tag_,
            "event_type": LoggedEvents(tag_).name,
            "block_height": height,
            "tx_hash": tx_hash,
            "tx_index": tx_index,
            "ordering": ordering,
            "token_address": token_address,
            "contract": instance_address,
        }
        _ = db_to_use[Collections.tokens_logged_events].bulk_write(
            [
                ReplaceOne(
                    {"_id": _id},
                    replacement=d,
                    upsert=True,
                )
            ]
        )


def execute_logged_event(
    db_to_use,
    tag_: int,
    result: Union[mintEvent, burnEvent, transferEvent, tokenMetadataEvent],
    instance_address: str,
    height: int,
):
    if tag_ == 255:
        save_transfer(db_to_use, instance_address, result, height)
    elif tag_ == 254:
        save_mint(db_to_use, instance_address, result, height)
    elif tag_ == 253:
        save_burn(db_to_use, instance_address, result, height)
    elif tag_ == 252:
        pass
        # we only save the logged event, but to not process this in
        # token_address or accounts.
        # save_operator(db_to_use, instance_address, result, height)
    elif tag_ == 251:
        save_metadata(db_to_use, instance_address, result, height)


def process_event(
    cis: CIS,
    db_to_use,
    instance_address: str,
    event: str,
    height: int,
    tx_hash: str,
    tx_index: int,
    ordering: int,
    _id_postfix: str,
):
    tag_, result = cis.process_log_events(event)
    if result:
        if tag_ in [255, 254, 253, 252, 251]:
            if tag_ == 252:
                token_address = f"{instance_address}-operator"
            else:
                token_address = f"{instance_address}-{result.token_id}"
            logged_event_id = f"{height}-{token_address}-{event}-{_id_postfix}"
            stored_logged_event = db_to_use[Collections.tokens_logged_events].find_one(
                {"_id": logged_event_id}
            )
            if not stored_logged_event:
                # we have not processed this log event before,
                # so we can safely process the current logged event
                save_logged_event(
                    db_to_use,
                    tag_,
                    result,
                    instance_address,
                    event,
                    height,
                    tx_hash,
                    tx_index,
                    ordering,
                    _id_postfix,
                )
                execute_logged_event(db_to_use, tag_, result, instance_address, height)

            else:
                console.log(f"Re-processing logged event: {logged_event_id}")
                # we are (re-) processing a block that is earlier than the last block
                # on which the state is based. We need to recalculate state from the beginning.
                save_logged_event(
                    db_to_use,
                    tag_,
                    result,
                    instance_address,
                    event,
                    height,
                    tx_hash,
                    tx_index,
                    ordering,
                    _id_postfix,
                )
                restore_state_for_token_address(db_to_use, token_address)

                # we will read all logged events for this token address from the collection
                # and process them in order.

                stored_logged_events_for_token_address = [
                    MongoTypeLoggedEvent(**x)
                    for x in db_to_use[Collections.tokens_logged_events]
                    .find({"token_address": token_address})
                    .sort(
                        [
                            ("block_height", ASCENDING),
                            ("tx_index", ASCENDING),
                            ("ordering", ASCENDING),
                        ]
                    )
                ]
                for event in stored_logged_events_for_token_address:
                    if event.tag == 255:
                        result = transferEvent(**event.result)
                    elif event.tag == 254:
                        result = mintEvent(**event.result)
                    elif event.tag == 253:
                        result = burnEvent(**event.result)
                    elif event.tag == 252:
                        result = updateOperatorEvent(**event.result)
                    elif event.tag == 251:
                        result = tokenMetadataEvent(**event.result)

                    execute_logged_event(
                        db_to_use,
                        event.tag,
                        result,
                        event.contract,
                        event.block_height,
                    )


net = "testnet"
db_to_use: Database = mongodb.testnet if net == "testnet" else mongodb.mainnet
db_to_use_for_views: Database = (
    mongodb.testnet_db if net == "testnet" else mongodb.mainnet_db
)

# REMOVE in HEARTBEAT
start_processing_at_height = 2806849
# start_processing_at_height = 0

if start_processing_at_height == 0:
    db_to_use[Collections.tokens_accounts].delete_many({})
    db_to_use[Collections.tokens_logged_events].delete_many({})
    db_to_use[Collections.tokens_token_addresses].delete_many({})
# REMOVE in HEARTBEAT


collections_in_db = db_to_use_for_views.list_collection_names()
if "transactions_view" in collections_in_db:
    db_to_use_for_views["transactions_view"].drop()
db_to_use_for_views.command(
    {
        "create": "transactions_view",
        "viewOn": "transactions",
        "pipeline": [{"$match": {"_id": {"$exists": True}}}],
    }
)

last_height = CCD_BlockItemSummary(
    **list(
        db_to_use_for_views["transactions_view"]
        .find()
        .sort("block_info.height", DESCENDING)
        .limit(1)
    )[0]
).block_info.height
console.log(f"Last block height from involved contracts: {last_height:,.0f}.")
result: list[CCD_BlockItemSummary] = [
    CCD_BlockItemSummary(**x)
    for x in db_to_use_for_views["transactions_view"]
    .find(
        {
            "$and": [
                {
                    "$or": [
                        {
                            "account_transaction.effects.contract_initialized": {
                                "$exists": True
                            }
                        },
                        {
                            "account_transaction.effects.contract_update_issued": {
                                "$exists": True
                            }
                        },
                    ]
                },
                {"block_info.height": {"$gte": start_processing_at_height}},
            ]
        }
    )
    .sort([("block_info.height", ASCENDING), ("index", ASCENDING)])
]

cis = CIS()
console.log(f"Found {len(result):,.0f} transactions.")
for tx in track(result):
    # this is the ordering of effects as encountered in the transaction
    ordering = 0

    if tx.account_transaction.effects.contract_initialized:
        contract_index = (
            tx.account_transaction.effects.contract_initialized.address.index
        )
        contract_subindex = (
            tx.account_transaction.effects.contract_initialized.address.subindex
        )
        instance_address = f"<{contract_index},{contract_subindex}>"
        entrypoint = f"{tx.account_transaction.effects.contract_initialized.init_name[5:]}.supports"
        cis = CIS(grpcclient, contract_index, contract_subindex, entrypoint, NET(net))
        supports_cis_1_2 = cis.supports_standards(
            [StandardIdentifiers.CIS_1, StandardIdentifiers.CIS_2]
        )
        if supports_cis_1_2:
            for index, event in enumerate(
                tx.account_transaction.effects.contract_initialized.events
            ):
                ordering += 1
                process_event(
                    cis,
                    db_to_use,
                    instance_address,
                    event,
                    tx.block_info.height,
                    tx.hash,
                    tx.index,
                    ordering,
                    f"initialized-{tx.index}-{index}",
                )

    if tx.account_transaction.effects.contract_update_issued:
        for effect_index, effect in enumerate(
            tx.account_transaction.effects.contract_update_issued.effects
        ):
            if effect:
                if effect.interrupted:
                    contract_index = effect.interrupted.address.index
                    contract_subindex = effect.interrupted.address.subindex
                    instance_address = f"<{contract_index},{contract_subindex}>"
                    instance = MongoTypeInstance(
                        **db_to_use[Collections.instances].find_one(
                            {"_id": instance_address}
                        )
                    )
                    if instance:
                        if instance.v1:
                            entrypoint = instance.v1.name[5:] + ".supports"
                            cis = CIS(
                                grpcclient,
                                contract_index,
                                contract_subindex,
                                entrypoint,
                                NET(net),
                            )
                            supports_cis_1_2 = cis.supports_standards(
                                [StandardIdentifiers.CIS_1, StandardIdentifiers.CIS_2]
                            )
                            if supports_cis_1_2:
                                for index, event in enumerate(
                                    effect.interrupted.events
                                ):
                                    ordering += 1
                                    process_event(
                                        cis,
                                        db_to_use,
                                        instance_address,
                                        event,
                                        tx.block_info.height,
                                        tx.hash,
                                        tx.index,
                                        ordering,
                                        f"interrupted-{tx.index}-{effect_index}-{index}",
                                    )

                if effect.updated:
                    contract_index = effect.updated.address.index
                    contract_subindex = effect.updated.address.subindex
                    instance_address = f"<{contract_index},{contract_subindex}>"
                    entrypoint = f"{effect.updated.receive_name.split('.')[0]}.supports"
                    cis = CIS(
                        grpcclient,
                        contract_index,
                        contract_subindex,
                        entrypoint,
                        NET(net),
                    )
                    supports_cis_1_2 = cis.supports_standards(
                        [StandardIdentifiers.CIS_1, StandardIdentifiers.CIS_2]
                    )
                    if supports_cis_1_2:
                        for index, event in enumerate(effect.updated.events):
                            ordering += 1
                            process_event(
                                cis,
                                db_to_use,
                                instance_address,
                                event,
                                tx.block_info.height,
                                tx.hash,
                                tx.index,
                                ordering,
                                f"updated-{tx.index}-{effect_index}-{index}",
                            )

pass


# for index, instance in enumerate(result):
#     if instance.v1:
#         entrypoint = instance.v1.name[5:] + ".supports"
#         if entrypoint in instance.v1.methods:
#             index = int(instance.id.split(",")[0][1:])
#             subindex = int(instance.id.split(",")[1][:-1])
#             cis = CIS(grpcclient, index, subindex, entrypoint, NET(net))
#             supports_cis_2 = cis.supports_standard(StandardIdentifiers.CIS_2)
#             if supports_cis_2:
#                 console.log(
#                     f"{instance.v1.name}: {instance.id}: Supports CIS-2 = {supports_cis_2}"
#                 )

#             # now look up transactions with this contract
#             result = [
#                 MongoTypeInvolvedContract(**x)
#                 for x in db_to_use_for_views["involved_contracts_view"].find(
#                     {"contract": instance.id}
#                 )
#             ]
#             tx_hashes = [x.id.split("-")[0] for x in result]
#             s = dt.datetime.utcnow()
#             int_result = (
#                 db_to_use[Collections.transactions]
#                 .find({"_id": {"$in": tx_hashes}})
#                 .sort([("block_info.height", ASCENDING), ("index", ASCENDING)])
#             )
#             console.log(
#                 f"{len(tx_hashes)=} for {instance.id} took {(dt.datetime.utcnow() - s).total_seconds():,.3f}s."
#             )
#             tx_result = [CCD_BlockItemSummary(**x) for x in int_result]

#             for loop_index, tx in enumerate(tx_result):
#                 # console.log(f"{loop_index}", end=" | ")
#                 if tx.account_transaction.effects.contract_initialized:
#                     for index, event in enumerate(
#                         tx.account_transaction.effects.contract_initialized.events
#                     ):
#                         process_event(
#                             cis,
#                             db_to_use,
#                             instance.id,
#                             event,
#                             tx.block_info.height,
#                             tx.hash,
#                             f"initialized-{index}",
#                         )

#                 if tx.account_transaction.effects.contract_update_issued:
#                     for (
#                         effect
#                     ) in tx.account_transaction.effects.contract_update_issued.effects:
#                         if effect:
#                             if effect.interrupted:
#                                 for index, event in enumerate(
#                                     effect.interrupted.events
#                                 ):
#                                     process_event(
#                                         cis,
#                                         db_to_use,
#                                         instance.id,
#                                         event,
#                                         tx.block_info.height,
#                                         tx.hash,
#                                         f"interrupted-{index}",
#                                     )
#                             if effect.updated:
#                                 for index, event in enumerate(effect.updated.events):
#                                     process_event(
#                                         cis,
#                                         db_to_use,
#                                         instance.id,
#                                         event,
#                                         tx.block_info.height,
#                                         tx.hash,
#                                         f"updated-{index}",
#                                     )

#             pass

# console.log(
#     f"Processed up to {last_height:,.0f}. Now start heartbeat from this point by adjusting the value in helpers."
# )
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
