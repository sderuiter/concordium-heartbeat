import asyncio
from sharingiscaring.GRPCClient import GRPCClient
from rich import print
import datetime as dt
from sharingiscaring.GRPCClient.CCD_Types import *
from sharingiscaring.GRPCClient.types_pb2 import Empty
from sharingiscaring.tooter import Tooter, TooterChannel, TooterType
from sharingiscaring.mongodb import MongoDB, Collections, MongoTypeInstance
from sharingiscaring.enums import NET
from sharingiscaring.node import ConcordiumNodeFromDashboard
from sharingiscaring.cis import (
    CIS,
    StandardIdentifiers,
    mintEvent,
    burnEvent,
    transferEvent,
    tokenMetadataEvent,
)
import sharingiscaring.GRPCClient.wadze as wadze
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo import ASCENDING, DESCENDING
from pymongo import ReplaceOne
import aiohttp
import json
import requests
from typing import Dict
from env import *
from datetime import timedelta
import io
import time
from rich.console import Console
import chardet

console = Console()
from copy import copy

tooter: Tooter = Tooter(
    ENVIRONMENT, BRANCH, NOTIFIER_API_TOKEN, API_TOKEN, FASTMAIL_TOKEN
)
mongodb = MongoDB(
    {
        "MONGODB_PASSWORD": MONGODB_PASSWORD,
        "MONGO_IP": MONGO_IP,
        "MONGO_PORT": MONGO_PORT,
    },
    tooter,
)


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


class ClassificationResult:
    """
    This is a result type to store the classification of a transaction,
    used to determine which indices, if any, need to be created for this
    transaction.
    """

    def __init__(self):
        self.sender = None
        self.receiver = None
        self.tx_hash = None
        self.memo = None
        self.contract = None
        self.amount = None
        self.type = None
        self.contents = None
        self.accounts_involved_all = False
        self.accounts_involved_transfer = False
        self.contracts_involved = False
        self.list_of_contracts_involved = []


class Heartbeat:
    def __init__(
        self, grpcclient: GRPCClient, tooter: Tooter, mongodb: MongoDB, net: str
    ):
        self.grpcclient = grpcclient
        self.tooter = tooter
        self.mongodb = mongodb
        self.net = net
        self.db: Dict[Collections, Collection] = (
            self.mongodb.mainnet if self.net == "mainnet" else self.mongodb.testnet
        )
        self.finalized_block_infos_to_process: list[CCD_BlockInfo] = []
        self.special_purpose_block_infos_to_process: list[CCD_BlockInfo] = []

        self.existing_source_modules: dict[CCD_ModuleRef, set] = {}
        self.queues: Dict[Collections, list] = {}
        for q in Queue:
            self.queues[q] = []

    def get_sum_amount_from_scheduled_transfer(self, schedule: list[CCD_NewRelease]):
        sum = 0
        for release in schedule:
            sum += release.amount
        return sum

    def decode_memo(self, hex):
        bs = io.BytesIO(bytes.fromhex(hex))
        n = int.from_bytes(bs.read(1), byteorder="little")
        value = bs.read()
        encoding_guess = chardet.detect(value)
        if encoding_guess["encoding"] and encoding_guess["confidence"] > 0.75:
            try:
                memo = bytes.decode(value, encoding_guess["encoding"])
                return memo
            except:
                return value
        else:
            return value

    def classify_transaction(self, tx: CCD_BlockItemSummary):
        """
        This classifies a transaction (based on GRPCv2 output)
        for inclusion in the index tables.
        """
        result = ClassificationResult()
        result.type = tx.type

        if tx.account_creation:
            result.sender = tx.account_creation.address

        if tx.account_transaction:
            result.sender = tx.account_transaction.sender

            if tx.account_transaction.outcome == "success":
                effects = tx.account_transaction.effects

                if effects.account_transfer:
                    result.accounts_involved_transfer = True

                    ac = effects.account_transfer
                    result.amount = ac.amount
                    result.receiver = ac.receiver
                    if ac.memo:
                        result.memo = self.decode_memo(ac.memo)

                elif effects.transferred_with_schedule:
                    result.accounts_involved_transfer = True

                    ts = effects.transferred_with_schedule
                    result.amount = self.get_sum_amount_from_scheduled_transfer(
                        ts.amount
                    )
                    result.receiver = ts.receiver
                    if ts.memo:
                        result.memo = self.decode_memo(ts.memo)

                elif effects.contract_initialized:
                    result.contracts_involved = True

                    ci = effects.contract_initialized
                    result.list_of_contracts_involved.append({"address": ci.address})

                elif effects.contract_update_issued:
                    result.contracts_involved = True

                    update_effects = effects.contract_update_issued.effects

                    for effect in update_effects:
                        if effect.interrupted:
                            result.list_of_contracts_involved.append(
                                {"address": effect.interrupted.address}
                            )

                        elif effect.resumed:
                            result.list_of_contracts_involved.append(
                                {"address": effect.resumed.address}
                            )

                        elif effect.updated:
                            result.list_of_contracts_involved.append(
                                {
                                    "address": effect.updated.address,
                                    "receive_name": effect.updated.receive_name,
                                }
                            )

                        elif effect.transferred:
                            if type(effect.transferred.sender) == CCD_ContractAddress:
                                result.list_of_contracts_involved.append(
                                    {"address": effect.transferred.sender}
                                )

                            if type(effect.transferred.receiver) == CCD_ContractAddress:
                                result.list_of_contracts_involved.append(
                                    {"address": effect.transferred.receiver}
                                )

        return result

    def index_transfer_and_all(
        self,
        tx: CCD_BlockItemSummary,
        result: ClassificationResult,
        block_info: CCD_BlockInfo,
    ):
        dct_transfer_and_all = {
            "_id": tx.hash,
            "sender": result.sender,
            "receiver": result.receiver,
            "sender_canonical": result.sender[:29] if result.sender else None,
            "receiver_canonical": result.receiver[:29] if result.receiver else None,
            "amount": result.amount,
            "type": result.type.dict(),
            "block_height": block_info.height,
        }
        if result.memo:
            dct_transfer_and_all.update({"memo": result.memo})

        return dct_transfer_and_all

    def index_contract(
        self,
        tx: CCD_BlockItemSummary,
        result: ClassificationResult,
        contract: CCD_ContractAddress,
        block_info: CCD_BlockInfo,
    ):
        _id = f"{tx.hash}-<{contract.index},{contract.subindex}>"
        dct = {
            "_id": _id,
            "index": contract.index,
            "subindex": contract.subindex,
            "contract": f"<{contract.index},{contract.subindex}>",
            "type": result.type.dict(),
            "block_height": block_info.height,
        }

        return dct

    def get_module_metadata(self, block_hash: str, module_ref: str) -> Dict[str, str]:
        # console.log(f"{module_ref=}")
        ms = self.grpcclient.get_module_source(module_ref, block_hash, NET(self.net))

        if ms.v0:
            bs = io.BytesIO(bytes.fromhex(ms.v0))
        else:
            bs = io.BytesIO(bytes.fromhex(ms.v1))

        module = wadze.parse_module(bs.read())

        results = {}

        if "export" in module.keys():
            for line in module["export"]:
                split_line = str(line).split("(")
                if split_line[0] == "ExportFunction":
                    split_line = str(line).split("'")
                    name = split_line[1]

                    if name[:5] == "init_":
                        results["module_name"] = name[5:]
                    else:
                        method_name = name.split(".")[1] if "." in name else name
                        if "methods" in results:
                            results["methods"].append(method_name)
                        else:
                            results["methods"] = [method_name]

        return results

    def add_back_updated_modules_to_queue(
        self, current_block_to_process: CCD_BlockInfo
    ):
        self.queues[Queue.modules] = []

        # make this into a set to remove duplicates...remember testnet with 991K module updates in 1K blocks...
        self.queues[Queue.updated_modules] = list(
            set(self.queues[Queue.updated_modules])
        )
        # for module_ref in self.existing_source_modules.keys():
        for module_ref in self.queues[Queue.updated_modules]:
            try:
                results = self.get_module_metadata(
                    current_block_to_process.hash, module_ref
                )
            except:
                results = {"module_name": "", "methods": []}
            module = {
                "_id": module_ref,
                "module_name": results["module_name"]
                if "module_name" in results.keys()
                else None,
                "methods": results["methods"] if "methods" in results.keys() else None,
                "contracts": list(self.existing_source_modules[module_ref]),
            }
            self.queues[Queue.modules].append(
                ReplaceOne({"_id": module_ref}, module, upsert=True)
            )

    def generate_indices_based_on_transactions(
        self,
        transactions: list[CCD_BlockItemSummary],
        block_info: CCD_BlockInfo,
    ):
        """
        Given a list of transactions, apply rules to determine which index needs to be updated.
        Add this to a to_be_sent_to_mongo list and do insert_many.
        """
        # console.log (f"Generating indices for {len(transactions):,.0f} transactions...")

        for tx in transactions:
            result = self.classify_transaction(tx)

            dct_transfer_and_all = self.index_transfer_and_all(tx, result, block_info)

            # always store tx in this collection
            self.queues[Queue.involved_all].append(
                ReplaceOne(
                    {"_id": dct_transfer_and_all["_id"]},
                    dct_transfer_and_all,
                    upsert=True,
                )
            )

            # only store tx in this collection if it's a transfer
            if result.accounts_involved_transfer:
                self.queues[Queue.involved_transfer].append(
                    ReplaceOne(
                        {"_id": dct_transfer_and_all["_id"]},
                        dct_transfer_and_all,
                        upsert=True,
                    )
                )

            # only store tx in this collection if it contains a smart contract
            if result.contracts_involved:
                for contract in result.list_of_contracts_involved:
                    index_contract = self.index_contract(
                        tx, result, contract["address"], block_info
                    )

                    try:
                        instance_info = self.grpcclient.get_instance_info(
                            index_contract["index"],
                            index_contract["subindex"],
                            block_info.hash,
                            NET(self.net),
                        )
                        instance_info: dict = instance_info.dict(exclude_none=True)

                        instance_info.update({"_id": index_contract["contract"]})
                        if instance_info["v0"]["source_module"] == "":
                            del instance_info["v0"]
                            _source_module = instance_info["v1"]["source_module"]
                        if instance_info["v1"]["source_module"] == "":
                            del instance_info["v1"]
                            _source_module = instance_info["v0"]["source_module"]

                        self.queues[Queue.instances].append(
                            ReplaceOne(
                                {"_id": index_contract["contract"]},
                                instance_info,
                                upsert=True,
                            )
                        )

                        if not _source_module in self.existing_source_modules.keys():
                            self.existing_source_modules[_source_module] = set()

                        self.existing_source_modules[_source_module].add(
                            index_contract["contract"]
                        )
                        self.queues[Queue.updated_modules].append(_source_module)

                        index_contract.update({"source_module": _source_module})

                        self.queues[Queue.involved_contract].append(
                            ReplaceOne(
                                {"_id": index_contract["_id"]},
                                index_contract,
                                upsert=True,
                            )
                        )
                    except:
                        console.log(
                            f"block or instance not found for {index_contract}..."
                        )

    def lookout_for_account_transaction(
        self, block_info: CCD_BlockInfo, tx: CCD_BlockItemSummary
    ):
        if tx.account_transaction:
            if tx.account_transaction.effects.data_registered:
                query = {"_id": "last_known_data_registered"}
                self.db[Collections.helpers].replace_one(
                    query,
                    {
                        "_id": "last_known_data_registered",
                        "tx_hash": tx.hash,
                        "hash": block_info.hash,
                        "height": block_info.height,
                    },
                    upsert=True,
                )

            elif tx.account_transaction.effects.account_transfer:
                query = {"_id": "last_known_transfer"}
                self.db[Collections.helpers].replace_one(
                    query,
                    {
                        "_id": "last_known_transfer",
                        "tx_hash": tx.hash,
                        "hash": block_info.hash,
                        "height": block_info.height,
                    },
                    upsert=True,
                )

            if tx.account_transaction.effects.baker_configured:
                query = {"_id": "last_known_baker_configured"}
                self.db[Collections.helpers].replace_one(
                    query,
                    {
                        "_id": "last_known_baker_configured",
                        "tx_hash": tx.hash,
                        "hash": block_info.hash,
                        "height": block_info.height,
                    },
                    upsert=True,
                )

        elif tx.account_creation:
            query = {"_id": "last_known_account_creation"}
            self.db[Collections.helpers].replace_one(
                query,
                {
                    "_id": "last_known_account_creation",
                    "tx_hash": tx.hash,
                    "hash": block_info.hash,
                    "height": block_info.height,
                },
                upsert=True,
            )

    def add_block_and_txs_to_queue(self, block_info: CCD_BlockInfo):
        try:
            json_block_info: dict = json.loads(block_info.json(exclude_none=True))
        except Exception as e:
            print(e)
        json_block_info.update({"_id": block_info.hash})
        json_block_info.update({"slot_time": block_info.slot_time})
        del json_block_info["arrive_time"]
        del json_block_info["receive_time"]
        json_block_info.update({"transaction_hashes": []})

        if block_info.transaction_count > 0:
            # console.log(block_info.height)
            block: CCD_Block = self.grpcclient.get_block_transaction_events(
                block_info.hash, NET(self.net)
            )

            json_block_info.update(
                {"transaction_hashes": [x.hash for x in block.transaction_summaries]}
            )

            for tx in block.transaction_summaries:
                json_tx: dict = json.loads(tx.json(exclude_none=True))

                json_tx.update({"_id": tx.hash})
                json_tx.update(
                    {
                        "block_info": {
                            "height": block_info.height,
                            "hash": block_info.hash,
                            "slot_time": block_info.slot_time,
                        }
                    }
                )
                self.queues[Queue.transactions].append(
                    ReplaceOne({"_id": tx.hash}, replacement=json_tx, upsert=True)
                )
                # self.lookout_for_account_transaction(block_info, tx)

            self.generate_indices_based_on_transactions(
                block.transaction_summaries, block_info
            )

        self.queues[Queue.blocks].append(
            ReplaceOne(
                {"_id": block_info.hash}, replacement=json_block_info, upsert=True
            )
        )
        self.queues[Queue.block_heights].append(block_info.height)

    def lookout_for_end_of_day(self, current_block_to_process: CCD_BlockInfo):
        end_of_day_timeframe_start = dt.time(0, 0, 0)
        end_of_day_timeframe_end = dt.time(0, 2, 0)

        if (
            current_block_to_process.slot_time.time() >= end_of_day_timeframe_start
        ) and (current_block_to_process.slot_time.time() <= end_of_day_timeframe_end):
            previous_block_height = current_block_to_process.height - 1
            previous_block_info = self.grpcclient.get_finalized_block_at_height(
                previous_block_height, NET(self.net)
            )

            if (
                current_block_to_process.slot_time.day
                != previous_block_info.slot_time.day
            ):
                start_of_day0 = previous_block_info.slot_time.replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                start_of_day1 = previous_block_info.slot_time.replace(
                    hour=0, minute=1, second=59, microsecond=999999
                )

                start_of_day_blocks = list(
                    self.db[Collections.blocks].find(
                        filter={
                            "$and": [
                                {"slot_time": {"$gte": start_of_day0}},
                                {"slot_time": {"$lte": start_of_day1}},
                            ]
                        }
                    )
                )

                if len(start_of_day_blocks) == 0:
                    start_of_day_blocks = [
                        self.grpcclient.get_finalized_block_at_height(0, NET(self.net))
                    ]
                    self.add_end_of_day_to_queue(
                        f"{previous_block_info.slot_time:%Y-%m-%d}",
                        start_of_day_blocks[0],
                        previous_block_info,
                    )
                else:
                    self.add_end_of_day_to_queue(
                        f"{previous_block_info.slot_time:%Y-%m-%d}",
                        CCD_BlockInfo(**start_of_day_blocks[0]),
                        previous_block_info,
                    )
                console.log(
                    f"End of day found for {previous_block_info.slot_time:%Y-%m-%d}"
                )

    def lookout_for_payday(self, current_block_to_process: CCD_BlockInfo):
        payday_timeframe_start = dt.time(7, 55, 0)
        payday_timeframe_end = dt.time(8, 10, 0)

        if (current_block_to_process.slot_time.time() > payday_timeframe_start) and (
            current_block_to_process.slot_time.time() < payday_timeframe_end
        ):
            special_events = self.grpcclient.get_block_special_events(
                current_block_to_process.hash, NET(self.net)
            )
            found = False
            for se in special_events:
                if se.payday_account_reward or se.payday_pool_reward:
                    found = True

                    # protection for slow payday calculation
                    # first get the current date that we have stored
                    # as last known payday.
                    # Then check if this date has already been picked up
                    # by the payday calculation by checking if there
                    # exists a payday with that date.
                    payday_not_yet_processed = True
                    while payday_not_yet_processed:
                        last_known_payday = self.db[Collections.helpers].find_one(
                            {"_id": "last_known_payday"}
                        )
                        # if we haven't started with the first payday
                        # we can continue.
                        if not last_known_payday:
                            payday_not_yet_processed = False
                            result = True
                        else:
                            result = self.db[Collections.paydays].find_one(
                                {"date": last_known_payday["date"]}
                            )
                        if not result:
                            console.log(
                                f"Payday {last_known_payday['date']} not yet processed. Sleeping for 10 sec."
                            )
                            time.sleep(10)
                        else:
                            payday_not_yet_processed = False

                    new_payday_date_string = (
                        f"{current_block_to_process.slot_time:%Y-%m-%d}"
                    )

                    query = {"_id": "last_known_payday"}
                    self.db[Collections.helpers].replace_one(
                        query,
                        {
                            "_id": "last_known_payday",
                            "date": new_payday_date_string,
                            "hash": current_block_to_process.hash,
                            "height": current_block_to_process.height,
                        },
                        upsert=True,
                    )
            if found:
                console.log(
                    f"Payday {current_block_to_process.slot_time:%Y-%m-%d} found!"
                )
                # time.sleep(90)

    def log_error_in_mongo(self, e, current_block_to_process: CCD_BlockInfo):
        query = {"_id": f"block_failure_{current_block_to_process.height}"}
        mongodb.self.db[Collections.helpers].replace_one(
            query,
            {
                "_id": f"block_failure_{current_block_to_process.height}",
                "height": current_block_to_process.height,
                "Exception": e,
            },
            upsert=True,
        )

    def log_last_processed_message_in_mongo(
        self, current_block_to_process: CCD_BlockInfo
    ):
        query = {"_id": "heartbeat_last_processed_block"}
        self.db[Collections.helpers].replace_one(
            query,
            {
                "_id": "heartbeat_last_processed_block",
                "height": current_block_to_process.height,
            },
            upsert=True,
        )

    async def send_to_mongo(self):
        """
        This method takes all queues with mongoDB messages and sends them to the
        respective collections.
        """
        while True:
            try:
                if len(self.queues[Queue.blocks]) > 0:
                    update_ = True
                    heartbeat_last_processed_block = CCD_BlockInfo(
                        **self.queues[Queue.blocks][-1]._doc
                    )
                    result = self.db[Collections.blocks].bulk_write(
                        self.queues[Queue.blocks]
                    )
                    if len(self.queues[Queue.block_heights]) == 1:
                        console.log(
                            f"Sent to Mongo  : {self.queues[Queue.block_heights][0]:,.0f}"
                        )
                    else:
                        console.log(
                            f"Sent to Mongo   : {self.queues[Queue.block_heights][0]:,.0f} - {self.queues[Queue.block_heights][-1]:,.0f}"
                        )
                    console.log(
                        f"B:  {len(self.queues[Queue.blocks]):5,.0f} | M {result.matched_count:5,.0f} | Mod {result.modified_count:5,.0f} | U {result.upserted_count:5,.0f}"
                    )
                    self.queues[Queue.blocks] = []
                    self.queues[Queue.block_heights] = []
                else:
                    update_ = False

                if len(self.queues[Queue.transactions]) > 0:
                    result = self.db[Collections.transactions].bulk_write(
                        self.queues[Queue.transactions]
                    )
                    console.log(
                        f"T:  {len(self.queues[Queue.transactions]):5,.0f} | M {result.matched_count:5,.0f} | Mod {result.modified_count:5,.0f} | U {result.upserted_count:5,.0f}"
                    )
                    self.queues[Queue.transactions] = []

                if len(self.queues[Queue.involved_all]) > 0:
                    result = self.db[Collections.involved_accounts_all].bulk_write(
                        self.queues[Queue.involved_all]
                    )
                    console.log(
                        f"A:  {len(self.queues[Queue.involved_all]):5,.0f} | M {result.matched_count:5,.0f} | Mod {result.modified_count:5,.0f} | U {result.upserted_count:5,.0f}"
                    )
                    self.queues[Queue.involved_all] = []
                if len(self.queues[Queue.involved_transfer]) > 0:
                    result = self.db[Collections.involved_accounts_transfer].bulk_write(
                        self.queues[Queue.involved_transfer]
                    )
                    console.log(
                        f"Tr: {len(self.queues[Queue.involved_transfer]):5,.0f} | M {result.matched_count:5,.0f} | Mod {result.modified_count:5,.0f} | U {result.upserted_count:5,.0f}"
                    )
                    self.queues[Queue.involved_transfer] = []

                if len(self.queues[Queue.involved_contract]) > 0:
                    result = self.db[Collections.involved_contracts].bulk_write(
                        self.queues[Queue.involved_contract]
                    )
                    console.log(
                        f"C:  {len(self.queues[Queue.involved_contract]):5,.0f} | M {result.matched_count:5,.0f} | Mod {result.modified_count:5,.0f} | U {result.upserted_count:5,.0f}"
                    )
                    self.queues[Queue.involved_contract] = []

                if len(self.queues[Queue.instances]) > 0:
                    result = self.db[Collections.instances].bulk_write(
                        self.queues[Queue.instances]
                    )
                    console.log(
                        f"I:  {len(self.queues[Queue.instances]):5,.0f} | M {result.matched_count:5,.0f} | Mod {result.modified_count:5,.0f} | U {result.upserted_count:5,.0f}"
                    )
                    self.queues[Queue.instances] = []

                if len(self.queues[Queue.modules]) > 0:
                    result = self.db[Collections.modules].bulk_write(
                        self.queues[Queue.modules]
                    )
                    console.log(
                        f"M:  {len(self.queues[Queue.modules]):5,.0f} | M {result.matched_count:5,.0f} | Mod {result.modified_count:5,.0f} | U {result.upserted_count:5,.0f}"
                    )
                    self.queues[Queue.modules] = []

                if len(self.queues[Queue.block_per_day]) > 0:
                    result = self.db[Collections.blocks_per_day].bulk_write(
                        self.queues[Queue.block_per_day]
                    )
                    console.log(f"End of day: U {result.upserted_count:5,.0f}")
                    self.queues[Queue.block_per_day] = []
                # this will only be set if the above store methods do not fail.

                # if update_:

            except Exception as e:
                # pass
                console.log(e)
                exit(1)

            await asyncio.sleep(1)

    def process_list_of_blocks(self, block_list: list):
        result = self.db[Collections.modules].find({})
        self.existing_source_modules: dict[CCD_ModuleRef, set] = {
            x["_id"]: set(x["contracts"]) for x in list(result)
        }
        self.queues[Queue.updated_modules] = []

        start = dt.datetime.now()
        while len(block_list) > 0:
            current_block_to_process: CCD_BlockInfo = block_list.pop(0)
            try:
                self.add_block_and_txs_to_queue(current_block_to_process)

                self.lookout_for_payday(current_block_to_process)
                self.lookout_for_end_of_day(current_block_to_process)

            except Exception as e:
                self.log_error_in_mongo(e, current_block_to_process)
        duration = dt.datetime.now() - start
        console.log(
            f"Spent {duration.total_seconds():,.0f} sec on {len(self.queues[Queue.transactions]):,.0f} txs."
        )

        if len(self.queues[Queue.instances]) > 0:
            self.add_back_updated_modules_to_queue(current_block_to_process)

        return current_block_to_process

    async def process_blocks(self):
        """
        This method takes the queue `finalized_block_infos_to_process` and processes
        each block.
        """
        while True:
            if len(self.finalized_block_infos_to_process) > 0:
                pp = copy(self.finalized_block_infos_to_process)
                # this is the last block that was processed
                current_block_to_process = self.process_list_of_blocks(
                    self.finalized_block_infos_to_process
                )

                self.log_last_processed_message_in_mongo(current_block_to_process)
                if len(pp) == 1:
                    console.log(f"Block processed: {pp[0].height:,.0f}")
                else:
                    console.log(
                        f"Blocks processed: {pp[0].height:,.0f} - {pp[-1].height:,.0f}"
                    )
            await asyncio.sleep(1)

    async def process_special_purpose_blocks(self):
        """
        This method takes the queue `special_purpose_block_infos_to_process` and processes
        each block.
        """
        while True:
            if len(self.special_purpose_block_infos_to_process) > 0:
                pp = copy(self.special_purpose_block_infos_to_process)
                # this is the last block that was processed

                _ = self.process_list_of_blocks(
                    self.special_purpose_block_infos_to_process
                )

                if len(pp) == 1:
                    console.log(f"SP Block processed: {pp[0].height:,.0f}")
                else:
                    console.log(
                        f"SP Blocks processed: {pp[0].height:,.0f} - {pp[-1].height:,.0f}"
                    )
            else:
                d = {"_id": "special_purpose_block_request", "heights": []}
                _ = self.db[Collections.helpers].bulk_write(
                    [
                        ReplaceOne(
                            {"_id": "special_purpose_block_request"},
                            replacement=d,
                            upsert=True,
                        )
                    ]
                )
                await asyncio.sleep(5)

    async def get_special_purpose_blocks(self):
        """
        This methods gets special purpose blocks from the chosen net.
        It batches blocks up to MAX_BLOCKS_PER_RUN and stores blocks to be
        processed in the queue `finalized_block_infos_to_process`.
        """
        while True:
            request_counter = 0
            result = self.db[Collections.helpers].find_one(
                {"_id": "special_purpose_block_request"}
            )
            if result:
                for height in result["heights"]:
                    self.special_purpose_block_infos_to_process.append(
                        self.grpcclient.get_finalized_block_at_height(
                            height, NET(self.net)
                        )
                    )

            await asyncio.sleep(10)

    async def get_finalized_blocks(self):
        """
        This methods gets finalized blocks from the chosen net.
        It batches blocks up to MAX_BLOCKS_PER_RUN and stores blocks to be
        processed in the queue `finalized_block_infos_to_process`.
        """
        while True:
            request_counter = 0
            result = self.db[Collections.helpers].find_one(
                {"_id": "heartbeat_last_processed_block"}
            )
            heartbeat_last_processed_block_height = result["height"]

            last_requested_block_not_finalized = False
            block_to_request_in_queue = False

            while (
                not (last_requested_block_not_finalized)
                and (request_counter < MAX_BLOCKS_PER_RUN)
                and not block_to_request_in_queue
            ):
                request_counter += 1

                # increment the block height to request
                heartbeat_last_processed_block_height += 1

                # check to see if we haven't finished processing the queue
                # If so, no need to request and add the same block again.
                block_to_request_in_queue = heartbeat_last_processed_block_height in [
                    x.height for x in self.finalized_block_infos_to_process
                ]

                # we haven't previously requested this block
                if not block_to_request_in_queue:
                    try:
                        finalized_block_info_at_height = (
                            self.grpcclient.get_finalized_block_at_height(
                                heartbeat_last_processed_block_height, NET(self.net)
                            )
                        )
                    except:
                        finalized_block_info_at_height = False

                    if finalized_block_info_at_height:
                        self.finalized_block_infos_to_process.append(
                            finalized_block_info_at_height
                        )
                    else:
                        last_requested_block_not_finalized = True

            if len(self.finalized_block_infos_to_process) > 0:
                if len(self.finalized_block_infos_to_process) == 1:
                    console.log(
                        f"Block retrieved: {self.finalized_block_infos_to_process[0].height:,.0f}"
                    )
                else:
                    console.log(
                        f"Blocks retrieved: {self.finalized_block_infos_to_process[0].height:,.0f} - {self.finalized_block_infos_to_process[-1].height:,.0f}"
                    )
            await asyncio.sleep(1)

    async def update_nodes_from_dashboard(self):
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    if self.net == "testnet":
                        url = "https://dashboard.testnet.concordium.com/nodesSummary"
                    else:
                        url = (
                            "https://dashboard.mainnet.concordium.software/nodesSummary"
                        )
                    async with session.get(url) as resp:
                        t = await resp.json()

                        queue = []

                        for raw_node in t:
                            node = ConcordiumNodeFromDashboard(**raw_node)
                            d = node.dict()
                            d["_id"] = node.nodeId

                            queue.append(
                                ReplaceOne({"_id": node.nodeId}, d, upsert=True)
                            )

                        _ = self.db[Collections.dashboard_nodes].delete_many({})
                        _ = self.db[Collections.dashboard_nodes].bulk_write(queue)

                        # update nodes status retrieval
                        query = {"_id": "heartbeat_last_timestamp_dashboard_nodes"}
                        self.db[Collections.helpers].replace_one(
                            query,
                            {
                                "_id": "heartbeat_last_timestamp_dashboard_nodes",
                                "timestamp": dt.datetime.utcnow(),
                            },
                            upsert=True,
                        )

            except Exception as e:
                self.tooter.send(
                    channel=TooterChannel.NOTIFIER,
                    message=f"Failed to get dashboard nodes. Error: {e}",
                    notifier_type=TooterType.REQUESTS_ERROR,
                )

            await asyncio.sleep(60)

    def add_end_of_day_to_queue(
        self, date_string: str, start_block: CCD_BlockInfo, end_block: CCD_BlockInfo
    ):
        """
        This method adds an end_of_day document to the queue.
        """
        dd = {}
        dd["_id"] = date_string
        dd["date"] = date_string
        dd["height_for_first_block"] = start_block.height
        dd["height_for_last_block"] = end_block.height
        dd["slot_time_for_first_block"] = start_block.slot_time
        dd["slot_time_for_last_block"] = end_block.slot_time
        dd["hash_for_first_block"] = start_block.hash
        dd["hash_for_last_block"] = end_block.hash

        self.queues[Queue.block_per_day].append(
            ReplaceOne({"_id": date_string}, dd, upsert=True)
        )

    def date_range_generator(self, start, end):
        """
        Range generator to generate all days.
        Used only if we need to re-create the block_per_day collection.
        """
        delta = end - start
        days = [start + timedelta(days=i) for i in range(delta.days + 1)]
        return days

    def create_block_per_day(self):
        """
        Method to re-create the blocks_per_day collections
        for testnet and mainnet.
        """
        if self.net == "testnet":
            start_date = dt.datetime(2022, 6, 13)
        else:
            start_date = dt.datetime(2021, 6, 9)

        end_date = dt.datetime.now()

        date_range = self.date_range_generator(start_date, end_date)

        for index, date in enumerate(date_range):
            date_string = f"{date:%Y-%m-%d}"
            if index == 0:
                if self.net == "testnet":
                    start_of_day0 = date.replace(
                        hour=10, minute=0, second=0, microsecond=0
                    )
                    start_of_day1 = date.replace(
                        hour=10, minute=1, second=59, microsecond=999999
                    )
                else:
                    start_of_day0 = date.replace(
                        hour=6, minute=0, second=0, microsecond=0
                    )
                    start_of_day1 = date.replace(
                        hour=6, minute=1, second=59, microsecond=999999
                    )

            else:
                start_of_day0 = date.replace(hour=0, minute=0, second=0, microsecond=0)
                start_of_day1 = date.replace(
                    hour=0, minute=1, second=59, microsecond=999999
                )

            end_of_day0 = date.replace(hour=23, minute=58, second=0, microsecond=0)
            end_of_day1 = date.replace(
                hour=23, minute=59, second=59, microsecond=999999
            )

            start_of_day_blocks = list(
                self.db[Collections.blocks].find(
                    filter={
                        "$and": [
                            {"slot_time": {"$gte": start_of_day0}},
                            {"slot_time": {"$lte": start_of_day1}},
                        ]
                    }
                )
            )
            end_of_day_blocks = list(
                self.db[Collections.blocks].find(
                    filter={
                        "$and": [
                            {"slot_time": {"$gte": end_of_day0}},
                            {"slot_time": {"$lte": end_of_day1}},
                        ]
                    }
                )
            )

            # if this isn't true, the day hasn't ended yet.
            if len(end_of_day_blocks) > 0:
                console.log(date_string)

                self.add_end_of_day_to_queue(
                    date_string,
                    CCD_BlockInfo(**start_of_day_blocks[0]),
                    CCD_BlockInfo(**end_of_day_blocks[-1]),
                )

        _ = self.db[Collections.blocks_per_day].bulk_write(
            self.queues[Queue.block_per_day]
        )

    def create_index(self, collection: Collections, key: str, direction, sparse=False):
        response = self.db[collection].create_index([(key, direction)], sparse=sparse)
        print(
            f"Reponse for index creation on collection '{collection.value}' for key '{key}': {response}."
        )

    def create_mongodb_indices(self):
        """
        If needed, we can use this method to re-create
        all indices on all collections (TODO: first delete
        existing indices?)
        """
        for collection in Collections:
            if collection == Collections.blocks:
                self.create_index(collection, "baker", ASCENDING)
                self.create_index(collection, "slot_time", ASCENDING)
                self.create_index(collection, "height", DESCENDING)
                self.create_index(collection, "transaction_count", ASCENDING)

            if collection == Collections.blocks_per_day:
                self.create_index(collection, "date", ASCENDING)

            if collection == Collections.instances:
                self.create_index(collection, "source_module", ASCENDING)
                self.create_index(collection, "owner", ASCENDING)

            if collection in [
                Collections.involved_accounts_all,
                Collections.involved_accounts_transfer,
            ]:
                self.create_index(collection, "block_height", ASCENDING)
                self.create_index(collection, "sender_canonical", ASCENDING)
                self.create_index(collection, "receiver_canonical", ASCENDING)
                self.create_index(collection, "type.type", ASCENDING)
                self.create_index(collection, "type.contents", ASCENDING)

            if collection == Collections.involved_contracts:
                self.create_index(collection, "source_module", ASCENDING)
                self.create_index(collection, "index", ASCENDING)
                self.create_index(collection, "contract", ASCENDING)
                self.create_index(collection, "block_height", ASCENDING)

            if collection == Collections.modules:
                self.create_index(collection, "module_name", ASCENDING)

            if collection == Collections.dashboard_nodes:
                self.create_index(collection, "consensusBakerId", ASCENDING)

            if collection == Collections.cns_domains:
                self.create_index(collection, "domain_name", ASCENDING)

            if collection == Collections.nightly_accounts:
                self.create_index(collection, "index", ASCENDING)

            if collection == Collections.transactions:
                self.create_index(collection, "type.type", ASCENDING)
                self.create_index(collection, "type.contents", ASCENDING)
                self.create_index(collection, "block_info.height", ASCENDING)
                self.create_index(collection, "block_info.slot_time", ASCENDING)
                self.create_index(
                    collection, "account_creation", ASCENDING, sparse=True
                )
                self.create_index(
                    collection,
                    "account_transaction.effects.account_transfer",
                    ASCENDING,
                    sparse=True,
                )
                self.create_index(
                    collection,
                    "account_transaction.effects.baker_configured",
                    ASCENDING,
                    sparse=True,
                )
                self.create_index(
                    collection,
                    "account_transaction.effects.delegation_configured",
                    ASCENDING,
                    sparse=True,
                )

            if self.net == "mainnet":
                if collection == Collections.paydays:
                    self.create_index(collection, "date", ASCENDING)
                    self.create_index(collection, "height_for_first_block", ASCENDING)
                    self.create_index(collection, "height_for_last_block", ASCENDING)

                if collection == Collections.paydays_performance:
                    self.create_index(collection, "date", ASCENDING)
                    self.create_index(collection, "baker_id", ASCENDING)

                if collection == Collections.paydays_rewards:
                    self.create_index(collection, "date", ASCENDING)
                    self.create_index(collection, "pool_owner", ASCENDING)


def main():
    """
    The Hearbeat repo is an endless async loop of three methods:
    1. `get_finalized_blocks`: this method looks up the last processed block
    in a mongoDB helper collection, and determines how many finalized
    blocks it needs to request from the node. These blocks are then added
    to the queue `finalized_block_infos_to_process`.
    2. `process_blocks` picks up this queue of blocks to process, and
    continues processing until the queue is empty again. For every block,
    we store the block_info (including tx hashes) into the collection `blocks`.
    Furthermore, we inspect all transactions for a block to determine whether we need
    to create any indices for them.
    3. `send_to_mongo`: this method takes all queues and sends them to the respective
    MongoDB collections.
    """
    console.log(f"{RUN_ON_NET=}")
    grpcclient = GRPCClient()

    heartbeat = Heartbeat(grpcclient, tooter, mongodb, RUN_ON_NET)

    # these to helper methods are not needed, only things
    # go really wrong...

    # heartbeat.create_mongodb_indices()
    # heartbeat.create_block_per_day()

    loop = asyncio.get_event_loop()

    loop.create_task(heartbeat.get_finalized_blocks())
    loop.create_task(heartbeat.process_blocks())
    loop.create_task(heartbeat.send_to_mongo())

    loop.create_task(heartbeat.get_special_purpose_blocks())
    loop.create_task(heartbeat.process_special_purpose_blocks())
    loop.create_task(heartbeat.update_nodes_from_dashboard())
    loop.run_forever()


if __name__ == "__main__":
    try:
        main()
    except Exception as f:
        console.log("main error: ", f)
