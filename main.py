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
    MongoTypeTokenAddress,
    MongoTypeLoggedEvent,
    MongoTypeTokenHolderAddress,
    MongoTypeTokenForAddress,
    mintEvent,
    transferEvent,
    burnEvent,
    tokenMetadataEvent,
    nonceEvent,
    updateOperatorEvent,
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


class AccountTransactionOutcome(Enum):
    Success = "success"
    Failure = "failure"


class ProvenanceMintAddress(Enum):
    mainnet = "3suZfxcME62akyyss72hjNhkzXeZuyhoyQz1tvNSXY2yxvwo53"
    testnet = "4AuT5RRmBwcdkLMA6iVjxTDb1FQmxwAh3wHBS22mggWL8xH6s3"


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

    ########### Token Accounting
    def mongo_save_for_token_address(
        self, token_address_as_class: MongoTypeTokenAddress
    ):
        repl_dict = token_address_as_class.dict()
        if "id" in repl_dict:
            del repl_dict["id"]

        sorted_holders = list(repl_dict["token_holders"].keys())
        sorted_holders.sort()
        token_holders_sorted = {
            i: repl_dict["token_holders"][i] for i in sorted_holders
        }
        token_holders_sorted = {
            k: v for k, v in token_holders_sorted.items() if int(v) > 0
        }
        repl_dict["token_holders"] = token_holders_sorted

        queue_item = ReplaceOne(
            {"_id": token_address_as_class.id},
            replacement=repl_dict,
            upsert=True,
        )
        return queue_item

    def mongo_save_for_address(self, address_to_save: MongoTypeTokenHolderAddress):
        repl_dict = address_to_save.dict()
        if "id" in repl_dict:
            del repl_dict["id"]

        sorted_tokens = list(repl_dict["tokens"].keys())
        sorted_tokens.sort()
        tokens_sorted = {i: repl_dict["tokens"][i] for i in sorted_tokens}
        tokens_sorted = {
            k: v for k, v in tokens_sorted.items() if int(v["token_amount"]) > 0
        }
        repl_dict["tokens"] = tokens_sorted

        queue_item = ReplaceOne(
            {"_id": address_to_save.id},
            replacement=repl_dict,
            upsert=True,
        )
        return queue_item

    def copy_token_holders_state_to_address_and_save(
        self,
        token_address_as_class: MongoTypeTokenAddress,
    ):
        _queue = []
        for address, token_amount in token_address_as_class.token_holders.items():
            address_to_save = self.db[Collections.tokens_accounts].find_one(
                {"_id": address}
            )
            # if this account does not exist yet, create empty dict.
            if not address_to_save:
                address_to_save = MongoTypeTokenHolderAddress(
                    **{
                        "_id": address,
                        "tokens": {},
                    }
                )
            else:
                address_to_save = MongoTypeTokenHolderAddress(**address_to_save)

            token_to_save = MongoTypeTokenForAddress(
                **{
                    "token_address": token_address_as_class.id,
                    "contract": token_address_as_class.contract,
                    "token_id": token_address_as_class.token_id,
                    "token_amount": str(token_amount),
                }
            )

            address_to_save.tokens[token_address_as_class.id] = token_to_save

            _queue.append(self.mongo_save_for_address(address_to_save))

        return _queue

    def save_mint(
        self, token_address_as_class: MongoTypeTokenAddress, log: MongoTypeLoggedEvent
    ):
        result = mintEvent(**log.result)
        token_holders: dict[
            CCD_AccountAddress, str
        ] = token_address_as_class.token_holders
        token_holders[result.to_address] = str(
            int(token_holders.get(result.to_address, "0")) + result.token_amount
        )
        token_address_as_class.token_amount = str(
            (int(token_address_as_class.token_amount) + result.token_amount)
        )
        token_address_as_class.token_holders = token_holders
        return token_address_as_class

    def save_metadata(
        self, token_address_as_class: MongoTypeTokenAddress, log: MongoTypeLoggedEvent
    ):
        result = tokenMetadataEvent(**log.result)
        token_address_as_class.metadata_url = result.metadata.url
        return token_address_as_class

    def save_transfer(
        self, token_address_as_class: MongoTypeTokenAddress, log: MongoTypeLoggedEvent
    ):
        result = transferEvent(**log.result)
        try:
            token_holders: dict[
                CCD_AccountAddress, str
            ] = token_address_as_class.token_holders
        except:
            console.log(
                f"{result.tag}: {token_address_as_class.token_id} | {token_address_as_class} has no field token_holders?"
            )

        token_holders[result.to_address] = str(
            int(token_holders.get(result.to_address, "0")) + result.token_amount
        )
        try:
            token_holders[result.from_address] = str(
                int(token_holders.get(result.from_address, "0")) - result.token_amount
            )
            if int(token_holders[result.from_address]) == 0:
                del token_holders[result.from_address]

        except:
            if result.token_amount > 0:
                console.log(
                    f"{result.tag}: {result.from_address} is not listed as token holder for {token_address_as_class.token_address}?"
                )

        token_address_as_class.token_holders = token_holders
        return token_address_as_class

    def save_burn(
        self, token_address_as_class: MongoTypeTokenAddress, log: MongoTypeLoggedEvent
    ):
        result = burnEvent(**log.result)
        token_holders: dict[
            CCD_AccountAddress, str
        ] = token_address_as_class.token_holders
        try:
            token_holders[result.from_address] = str(
                int(token_holders.get(result.from_address, "0")) - result.token_amount
            )
            if int(token_holders[result.from_address]) == 0:
                del token_holders[result.from_address]

            token_address_as_class.token_amount = str(
                (int(token_address_as_class.token_amount) - result.token_amount)
            )
            token_address_as_class.token_holders = token_holders

        except:
            console.log(
                f"{result.tag}: {result.from_address} is not listed as token holder for {token_address_as_class.token_address}?"
            )
            # exit

        token_address_as_class.token_holders = token_holders
        return token_address_as_class

    def create_new_token_address(self, token_address: str) -> MongoTypeTokenAddress:
        instance_address = token_address.split("-")[0]
        token_id = token_address.split("-")[1]
        token_address = MongoTypeTokenAddress(
            **{
                "_id": token_address,
                "contract": instance_address,
                "token_id": token_id,
                "token_amount": str(int(0)),  # mongo limitation on int size
                "token_holders": {},  # {CCD_AccountAddress, str(token_amount)}
                "last_height_processed": -1,
            }
        )
        return token_address

    def execute_logged_event(
        self, token_address_as_class: MongoTypeTokenAddress, log: MongoTypeLoggedEvent
    ):
        if log.tag == 255:
            token_address_as_class = self.save_transfer(token_address_as_class, log)
        elif log.tag == 254:
            token_address_as_class = self.save_mint(token_address_as_class, log)
        elif log.tag == 253:
            token_address_as_class = self.save_burn(token_address_as_class, log)
        elif log.tag == 251:
            token_address_as_class = self.save_metadata(token_address_as_class, log)

        return token_address_as_class

    ########### Token Accounting

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

    def decode_cis_logged_events(
        self,
        tx: CCD_BlockItemSummary,
        block_info: CCD_BlockInfo,
        special_purpose: bool = False,
    ):
        """
        This method takes a transaction as input and tries to find
        CIS logged events (mint, transfer, metadata, operator and burn).
        Logged events are stored in a collection. Depending on the tag,
        the logged event is executed and the result is stored in the
        collections accounts and token_addresses.
        """
        # this is the ordering of effects as encountered in the transaction
        ordering = 0
        logged_events = []
        token_addresses_to_redo_accounting = []
        provenance_contracts_to_add = []
        if not tx.account_transaction:
            return (
                logged_events,
                token_addresses_to_redo_accounting,
                provenance_contracts_to_add,
            )

        if tx.account_transaction.effects.contract_initialized:
            contract_index = (
                tx.account_transaction.effects.contract_initialized.address.index
            )
            contract_subindex = (
                tx.account_transaction.effects.contract_initialized.address.subindex
            )
            instance_address = f"<{contract_index},{contract_subindex}>"
            entrypoint = f"{tx.account_transaction.effects.contract_initialized.init_name[5:]}.supports"
            cis = CIS(
                self.grpcclient,
                contract_index,
                contract_subindex,
                entrypoint,
                NET(self.net),
            )
            supports_cis_1_2 = cis.supports_standards(
                [StandardIdentifiers.CIS_1, StandardIdentifiers.CIS_2]
            )
            if supports_cis_1_2:
                for index, event in enumerate(
                    tx.account_transaction.effects.contract_initialized.events
                ):
                    ordering += 1
                    tag, logged_event, token_address = cis.process_event(
                        # cis,
                        self.db,
                        instance_address,
                        event,
                        block_info.height,
                        tx.hash,
                        tx.index,
                        ordering,
                        f"initialized-{tx.index}-{index}",
                    )
                    if logged_event:
                        logged_events.append(logged_event)

                    if special_purpose:
                        token_addresses_to_redo_accounting.append(token_address)

                    if (tag == 254) and (
                        tx.account_transaction.sender
                        == ProvenanceMintAddress[self.net].value
                    ):
                        contract_to_add = token_address.split("-")[0]
                        provenance_contracts_to_add.append(contract_to_add)

        if tx.account_transaction.effects.contract_update_issued:
            for effect_index, effect in enumerate(
                tx.account_transaction.effects.contract_update_issued.effects
            ):
                if effect.interrupted:
                    contract_index = effect.interrupted.address.index
                    contract_subindex = effect.interrupted.address.subindex
                    instance_address = f"<{contract_index},{contract_subindex}>"
                    instance = MongoTypeInstance(
                        **self.db[Collections.instances].find_one(
                            {"_id": instance_address}
                        )
                    )
                    if instance and instance.v1:
                        entrypoint = instance.v1.name[5:] + ".supports"
                        cis = CIS(
                            self.grpcclient,
                            contract_index,
                            contract_subindex,
                            entrypoint,
                            NET(self.net),
                        )
                        supports_cis_1_2 = cis.supports_standards(
                            [
                                StandardIdentifiers.CIS_1,
                                StandardIdentifiers.CIS_2,
                            ]
                        )
                        if supports_cis_1_2:
                            for index, event in enumerate(effect.interrupted.events):
                                ordering += 1
                                (
                                    tag,
                                    logged_event,
                                    token_address,
                                ) = cis.process_event(
                                    self.db,
                                    instance_address,
                                    event,
                                    block_info.height,
                                    tx.hash,
                                    tx.index,
                                    ordering,
                                    f"interrupted-{tx.index}-{effect_index}-{index}",
                                )
                                if logged_event:
                                    logged_events.append(logged_event)

                                if special_purpose:
                                    token_addresses_to_redo_accounting.append(
                                        token_address
                                    )

                                if (tag == 254) and (
                                    tx.account_transaction.sender
                                    == ProvenanceMintAddress[self.net].value
                                ):
                                    contract_to_add = token_address.split("-")[0]
                                    provenance_contracts_to_add.append(contract_to_add)
                if effect.updated:
                    contract_index = effect.updated.address.index
                    contract_subindex = effect.updated.address.subindex
                    instance_address = f"<{contract_index},{contract_subindex}>"
                    entrypoint = f"{effect.updated.receive_name.split('.')[0]}.supports"
                    cis = CIS(
                        self.grpcclient,
                        contract_index,
                        contract_subindex,
                        entrypoint,
                        NET(self.net),
                    )
                    supports_cis_1_2 = cis.supports_standards(
                        [StandardIdentifiers.CIS_1, StandardIdentifiers.CIS_2]
                    )
                    if supports_cis_1_2:
                        for index, event in enumerate(effect.updated.events):
                            ordering += 1
                            (
                                tag,
                                logged_event,
                                token_address,
                            ) = cis.process_event(
                                # cis,
                                self.db,
                                instance_address,
                                event,
                                block_info.height,
                                tx.hash,
                                tx.index,
                                ordering,
                                f"updated-{tx.index}-{effect_index}-{index}",
                            )
                            if logged_event:
                                logged_events.append(logged_event)

                            if special_purpose:
                                token_addresses_to_redo_accounting.append(token_address)

                            if (tag == 254) and (
                                tx.account_transaction.sender
                                == ProvenanceMintAddress[self.net].value
                            ):
                                contract_to_add = token_address.split("-")[0]
                                provenance_contracts_to_add.append(contract_to_add)

        return (
            logged_events,
            token_addresses_to_redo_accounting,
            provenance_contracts_to_add,
        )

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

            if (
                tx.account_transaction.outcome
                == AccountTransactionOutcome.Success.value
            ):
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
        special_purpose: bool = False,
    ):
        """
        Given a list of transactions, apply rules to determine which index needs to be updated.
        Add this to a to_be_sent_to_mongo list and do insert_many.
        """
        for tx in transactions:
            (
                logged_events,
                token_addresses_to_redo_accounting,
                provenance_contracts_to_add,
            ) = self.decode_cis_logged_events(tx, block_info, special_purpose)

            if len(logged_events) > 0:
                self.queues[Queue.logged_events].extend(logged_events)

            if len(token_addresses_to_redo_accounting) > 0:
                self.queues[Queue.token_addresses_to_redo_accounting].extend(
                    token_addresses_to_redo_accounting
                )

            if len(provenance_contracts_to_add) > 0:
                self.queues[Queue.provenance_contracts_to_add].extend(
                    provenance_contracts_to_add
                )

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

    def add_block_and_txs_to_queue(
        self, block_info: CCD_BlockInfo, special_purpose: bool = False
    ):
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
                block.transaction_summaries, block_info, special_purpose
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
        self.db[Collections.helpers].replace_one(
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

    def log_last_token_accounted_message_in_mongo(self, height: int):
        query = {"_id": "token_accounting_last_processed_block"}
        self.db[Collections.helpers].replace_one(
            query,
            {
                "_id": "token_accounting_last_processed_block",
                "height": height,
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

                if len(self.queues[Queue.logged_events]) > 0:
                    result = self.db[Collections.tokens_logged_events].bulk_write(
                        self.queues[Queue.logged_events]
                    )
                    console.log(
                        f"E:  {len(self.queues[Queue.logged_events]):5,.0f} | M {result.matched_count:5,.0f} | Mod {result.modified_count:5,.0f} | U {result.upserted_count:5,.0f}"
                    )
                    self.queues[Queue.logged_events] = []

                if len(self.queues[Queue.token_addresses_to_redo_accounting]) > 0:
                    query = {"_id": "redo_token_addresses"}
                    self.db[Collections.helpers].replace_one(
                        query,
                        {
                            "_id": "redo_token_addresses",
                            "token_addresses": list(
                                set(
                                    self.queues[
                                        Queue.token_addresses_to_redo_accounting
                                    ]
                                )
                            ),
                        },
                        upsert=True,
                    )
                    console.log(
                        f"Added {len(self.queues[Queue.token_addresses_to_redo_accounting]):,.0f} token_addresses to redo accounting."
                    )
                    self.queues[Queue.token_addresses_to_redo_accounting] = []

                if len(self.queues[Queue.provenance_contracts_to_add]) > 0:
                    query = {"_id": "provenance"}
                    current_content = self.db[Collections.tokens_tags].find_one(query)
                    if not current_content:
                        current_content = {
                            "_id": "provenance",
                            "contracts": [],
                            "tag_template": True,
                            "single_use_token": False,
                        }
                    current_contracts: list = current_content["contracts"]
                    current_contracts.extend(
                        list(set(self.queues[Queue.provenance_contracts_to_add]))
                    )

                    current_contracts = list(set(current_contracts))
                    current_contracts.sort()
                    current_content.update({"contracts": current_contracts})
                    self.db[Collections.tokens_tags].replace_one(
                        query,
                        replacement=current_content,
                        upsert=True,
                    )
                    console.log(
                        f"Added {len(self.queues[Queue.provenance_contracts_to_add]):,.0f} contracts to provenance for {self.net}."
                    )
                    self.queues[Queue.provenance_contracts_to_add] = []
                # this will only be set if the above store methods do not fail.

            except Exception as e:
                # pass
                console.log(e)
                exit(1)

            await asyncio.sleep(1)

    def process_list_of_blocks(self, block_list: list, special_purpose: bool = False):
        result = self.db[Collections.modules].find({})
        self.existing_source_modules: dict[CCD_ModuleRef, set] = {
            x["_id"]: set(x["contracts"]) for x in list(result)
        }
        self.queues[Queue.updated_modules] = []

        start = dt.datetime.now()
        while len(block_list) > 0:
            current_block_to_process: CCD_BlockInfo = block_list.pop(0)
            try:
                self.add_block_and_txs_to_queue(
                    current_block_to_process, special_purpose
                )

                self.lookout_for_payday(current_block_to_process)
                self.lookout_for_end_of_day(current_block_to_process)

                if special_purpose:
                    # if it's a special purpose block, we need to remove it from the helper
                    result = self.db[Collections.helpers].find_one(
                        {"_id": "special_purpose_block_request"}
                    )
                    if result:
                        heights = result["heights"]
                        heights.remove(current_block_to_process.height)
                        result.update({"heights": heights})
                    _ = self.db[Collections.helpers].bulk_write(
                        [
                            ReplaceOne(
                                {"_id": "special_purpose_block_request"},
                                replacement=result,
                                upsert=True,
                            )
                        ]
                    )
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
                    self.special_purpose_block_infos_to_process, special_purpose=True
                )

                if len(pp) == 1:
                    console.log(f"SP Block processed: {pp[0].height:,.0f}")
                else:
                    console.log(
                        f"SP Blocks processed: {pp[0].height:,.0f} - {pp[-1].height:,.0f}"
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
                            int(height), NET(self.net)
                        )
                    )

            await asyncio.sleep(10)

    async def get_redo_token_addresses(self):
        """
        This methods gets token_addresses that need to have their
        token accounting redone.
        """
        while True:
            result = self.db[Collections.helpers].find_one(
                {"_id": "redo_token_addresses"}
            )
            if result:
                for token_address in result["token_addresses"]:
                    token_address_as_class = MongoTypeTokenAddress(
                        **self.db[Collections.tokens_token_addresses].find_one(
                            {"_id": token_address}
                        )
                    )
                    # update the last_height_processed to -1, this will trigger
                    # a redoof the token accounting.
                    token_address_as_class.last_height_processed = -1

                    # Write the token_address_as_class back to the collection.
                    _ = self.db[Collections.tokens_token_addresses].bulk_write(
                        [self.mongo_save_for_token_address(token_address_as_class)]
                    )

                _ = self.db[Collections.helpers].bulk_write(
                    [
                        ReplaceOne(
                            {"_id": "redo_token_addresses"},
                            replacement={"token_addresses": []},
                            upsert=True,
                        )
                    ]
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

    def token_accounting_for_token_address(
        self,
        token_address: str,
        log: MongoTypeLoggedEvent,
        events_by_token_address: dict,
        token_accounting_last_processed_block: int = -1,
    ):
        queue = []
        # if we start at the beginning of the chain for token accounting
        # create an empty token address as class to start
        if token_accounting_last_processed_block == -1:
            token_address_as_class = self.create_new_token_address(token_address)
        else:
            # Retrieve the token_address document from the collection
            token_address_as_class = self.db[
                Collections.tokens_token_addresses
            ].find_one({"_id": token_address})

            # If it's not there, create an new token_address
            if not token_address_as_class:
                token_address_as_class = self.create_new_token_address(token_address)
            else:
                # make sure the token_address_as_call is actually typed correctly.
                token_address_as_class = MongoTypeTokenAddress(**token_address_as_class)

        # This is the list of logged events for the selected token_address
        logs_for_token_address = events_by_token_address[token_address]
        for log in logs_for_token_address:
            # Perform token accounting for this logged event
            # This function works on and returns 'token_address_as_class'.
            token_address_as_class = self.execute_logged_event(
                token_address_as_class,
                log,
            )

        # Set the last block_height that affected the token accounting
        # for this token_address to the last logged event block_height.
        token_address_as_class.last_height_processed = log.block_height

        # Write the token_address_as_class back to the collection.
        _ = self.db[Collections.tokens_token_addresses].bulk_write(
            [self.mongo_save_for_token_address(token_address_as_class)]
        )

        # All logs for token_address are processed,
        # now copy state from token holders to _accounts
        queue = self.copy_token_holders_state_to_address_and_save(
            token_address_as_class
        )

        # Only write to the collection if there are accounts that
        # have been modified.
        if len(queue) > 0:
            try:
                _ = self.db[Collections.tokens_accounts].bulk_write(queue)
            except:
                console.log(token_address_as_class)
        else:
            pass

    async def update_token_accounting(self):
        """
        This method takes logged events and processes them for
        token accounting.
        The starting point is reading the helper document
        'token_accounting_last_processed_block', if that is either
        not there or set to -1, all token_addresses (and associated
        token_accounts) will be reset.
        """
        while True:
            try:
                # Read token_accounting_last_processed_block
                result = self.db[Collections.helpers].find_one(
                    {"_id": "token_accounting_last_processed_block"}
                )
                # If it's not set, set to -1, which leads to resetting
                # all token addresses and accounts, basically starting
                # over with token accounting.
                if result:
                    token_accounting_last_processed_block = result["height"]
                else:
                    token_accounting_last_processed_block = -1

                # Query the logged events collection for all logged events
                # after 'token_accounting_last_processed_block'.
                # Logged events are ordered by block_height, then by
                # transaction index (tx_index) and finally by event index
                # (ordering).
                result: list[MongoTypeLoggedEvent] = [
                    MongoTypeLoggedEvent(**x)
                    for x in self.db[Collections.tokens_logged_events]
                    .find(
                        {"block_height": {"$gt": token_accounting_last_processed_block}}
                    )
                    .sort(
                        [
                            ("block_height", ASCENDING),
                            ("tx_index", ASCENDING),
                            ("ordering", ASCENDING),
                        ]
                    )
                ]

                # Only continue if there are logged events to process...
                if len(result) > 0:
                    # When all logged events are processed,
                    # 'token_accounting_last_processed_block' is set to
                    # 'token_accounting_last_processed_block_when_done'
                    # such that next iteration, we will not be re-processing
                    # logged events we already have processed.
                    token_accounting_last_processed_block_when_done = max(
                        [x.block_height for x in result]
                    )

                    # Dict 'events_by_token_address' is keyed on token_address
                    # and contains an ordered list of logged events related to
                    # this token_address.
                    events_by_token_address: dict[str, list] = {}
                    for log in result:
                        events_by_token_address[
                            log.token_address
                        ] = events_by_token_address.get(log.token_address, [])
                        events_by_token_address[log.token_address].append(log)

                    console.log(
                        f"Token accounting: Starting at {(token_accounting_last_processed_block+1):,.0f}, I found {len(result):,.0f} logged events on {self.net} to process from {len(list(events_by_token_address.keys()))} token addresses."
                    )

                    # Looping through all token_addresses that have logged_events
                    for token_address in list(events_by_token_address.keys()):
                        self.token_accounting_for_token_address(
                            token_address,
                            log,
                            events_by_token_address,
                            token_accounting_last_processed_block,
                        )

                        # Finally, after all logged events are processed for all
                        # token addresses, write back to the helper collection
                        # the block_height (+1) where to start next iteration of
                        # token accounting.
                        self.log_last_token_accounted_message_in_mongo(
                            token_accounting_last_processed_block_when_done
                        )

            except Exception as e:
                console.log(e)

            await asyncio.sleep(1)

    async def special_purpose_token_accounting(self):
        """
        This method looks at all token_addresses and then inspects the
        last_height_processed property. If it's set to -1, this means
        we need to redo token accounting for this token_address.
        It's set to -1 if a special purpose block with cis events is
        detected.
        """
        while True:
            try:
                result = [
                    MongoTypeTokenAddress(**x)
                    for x in self.db[Collections.tokens_token_addresses].find(
                        {"last_height_processed": -1}
                    )
                ]

                token_addresses_to_process = [x.id for x in result]

                # Logged events are ordered by block_height, then by
                # transaction index (tx_index) and finally by event index
                # (ordering).
                for token_address in token_addresses_to_process:
                    events_for_token_address = [
                        MongoTypeLoggedEvent(**x)
                        for x in self.db[Collections.tokens_logged_events]
                        .find({"token_address": token_address})
                        .sort(
                            [
                                ("block_height", ASCENDING),
                                ("tx_index", ASCENDING),
                                ("ordering", ASCENDING),
                            ]
                        )
                    ]
                    events_by_token_address = {}
                    events_by_token_address[token_address] = events_for_token_address
                    # Only continue if there are logged events to process...
                    if len(events_for_token_address) > 0:
                        # When all logged events are processed,
                        # 'token_accounting_last_processed_block' is set to
                        # 'token_accounting_last_processed_block_when_done'
                        # such that next iteration, we will not be re-processing
                        # logged events we already have processed.
                        token_accounting_last_processed_block_when_done = max(
                            [x.block_height for x in events_for_token_address]
                        )

                        console.log(
                            f"Token accounting for Special purpose: Redo {token_address} with {len(events_for_token_address):,.0f} logged events on {self.net}."
                        )

                        # Looping through all token_addresses that have logged_events
                        for log in events_for_token_address:
                            self.token_accounting_for_token_address(
                                token_address,
                                log,
                                events_by_token_address,
                                -1,
                            )

            except Exception as e:
                console.log(e)

            await asyncio.sleep(1)

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

    def create_compound_index(
        self, collection: Collections, index_tuple: tuple, sparse=False
    ):
        response = self.db[collection].create_index([index_tuple], sparse=sparse)
        print(
            f"Reponse for index creation on collection '{collection.value}' for '{index_tuple}': {response}."
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

            if collection == Collections.tokens_token_addresses:
                self.create_index(collection, "contract", ASCENDING)

            if collection == Collections.tokens_logged_events:
                self.create_index(collection, "block_height", ASCENDING)
                self.create_index(collection, "token_address", ASCENDING)
                self.create_index(collection, "contract", ASCENDING)
                self.create_compound_index(
                    collection,
                    [
                        ("block_height", ASCENDING),
                        ("tx_index", ASCENDING),
                        ("ordering", ASCENDING),
                    ],
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
    loop.create_task(heartbeat.update_token_accounting())

    loop.create_task(heartbeat.get_special_purpose_blocks())
    loop.create_task(heartbeat.process_special_purpose_blocks())

    loop.create_task(heartbeat.get_redo_token_addresses())
    loop.create_task(heartbeat.special_purpose_token_accounting())

    loop.create_task(heartbeat.update_nodes_from_dashboard())
    loop.run_forever()


if __name__ == "__main__":
    try:
        main()
    except Exception as f:
        console.log("main error: ", f)
