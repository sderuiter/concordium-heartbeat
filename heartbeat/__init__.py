# ruff: noqa: F403, F405, E402, E501, E722
import asyncio
from sharingiscaring.GRPCClient import GRPCClient
from rich import print
from rich.progress import track
import requests
import datetime as dt


from sharingiscaring.GRPCClient.CCD_Types import *
from sharingiscaring.tooter import Tooter, TooterChannel, TooterType
from sharingiscaring.mongodb import (
    MongoDB,
    Collections,
    MongoMotor,
)
from itertools import chain
from sharingiscaring.enums import NET
from sharingiscaring.cis import (
    CIS,
    MongoTypeTokenAddress,
    MongoTypeTokensTag,
)

from pymongo.collection import Collection
from pymongo import ReplaceOne
import aiohttp
from typing import Dict
from env import *


from rich.console import Console
import urllib3

from .nodes_from_dashboard import Nodes as _nodes
from .token_exchange_rates import ExchangeRates as _exchange_rates
from .impacted_addresses import ImpactedAddresses as _impacted_addresses
from .token_accounting import TokenAccounting as _token_accounting
from .start_over import StartOver as _start_over
from .send_to_mongo import SendToMongo as _send_to_mongo
from .block_loop import BlockLoop as _block_loop
from .utils import Queue

urllib3.disable_warnings()
console = Console()


class Heartbeat(
    _block_loop,
    _nodes,
    _exchange_rates,
    _impacted_addresses,
    _token_accounting,
    _start_over,
    _send_to_mongo,
):
    def __init__(
        self,
        grpcclient: GRPCClient,
        tooter: Tooter,
        mongodb: MongoDB,
        motormongo: MongoMotor,
        net: str,
    ):
        self.grpcclient = grpcclient
        self.tooter = tooter
        self.mongodb = mongodb
        self.motormongo = motormongo
        self.net = net
        self.db: Dict[Collections, Collection] = (
            self.mongodb.mainnet if self.net == "mainnet" else self.mongodb.testnet
        )
        self.motordb: Dict[Collections, Collection] = (
            self.motormongo.testnet if net == "testnet" else self.motormongo.mainnet
        )
        self.finalized_block_infos_to_process: list[CCD_BlockInfo] = []
        self.special_purpose_block_infos_to_process: list[CCD_BlockInfo] = []

        self.existing_source_modules: dict[CCD_ModuleRef, set] = {}
        self.queues: Dict[Collections, list] = {}
        for q in Queue:
            self.queues[q] = []

    def init_cis(self, contract_index, contract_subindex, entrypoint):
        cis = CIS(
            self.grpcclient,
            contract_index,
            contract_subindex,
            entrypoint,
            NET(self.net),
        )

        return cis

    async def update_memos_to_hashes(self):
        while True:
            # self.db[Collections.memos_to_hashes].delete_many({})
            # Read heartbeat_memos_last_processed_block
            result = self.db[Collections.helpers].find_one(
                {"_id": "heartbeat_memos_last_processed_block"}
            )
            # If it's not set, set to -1, which leads to resetting
            # memo search.
            if result:
                heartbeat_memos_last_processed_block = result["height"]
            else:
                heartbeat_memos_last_processed_block = -1

            pipeline = [
                {
                    "$match": {
                        "block_height": {"$gt": heartbeat_memos_last_processed_block}
                    }
                },
                {"$match": {"memo": {"$exists": True}}},
                {"$project": {"memo": 1, "block_height": 1, "_id": 1}},
            ]
            result = self.db[Collections.involved_accounts_transfer].aggregate(pipeline)

            data = list(result)
            memos: list(dict) = []
            max_block_height = 0
            if len(data) > 0:
                for x in track(data):
                    max_block_height = max(max_block_height, x["block_height"])
                    hex_to_decode = x["memo"]

                    decode_error, decoded_memo = self.decode_memo(hex_to_decode)
                    if not decode_error:
                        decoded_memo = decoded_memo.encode("ascii", "ignore")
                        decoded_memo = decoded_memo.decode()
                        memos.append({"memo": decoded_memo.lower(), "hash": x["_id"]})

                if len(memos) > 0:
                    # only if there are new memos to add to the list, should we read in
                    # the collection again, otherwise it's a waste of resources.

                    # this is the queue of collection documents to be added and/or replaced
                    queue = []
                    # if we find new memos, add them here, this leads to a new document in the queue
                    new_memos = {}
                    # if we find an existin memo, add them here, this leads to a replace document in the queue.
                    updated_memos = {}

                    set_memos = {
                        x["_id"]: x["tx_hashes"]
                        for x in self.db[Collections.memos_to_hashes].find({})
                    }
                    # old_len_set_memos = len(set_memos)
                    for memo in memos:
                        current_list_of_tx_hashes_for_memo = set_memos.get(
                            memo["memo"], None
                        )

                        # a tx with a memo we have already seen
                        # hence we need to replace the current document with the updated one
                        # as we possibly can have multiple updates (txs) to the same memo
                        # we need to store the updates in a separate variable.
                        if current_list_of_tx_hashes_for_memo:
                            current_list_of_tx_hashes_for_memo.append(memo["hash"])

                            updated_memos[memo["memo"]] = list(
                                set(current_list_of_tx_hashes_for_memo)
                            )

                        # this is a new memo
                        else:
                            new_memos[memo["memo"]] = [memo["hash"]]

                    # self.transfer_memos = set_memos

                    # make list for new and updated items
                    for memo_key, tx_hashes in updated_memos.items():
                        queue.append(
                            ReplaceOne(
                                {"_id": memo_key},
                                replacement={"_id": memo_key, "tx_hashes": tx_hashes},
                                upsert=True,
                            )
                        )

                    for memo_key, tx_hashes in new_memos.items():
                        queue.append(
                            ReplaceOne(
                                {"_id": memo_key},
                                replacement={"_id": memo_key, "tx_hashes": tx_hashes},
                                upsert=True,
                            )
                        )

                    self.db[Collections.memos_to_hashes].bulk_write(queue)
                    self.log_last_heartbeat_memo_to_hashes_in_mongo(max_block_height)
                    # console.log(
                    #     f"Updated memos to hashes. Last block height processed: {max_block_height:,.0f}."
                    # )
                    # console.log(
                    #     f"Added {len(new_memos):,.0f} key(s) in this run and updated {len(updated_memos):,.0f} key(s)."
                    # )
                    self.tooter.send(
                        channel=TooterChannel.NOTIFIER,
                        message=f"Updated memos to hashes. Last block height processed: {max_block_height:,.0f}.\nAdded {len(new_memos):,.0f} key(s) in this run and updated {len(updated_memos):,.0f} key(s).",
                        notifier_type=TooterType.INFO,
                    )

            await asyncio.sleep(60 * 5)

    async def update_involved_accounts_all_top_list(self):
        while True:
            try:
                pipeline = [
                    {
                        "$group": {
                            "_id": "$impacted_address_canonical",
                            "count": {"$sum": 1},
                        }
                    },
                    {"$sort": {"count": -1}},
                ]
                result = (
                    await self.motordb[Collections.impacted_addresses]
                    .aggregate(pipeline)
                    .to_list(50)
                )

                local_queue = []
                for r in result:
                    local_queue.append(ReplaceOne({"_id": r["_id"]}, r, upsert=True))

                _ = self.db[Collections.involved_accounts_all_top_list].delete_many({})
                _ = self.db[Collections.involved_accounts_all_top_list].bulk_write(
                    local_queue
                )

                # update top_list status retrieval
                query = {
                    "_id": "heartbeat_last_timestamp_involved_accounts_all_top_list"
                }
                self.db[Collections.helpers].replace_one(
                    query,
                    {
                        "_id": "heartbeat_last_timestamp_involved_accounts_all_top_list",
                        "timestamp": dt.datetime.utcnow(),
                    },
                    upsert=True,
                )

            except Exception as e:
                self.tooter.send(
                    channel=TooterChannel.NOTIFIER,
                    message=f"Failed to get involved_accounts_all_top_list. Error: {e}",
                    notifier_type=TooterType.REQUESTS_ERROR,
                )

            await asyncio.sleep(3 * 60)

    async def get_domain_name_from_metadata(self, dom: MongoTypeTokenAddress):
        async with aiohttp.ClientSession() as session:
            url = f"{dom.metadata_url}"
            async with session.get(url) as resp:
                t = await resp.json()
                # print(t)
                try:
                    return t["name"]
                except:
                    return None

    async def web23_domain_name_metadata(self):
        """
        This method looks into the token_addresses collection specifically for
        tokenIDs from contract 9377 (the Web23 CCD contract).
        As they have not implemented the metadata log event, we need to perform
        this ourselves.
        As such, every time this runs, it retrieves all tokenIDs from this contract
        and loops through all tokenIDs that do not have metadata set (mostly new, could
        also be that in a previous run, there was a http issue).
        For every tokenID withou metadata, there is a call to the wallet-proxy to get
        the metadataURL, which is then stored in the collection. Finally, we read the
        metadataURL to determine the actual domainname and store this is a separate
        collection.
        """
        while True:
            try:
                ccd_token_tags = self.db[Collections.tokens_tags].find_one(
                    {"_id": ".ccd"}
                )

                if ccd_token_tags:
                    contracts_in_ccd_token_tag = MongoTypeTokensTag(
                        **ccd_token_tags
                    ).contracts
                    query = {"contract": {"$in": contracts_in_ccd_token_tag}}

                    current_content = [
                        MongoTypeTokenAddress(**x)
                        for x in self.db[Collections.tokens_token_addresses].find(query)
                    ]
                else:
                    current_content = []

                for dom in current_content:
                    if dom.token_metadata:
                        continue
                    contract_index = CCD_ContractAddress.from_str(dom.contract).index
                    if self.net == "testnet":
                        url_to_fetch_metadata = f"https://wallet-proxy.testnet.concordium.com/v0/CIS2TokenMetadata/{contract_index}/0?tokenId={dom.token_id}"
                    else:
                        url_to_fetch_metadata = f"https://wallet-proxy.mainnet.concordium.software/v0/CIS2TokenMetadata/{contract_index}/0?tokenId={dom.token_id}"
                    timeout = 1  # sec
                    print(url_to_fetch_metadata)
                    try:
                        r = requests.get(
                            url=url_to_fetch_metadata, verify=False, timeout=timeout
                        )
                        dom.metadata_url = None
                        if r.status_code == 200:
                            try:
                                token_metadata = r.json()
                                if "metadata" in token_metadata:
                                    if "metadataURL" in token_metadata["metadata"][0]:
                                        dom.metadata_url = token_metadata["metadata"][
                                            0
                                        ]["metadataURL"]
                                        self.read_and_store_metadata(dom)
                            except Exception as e:
                                console.log(e)
                                dom.metadata_url = None
                    except:
                        pass

            except Exception as e:
                console.log(e)

            await asyncio.sleep(59)

    async def read_token_metadata_if_not_present(self):
        """
        We only try to read metadata for recognized tokens.
        Too much noise and unreliable urls otherwise.
        """
        while True:
            try:
                token_tags = self.db[Collections.tokens_tags].find({})

                recognized_contracts = [
                    MongoTypeTokensTag(**x).contracts for x in token_tags
                ]

                query = {
                    "contract": {"$in": list(chain.from_iterable(recognized_contracts))}
                }

                current_content = [
                    MongoTypeTokenAddress(**x)
                    for x in self.db[Collections.tokens_token_addresses].find(query)
                ]

                for dom in current_content:
                    if dom.token_metadata:
                        continue

                    self.read_and_store_metadata(dom)

            except Exception as e:
                console.log(e)

            await asyncio.sleep(500)
