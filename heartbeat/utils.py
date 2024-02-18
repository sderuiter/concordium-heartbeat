# ruff: noqa: F403, F405, E402, E501, E722
from enum import Enum
from sharingiscaring.GRPCClient.CCD_Types import *
from sharingiscaring.mongodb import Collections, MongoTypeInstance
from sharingiscaring.cis import CIS, StandardIdentifiers
from sharingiscaring.tooter import TooterChannel, TooterType
from sharingiscaring.enums import NET
from pymongo import ReplaceOne
import io
import operator
import asyncio
import chardet


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
    token_accounts = 13
    token_addresses = 14
    token_links = 15


class ProvenanceMintAddress(Enum):
    mainnet = ["3suZfxcME62akyyss72hjNhkzXeZuyhoyQz1tvNSXY2yxvwo53"]
    testnet = [
        "4AuT5RRmBwcdkLMA6iVjxTDb1FQmxwAh3wHBS22mggWL8xH6s3",
        "4s3QS7Vdp7b6yrLngKQwCQcexKVQLKifcGKgmVXoH6wffZMQhM",
    ]


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

    def log_last_token_accounted_message_in_mongo(self, height: int):
        query = {"_id": "token_accounting_last_processed_block_v2"}
        self.db[Collections.helpers].replace_one(
            query,
            {
                "_id": "token_accounting_last_processed_block_v2",
                "height": height,
            },
            upsert=True,
        )

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
        self.internal_freqency_timer = dt.datetime.now().astimezone(tz=dt.timezone.utc)

    def log_last_heartbeat_memo_to_hashes_in_mongo(self, height: int):
        query = {"_id": "heartbeat_memos_last_processed_block"}
        self.db[Collections.helpers].replace_one(
            query,
            {
                "_id": "heartbeat_memos_last_processed_block",
                "height": height,
            },
            upsert=True,
        )

    def decode_memo(self, hex):
        # bs = bytes.fromhex(hex)
        # return bytes.decode(bs[1:], 'UTF-8')
        try:
            bs = io.BytesIO(bytes.fromhex(hex))
            value = bs.read()

            encoding_guess = chardet.detect(value)
            if encoding_guess["confidence"] < 0.1:
                encoding_guess = chardet.detect(value[2:])
                value = value[2:]

            if encoding_guess["encoding"] and encoding_guess["confidence"] > 0.5:
                try:
                    memo = bytes.decode(value, encoding_guess["encoding"])

                    # memo = bytes.decode(value, "UTF-8")
                    return False, memo[1:]
                except UnicodeDecodeError:
                    memo = bytes.decode(value[1:], "UTF-8")
                    return False, memo[1:]
            else:
                return True, "Decoding failure..."
        except:
            return True, "Decoding failure..."

    def init_cis(self, contract_index, contract_subindex, entrypoint):
        cis = CIS(
            self.grpcclient,
            contract_index,
            contract_subindex,
            entrypoint,
            NET(self.net),
        )

        return cis

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
            cis = self.init_cis(contract_index, contract_subindex, entrypoint)
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
                        in ProvenanceMintAddress[self.net].value
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
                    try:
                        instance = MongoTypeInstance(
                            **self.db[Collections.instances].find_one(
                                {"_id": instance_address}
                            )
                        )
                    except:
                        instance = None
                    if instance and instance.v1:
                        entrypoint = instance.v1.name[5:] + ".supports"
                        cis = self.init_cis(
                            contract_index, contract_subindex, entrypoint
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
                                    in ProvenanceMintAddress[self.net].value
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
                                in ProvenanceMintAddress[self.net].value
                            ):
                                contract_to_add = token_address.split("-")[0]
                                provenance_contracts_to_add.append(contract_to_add)

        return (
            logged_events,
            token_addresses_to_redo_accounting,
            provenance_contracts_to_add,
        )

    async def update_impacted_addresses_all_top_list(self):
        while True:
            try:
                # this is the block we last processed making the top list
                bot_last_processed_block_for_top_list = self.db[
                    Collections.helpers
                ].find_one(
                    {
                        "_id": "heartbeat_last_block_processed_impacted_addresses_all_top_list"
                    }
                )
                bot_last_processed_block_for_top_list_height = (
                    bot_last_processed_block_for_top_list["height"]
                )

                # this is the last finalized block in the collection of blocks
                heartbeat_last_processed_block = self.db[Collections.helpers].find_one(
                    {"_id": "heartbeat_last_processed_block"}
                )
                heartbeat_last_processed_block_height = heartbeat_last_processed_block[
                    "height"
                ]

                pipeline = [
                    {
                        "$match": {
                            "block_height": {
                                "$gt": bot_last_processed_block_for_top_list_height
                            }
                        }
                    },
                    {
                        "$match": {
                            "block_height": {
                                "$lte": heartbeat_last_processed_block_height
                            }
                        }
                    },
                    {  # this filters out account rewards, as they are special events
                        "$match": {"tx_hash": {"$exists": True}},
                    },
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
                    .to_list(100)
                )

                local_queue = []

                # get previously stored results
                previous_result = self.db[
                    Collections.impacted_addresses_all_top_list
                ].find({})
                previous_accounts_dict = {x["_id"]: x["count"] for x in previous_result}

                for r in result:
                    if previous_accounts_dict.get(r["_id"]):
                        r["count"] += previous_accounts_dict.get(r["_id"])
                    local_queue.append(ReplaceOne({"_id": r["_id"]}, r, upsert=True))

                # _ = self.db[Collections.impacted_addresses_all_top_list].delete_many({})
                if len(local_queue) > 0:
                    _ = self.db[Collections.impacted_addresses_all_top_list].bulk_write(
                        local_queue
                    )

                    # # remove results from collection such that only top 100
                    # previous_result = self.db[
                    #     Collections.impacted_addresses_all_top_list
                    # ].find({})
                    # previous_accounts_dict = {
                    #     x["_id"]: x["count"] for x in previous_result
                    # }
                    # sorted_dict = dict(
                    #     sorted(
                    #         previous_accounts_dict.items(),
                    #         key=operator.itemgetter(1),
                    #         reverse=True,
                    #     )
                    # )
                    # top_100_keys =
                    # update top_list status retrieval
                    query = {
                        "_id": "heartbeat_last_block_processed_impacted_addresses_all_top_list"
                    }
                    self.db[Collections.helpers].replace_one(
                        query,
                        {
                            "_id": "heartbeat_last_block_processed_impacted_addresses_all_top_list",
                            "height": heartbeat_last_processed_block_height,
                        },
                        upsert=True,
                    )
                    query = {
                        "_id": "heartbeat_last_timestamp_impacted_addresses_all_top_list"
                    }
                    self.db[Collections.helpers].replace_one(
                        query,
                        {
                            "_id": "heartbeat_last_timestamp_impacted_addresses_all_top_list",
                            "timestamp": dt.datetime.utcnow(),
                        },
                        upsert=True,
                    )

            except Exception as e:
                self.tooter.send(
                    channel=TooterChannel.NOTIFIER,
                    message=f"Failed to get impacted_addresses_all_top_list. Error: {e}",
                    notifier_type=TooterType.REQUESTS_ERROR,
                )

            await asyncio.sleep(1 * 60)

    # currently not used
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
