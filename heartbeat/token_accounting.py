# ruff: noqa: F403, F405, E402, E501, E722
from .utils import Utils, Queue
from sharingiscaring.GRPCClient.CCD_Types import *
from sharingiscaring.mongodb import (
    Collections,
)
from sharingiscaring.cis import (
    MongoTypeTokenAddress,
    TokenMetaData,
    MongoTypeLoggedEvent,
    MongoTypeTokenHolderAddress,
    FailedAttempt,
    MongoTypeTokenForAddress,
    mintEvent,
    transferEvent,
    burnEvent,
    tokenMetadataEvent,
    MongoTypeTokensTag,
)
import aiohttp
from itertools import chain
from pymongo import ReplaceOne, ASCENDING
from pymongo.collection import Collection
import requests
from datetime import timezone
from rich.console import Console
import asyncio

console = Console()


########### Token Accounting
class TokenAccounting(Utils):
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

    async def update_token_accounting(self):
        """
        This method takes logged events and processes them for
        token accounting. Note that token accounting only processes events with
        tag 255, 254, 253 and 251, which are transfer, mint, burn and metadata.
        The starting point is reading the helper document
        'token_accounting_last_processed_block', if that is either
        not there or set to -1, all token_addresses (and associated
        token_accounts) will be reset.
        """
        self.db: dict[Collections, Collection]
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
                        events_by_token_address[log.token_address] = (
                            events_by_token_address.get(log.token_address, [])
                        )
                        events_by_token_address[log.token_address].append(log)

                    console.log(
                        f"Token accounting: Starting at {(token_accounting_last_processed_block+1):,.0f}, I found {len(result):,.0f} logged events on {self.net} to process from {len(list(events_by_token_address.keys()))} token addresses."
                    )

                    # Looping through all token_addresses that have logged_events
                    for token_address in list(events_by_token_address.keys()):
                        self.token_accounting_for_token_address(
                            token_address,
                            events_by_token_address,
                            token_accounting_last_processed_block,
                        )

                        # Finally, after all logged events are processed for all
                        # token addresses, write back to the helper collection
                        # the block_height where to start next iteration of
                        # token accounting.
                        self.log_last_token_accounted_message_in_mongo(
                            token_accounting_last_processed_block_when_done
                        )

                    self.send_token_queues_to_mongo(0)

            except Exception as e:
                console.log(e)

            await asyncio.sleep(1)

    def send_token_queues_to_mongo(self, limit: int = 0):
        self.queues: dict[Collections, list]
        if len(self.queues[Queue.token_addresses]) > limit:
            _ = self.db[Collections.tokens_token_addresses].bulk_write(
                self.queues[Queue.token_addresses]
            )
            console.log(
                f"Updated accounting for {len(self.queues[Queue.token_addresses])} token addresses."
            )

            self.queues[Queue.token_addresses] = []

        if len(self.queues[Queue.token_accounts]) > limit:
            _ = self.db[Collections.tokens_accounts].bulk_write(
                self.queues[Queue.token_accounts]
            )
            console.log(
                f"Updated accounting for {len(self.queues[Queue.token_accounts])} token accounts."
            )

            self.queues[Queue.token_accounts] = []

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
                        # token_accounting_last_processed_block_when_done = max(
                        #     [x.block_height for x in events_for_token_address]
                        # )

                        console.log(
                            f"Token accounting for Special purpose: Redo {token_address} with {len(events_for_token_address):,.0f} logged events on {self.net}."
                        )

                        # Looping through all token_addresses that have logged_events
                        # for log in events_for_token_address:
                        self.token_accounting_for_token_address(
                            token_address,
                            events_by_token_address,
                            -1,
                        )

                    self.send_token_queues_to_mongo(49)
                self.send_token_queues_to_mongo(0)

            except Exception as e:
                console.log(e)

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
                    # a redo of the token accounting.
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

    def token_accounting_for_token_address(
        self,
        token_address: str,
        events_by_token_address: dict,
        token_accounting_last_processed_block: int = -1,
    ):
        self.queues: dict[Collections, list]
        queue = []
        # if we start at the beginning of the chain for token accounting
        # create an empty token address as class to start
        if token_accounting_last_processed_block == -1:
            token_address_as_class = self.create_new_token_address(token_address)
            # token_holders_before_executing_logged_events = [
            #     x["_id"] for x in self.db[Collections.tokens_accounts].find()
            # ]

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

            # token_holders_before_executing_logged_events = list(
            #     token_address_as_class.token_holders.keys()
            # ).copy()

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
        self.queues[Queue.token_addresses].append(
            self.mongo_save_for_token_address(token_address_as_class)
        )
        # _ = self.db[Collections.tokens_token_addresses].bulk_write(
        #     [self.mongo_save_for_token_address(token_address_as_class)]
        # )

        # All logs for token_address are processed,
        # now copy state from token holders to _accounts
        queue = self.copy_token_holders_state_to_address_and_save(
            token_address_as_class
        )
        self.queues[Queue.token_accounts].extend(queue)

        # already see if we need to send to mongo to keep memory down.
        self.send_token_queues_to_mongo(49)

        # Perform a last check if there are accounts that no longer
        # have this token. They need to have this token removed from
        # their tokens_account document.
        # queue_zero = self.update_accounts_for_zero_amounts(
        #     token_holders_before_executing_logged_events, token_address_as_class
        # )

        # if len(queue_zero) > 0:
        #     queue.extend(queue_zero)

        # Only write to the collection if there are accounts that
        # have been modified.

        ##### this bulk write is now located in the main loop

        # if len(queue) > 0:
        #     try:
        #         _ = self.db[Collections.tokens_accounts].bulk_write(queue)
        #         console.log(f"Updated token accounting for {len(queue)} accounts.")
        #     except:
        #         console.log(token_address_as_class)
        # else:
        #     pass

    def mongo_save_for_token_address(
        self, token_address_as_class: MongoTypeTokenAddress
    ):
        repl_dict = token_address_as_class.model_dump()
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
        repl_dict = address_to_save.model_dump()
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
                        "account_address_canonical": address[:29],
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

    def update_accounts_for_zero_amounts(
        self,
        token_holders_before_executing_logged_events: list[CCD_Address],
        token_address_as_class: MongoTypeTokenAddress,
    ):
        _queue = []

        # token_holders according to the token_address (this
        # was just updated, so correct.) We need to compare this with
        # token_holders_before_executing_logged_events.

        token_holders_from_address = token_address_as_class.token_holders.keys()

        token_holders_zero_amounts = list(
            set(token_holders_before_executing_logged_events)
            - set(token_holders_from_address)
        )

        for address in token_holders_zero_amounts:
            address_to_save = self.db[Collections.tokens_accounts].find_one(
                {"_id": address}
            )
            if not address_to_save:
                # this should not be possible...
                pass
            else:
                address_to_save = MongoTypeTokenHolderAddress(**address_to_save)
                # delete this token from the tokens_list
                try:
                    del address_to_save.tokens[token_address_as_class.id]
                except KeyError:
                    pass

                _queue.append(self.mongo_save_for_address(address_to_save))

        return _queue

    def save_mint(
        self, token_address_as_class: MongoTypeTokenAddress, log: MongoTypeLoggedEvent
    ):
        result = mintEvent(**log.result)
        token_holders: dict[CCD_AccountAddress, str] = (
            token_address_as_class.token_holders
        )
        token_holders[result.to_address] = str(
            int(token_holders.get(result.to_address, "0")) + result.token_amount
        )
        token_address_as_class.token_amount = str(
            (int(token_address_as_class.token_amount) + result.token_amount)
        )
        token_address_as_class.token_holders = token_holders
        return token_address_as_class

    def read_and_store_metadata(self, token_address_as_class: MongoTypeTokenAddress):
        timeout = 1  # sec

        url = token_address_as_class.metadata_url
        error = None

        try:
            do_request = url is not None
            if token_address_as_class.failed_attempt:
                timeout = 2

                if dt.datetime.now(
                    tz=timezone.utc
                ) < token_address_as_class.failed_attempt.do_not_try_before.astimezone(
                    timezone.utc
                ):
                    do_request = False
                else:
                    console.log(
                        f"Trying{token_address_as_class.token_id} now: Current FA: {token_address_as_class.failed_attempt} "
                    )
            if do_request:
                r = requests.get(url=url, verify=False, timeout=timeout)

                metadata = None
                if r.status_code == 200:
                    try:
                        metadata = TokenMetaData(**r.json())
                        token_address_as_class.token_metadata = metadata
                        token_address_as_class.failed_attempt = None
                        dom_dict = token_address_as_class.model_dump(exclude_none=True)
                        if "id" in dom_dict:
                            del dom_dict["id"]
                        self.db[Collections.tokens_token_addresses].replace_one(
                            {"_id": token_address_as_class.id},
                            replacement=dom_dict,
                            upsert=True,
                        )
                    except Exception as e:
                        error = str(e)
                else:
                    error = f"Request status code: {r.status_code}"

        except Exception as e:
            error = str(e)

        if error:
            failed_attempt = token_address_as_class.failed_attempt
            if not failed_attempt:
                failed_attempt = FailedAttempt(
                    **{
                        "attempts": 1,
                        "do_not_try_before": dt.datetime.now(tz=timezone.utc)
                        + dt.timedelta(hours=2),
                        "last_error": error,
                    }
                )
            else:
                failed_attempt.attempts += 1
                failed_attempt.do_not_try_before = dt.datetime.now(
                    tz=timezone.utc
                ) + dt.timedelta(hours=failed_attempt.attempts)
                failed_attempt.last_error = error

            token_address_as_class.failed_attempt = failed_attempt
            dom_dict = token_address_as_class.model_dump(exclude_none=True)
            if "id" in dom_dict:
                del dom_dict["id"]
            self.db[Collections.tokens_token_addresses].replace_one(
                {"_id": token_address_as_class.id},
                replacement=dom_dict,
                upsert=True,
            )

    def save_metadata(
        self, token_address_as_class: MongoTypeTokenAddress, log: MongoTypeLoggedEvent
    ):
        result = tokenMetadataEvent(**log.result)
        token_address_as_class.metadata_url = result.metadata.url
        self.read_and_store_metadata(token_address_as_class)
        return token_address_as_class

    def save_transfer(
        self, token_address_as_class: MongoTypeTokenAddress, log: MongoTypeLoggedEvent
    ):
        result = transferEvent(**log.result)
        try:
            token_holders: dict[CCD_AccountAddress, str] = (
                token_address_as_class.token_holders
            )
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
            # if int(token_holders[result.from_address]) == 0:
            #     del token_holders[result.from_address]

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
        token_holders: dict[CCD_AccountAddress, str] = (
            token_address_as_class.token_holders
        )
        try:
            token_holders[result.from_address] = str(
                int(token_holders.get(result.from_address, "0")) - result.token_amount
            )
            # if int(token_holders[result.from_address]) == 0:
            #     del token_holders[result.from_address]

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
                "hidden": False,
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
