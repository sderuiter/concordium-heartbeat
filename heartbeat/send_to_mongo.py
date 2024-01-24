# ruff: noqa: F403, F405, E402, E501, E722
from .utils import Utils, Queue
from sharingiscaring.mongodb import Collections
from pymongo.collection import Collection
from env import *
import asyncio
from rich.console import Console

console = Console()


class SendToMongo(Utils):
    async def send_to_mongo(self):
        """
        This method takes all queues with mongoDB messages and sends them to the
        respective collections.
        """
        self.queues: dict[Collections, list]
        self.db: dict[Collections, Collection]
        while True:
            try:
                if len(self.queues[Queue.blocks]) > 0:
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
                    pass
                    # update_ = False

                if len(self.queues[Queue.special_events]) > 0:
                    result = self.db[Collections.special_events].bulk_write(
                        self.queues[Queue.special_events]
                    )
                    self.queues[Queue.special_events] = []

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

                if len(self.queues[Queue.impacted_addresses]) > 0:
                    result = self.db[Collections.impacted_addresses].bulk_write(
                        self.queues[Queue.impacted_addresses]
                    )
                    console.log(
                        f"IA: {len(self.queues[Queue.impacted_addresses]):5,.0f} | M {result.matched_count:5,.0f} | Mod {result.modified_count:5,.0f} | U {result.upserted_count:5,.0f}"
                    )
                    self.queues[Queue.impacted_addresses] = []

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
