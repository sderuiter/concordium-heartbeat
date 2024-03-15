# ruff: noqa: F403, F405, E402, E501, E722
from .utils import Utils, Queue
from ccdefundamentals.mongodb import Collections
from ccdefundamentals.tooter import TooterChannel, TooterType

from pymongo.collection import Collection
from pymongo import ReplaceOne
from env import *
import asyncio
from rich.console import Console

console = Console()


class PreRenderers(Utils):
    async def pre_main_tokens_page(self):
        self.db: dict[Collections, Collection]
        while True:
            try:
                result = (
                    await self.motordb[Collections.tokens_logged_events]
                    .aggregate(
                        [
                            {"$group": {"_id": "$contract", "count": {"$sum": 1}}},
                            {"$sort": {"count": -1}},
                        ]
                    )
                    .to_list(1_000_000_000)
                )
                logged_events_by_contract = {x["_id"]: x["count"] for x in result}

                queue = []
                for ctr_id, count in logged_events_by_contract.items():
                    repl_dict = {"_id": ctr_id, "count": count}
                    if "id" in repl_dict:
                        del repl_dict["id"]
                    queue_item = ReplaceOne(
                        {"_id": ctr_id},
                        replacement=repl_dict,
                        upsert=True,
                    )
                    queue.append(queue_item)
                _ = self.db[Collections.pre_tokens_overview].bulk_write(queue)

            except Exception as e:
                self.tooter.relay(
                    channel=TooterChannel.NOTIFIER,
                    title="",
                    chat_id=913126895,
                    body=f"Heartbeat on {self.net} prerender pre_main_tokens_page: {e}",
                    notifier_type=TooterType.MONGODB_ERROR,
                )
            await asyncio.sleep(60)

    async def pre_main_tokens_by_address_canonical(self):
        self.db: dict[Collections, Collection]
        while True:
            try:
                result = (
                    await self.motordb[Collections.tokens_links_v2]
                    .aggregate(
                        [
                            {
                                "$group": {
                                    "_id": "$account_address_canonical",
                                    "count": {"$sum": 1},
                                }
                            },
                            {"$sort": {"count": -1}},
                        ]
                    )
                    .to_list(1_000_000_000)
                )
                tokens_by_address = {x["_id"]: x["count"] for x in result}
                # console.log("pre_main_tokens_by_address_canonical")
                queue = []
                for account_address_canonical, count in tokens_by_address.items():
                    repl_dict = {"_id": account_address_canonical, "count": count}
                    if "id" in repl_dict:
                        del repl_dict["id"]
                    queue_item = ReplaceOne(
                        {"_id": account_address_canonical},
                        replacement=repl_dict,
                        upsert=True,
                    )
                    queue.append(queue_item)
                _ = self.db[Collections.pre_tokens_by_address].bulk_write(queue)

            except Exception as e:
                self.tooter.relay(
                    channel=TooterChannel.NOTIFIER,
                    title="",
                    chat_id=913126895,
                    body=f"Heartbeat on {self.net} prerender pre_main_tokens_by_address_canonical: {e}",
                    notifier_type=TooterType.MONGODB_ERROR,
                )

            await asyncio.sleep(60)

    async def pre_addresses_by_contract_count(self):
        self.db: dict[Collections, Collection]
        while True:
            try:
                result = (
                    await self.motordb[Collections.tokens_token_addresses_v2]
                    .aggregate(
                        [
                            {"$group": {"_id": "$contract", "count": {"$sum": 1}}},
                            {"$sort": {"count": -1}},
                        ]
                    )
                    .to_list(1_000_000_000)
                )
                addresses_by_contract = {x["_id"]: x["count"] for x in result}

                queue = []
                for ctr_id, count in addresses_by_contract.items():
                    repl_dict = {"_id": ctr_id, "count": count}
                    if "id" in repl_dict:
                        del repl_dict["id"]
                    queue_item = ReplaceOne(
                        {"_id": ctr_id},
                        replacement=repl_dict,
                        upsert=True,
                    )
                    queue.append(queue_item)
                _ = self.db[Collections.pre_addresses_by_contract_count].bulk_write(
                    queue
                )

            except Exception as e:
                self.tooter.relay(
                    channel=TooterChannel.NOTIFIER,
                    title="",
                    chat_id=913126895,
                    body=f"Heartbeat on {self.net} prerender pre_addresses_by_contract_count: {e}",
                    notifier_type=TooterType.MONGODB_ERROR,
                )

            await asyncio.sleep(60)
