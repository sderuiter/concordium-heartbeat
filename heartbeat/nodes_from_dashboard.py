from .utils import Utils
from sharingiscaring.mongodb import Collections
from sharingiscaring.node import ConcordiumNodeFromDashboard
from sharingiscaring.tooter import TooterChannel, TooterType
from pymongo import ReplaceOne
from pymongo.collection import Collection
import aiohttp
import asyncio
import datetime as dt


class Nodes(Utils):
    async def update_nodes_from_dashboard(self):
        self.db: dict[Collections, Collection]
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
                            d = node.model_dump()
                            d["_id"] = node.nodeId

                            for k, v in d.items():
                                if isinstance(v, int):
                                    d[k] = str(v)

                            queue.append(
                                ReplaceOne({"_id": node.nodeId}, d, upsert=True)
                            )

                        _ = self.db[Collections.dashboard_nodes].delete_many({})
                        _ = self.db[Collections.dashboard_nodes].bulk_write(queue)
                        #
                        #
                        # update nodes status retrieval
                        query = {"_id": "heartbeat_last_timestamp_dashboard_nodes"}
                        self.db[Collections.helpers].replace_one(
                            query,
                            {
                                "_id": "heartbeat_last_timestamp_dashboard_nodes",
                                "timestamp": dt.datetime.now().astimezone(
                                    tz=dt.timezone.utc
                                ),
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
