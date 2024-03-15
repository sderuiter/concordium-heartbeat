from ccdefundamentals.mongodb import (
    MongoDB,
    MongoMotor,
    Collections,
    MongoImpactedAddress,
)
from ccdefundamentals.GRPCClient import GRPCClient
from ccdefundamentals.tooter import Tooter, TooterChannel, TooterType
from pymongo import ASCENDING, DESCENDING, ReplaceOne, DeleteOne
from ccdefundamentals.GRPCClient.CCD_Types import *
from ccdefundamentals.cis import MongoTypeTokenAddress
import datetime as dt
from env import *
from rich import print
from bisect import bisect_right

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


def find_date_and_date_str_for_height(heights, block_end_of_day_dict, height):
    found_index = bisect_right(heights, height)
    # meaning it's today...
    if found_index == len(heights):
        return f"{dt.datetime.utcnow():%Y-%m-%d}"
    else:
        return block_end_of_day_dict[heights[found_index]]


def add_date_to_ia(ia: MongoImpactedAddress, heights, block_end_of_day_dict):
    date = find_date_and_date_str_for_height(
        heights, block_end_of_day_dict, ia.block_height
    )
    ia.date = date
    return ia


net = "testnet"
db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet


result = list(
    db_to_use[Collections.blocks_per_day].find(
        filter={},
        projection={
            "_id": 0,
            "date": 1,
            "height_for_last_block": 1,
            "slot_time_for_last_block": 1,
        },
    )
)
block_end_of_day_dict = {x["height_for_last_block"]: x["date"] for x in result}
heights = list(block_end_of_day_dict.keys())


queue = []
start = dt.datetime.now()

result = (
    db_to_use[Collections.impacted_addresses]
    .find({"date": {"$exists": False}})
    .limit(20000)
)

ll = list(result)
while len(ll) > 0:
    ia = [MongoImpactedAddress(**x) for x in ll]

    for index, ia in enumerate(ia):
        ia: MongoImpactedAddress
        ia = add_date_to_ia(ia, heights, block_end_of_day_dict)
        repl_dict = ia.model_dump(exclude_none=True)
        if "id" in repl_dict:
            del repl_dict["id"]

        queue.append(
            ReplaceOne(
                {"_id": ia.id},
                repl_dict,
                upsert=True,
            )
        )
        # _ = mongodb.mainnet[Collections.impacted_addresses].bulk_write(queue)
        # queue = []
        if len(queue) > 20_000:
            end = dt.datetime.now()

            print(f"IA: {index:,.0f}: Processed")

            tooter.relay(
                channel=TooterChannel.NOTIFIER,
                title="",
                chat_id=913126895,
                body=f"Block: {index:,.0f}: Processed {len(queue):,.0f} IAs in {(end-start).total_seconds():,.0f} sec.",
                notifier_type=TooterType.INFO,
            )
            _ = db_to_use[Collections.impacted_addresses].bulk_write(queue)
            queue = []
            start = dt.datetime.now()

    result = (
        db_to_use[Collections.impacted_addresses]
        .find({"date": {"$exists": False}})
        .limit(20000)
    )
    ll = list(result)
_ = db_to_use[Collections.impacted_addresses].bulk_write(queue)
