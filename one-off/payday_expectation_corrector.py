# ruff: noqa: F403, F405, E402, E501, E722
from ccdefundamentals.mongodb import (
    MongoDB,
    MongoMotor,
    Collections,
    MongoTypePaydaysPerformance,
)
from ccdefundamentals.GRPCClient import GRPCClient
from ccdefundamentals.tooter import Tooter, TooterChannel, TooterType
from pymongo import ASCENDING, DESCENDING, ReplaceOne, DeleteOne
from ccdefundamentals.GRPCClient.CCD_Types import *
from ccdefundamentals.cis import MongoTypeTokenAddress
import datetime as dt
from env import *
from rich import print
import json

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

net = "testnet"
db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet


queue = []
start = dt.datetime.now()


def get_blocks_per_payday(mongodb: MongoDB):
    result = {
        x["date"]: (x["height_for_last_block"] - x["height_for_first_block"] + 1)
        for x in mongodb.mainnet[Collections.paydays].find({})
    }
    return result


blocks_per_payday = get_blocks_per_payday(mongodb)

for date in list(blocks_per_payday.keys()):
    queue = []
    result = list(mongodb.mainnet[Collections.paydays_performance].find({"date": date}))

    daily_payday_performance = [
        MongoTypePaydaysPerformance(**x)
        for x in result
        if x["baker_id"] != "passive_delegation"
    ]
    for dpr in daily_payday_performance:
        dpr.expectation = (
            dpr.pool_status.current_payday_info.lottery_power * blocks_per_payday[date]
        )

        d: dict = json.loads(dpr.model_dump_json(exclude_none=True))
        d["_id"] = dpr.id
        del d["id"]

        queue.append(
            ReplaceOne(
                {"_id": dpr.id},
                replacement=d,
                upsert=True,
            )
        )
        pass

    _ = mongodb.mainnet[Collections.paydays_performance].bulk_write(queue)
    print(f"Payday: {date} Processed {len(queue):,.0f} expectations")
    queue = []
    # tooter.relay(
    #     channel=TooterChannel.NOTIFIER,
    #     title="",
    #     chat_id=913126895,
    #     body=f"Payday: {i:,.0f}: Processed {len(queue):,.0f} blocks for special events in {(end-start).total_seconds():,.0f} sec.",
    #     notifier_type=TooterType.INFO,
    # )

    start = dt.datetime.now()
