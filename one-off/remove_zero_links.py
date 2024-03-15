from ccdefundamentals.mongodb import MongoDB, MongoMotor, Collections
from ccdefundamentals.GRPCClient import GRPCClient
from ccdefundamentals.tooter import Tooter, TooterChannel, TooterType
from pymongo import ASCENDING, DESCENDING, ReplaceOne, DeleteOne
from ccdefundamentals.GRPCClient.CCD_Types import *
from ccdefundamentals.cis import MongoTypeTokenAddress
import datetime as dt
from env import *
from rich import print
from rich.progress import track

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

net = "mainnet"
db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet


queue = []
zero_links = list(
    db_to_use[Collections.tokens_links_v2].aggregate(
        [
            {
                "$match": {"token_holding.token_amount": "0"},
            },
        ]
    )
)
start = dt.datetime.now()

for link in track(zero_links):
    # dd = mongodb.mainnet[Collections.tokens_links_v2].find_one({"_id": link["_id"]})
    # if dd:
    #     print("panic)")
    queue.append(
        DeleteOne(
            {"_id": link["_id"]},
        )
    )
    # pass
    # print("len")
    if len(queue) > 20_000:
        end = dt.datetime.now()

        print(
            f"Block: {i:,.0f}: Processed {len(queue):,.0f} items in {(end-start).total_seconds():,.0f} sec."
        )

        tooter.relay(
            channel=TooterChannel.NOTIFIER,
            title="",
            chat_id=913126895,
            body=f"Block: {i:,.0f}: Processed {len(queue):,.0f} blocks for special events in {(end-start).total_seconds():,.0f} sec.",
            notifier_type=TooterType.INFO,
        )
        # _ = mongodb.mainnet[Collections.special_events].bulk_write(queue)
        queue = []
        start = dt.datetime.now()

_ = db_to_use[Collections.tokens_links_v2].bulk_write(queue)
