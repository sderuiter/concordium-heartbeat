from ccdefundamentals.mongodb import MongoDB, MongoMotor, Collections
from ccdefundamentals.GRPCClient import GRPCClient
from ccdefundamentals.tooter import Tooter, TooterChannel, TooterType
from pymongo import ASCENDING, DESCENDING, ReplaceOne, DeleteOne
from ccdefundamentals.GRPCClient.CCD_Types import *
from ccdefundamentals.cis import MongoTypeTokenAddress
import datetime as dt
from env import *
import json

from ccdefundamentals.cis import (
    MongoTypeTokenAddress,
    TokenMetaData,
    MongoTypeLoggedEvent,
    MongoTypeTokenHolderAddress,
    MongoTypeTokenLink,
    FailedAttempt,
    MongoTypeTokenForAddress,
    mintEvent,
    transferEvent,
    burnEvent,
    tokenMetadataEvent,
    MongoTypeTokensTag,
)
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

net = "testnet"
db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet


queue = []
tokens_from_events = [
    MongoTypeLoggedEvent(**x)
    for x in db_to_use[Collections.tokens_logged_events].aggregate(
        [
            # {
            #     "$match": {"contract": "<7947,0>"},
            # },
            {
                "$match": {"tag": 254},
            },
        ]
    )
]
tokens_from_addresses = {
    x["_id"]: MongoTypeTokenAddress(**x)
    for x in db_to_use[Collections.tokens_token_addresses_v2].aggregate(
        [
            # {
            #     "$match": {"contract": "<7947,0>"},
            # },
        ]
    )
}
start = dt.datetime.now()
count = 0
queue = []
for token_from_event in track(tokens_from_events):
    if not tokens_from_addresses.get(token_from_event.token_address):
        count += 1
        queue.append(token_from_event.token_address)
        # if count > 500:
d = {"_id": "redo_token_addresses", "token_addresses": queue}
_ = db_to_use[Collections.helpers].bulk_write(
    [
        ReplaceOne(
            {"_id": "redo_token_addresses"},
            replacement=d,
            upsert=True,
        )
    ]
)
# count = 0
# queue = []

print(count)
# dd = mongodb.mainnet[Collections.tokens_links_v2].find_one({"_id": link["_id"]})
# if dd:
#     print("panic)")
#     nm.token_metadata = metadata
#     nm.token_metadata.attributes[0].value = nm.token_id
#     repl_dict = nm.model_dump(exclude_none=True)
#     if "id" in repl_dict:
#         del repl_dict["id"]
#     queue.append(ReplaceOne({"_id": nm.id}, replacement=repl_dict, upsert=True))

#     # pass
#     # print("len")
#     # if len(queue) > 20_000:
#     #     end = dt.datetime.now()

#     #     print(
#     #         f"Block: {i:,.0f}: Processed {len(queue):,.0f} items in {(end-start).total_seconds():,.0f} sec."
#     #     )

#     #     tooter.relay(
#     #         channel=TooterChannel.NOTIFIER,
#     #         title="",
#     #         chat_id=913126895,
#     #         body=f"Block: {i:,.0f}: Processed {len(queue):,.0f} blocks for special events in {(end-start).total_seconds():,.0f} sec.",
#     #         notifier_type=TooterType.INFO,
#     #     )
#     #     # _ = mongodb.mainnet[Collections.special_events].bulk_write(queue)
#     #     queue = []
#     #     start = dt.datetime.now()

# _ = db_to_use[Collections.tokens_token_addresses_v2].bulk_write(queue)
