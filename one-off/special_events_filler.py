from ccdefundamentals.mongodb import MongoDB, MongoMotor, Collections
from ccdefundamentals.GRPCClient import GRPCClient
from ccdefundamentals.tooter import Tooter, TooterChannel, TooterType
from pymongo import ASCENDING, DESCENDING, ReplaceOne, DeleteOne
from ccdefundamentals.GRPCClient.CCD_Types import *
from ccdefundamentals.cis import MongoTypeTokenAddress
import datetime as dt
from env import *
from rich import print

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
for i in range(0, 8321681):
    # se = grpcclient.get_block_special_events(i, NET.testnet)
    se_list = [x.model_dump(exclude_none=True) for x in se]
    # block_info = CCD_Block(**db_to_use[Collections.blocks].find_one({"block_info.height": i}))

    d = {"_id": i, "special_events": se_list}
    queue.append(
        ReplaceOne(
            {"_id": i},
            replacement=d,
            upsert=True,
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
        _ = mongodb.mainnet[Collections.special_events].bulk_write(queue)
        queue = []
        start = dt.datetime.now()

_ = mongodb.mainnet[Collections.special_events].bulk_write(queue)

# bc = {
#     x["_id"]: CCD_BlockItemSummary(**x)
#     for x in db_to_use[Collections.transactions].aggregate(
#         [
#             {"$match": {"update": {"$exists": True}}},
#             {
#                 "$match": {
#                     "update.payload.micro_ccd_per_euro_update": {"$exists": False}
#                 }
#             },
#             {"$sort": {"block_info.height": DESCENDING}},
#             # {"_id": 0, "block_info.height": 1},
#         ]
#     )
# }
# print(bc)
# all_heights_in_db = [
#     x["height"] for x in db_to_use[Collections.blocks].find({}, {"_id": 0, "height": 1})
# ]
# all_heights = set(range(0, max(all_heights_in_db)))
# missing_heights = list(set(all_heights) - set(all_heights_in_db))
# print(f"{len(missing_heights):,.0f} missing blocks on {net}.")
# print(missing_heights)


# d = {"_id": "special_purpose_block_request", "heights": bc}
# _ = db_to_use[Collections.helpers].bulk_write(
#     [
#         ReplaceOne(
#             {"_id": "special_purpose_block_request"},
#             replacement=d,
#             upsert=True,
#         )
#     ]
# )


# result = [
#     x
#     for x in mongodb.mainnet[Collections.transactions].aggregate(pipeline)
#     if x["effect_count"] == 0
# ]
# print(
#     f"account_transaction.effects.{action_type}_configured: # txs with 0 effects: {len(result)}."
# )
# heights = [x["block_info"]["height"] for x in result]

# d = {"_id": "special_purpose_block_request", "heights": heights}
# _ = mongodb.mainnet[Collections.helpers].bulk_write(
#     [
#         ReplaceOne(
#             {"_id": "special_purpose_block_request"},
#             replacement=d,
#             upsert=True,
#         )
#     ]
# )
pass
