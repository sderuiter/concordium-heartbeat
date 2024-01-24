from sharingiscaring.mongodb import MongoDB, MongoMotor, Collections
from sharingiscaring.GRPCClient import GRPCClient
from sharingiscaring.tooter import Tooter
from pymongo import ASCENDING, DESCENDING, ReplaceOne, DeleteOne
from sharingiscaring.GRPCClient.CCD_Types import *
from sharingiscaring.cis import MongoTypeTokenAddress

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

net = "mainnet"
db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet


result = db_to_use[Collections.helpers].find({})

queue = []
for r in result:
    if "contract" in r:
        # queue.append(
        #     DeleteOne(
        #         {"_id": r["_id"]},
        #     )
        # )
    # _ = db_to_use[Collections.tokens_token_addresses].bulk_write(queue)
print(len(queue))
pass
_ = db_to_use[Collections.helpers].bulk_write(queue)

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
