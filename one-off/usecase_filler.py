from ccdefundamentals.mongodb import (
    MongoDB,
    MongoMotor,
    Collections,
    CollectionsUtilities,
)
from ccdefundamentals.GRPCClient import GRPCClient
from ccdefundamentals.tooter import Tooter
from pymongo import ASCENDING, DESCENDING, ReplaceOne, DeleteOne
from ccdefundamentals.GRPCClient.CCD_Types import *
from ccdefundamentals.cis import MongoTypeTokenAddress

from env import *
from rich import print

tooter: Tooter = Tooter(
    ENVIRONMENT, BRANCH, NOTIFIER_API_TOKEN, API_TOKEN, FASTMAIL_TOKEN
)
grpcclient = GRPCClient()
mongodb = MongoDB(
    MONGO_URI,
    tooter,
)

net = "mainnet"
db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet

provenance = [
    x
    for x in mongodb.utilities[CollectionsUtilities.labeled_accounts].find({})
    if "Provenance Tags" == x["label"]
]
print(provenance)

queue = []
for p in provenance:
    _id = f"provenance-address-{p['account_index']}"
    d = {
        "_id": _id,
        "usecase_id": "provenance",
        "type": "address",
        "account_index": p["account_index"],
        "account_address": p["_id"],
    }
    queue.append(
        ReplaceOne(
            {"_id": _id},
            replacement=d,
            upsert=True,
        )
    )
_ = mongodb.mainnet[Collections.usecases].bulk_write(queue)

# all_heights = set(range(13000000, max(all_heights_in_db)))
# missing_heights = list(set(all_heights) - set(all_heights_in_db))
# print(f"{len(missing_heights):,.0f} missing blocks on {net}.")
# print(missing_heights)


# d = {"_id": "special_purpose_block_request", "heights": missing_heights}
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

# {
#   "_id": "4MwARWeXdMs3YZ5MPPn2561ceani6AJAVTNPtwS6tceaG2qatK",
#   "label_group": "general",
#   "label": "AESIRX MC",
#   "account_index": 88533
# }
