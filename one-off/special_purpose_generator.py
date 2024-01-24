from sharingiscaring.mongodb import MongoDB, Collections
from sharingiscaring.GRPCClient import GRPCClient
from sharingiscaring.tooter import Tooter
from pymongo import ASCENDING, DESCENDING, ReplaceOne
from env import *

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

action_type = "delegation"
pipeline = [
    {
        "$match": {
            f"account_transaction.effects.{action_type}_configured": {"$exists": True}
        }
    },
    {
        "$project": {
            "block_info.height": 1,
            "name": "name",
            "effect_count": {
                "$size": f"$account_transaction.effects.{action_type}_configured.events"
            },
        },
    },
]
result = [
    x
    for x in mongodb.mainnet[Collections.transactions].aggregate(pipeline)
    if x["effect_count"] == 0
]
print(
    f"account_transaction.effects.{action_type}_configured: # txs with 0 effects: {len(result)}."
)
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
