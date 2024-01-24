from sharingiscaring.mongodb import MongoDB, MongoMotor, Collections
from sharingiscaring.GRPCClient import GRPCClient
from sharingiscaring.tooter import Tooter
from pymongo import ASCENDING, DESCENDING, ReplaceOne
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

result = list(
    db_to_use[Collections.blocks].find(
        {}, {"_id": 0, "transaction_hashes": 1, "height": 1}
    )
)


all_tx_hashes_from_blocks_in_db = [x["transaction_hashes"] for x in result]

all_tx_hashes_from_blocks_in_db_dict = {}
for r in result:
    for tx_hash in r["transaction_hashes"]:
        all_tx_hashes_from_blocks_in_db_dict[tx_hash] = r["height"]

all_tx_hashes_from_blocks_in_db = [
    item for sublist in all_tx_hashes_from_blocks_in_db for item in sublist
]

all_tx_hashes_from_transactions_in_db = [
    x["_id"] for x in db_to_use[Collections.transactions].find({}, {"_id": 1})
]


missing_txs = list(
    set(all_tx_hashes_from_blocks_in_db) - set(all_tx_hashes_from_transactions_in_db)
)
print(f"{len(missing_txs):,.0f} missing transactions on {net}.")
print(missing_txs)

missing_heights = []
for tx_hash in missing_txs:
    missing_heights.append(all_tx_hashes_from_blocks_in_db_dict[tx_hash])

missing_heights = list(set(missing_heights))
print(missing_heights)

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
# pass

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
