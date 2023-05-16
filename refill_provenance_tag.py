from sharingiscaring.mongodb import MongoDB, MongoMotor, Collections
from sharingiscaring.GRPCClient import GRPCClient
from sharingiscaring.tooter import Tooter
from pymongo import ASCENDING, DESCENDING, ReplaceOne
import pymongo
from env import *
from rich import print
from rich.progress import track
from sharingiscaring.GRPCClient.CCD_Types import *
from sharingiscaring.cis import (
    CIS,
    StandardIdentifiers,
    MongoTypeTokenAddress,
    MongoTypeLoggedEvent,
)

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

result = [
    CCD_BlockItemSummary(**x)
    for x in db_to_use[Collections.transactions].aggregate(
        [
            {
                "$match": {
                    "account_transaction.sender": "4AuT5RRmBwcdkLMA6iVjxTDb1FQmxwAh3wHBS22mggWL8xH6s3"
                },
            },
        ]
    )
]
pass

contracts_to_add = []
for tx in track(result):
    logged_events = list(
        db_to_use[Collections.tokens_logged_events].aggregate(
            [
                {
                    "$match": {"tx_hash": tx.hash},
                },
                {
                    "$match": {"tag": 254},
                },
            ]
        )
    )
    if len(logged_events) > 0:
        for event in logged_events:
            contracts_to_add.append(MongoTypeLoggedEvent(**event).contract)

print(set(contracts_to_add))
query = {"_id": "provenance"}
current_content = db_to_use[Collections.tokens_tags].find_one(query)
if not current_content:
    current_content = {
        "_id": "provenance",
        "contracts": [],
        "tag_template": True,
        "single_use_token": False,
    }
current_content.update({"contracts": list(set(contracts_to_add))})
db_to_use[Collections.tokens_tags].replace_one(
    query,
    replacement=current_content,
    upsert=True,
)


# db_to_use[Collections.tokens_logged_events].bulk_write(
#     [pymongo.operations.DeleteMany({"event_type":"operator_event"})]
# )
print("hello")
# for tx_hash in missing_txs:
#     missing_heights.append(all_tx_hashes_from_blocks_in_db_dict[tx_hash])
# missing_heights = list(range(block_init, block_transfer))

# missing_heights = list(set(missing_heights))
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
pass

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
