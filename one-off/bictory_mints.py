from sharingiscaring.mongodb import MongoDB, MongoMotor, Collections
from sharingiscaring.GRPCClient import GRPCClient
from sharingiscaring.tooter import Tooter
from pymongo import ASCENDING, DESCENDING, ReplaceOne
from env import *
from rich import print
from rich.progress import track
from sharingiscaring.GRPCClient.CCD_Types import *
from sharingiscaring.cns import CNSDomain


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


def get_domain_from_collection(token_id):
    result = db_to_use[Collections.cns_domains].find_one({"_id": token_id})
    if result:
        return result["domain_name"]
    else:
        return None


queue = []
cns = CNSDomain()
txs_updates_issued = {
    x["_id"]: CCD_BlockItemSummary(**x)
    for x in db_to_use[Collections.transactions].find(
        {"type.contents": "contract_update_issued"}
    )
}
for tx_hash, tx in txs_updates_issued.items():
    if len(tx.account_transaction.effects.contract_update_issued.effects) == 27:
        # token_id = cns.decode_token_id_from(
        #     tx.account_transaction.effects.contract_update_issued.effects[
        #         4
        #     ].updated.parameter
        # )
        # domain = get_domain_from_collection(token_id)
        print(f"{tx.account_transaction.sender},{tx_hash}")
# # get the these from transactions collection
# txs_from_transfers_collection = {
#     x["_id"]: x
#     for x in db_to_use[Collections.transactions].find(
#         {"_id": {"$in": list(transfers_with_memos.keys())}}
#     )
# }

# for hash, old in track(transfers_with_memos.items()):
#     from_transaction_collection = txs_from_transfers_collection[hash]
#     if (
#         "account_transfer"
#         in from_transaction_collection["account_transaction"]["effects"]
#     ):
#         old["memo"] = from_transaction_collection["account_transaction"]["effects"][
#             "account_transfer"
#         ]["memo"]
#     else:
#         old["memo"] = from_transaction_collection["account_transaction"]["effects"][
#             "transferred_with_schedule"
#         ]["memo"]

#     queue.append(
#         ReplaceOne(
#             {"_id": hash},
#             old,
#             upsert=True,
#         )
#     )

# db_to_use[Collections.involved_accounts_transfer].bulk_write(queue)

# print(len(transfers_with_memos), len(txs_from_transfers_collection))
# # all_heights = set(range(0, max(all_heights_in_db)))
# # missing_heights = list(set(all_heights) - set(all_heights_in_db))
# # print(f"{len(missing_heights):,.0f} missing blocks on {net}.")
# # print(missing_heights)


# # d = {"_id": "special_purpose_block_request", "heights": missing_heights}
# # _ = db_to_use[Collections.helpers].bulk_write(
# #     [
# #         ReplaceOne(
# #             {"_id": "special_purpose_block_request"},
# #             replacement=d,
# #             upsert=True,
# #         )
# #     ]
# # )


# # result = [
# #     x
# #     for x in mongodb.mainnet[Collections.transactions].aggregate(pipeline)
# #     if x["effect_count"] == 0
# # ]
# # print(
# #     f"account_transaction.effects.{action_type}_configured: # txs with 0 effects: {len(result)}."
# # )
# # heights = [x["block_info"]["height"] for x in result]

# # d = {"_id": "special_purpose_block_request", "heights": heights}
# # _ = mongodb.mainnet[Collections.helpers].bulk_write(
# #     [
# #         ReplaceOne(
# #             {"_id": "special_purpose_block_request"},
# #             replacement=d,
# #             upsert=True,
# #         )
# #     ]
# # )
# pass
