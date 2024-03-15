from ccdefundamentals.mongodb import MongoDB, Collections
from ccdefundamentals.GRPCClient import GRPCClient
from ccdefundamentals.tooter import Tooter
from pymongo import ASCENDING, DESCENDING, ReplaceOne
from env import *
import datetime as dt
from datetime import timedelta
from ccdefundamentals.GRPCClient.CCD_Types import CCD_BlockItemSummary


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

pipeline = [
    {"$match": {"account_transaction": {"$exists": True}}},
    {
        "$match": {
            "block_info.slot_time": {"$gt": dt.datetime.utcnow() - timedelta(days=7)}
        }
    },
    # {"$group": {"_id": "$account_transaction.sender", "count": {"$sum": 1}}},
    {"$sort": {"account_transaction.effects.account_transfer.amount": DESCENDING}},
    {"$limit": 10},
]

largest = [
    CCD_BlockItemSummary(**x)
    for x in mongodb.mainnet[Collections.transactions].aggregate(pipeline)
]
pass
