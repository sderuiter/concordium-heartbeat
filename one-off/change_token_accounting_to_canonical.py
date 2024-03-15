from ccdefundamentals.mongodb import (
    MongoDB,
    MongoMotor,
    Collections,
    MongoImpactedAddress,
)
from ccdefundamentals.GRPCClient import GRPCClient
from ccdefundamentals.tooter import Tooter, TooterChannel, TooterType
from pymongo import ASCENDING, DESCENDING, ReplaceOne, DeleteOne
from ccdefundamentals.GRPCClient.CCD_Types import *
from ccdefundamentals.cis import MongoTypeTokenAddress, MongoTypeTokenHolderAddress
import datetime as dt
from env import *
from rich import print
from bisect import bisect_right

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

result = [
    MongoTypeTokenHolderAddress(**x)
    for x in db_to_use[Collections.tokens_accounts].find({})
]

for token_holder in list(result):
    # token_holder = MongoTypeTokenHolderAddress(**token_holder)
    token_holder.account_address_canonical = token_holder.id[:29]
    repl_dict = token_holder.model_dump(exclude_none=True)
    if "id" in repl_dict:
        del repl_dict["id"]

    queue.append(
        ReplaceOne(
            {"_id": token_holder.id},
            repl_dict,
            upsert=True,
        )
    )

_ = db_to_use[Collections.tokens_accounts].bulk_write(queue)
