from sharingiscaring.mongodb import MongoDB, MongoMotor, Collections
from sharingiscaring.GRPCClient import GRPCClient
from sharingiscaring.tooter import Tooter, TooterChannel, TooterType
from pymongo import ASCENDING, DESCENDING, ReplaceOne, DeleteOne
from sharingiscaring.GRPCClient.CCD_Types import *
from sharingiscaring.cis import MongoTypeTokenAddress
import datetime as dt
from env import *
import json

from sharingiscaring.cis import (
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

net = "mainnet"
db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet


raw_json = """
{"name":"“WhiteLabel #1","unique":true,"description":"“Provenance Tags WhiteLabel #1","attributes":[{"type":"string","name":"nfc_id","value":"2BA09F557A7A0BD300FFF85E786CB34FB8036DC257C795EBE8EA618768BDA820"}],"display":{"url":"https://storage.googleapis.com/provenance_images/whitelabel.png","hash":null},"thumbnail":{"url":"https://storage.googleapis.com/provenance_images/whitelabel.png","hash":null}}
"""
metadata = TokenMetaData(**json.loads(raw_json))

queue = []
no_metadata = [
    MongoTypeTokenAddress(**x)
    for x in db_to_use[Collections.tokens_token_addresses_v2].aggregate(
        [
            {
                "$match": {"contract": "<9403,0>"},
            },
        ]
    )
]
start = dt.datetime.now()

for nm in track(no_metadata):

    # dd = mongodb.mainnet[Collections.tokens_links_v2].find_one({"_id": link["_id"]})
    # if dd:
    #     print("panic)")
    nm.token_metadata = metadata
    nm.token_metadata.attributes[0].value = nm.token_id
    repl_dict = nm.model_dump(exclude_none=True)
    if "id" in repl_dict:
        del repl_dict["id"]
    queue.append(ReplaceOne({"_id": nm.id}, replacement=repl_dict, upsert=True))

    # pass
    # print("len")
    # if len(queue) > 20_000:
    #     end = dt.datetime.now()

    #     print(
    #         f"Block: {i:,.0f}: Processed {len(queue):,.0f} items in {(end-start).total_seconds():,.0f} sec."
    #     )

    #     tooter.relay(
    #         channel=TooterChannel.NOTIFIER,
    #         title="",
    #         chat_id=913126895,
    #         body=f"Block: {i:,.0f}: Processed {len(queue):,.0f} blocks for special events in {(end-start).total_seconds():,.0f} sec.",
    #         notifier_type=TooterType.INFO,
    #     )
    #     # _ = mongodb.mainnet[Collections.special_events].bulk_write(queue)
    #     queue = []
    #     start = dt.datetime.now()

_ = db_to_use[Collections.tokens_token_addresses_v2].bulk_write(queue)
