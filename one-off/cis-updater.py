from ccdefundamentals.mongodb import (
    MongoDB,
    Collections,
    MongoTypeModule,
    MongoTypeInstance,
)
from ccdefundamentals.GRPCClient import GRPCClient
from ccdefundamentals.cis import CIS, StandardIdentifiers
from ccdefundamentals.enums import NET
from ccdefundamentals.tooter import Tooter
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

result = [MongoTypeModule(**x) for x in mongodb.testnet[Collections.modules].find()]

for module in result:
    # print(module.module_name)
    if module.methods:
        if "supports" in module.methods:
            for contract in module.contracts:
                entrypoint = module.module_name + ".supports"
                index = int(contract.split(",")[0][1:])
                subindex = int(contract.split(",")[1][:-1])
                cis = CIS(grpcclient, index, subindex, entrypoint, NET.TESTNET)
                supports_cis_2 = cis.supports_standard(StandardIdentifiers.CIS_2)
                if supports_cis_2:
                    print(
                        f"{module.module_name}: {contract}: Supports CIS-2 = {supports_cis_2}"
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
