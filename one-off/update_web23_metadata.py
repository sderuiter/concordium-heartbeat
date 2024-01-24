from sharingiscaring.mongodb import MongoDB, MongoMotor, Collections
from sharingiscaring.GRPCClient import GRPCClient
from sharingiscaring.tooter import Tooter
from pymongo import ASCENDING, DESCENDING, ReplaceOne
import pymongo
from env import *
import asyncio
from rich import print
import aiohttp

from rich.progress import track
from sharingiscaring.cis import (
    CIS,
    StandardIdentifiers,
    MongoTypeTokenAddress,
    MongoTypeLoggedEvent,
    MongoTypeTokenHolderAddress,
    MongoTypeTokenForAddress,
    mintEvent,
    transferEvent,
    burnEvent,
    tokenMetadataEvent,
)
from sharingiscaring.GRPCClient.CCD_Types import *
from sharingiscaring.cis import (
    CIS,
    StandardIdentifiers,
    MongoTypeTokenAddress,
    TokenMetaData,
    MongoTypeTokensTag,
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


async def web23_domain_name_metadata():
    net = "testnet"
    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet
    # ccd_token_tags = db_to_use[Collections.tokens_tags].find_one({"_id": ".ccd"})

    # if not ccd_token_tags:
    #     return

    # contracts_in_ccd_token_tag = MongoTypeTokensTag(**ccd_token_tags).contracts
    # query = {"contract": {"$in": contracts_in_ccd_token_tag}}

    current_content = [
        MongoTypeTokenAddress(**x)
        for x in db_to_use[Collections.tokens_token_addresses].find({})
    ]
    timeout = aiohttp.ClientTimeout(total=1)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for token_address_as_class in current_content:
            # if token_address_as_class.token_metadata:
            #     break

            url = token_address_as_class.metadata_url
            print(token_address_as_class.id, url)
            try:
                async with session.get(url) as resp:
                    try:
                        metadata = TokenMetaData(**await resp.json())
                        # print(metadata)

                    except Exception as e:
                        metadata = None
                    token_address_as_class.token_metadata = metadata
                    dom_dict = token_address_as_class.dict()
                    if "id" in dom_dict:
                        del dom_dict["id"]
                    db_to_use[Collections.tokens_token_addresses].replace_one(
                        {"_id": token_address_as_class.id},
                        replacement=dom_dict,
                        upsert=True,
                    )
            except Exception as e:
                print(f"{e}")
            # except asyncio.TimeoutError:
            #     print(f"timeout error on {url}")


# async def web23_domain_name_metadata():
#     net = "mainnet"
#     db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet
#     ccd_token_tags = db_to_use[Collections.tokens_tags].find_one({"_id": ".ccd"})

#     if not ccd_token_tags:
#         return

#     contracts_in_ccd_token_tag = MongoTypeTokensTag(**ccd_token_tags).contracts
#     query = {"contract": {"$in": contracts_in_ccd_token_tag}}

#     current_content = [
#         MongoTypeTokenAddress(**x)
#         for x in db_to_use[Collections.tokens_token_addresses].find(query)
#     ]
#     for dom in current_content:
#         if not dom.metadata_url:
#             break

#         async with aiohttp.ClientSession() as session:
#             url = f"https://wallet-proxy.mainnet.concordium.software/v0/CIS2TokenMetadata/9377/0?tokenId={dom.token_id}"
#             async with session.get(url) as resp:
#                 t = await resp.json()
#                 # print(t)
#                 try:
#                     metadataURL = t["metadata"][0]["metadataURL"]
#                     query = {"_id": f"<9377,0>-{dom.token_id}"}
#                     dom.metadata_url = metadataURL
#                     dom_dict = dom.dict()
#                     if "id" in dom_dict:
#                         del dom_dict["id"]
#                     db_to_use[Collections.tokens_token_addresses].replace_one(
#                         query,
#                         replacement=dom_dict,
#                         upsert=True,
#                     )

#                     # now go into the metadataURL to retrieve the domain name.
#                     async with aiohttp.ClientSession() as session:
#                         url = f"{dom.metadata_url}"
#                         async with session.get(url) as resp:
#                             t = await resp.json()
#                             # print(t)
#                             try:
#                                 domain_name = t["name"]
#                                 query = {"_id": f"{dom.token_id}"}
#                                 db_to_use[Collections.web23_ccd_domains].replace_one(
#                                     query,
#                                     replacement={"name": domain_name},
#                                     upsert=True,
#                                 )
#                             except:
#                                 pass

#                 except:
#                     pass

#     for dom in current_content:
#         if dom.metadata_url:
#             async with aiohttp.ClientSession() as session:
#                 url = f"{dom.metadata_url}"
#                 async with session.get(url) as resp:
#                     t = await resp.json()
#                     print(t)
#                     try:
#                         domain_name = t["name"]
#                         query = {"_id": f"{dom.token_id}"}
#                         db_to_use[Collections.web23_ccd_domains].replace_one(
#                             query,
#                             replacement={"name": domain_name},
#                             upsert=True,
#                         )
#                     except:
#                         pass

# if not current_content:
#     current_content = {
#         "_id": "provenance",
#         "contracts": [],
#         "tag_template": True,
#         "single_use_token": False,
#     }
# current_content.update({"contracts": list(set(contracts_to_add))})
# db_to_use[Collections.tokens_tags].replace_one(
#     query,
#     replacement=current_content,
#     upsert=True,
# )


def main():
    loop = asyncio.get_event_loop()

    loop.create_task(web23_domain_name_metadata())
    loop.run_forever()


if __name__ == "__main__":
    main()
