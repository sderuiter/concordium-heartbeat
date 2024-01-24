from sharingiscaring.mongodb import MongoDB, MongoMotor, Collections
from sharingiscaring.GRPCClient import GRPCClient
from sharingiscaring.tooter import Tooter
from pymongo import ASCENDING, DESCENDING, ReplaceOne
from env import *
from rich import print
import requests

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

for i in range(100):
    url = "https://api.northstake.dk/api/sendnotarytransaction"
    myobj = {
        "file": f"hello-{i}",
        "hash": f"{i}-3BFChzvx3783jGUKgHVCanFVxyDAn5xT3Y5NL5FKydVMuBa7Bm",
    }

    x = requests.post(url, json=myobj)

    print(x.text)
