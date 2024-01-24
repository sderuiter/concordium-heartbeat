import asyncio
from sharingiscaring.GRPCClient import GRPCClient
from sharingiscaring.GRPCClient.queries import *
from rich import print
from rich.progress import track
import datetime as dt
from sharingiscaring.GRPCClient.CCD_Types import *
from sharingiscaring.GRPCClient.types_pb2 import Empty
from sharingiscaring.tooter import Tooter, TooterType, TooterChannel
from sharingiscaring.mongodb import MongoDB, TestNetCollections
import os
import json
import dateutil

###############################################
#
# This script can be used to start from scratch.
# To get started, recreate the blocks and transactions
# collections (currently called _grpc, will drop)
# in MongoDB and set the start, end parameters
# in the main to 0 and last finalized block height 
# (look up on dashboard).
# This script should run in less than 24 hours (5M blocks).
###############################################



def exists(path):
    """Test whether a path exists.  Returns False for broken symbolic links"""
    try:
        os.stat(path)
    except (OSError, ValueError):
        return False
    return True

file_exists = exists('env.py')
if file_exists:
    from envdonotcommit import MONGODB_PASSWORD_LOCAL
else:
    MONGODB_PASSWORD_LOCAL = None
    
API_TOKEN = os.environ.get('API_TOKEN', '5456222180:AAF6RYi_zNziRi4DrE_GCffqHMrP5EHVR-A') 
ENVIRONMENT = 'dev' if API_TOKEN == '5456222180:AAF6RYi_zNziRi4DrE_GCffqHMrP5EHVR-A' else 'prod'
ON_SERVER = os.environ.get('ON_SERVER', False)
BRANCH    = os.environ.get('BRANCH', 'dev') 
MONGO_IP = os.environ.get('MONGO_IP', '31.20.212.96')    
MONGO_PORT = os.environ.get('MONGO_PORT', '27027')    
MONGODB_PASSWORD = os.environ.get('MONGODB_PASSWORD', MONGODB_PASSWORD_LOCAL)   
NOTIFIER_API_TOKEN          = os.environ.get('NOTIFIER_API_TOKEN', '5940257423:AAHqd5kqm0NZsYCyJDK20GSAk0MHitEDfpU') 
FASTMAIL_TOKEN              = os.environ.get('FASTMAIL_TOKEN', '3y9k8csgylkeh9hm')
REQUESTOR_NODES             = os.environ.get('REQUESTOR_NODES', '31.20.212.96')
REQUESTOR_NODES             = REQUESTOR_NODES.split(',')

tooter: Tooter = Tooter(ENVIRONMENT, BRANCH, NOTIFIER_API_TOKEN, API_TOKEN, FASTMAIL_TOKEN)
mongodb = MongoDB({'MONGODB_PASSWORD': MONGODB_PASSWORD, 'MONGO_IP': MONGO_IP, 'MONGO_PORT': MONGO_PORT}, tooter)


class Heartbeat:
    def __init__(self, grpcclient: GRPCClient, tooter: Tooter, mongodb: MongoDB):
        self.grpcclient = grpcclient
        self.tooter = tooter
        self.mongodb = mongodb
        self.finalized_block_to_process = []
        self.block_counter = 0
        self.tx_counter = 0
        self.blocks = []
        self.txs = []
    
    def loop(self, start, end, report_interval=1_000):
        start_time = dt.datetime.now()
        prev_tx_count = 0
        for block_height in track(range(start, end)):
            block_hash = self.grpcclient.get_blocks_at_height(block_height)[0]
            try:
                block = self.grpcclient.get_block_transaction_events(block_hash)
                block_info = self.grpcclient.get_block_info(block_hash)
            except Exception as e:
                print (f"Height: {block_height:,.0f}: Error: {e}")
            self.block_counter += 1
            self.tx_counter += len(block.transaction_summaries)

            json_block_info = json.loads(block_info.json(exclude_none=True))
            json_block_info.update({"_id": block_info.hash})
            json_block_info.update({"slot_time": block_info.slot_time})
            json_block_info.update({"transaction_hashes": [x.hash for x in block.transaction_summaries]})
            del json_block_info['arrive_time']
            del json_block_info['receive_time']

            self.blocks.append(json_block_info)

            for tx in block.transaction_summaries:
                json_tx = json.loads(tx.json(exclude_none=True))
                json_tx.update({"_id": tx.hash})
                json_tx.update({"block_info": {
                    'height': block_info.height, 
                    'hash': block_info.hash,
                    'slot_time': block_info.slot_time
                    }})
                self.txs.append(json_tx)


            if (self.block_counter/report_interval) == int(self.block_counter/report_interval):
                duration = (dt.datetime.now() - start_time).total_seconds()

                print (f"# Block group end: {block_height:8,.0f}, # Transactions: {(self.tx_counter - prev_tx_count):4,.0f}, Process Time: {duration:3,.3f}s")
                start_time = dt.datetime.now()
                prev_tx_count = self.tx_counter

                mongodb.collection_blocks_grpc.insert_many(self.blocks)
                if len(self.txs) > 0:
                    mongodb.collection_transactions_grpc.insert_many(self.txs)
                self.blocks = []
                self.txs = []
                duration = (dt.datetime.now() - start_time).total_seconds()
                print (f'saving to MongoDB took {duration:3,.3f}s.')
                start_time = dt.datetime.now()


def main():
    grpcclient = GRPCClient()
    heartbeat = Heartbeat(grpcclient, tooter, mongodb)
    heartbeat.loop(1_439_757, 3_000_000, 1)
    


def instances(grpcclient: GRPCClient, testnet=False):
    if testnet:
        block_hash = grpcclient.get_blocks_at_height(1592935)[0]
    else:
        block_hash = "88f49b88a5c22bea0a5a23663c6e99485dd10cd83f6175c122bfd19f5e2fc742"
    il = grpcclient.get_instance_list(block_hash)
    for index, instance in enumerate(il):
        ii = grpcclient.get_instance_info(instance['index'], instance['subindex'], block_hash)
        print (instance['index'], end=" | ")
        if ii.v0.source_module != '':
            jj = json.loads(ii.v0.json(exclude_none=True))
            jj.update({'_id': f"<{instance['index']},{instance['subindex']}>"})
            if testnet:
                query = {"_id": jj["_id"] }
                mongodb.testnet[TestNetCollections.instances].replace_one(query, jj, upsert=True)
                
            else:
                query = {"_id": jj["_id"] }
                mongodb.collection_instances.replace_one(query, jj, upsert=True)
                # mongodb.collection_instances.insert_one(jj)
        elif ii.v1.source_module != '':
            jj = json.loads(ii.v1.json(exclude_none=True))
            jj.update({'_id': f"<{instance['index']},{instance['subindex']}>"})
            
            if testnet:
                query = {"_id": jj["_id"] }
                mongodb.testnet[TestNetCollections.instances].replace_one(query, jj, upsert=True)
                
            else:
                query = {"_id": jj["_id"] }
                mongodb.collection_instances.replace_one(query, jj, upsert=True)


if __name__ == '__main__':
    testnet = True
    if testnet:
        grpcclient = GRPCClient('207.180.201.8', 20001)
    else:
        grpcclient = GRPCClient()
    try:
        instances(grpcclient, testnet=True)
    except Exception as f:
        print('main error: ', f)