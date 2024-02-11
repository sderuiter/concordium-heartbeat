# ruff: noqa: F403, F405, E402, E501, E722
from sharingiscaring.GRPCClient import GRPCClient
from sharingiscaring.GRPCClient.CCD_Types import *
from sharingiscaring.tooter import Tooter
from sharingiscaring.mongodb import (
    MongoDB,
    Collections,
    MongoMotor,
)

from pymongo.collection import Collection
from env import *
from rich.console import Console
import urllib3

from .nodes_from_dashboard import Nodes as _nodes
from .token_exchange_rates import ExchangeRates as _exchange_rates
from .impacted_addresses import ImpactedAddresses as _impacted_addresses
from .token_accounting import TokenAccounting as _token_accounting
from .start_over import StartOver as _start_over
from .send_to_mongo import SendToMongo as _send_to_mongo
from .block_loop import BlockLoop as _block_loop
from .pre_renderers import PreRenderers as _pre_renderers
from .consistency import Consistency as _consistency
from .utils import Queue

urllib3.disable_warnings()
console = Console()


class Heartbeat(
    _block_loop,
    _nodes,
    _exchange_rates,
    _impacted_addresses,
    _token_accounting,
    _start_over,
    _send_to_mongo,
    _pre_renderers,
    _consistency,
):
    def __init__(
        self,
        grpcclient: GRPCClient,
        tooter: Tooter,
        mongodb: MongoDB,
        motormongo: MongoMotor,
        net: str,
    ):
        self.grpcclient = grpcclient
        self.tooter = tooter
        self.mongodb = mongodb
        self.motormongo = motormongo
        self.net = net
        self.utilities: dict[Collections, Collection] = self.mongodb.utilities
        self.db: dict[Collections, Collection] = (
            self.mongodb.mainnet if self.net == "mainnet" else self.mongodb.testnet
        )
        self.motordb: dict[Collections, Collection] = (
            self.motormongo.testnet if net == "testnet" else self.motormongo.mainnet
        )
        self.finalized_block_infos_to_process: list[CCD_BlockInfo] = []
        self.special_purpose_block_infos_to_process: list[CCD_BlockInfo] = []

        self.existing_source_modules: dict[CCD_ModuleRef, set] = {}
        self.queues: dict[Collections, list] = {}
        for q in Queue:
            self.queues[q] = []
