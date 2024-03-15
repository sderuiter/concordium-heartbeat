import os
import ast
from dotenv import load_dotenv

load_dotenv()


BRANCH = os.environ.get("BRANCH", "dev")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "prod")
NOTIFIER_API_TOKEN = os.environ.get("NOTIFIER_API_TOKEN")
API_TOKEN = os.environ.get("API_TOKEN")
FASTMAIL_TOKEN = os.environ.get("FASTMAIL_TOKEN")
FALLBACK_URI = os.environ.get("FALLBACK_URI")
MONGO_URI = os.environ.get("MONGO_URI")
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID")
MAILTO_LINK = os.environ.get("MAILTO_LINK")
MAILTO_USER = os.environ.get("MAILTO_USER")
GRPC_MAINNET = ast.literal_eval(os.environ["GRPC_MAINNET"])
GRPC_TESTNET = ast.literal_eval(os.environ["GRPC_TESTNET"])
COIN_API_KEY = os.environ.get("COIN_API_KEY")
DEBUG = False if os.environ.get("DEBUG", False) == "False" else True
MAX_BLOCKS_PER_RUN = int(os.environ.get("MAX_BLOCKS_PER_RUN", 100))

RUN_ON_NET = os.environ.get("NET", "testnet")
