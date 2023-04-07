import os


def exists(path):
    """Test whether a path exists.  Returns False for broken symbolic links"""
    try:
        os.stat(path)
    except (OSError, ValueError):
        return False
    return True


file_exists = exists("envdonotcommit.py")
if file_exists:
    from envdonotcommit import (
        MONGODB_PASSWORD_LOCAL,
        API_TOKEN_LOCAL,
        MONGO_IP_LOCAL,
        MONGO_PORT_LOCAL,
        NOTIFIER_API_TOKEN_LOCAL,
        FASTMAIL_TOKEN_LOCAL,
        REQUESTOR_NODES_LOCAL,
        TESTNET_IP_LOCAL,
        MAINNET_IP_LOCAL,
        TESTNET_PORT_LOCAL,
        MAINNET_PORT_LOCAL,
        NET_LOCAL,
    )

else:
    MONGODB_PASSWORD_LOCAL = None
    API_TOKEN_LOCAL = None
    MONGO_IP_LOCAL = None
    MONGO_PORT_LOCAL = None
    NOTIFIER_API_TOKEN_LOCAL = None
    FASTMAIL_TOKEN_LOCAL = None
    REQUESTOR_NODES_LOCAL = None
    TESTNET_IP_LOCAL = None
    MAINNET_IP_LOCAL = None
    TESTNET_PORT_LOCAL = None
    MAINNET_PORT_LOCAL = None
    NET_LOCAL = None


API_TOKEN = os.environ.get("API_TOKEN", API_TOKEN_LOCAL)
ENVIRONMENT = "dev" if API_TOKEN == API_TOKEN_LOCAL else "prod"
ON_SERVER = os.environ.get("ON_SERVER", False)
BRANCH = os.environ.get("BRANCH", "dev")
MONGO_IP = os.environ.get("MONGO_IP", MONGO_IP_LOCAL)
MONGO_PORT = os.environ.get("MONGO_PORT", MONGO_PORT_LOCAL)
MONGODB_PASSWORD = os.environ.get("MONGODB_PASSWORD", MONGODB_PASSWORD_LOCAL)
NOTIFIER_API_TOKEN = os.environ.get("NOTIFIER_API_TOKEN", NOTIFIER_API_TOKEN_LOCAL)
FASTMAIL_TOKEN = os.environ.get("FASTMAIL_TOKEN", FASTMAIL_TOKEN_LOCAL)
REQUESTOR_NODES = os.environ.get("REQUESTOR_NODES", REQUESTOR_NODES_LOCAL)
REQUESTOR_NODES = REQUESTOR_NODES.split(",")

TESTNET_IP = os.environ.get("TESTNET_IP", TESTNET_IP_LOCAL)
MAINNET_IP = os.environ.get("MAINNET_IP", MAINNET_IP_LOCAL)

TESTNET_PORT = os.environ.get("TESTNET_PORT", TESTNET_PORT_LOCAL)
MAINNET_PORT = os.environ.get("MAINNET_PORT", MAINNET_PORT_LOCAL)

MAX_BLOCKS_PER_RUN = int(os.environ.get("MAX_BLOCKS_PER_RUN", 1000))

NET = os.environ.get("NET", NET_LOCAL)
TESTNET = False if NET == "MAINNET" else True
