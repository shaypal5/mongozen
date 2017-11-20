"""mongozen constants."""

import os


HOMEDIR = os.path.expanduser("~")
MONGOZEN_DIR_NAME = '.mongozen'
MONGOZEN_DIR_PATH = os.path.join(HOMEDIR, MONGOZEN_DIR_NAME)
os.makedirs(MONGOZEN_DIR_PATH, exist_ok=True)

DATA_DIR_NAME = 'data'
DATA_DIR_PATH = os.path.join(MONGOZEN_DIR_PATH, DATA_DIR_NAME)
os.makedirs(DATA_DIR_PATH, exist_ok=True)


MONGO_CRED_FNAME = 'mongozen_credentials.yml'
MONGO_CRED_FPATH = os.path.abspath(os.path.join(
    MONGOZEN_DIR_PATH, MONGO_CRED_FNAME))

PERSONAL_MONGO_CFG_FNAME = 'mongozen_cfg.yml'
PERSONAL_MONGO_CFG_FPATH = os.path.abspath(os.path.join(
    MONGOZEN_DIR_PATH, PERSONAL_MONGO_CFG_FNAME))

CONNECTION_TIMEOUT_IN_MS = 2500

ACCESS_MODES = ['reading', 'writing']

MONGODB_BULK_WRITE_LIMIT = 1000
