"""mongozen constants."""

import os


HOMEDIR = os.path.expanduser("~")
# PACKAGE_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
DATA_DIR_NAME = 'data'
DATA_DIR_PATH = os.path.join(HOMEDIR, DATA_DIR_NAME)

MONGO_CRED_FNAME = 'mongozen_credentials.yml'
MONGO_CRED_FPATH = os.path.abspath(os.path.join(HOMEDIR, MONGO_CRED_FNAME))

PERSONAL_MONGO_CFG_FNAME = 'mongozen_cfg.yml'
PERSONAL_MONGO_CFG_FPATH = os.path.abspath(os.path.join(
    HOMEDIR, PERSONAL_MONGO_CFG_FNAME))

CONNECTION_TIMEOUT_IN_MS = 2500

ACCESS_MODES = ['reading', 'writing']

MONGODB_BULK_WRITE_LIMIT = 1000
