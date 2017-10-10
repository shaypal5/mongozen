"""Schema-aware collection configuration."""

import os
try:
    import cPickle as pickle  # for python 2
except ImportError:
    import pickle

import yaml
from pymongo.errors import (
    OperationFailure,
    ServerSelectionTimeoutError,
)
from tqdm import tqdm

import strct

from .shared import (
    _get_mongo_cred,
)
from .core import get_reading_client
from .mongozen_objs import SHARED_CFG_FOLDER_PATH


def _build_collection_cfg(db, collection_name):
    # look at last and random 10 documents to find fields of this collection
    try:
        collection = db[collection_name]
        last_ten = list(collection.find(sort=[('_id', -1)], limit=10))
        randoms = list(collection.aggregate([
            {'$filter': {}},
            {'$sample': {'size': 10}}
        ]))
        united_doc = strct.dict.unite_dicts(last_ten + randoms)
        cfg = {}
        col_fields = {}
        for field in united_doc:
            col_fields[field] = type(united_doc[field])
        cfg['fields'] = col_fields
    except OperationFailure:
        return
    col_cfg_fname = collection._get_cfg_file_path()
    with open(col_cfg_fname, 'wb') as col_cfg_file:
        pickle.dump(cfg, col_cfg_file)
        # yaml.dump(cfg, col_cfg_file, indent=4,
        #           default_flow_style=False)


def _build_db_cfg(db, col_names):
    init_fpath = os.path.abspath(
        os.path.join(
            db._get_cfg_folder_path(),
            '__init__.py'
        )
    )
    with open(init_fpath, 'w+') as initfile:
        yaml.dump({}, initfile)
    db_cfg_fname = db._get_cfg_file_path()
    with open(db_cfg_fname, 'w+') as db_cfg_file:
        yaml.dump(col_names, db_cfg_file, indent=4,
                  default_flow_style=False)


def _build_server_cfg(client, db_names):
    init_fpath = os.path.abspath(
        os.path.join(
            client._get_env_cfg_folder_path(client.env),
            '__init__.py'
        )
    )
    with open(init_fpath, 'w+') as initfile:
        yaml.dump({}, initfile)
    init_fpath = os.path.abspath(
        os.path.join(
            client._get_cfg_folder_path(),
            '__init__.py'
        )
    )
    with open(init_fpath, 'w+') as initfile:
        yaml.dump({}, initfile)
    server_cfg_fname = client._get_cfg_file_path()
    with open(server_cfg_fname, 'w+') as server_cfg_file:
        yaml.dump(db_names, server_cfg_file, indent=4,
                  default_flow_style=False)


def rebuild_collection_cfg_files():
    """Rebuilds the cfg files for all of the MongoDB collections in accessible
    servers, according to the latest entries in each collection."""
    print("Rebuilds the cfg files for all of the MongoDB collections in "
          "accessible servers, according to the latest entries in each "
          "collection.")
    os.makedirs(SHARED_CFG_FOLDER_PATH, exist_ok=True)
    error_log = ""
    # env_pbar = tqdm(_get_mongo_cred(), "Per environment")
    for env in tqdm(_get_mongo_cred(), "Per environment"):
        server_pbar = tqdm(_get_mongo_cred()[env])
        for server in server_pbar:
            server_pbar.set_description(
                "Per server in {}".format(env))
            server_pbar.refresh()
            try:
                client = get_reading_client(server, env)
                os.makedirs(client._get_cfg_folder_path(), exist_ok=True)
                db_names = client.database_names()
                _build_server_cfg(client, db_names)
                db_pbar = tqdm(db_names)
                for db_name in db_pbar:
                    if db_name in ['local', 'admin', 'config']:
                        continue
                    db_pbar.set_description(
                        "Per db defined in {}.{}".format(env, server))
                    db_pbar.refresh()
                    db = client[db_name]
                    os.makedirs(db._get_cfg_folder_path(), exist_ok=True)
                    col_names = db.collection_names()
                    _build_db_cfg(db, col_names)
                    col_pbar = tqdm(col_names)
                    for col_name in col_pbar:
                        col_pbar.set_description(
                            "Per collection in {}.{}.{}".format(
                                env, server, db_name))
                        col_pbar.refresh()
                        tqdm([], "Processing {}".format(col_name))
                        if col_name not in ['$cmd', 'system.users']:
                            _build_collection_cfg(db, col_name)
            except (OperationFailure, ServerSelectionTimeoutError):
                error_log += (
                    'mongozen: Connection to {} {} server failed while ' \
                        'rebuilding an inference map.\n'.format(env, server))
    print(error_log)
