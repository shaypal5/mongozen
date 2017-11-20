"""Defines access to different MongoDB mappings used for inference."""

from pymongo.errors import (
    ServerSelectionTimeoutError,
    OperationFailure
)
from tqdm import tqdm

from .shared import (
    _save_map,
    ParamInferMap,
    _env_list,
    _server_list
)
from .core import (
    _get_client,
    get_db
)


def _db_names_by_server_and_env(server_name, env_name):
    return _get_client(server_name, env_name).database_names()


def _collection_list(db_name, server_name, env_name):
    return get_db(db_name, server_name, env_name).collection_names()


def _add_to_map(map_obj, key, val):
    value_set = map_obj.get(key, set())
    value_set.add(val)
    map_obj[key] = value_set


def rebuild_all_maps():
    """Rebuilds all maps mongozen uses for parameter inference."""
    print("Rebuilding all maps mongozen uses for parameter inference.")
    # get_db maps
    db2env = {}
    dbserv2env = {}
    dbenv2serv = {}
    # get_collection maps
    col2env = {}
    coldb2env = {}
    colserv2env = {}
    coldbserv2env = {}
    colenv2serv = {}
    coldbenv2serv = {}
    colservenv2db = {}
    print("Running per environment in your credentials file...")
    for env in _env_list():
        print("Running per server defined for {}...".format(env))
        for serv in _server_list(env):
            try:
                print("Running per db found on {}.{}...".format(env, serv))
                for db in tqdm(_db_names_by_server_and_env(serv, env)):
                    # get_db maps
                    _add_to_map(db2env, db, env)
                    _add_to_map(dbserv2env, (db, serv), env)
                    _add_to_map(dbenv2serv, (db, env), serv)
                    # print("Running per collection in on {}.{}.{}...".format(
                    #     env, serv, db))
                    for col in _collection_list(db, serv, env):
                        # get_collection maps
                        _add_to_map(col2env, col, env)
                        _add_to_map(coldb2env, (col, db), env)
                        _add_to_map(colserv2env, (col, serv), env)
                        _add_to_map(coldbserv2env, (col, db, serv), env)
                        _add_to_map(colenv2serv, (col, env), serv)
                        _add_to_map(coldbenv2serv, (col, db, env), serv)
                        _add_to_map(colservenv2db, (col, serv, env), db)
            except (ServerSelectionTimeoutError, OperationFailure):
                print('mongozen: Connection to {} {} server failed while ' \
                      'rebuilding an inference map.'.format(env, serv))
    # get_db maps
    _save_map(db2env, ParamInferMap.DB_TO_ENV)
    _save_map(dbserv2env, ParamInferMap.DB_N_SERVER_TO_ENV)
    _save_map(dbenv2serv, ParamInferMap.DB_N_ENV_TO_SERVER)
    # get_collection maps
    _save_map(col2env, ParamInferMap.COL_2_ENV)
    _save_map(coldb2env, ParamInferMap.COL_N_DB_2_ENV)
    _save_map(colserv2env, ParamInferMap.COL_N_SERVER_2_ENV)
    _save_map(coldbserv2env, ParamInferMap.COL_N_DB_N_SERVER_2_ENV)
    _save_map(colenv2serv, ParamInferMap.COL_N_ENV_2_SERVER)
    _save_map(coldbenv2serv, ParamInferMap.COL_N_DB_N_ENV_2_SERVER)
    _save_map(colservenv2db, ParamInferMap.COL_N_SERVER_N_ENV_2_DB)
