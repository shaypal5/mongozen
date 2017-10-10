"""Inferring parameters for mongozen functions."""

from strct.set import get_priority_elem_in_set

from .shared import (
    _get_map,
    Map,
    _mongozen_cfg,
    CfgKey
)


DEF_ENV = _mongozen_cfg()['default_env']
DEF_SERVER = _mongozen_cfg()['default_server']
INFER_PARAM = _mongozen_cfg()['infer_parameters']


def _infer_get_client_params(server_name, env_name):
    if server_name is None:
        if INFER_PARAM:
            server_name = DEF_SERVER
        else:
            raise ValueError("Missing value for server_name parameter!")
    if env_name is None:
        if INFER_PARAM:
            env_name = DEF_ENV
        else:
            raise ValueError("Missing value for env_name parameter!")
    return server_name, env_name


def _infer_get_db_params(db_name, server_name, env_name):
    if env_name is None:
        if not INFER_PARAM:
            raise ValueError("Missing value for the env_name parameter!")
        if server_name is None:
            possible_envs = _get_map(Map.DB_2_ENV)[db_name]
            env_name = get_priority_elem_in_set(
                possible_envs, _mongozen_cfg()[CfgKey.ENV_PRIORITY.value])
        else:
            possible_envs = _get_map(Map.DB_N_SERVER_2_ENV)[(
                db_name, server_name)]
            env_name = get_priority_elem_in_set(
                possible_envs, _mongozen_cfg()[CfgKey.ENV_PRIORITY.value])
    if server_name is None:
        if not INFER_PARAM:
            raise ValueError("Missing value for server_name parameters!")
        possible_servers = _get_map(Map.DB_N_ENV_2_SERVER)[(db_name, env_name)]
        server_name = get_priority_elem_in_set(
            possible_servers, _mongozen_cfg()[
                CfgKey.SERVER_PRIORITY.value][env_name])
    return server_name, env_name


def _filter_dbs(db_names):
    return [
        name
        for name
        in db_names
        if all([term not in name for term in _mongozen_cfg()['bad_db_terms']])
    ]


def _infer_get_collection_params(collection_name, db_name, server_name,
                                 env_name):
    if env_name is None:
        if not INFER_PARAM:
            raise ValueError("Missing value for the env_name parameter!")
        if server_name is None:
            if db_name is None:
                possible_envs = _get_map(Map.COL_2_ENV)[collection_name]
                env_name = get_priority_elem_in_set(
                    possible_envs, _mongozen_cfg()[CfgKey.ENV_PRIORITY.value])
            else:
                possible_envs = _get_map(
                    Map.COL_N_DB_2_ENV)[(collection_name, db_name)]
                env_name = get_priority_elem_in_set(
                    possible_envs, _mongozen_cfg()[CfgKey.ENV_PRIORITY.value])
        else:
            if db_name is None:
                possible_envs = _get_map(
                    Map.COL_N_SERVER_2_ENV)[(collection_name, server_name)]
                env_name = get_priority_elem_in_set(
                    possible_envs, _mongozen_cfg()[CfgKey.ENV_PRIORITY.value])
            else:
                possible_envs = _get_map(Map.COL_N_DB_N_SERVER_2_ENV)[
                    (collection_name, db_name, server_name)]
                env_name = get_priority_elem_in_set(
                    possible_envs, _mongozen_cfg()[CfgKey.ENV_PRIORITY.value])
    if server_name is None:
        if not INFER_PARAM:
            raise ValueError("Missing value for the server_name parameter!")
        if db_name is None:
            possible_servers = _get_map(Map.COL_N_ENV_2_SERVER)[
                (collection_name, env_name)]
            server_name = get_priority_elem_in_set(
                possible_servers,
                _mongozen_cfg()[CfgKey.SERVER_PRIORITY.value][env_name])
        else:
            possible_servers = _get_map(Map.COL_N_DB_N_ENV_2_SERVER)[
                (collection_name, db_name, env_name)]
            server_name = get_priority_elem_in_set(
                possible_servers,
                _mongozen_cfg()[CfgKey.SERVER_PRIORITY.value][env_name])
    if db_name is None:
        possible_dbs = _get_map(Map.COL_N_SERVER_N_ENV_2_DB)[
            (collection_name, server_name, env_name)]
        possible_dbs = _filter_dbs(possible_dbs)
        db_name = possible_dbs[0]
    return db_name, server_name, env_name
