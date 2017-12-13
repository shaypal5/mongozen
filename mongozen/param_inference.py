"""Inferring parameters for mongozen functions."""

from strct.sets import get_priority_elem_in_set

from .shared import (
    _get_map,
    ParamInferMap,
    _mongozen_cfg,
    CfgKey
)


DEF_ENV = _mongozen_cfg().get('default_env', None)
DEF_SERVER = _mongozen_cfg().get('default_server', None)
INFER_PARAM = _mongozen_cfg().get('infer_parameters', False)


def _infer_get_client_params(server_name, env_name):
    if server_name is None:
        if INFER_PARAM and DEF_SERVER:
            server_name = DEF_SERVER
        else:
            raise ValueError("Missing value for server_name parameter!")
    if env_name is None:
        if INFER_PARAM and DEF_ENV:
            env_name = DEF_ENV
        else:
            raise ValueError("Missing value for env_name parameter!")
    return server_name, env_name


def _infer_get_db_params(db_name, server_name, env_name):
    if env_name is None:
        if not INFER_PARAM:
            raise ValueError("Missing value for the env_name parameter!")
        if server_name is None:
            possible_envs = _get_map(ParamInferMap.DB_TO_ENV)[db_name]
            env_name = get_priority_elem_in_set(
                possible_envs, _mongozen_cfg()[CfgKey.ENV_PRIORITY.value])
        else:
            possible_envs = _get_map(ParamInferMap.DB_N_SERVER_TO_ENV)[(
                db_name, server_name)]
            env_name = get_priority_elem_in_set(
                possible_envs, _mongozen_cfg()[CfgKey.ENV_PRIORITY.value])
    if server_name is None:
        if not INFER_PARAM:
            raise ValueError("Missing value for server_name parameters!")
        possible_servers = _get_map(ParamInferMap.DB_N_ENV_TO_SERVER)[
            (db_name, env_name)]
        server_name = get_priority_elem_in_set(
            possible_servers, _mongozen_cfg()[
                CfgKey.SERVER_PRIORITY.value][env_name])
    return server_name, env_name


def _filter_dbs(db_names):
    try:
        bad_terms = _mongozen_cfg()[CfgKey.BAD_DB_TERMS.value]
        return [
            name
            for name
            in db_names
            if all([term not in name for term in bad_terms])
        ]
    except KeyError:
        return list(db_names)


def _infer_get_collection_params(collection_name, db_name, server_name,
                                 env_name):
    if env_name is None:
        if not INFER_PARAM:
            raise ValueError("Missing value for the env_name parameter!")
        if server_name is None:
            if db_name is None:
                possible_envs = _get_map(ParamInferMap.COL_2_ENV)[collection_name]
                env_name = get_priority_elem_in_set(
                    possible_envs, _mongozen_cfg()[CfgKey.ENV_PRIORITY.value])
            else:
                possible_envs = _get_map(
                    ParamInferMap.COL_N_DB_2_ENV)[(collection_name, db_name)]
                env_name = get_priority_elem_in_set(
                    possible_envs, _mongozen_cfg()[CfgKey.ENV_PRIORITY.value])
        else:
            if db_name is None:
                possible_envs = _get_map(
                    ParamInferMap.COL_N_SERVER_2_ENV)[
                        (collection_name, server_name)]
                env_name = get_priority_elem_in_set(
                    possible_envs, _mongozen_cfg()[CfgKey.ENV_PRIORITY.value])
            else:
                possible_envs = _get_map(
                    ParamInferMap.COL_N_DB_N_SERVER_2_ENV)[
                        (collection_name, db_name, server_name)]
                env_name = get_priority_elem_in_set(
                    possible_envs, _mongozen_cfg()[CfgKey.ENV_PRIORITY.value])
    if server_name is None:
        if not INFER_PARAM:
            raise ValueError("Missing value for the server_name parameter!")
        if db_name is None:
            possible_servers = _get_map(ParamInferMap.COL_N_ENV_2_SERVER)[
                (collection_name, env_name)]
            server_name = get_priority_elem_in_set(
                possible_servers,
                _mongozen_cfg()[CfgKey.SERVER_PRIORITY.value][env_name])
        else:
            possible_servers = _get_map(ParamInferMap.COL_N_DB_N_ENV_2_SERVER)[
                (collection_name, db_name, env_name)]
            server_name = get_priority_elem_in_set(
                possible_servers,
                _mongozen_cfg()[CfgKey.SERVER_PRIORITY.value][env_name])
    if db_name is None:
        possible_dbs = _get_map(ParamInferMap.COL_N_SERVER_N_ENV_2_DB)[
            (collection_name, server_name, env_name)]
        possible_dbs = _filter_dbs(possible_dbs)
        db_name = possible_dbs[0]
    return db_name, server_name, env_name
