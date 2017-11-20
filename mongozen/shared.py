"""Functions and variables shared by several modules of mongozen."""

import os
from warnings import warn
from urllib.parse import quote  # for handling @ and : in passwords
from enum import Enum  # For ConfigKeys enum
import traceback
# for automatic caching of return values of functions
from functools import lru_cache

import yaml  # for reading yaml config files

import comath

from .util_constants import (
    MONGO_CRED_FPATH,
    ACCESS_MODES,
    PERSONAL_MONGO_CFG_FPATH,
    DATA_DIR_PATH
)


class CfgKey(Enum):
    """Details the keys of the mongozen config yaml file."""
    ENVS = 'envs'
    GLOBAL_PARAMS = 'global_params'
    MONGOZEN_ENV_PARAMS = 'mongozen_env_params'
    MONGOZEN_SERVER_PARAMS = 'mongozen_server_params'
    # advanced configuration parameters
    INFER_PARAM = 'infer_parameters'
    DEF_ENV = 'default_env'
    DEF_SERVER = 'default_server'
    ENV_PRIORITY = 'env_priority'
    SERVER_PRIORITY = 'server_priority'
    BAD_DB_TERMS = 'bad_db_terms'
    BAD_COL_NAMES = 'bad_collection_names'

DEFAULT_CFG = {
    CfgKey.INFER_PARAM: False,
}


# ==== functions====

@lru_cache(maxsize=2)
def _get_mongo_cred():
    try:
        with open(MONGO_CRED_FPATH, 'r') as mongo_cred_file:
            return yaml.load(mongo_cred_file)
    except FileNotFoundError:
        msg = 'MongoDB credentials for mongozen need to be set by '
        # msg += 'either the appropriate environment variables or  '
        msg += 'a credentials file. The credentials file should be named '
        msg += '.mongozen_credentials.yml, should be placed in your home'
        msg += ' folder, and constructed in the following format:\n'
        msg += '---- Format begins below ----\n'
        msg += 'production:\n'
        msg += '  transaction_server:\n'
        msg += '    reading:\n'
        msg += '      username: my_username\n'
        msg += '      password: my_password\n'
        msg += '---- Format ended above ----\n'
        msg += 'Repeat the same for any environment and server you wish to'
        msg += ' connect to. See {} for further details.'.format(
            "https://github.com/shaypal5/mongozen")
        warn(msg)
        return {}


@lru_cache(maxsize=2)
def _mongozen_cfg():
    try:
        with open(PERSONAL_MONGO_CFG_FPATH, 'r') as mongo_cfg_file:
            personal_mongo_cfg = yaml.load(mongo_cfg_file)
            return personal_mongo_cfg
    except FileNotFoundError:
        with open(PERSONAL_MONGO_CFG_FPATH, 'w+') as mongo_cfg_file:
            yaml.dump(DEFAULT_CFG, mongo_cfg_file)
        warn(
            "A ~/.mongozen/mongozen_cfg.yml file was not found. "
            "A file was created with default values."
            "See {} for further details.".format(
                "https://github.com/shaypal5/mongozen"))
        return DEFAULT_CFG


def _build_mongo_uri(server_cfg, mode_cred):
    return [
        'mongodb://{user}:{pswd}@{host}:{port}'.format(
            user=mode_cred['username'],
            pswd=quote(mode_cred['password']),
            host=hostname,
            port=server_cfg['port']
        ) for hostname in server_cfg['host']
    ]


MISSING_CRED_ERROR_MSG = "Missing mongozen credentials for {} from/to {}.{}!" \
                         " Please see the documentation."

@lru_cache(maxsize=128)
def _get_server_cfg(server_name, env_name, mode='reading'):
    cfg = _mongozen_cfg()
    cred = _get_mongo_cred()
    try:
        global_params = cfg.get(CfgKey.GLOBAL_PARAMS.value, {})
        envs = cfg[CfgKey.ENVS.value]
        env = envs[env_name]
        env_params = env.get(CfgKey.MONGOZEN_ENV_PARAMS.value, {})
        server_cfg = env[server_name].copy()
    except KeyError:
        traceback.print_stack()
        raise ValueError(
            "Bad env or server name: env={}, server={}. ".format(
                env_name, server_name) + \
            "Found cfg={}. Found cred={}".format(cfg, cred))

    try:
        mode_cred = cred[env_name][server_name][mode].copy()
    except KeyError:
        if mode in ACCESS_MODES:
            raise Exception(MISSING_CRED_ERROR_MSG.format(
                mode, env_name, server_name
            ))
        raise ValueError(
            "Bad mode name: {}. Found cfg={}. Found cred={}".format(
                mode, cfg, cred))
    except TypeError:
        raise Exception(MISSING_CRED_ERROR_MSG.format(
            mode, env_name, server_name))
    server_params = server_cfg.get(CfgKey.MONGOZEN_SERVER_PARAMS.value, {})
    resolved_server_cfg = {
        'host': _build_mongo_uri(server_cfg, mode_cred)
    }
    for key in global_params:
        resolved_server_cfg[key] = global_params[key]
    for key in env_params:
        resolved_server_cfg[key] = env_params[key]
    for key in server_params:
        resolved_server_cfg[key] = server_params[key]
    return resolved_server_cfg


@lru_cache(maxsize=2)
def _env_list():
    try:
        env_priority = _mongozen_cfg()[CfgKey.ENV_PRIORITY.value].copy()
        env_priority.reverse()
        envs = [
            env for env in env_priority
            if env in _get_mongo_cred()
        ]
        return envs
    except (KeyError, TypeError):
        envs = list(_get_mongo_cred().keys())
        envs.reverse()
        return envs


@lru_cache(maxsize=comath.func.closest_larger_power_of_2(
    len(_get_mongo_cred().keys())))
def _server_list(env_name):
    try:
        server_priority = _mongozen_cfg(
            )[CfgKey.SERVER_PRIORITY.value][env_name]
        server_priority.reverse()
        servers = [
            server for server in server_priority
            if server in _get_mongo_cred()[env_name].keys()
        ]
        return servers
    except (KeyError, TypeError):
        servers = list(_get_mongo_cred()[env_name].keys())
        servers.reverse()
        return servers


def get_bad_db_terms():
    """Returns a list of bad DB terms."""
    try:
        return _mongozen_cfg()[CfgKey.BAD_DB_TERMS.value]
    except KeyError:
        return []


def get_bad_col_names():
    """Returns a list of bad DB terms."""
    try:
        return _mongozen_cfg()[CfgKey.BAD_COL_NAMES.value]
    except KeyError:
        return []


def get_env_list():
    """Returns a list of defined environments."""
    try:
        return list(_mongozen_cfg()[CfgKey.ENVS.value].keys())
    except KeyError:
        return []


def get_server_list(env):
    """Returns a list of defined servers for the given environment."""
    try:
        return [
            srv for srv in _mongozen_cfg()[CfgKey.ENVS.value][env].keys()
            if srv != CfgKey.MONGOZEN_ENV_PARAMS.value
        ]
    except KeyError:
        return []

def get_host_list(env, server):
    try:
        return _mongozen_cfg()[CfgKey.ENVS.value][env][server]['host']
    except KeyError:
        return []


# ==== Utility Methods ====

@lru_cache(maxsize=2)
def _get_host_to_server_map():
    host_to_server_map = {}
    cfg = _mongozen_cfg()
    for env in get_env_list():
        for server in get_server_list(env):
            for host in get_host_list(env, server):
                host_to_server_map[host] = server
    return host_to_server_map


@lru_cache(maxsize=2)
def _get_host_to_env_map():
    host_to_env_map = {}
    cfg = _mongozen_cfg()
    for env in get_env_list():
        for server in get_server_list(env):
            for host in get_host_list(env, server):
                host_to_env_map[host] = env
    return host_to_env_map


# ==== parameter inference maps ====

MAP_DIR_NAME = 'param_inference_maps'
MAP_DIR_PATH = os.path.abspath(os.path.join(DATA_DIR_PATH, MAP_DIR_NAME))
os.makedirs(MAP_DIR_PATH, exist_ok=True)


def _get_map_fpath(map_enum):
    return os.path.abspath(os.path.join(MAP_DIR_PATH, map_enum.value + '.py'))


def _save_map(mab_obj, map_enum):
    map_fpath = _get_map_fpath(map_enum)
    with open(map_fpath, 'w+') as map_file:
        yaml.dump(mab_obj, map_file)


@lru_cache(maxsize=16)
def _get_map(map_enum):
    map_fpath = _get_map_fpath(map_enum)
    with open(map_fpath) as map_file:
        return yaml.load(map_file)


class ParamInferMap(Enum):
    """Names of maps used for parameter inference."""
    DB_TO_ENV = 'db_to_env_map'
    DB_N_SERVER_TO_ENV = 'db_n_server_to_env_map'
    DB_N_ENV_TO_SERVER = 'db_n_env_to_server_map'
    COL_2_ENV = 'collection_to_env_map'
    COL_N_DB_2_ENV = 'collection_n_db_to_env_map'
    COL_N_SERVER_2_ENV = 'collection_n_server_to_env_map'
    COL_N_DB_N_SERVER_2_ENV = 'collection_n_db_n_server_to_env_map'
    COL_N_ENV_2_SERVER = 'collection_n_env_to_server_map'
    COL_N_DB_N_ENV_2_SERVER = 'collection_n_db_n_env_to_server_map'
    COL_N_SERVER_N_ENV_2_DB = 'collection_n_server_n_env_to_db_map'
