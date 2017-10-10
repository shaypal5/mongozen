"""Core functions for the mongozen package."""

try:  # for automatic caching of return values of functions
    from functools import lru_cache
except ImportError:
    from functools32 import lru_cache  # pylint: disable=E0401

# to send JS functions as arguments to map reduce operations
# from bson.code import Code

from .shared import (
    _get_server_cfg,
    _env_list,
    _server_list
)
from .mongozen_objs import MongozenClient
from .param_inference import (
    _infer_get_client_params,
    _infer_get_db_params,
    _infer_get_collection_params
)


def _server_descriptor_builder(server_name):
    class _ServerDescriptor(object):
        def __init__(self, server_name):
            self.server_name = server_name
        def __get__(self, instance, owner):
            return get_reading_client(
                server_name=self.server_name,
                env_name=instance._env_name
            )
        def __set__(self, instance, value):
            pass
    return _ServerDescriptor(server_name)


class _EnvProperty(object):
    """Env property."""

    def __init__(self, env_name, server_list):
        self._env_name = env_name
        self._server_list = server_list
        for server in server_list:
            setattr(
                _EnvProperty, server, _server_descriptor_builder(server))

    def __getattr__(self, name):
        if name in self._server_list:
            return get_reading_client(
                server_name=name,
                env_name=self._env_name
            )
        raise AttributeError(
            "Environment {} has no server named {}".format(
                self._env_name, name))


def _add_env_attr(module):
    for env in _env_list():
        setattr(module, env, _EnvProperty(env, _server_list(env)))


@lru_cache(maxsize=32)
def get_mongo_uri(server_name, env_name, mode='reading'):
    """Gets the URI of the requested MongoDB server and
    premissions."""
    return _get_server_cfg(server_name, env_name, mode)['host']


@lru_cache(maxsize=32)
def _get_client(server_name, env_name, mode='reading'):
    server_name, env_name = _infer_get_client_params(server_name, env_name)
    # print('getting a mongo client:')
    # print(_get_server_cfg(server_name, env_name, mode))
    return MongozenClient(**_get_server_cfg(server_name, env_name, mode))


# ======= server/db/collection access =====

def _get_reading_client(server_name=None, env_name=None):
    """Returns a MongoClient, with reading permissions, connected to the
    desired server.

    Arguments
    ---------
    server_name : str
        The name of the server to connect to.
    env_name : str
        The name of the environment to connect to.

    Returns
    -------
    pymongo.mongo_client.MongoClient
        A reading client connected to the desired server.
    """
    return _get_client(server_name, env_name, mode='reading')


class _manageClient(object):

    def __init__(self, callback):
        self.callback = callback

    def __enter__(self):
        return self

    def __exit__(self, ex_typ, ex_val, traceback):
        return self.close()

    def __call__(self, *args, **kwargs):
        return self.callback(*args, **kwargs)

# pylint: disable=C0103
get_reading_client = _manageClient(_get_reading_client)
# pylint: disable=W0201
get_reading_client.__doc__ = _get_reading_client.__doc__


def get_writing_client(server_name=None, env_name=None):
    """Returns a MongoDB client, with writing permissions, connected to the
    desired server.

    Arguments
    ---------
    server_name : str
        The name of the server to connect to.
    env_name : str
        The name of the environment to connect to.

    Returns
    -------
    mongozen.mongozen_objs.MongozenClient
        A writing client connected to the desired server.
    """
    return _get_client(server_name, env_name, mode='writing')


@lru_cache(maxsize=1024)
def get_db(db_name, server_name=None, env_name=None, mode='reading'):
    """Returns the MongoDB db of the given name.

    Arguments
    ---------
    db_name : str
        The name of the db to return.
    server_name (optional) : str
        The name of the server from which to fetch the db. If missing, the
        correct server is inferred from the db name, where ambiguity is
        resolved with priority reverse to the ordering of servers in
        the 'mongo_cfg.yml' configuration file.

    Returns:
    mongozen.mongozen_objs.MongozenDatabase
        The database object connected to the desired database.
    """
    server_name, env_name = _infer_get_db_params(
        db_name, server_name, env_name)
    return _get_client(server_name, env_name, mode)[db_name]


def get_collection(collection_name, db_name=None, server_name=None,
                   env_name=None, mode='reading'):
    """Returns the mongo collection of the given name.

    Arguments
    ---------
    collection_name : str
        The name of the collection to return.
    db_name (optional) : str
        The name of the db from which to take the collection. If missing,
        can be inferred from other parameters and possibly user
        configuration if parameter inference is turned on.
    server_name (optional) : str
        The name of the server from which to fetch the db. If missing, can
        be inferred from other parameters and possibly user configuration
        if parameter inference is turned on.
    env_name (optional) : str
        The name of the environment in which the server resides. If
        missing, can inferred from other parameters and possibly user
        configuration if parameter inference is turned on.
    mode (optional) : str
        Either 'reading' or 'writing'. Defaults to 'reading'.

    Returns:
    mongozen.mongozen_objs.MongozenCollection
        The Collection object representing the desired collection.
    """
    db_name, server_name, env_name = _infer_get_collection_params(
        collection_name, db_name, server_name, env_name)
    db_obj = get_db(db_name, server_name, env_name, mode)
    return db_obj[collection_name]


def free_unused_clients():
    """Frees unused client instances."""
    _get_client.cache_clear()
