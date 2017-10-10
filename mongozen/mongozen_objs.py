"""Defines smart wrappes for pymongo objects."""

import os
try:  # for automatic caching of return values of functions
    from functools import lru_cache
except ImportError:
    from functools32 import lru_cache  # pylint: disable=E0401
try:
    import cPickle as pickle  # for python 2
except ImportError:
    import pickle

import yaml
from pymongo.mongo_client import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import CollectionInvalid

from utilp.classes import (
    InheritableDocstrings,
    copy_ancestor_docstring
)
from .shared import (
    DATA_DIR_PATH,
    CfgKey,
    _mongozen_cfg
)

COLLECTION_CFG_DIR_NAME = 'collection_cfg'
COLLECTION_CFG_DIR_PATH = os.path.abspath(
    os.path.join(
        DATA_DIR_PATH,
        COLLECTION_CFG_DIR_NAME
    )
)
SHARED_CFG_FOLDER_NAME = 'shared'
SHARED_CFG_FOLDER_PATH = os.path.abspath(
    os.path.join(
        COLLECTION_CFG_DIR_PATH,
        SHARED_CFG_FOLDER_NAME
    )
)


# ==== Utility Methods ====

@lru_cache(maxsize=2)
def _get_host_to_server_map():
    host_to_server_map = {}
    cfg = _mongozen_cfg()
    for env in cfg[CfgKey.ENVS.value]:
        for server in cfg[CfgKey.ENVS.value][env]:
            for host in cfg[CfgKey.ENVS.value][env][server]['host']:
                host_to_server_map[host] = server
    return host_to_server_map


@lru_cache(maxsize=2)
def _get_host_to_env_map():
    host_to_env_map = {}
    cfg = _mongozen_cfg()
    for env in cfg[CfgKey.ENVS.value]:
        for server in cfg[CfgKey.ENVS.value][env]:
            for host in cfg[CfgKey.ENVS.value][env][server]['host']:
                host_to_env_map[host] = env
    return host_to_env_map


# ==== Classes Definitions ====

class MongozenClient(MongoClient, metaclass=InheritableDocstrings):
    """A wrapper class for pymongo.mongo_client.MongoClient that behaves
    identically to it, except it returns MongozenDatabase objects instead of
    pymongo.database.Database objects. Original documentation below:

    """
    # I need to incorporate this into MongozenClient:
    # https://github.com/arngarden/MongoDBProxy

    __doc__ = __doc__ + MongoClient.__init__.__doc__

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            host = kwargs['host']
        except KeyError:
            if args:
                host = args[0]
            else:
                host = None
        try:
            if not isinstance(host, str):
                host = host[0]
            host = host[host.rfind('@')+1:host.rfind(':')]
        except (TypeError, AttributeError):
            raise TypeError("Host must be a string or list of strings")
        self.server = _get_host_to_server_map()[host]
        self.env = _get_host_to_env_map()[host]
        cfg = self._get_server_cfg()
        dbs_to_init = [
            db for db in cfg
            if db not in _mongozen_cfg()[CfgKey.BAD_DB_TERMS.value]
        ]
        for db_name in dbs_to_init:
            setattr(self, db_name, self[db_name])

    @copy_ancestor_docstring
    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError(
                "MongoClient has no attribute %r. To access the %s"
                " database, use client[%r]." % (name, name, name))
        return self.__getitem__(name)

    @copy_ancestor_docstring
    def __getitem__(self, name):
        return MongozenDatabase(self, name)

    @copy_ancestor_docstring
    def get_database(self, name, codec_options=None, read_preference=None,
                     write_concern=None, read_concern=None):
        return MongozenDatabase(self, name, codec_options, read_preference,
                                write_concern, read_concern)



    def _get_env_cfg_folder_path(self, env_name):
        return os.path.abspath(
            os.path.join(
                COLLECTION_CFG_DIR_PATH,
                env_name
            )
        )

    def _get_cfg_folder_path(self):
        return os.path.abspath(
            os.path.join(
                self._get_env_cfg_folder_path(self.env),
                self.server
            )
        )

    def _get_cfg_file_path(self):
        return os.path.abspath(
            os.path.join(
                self._get_env_cfg_folder_path(self.env),
                self.server + '.py'
            )
        )

    def _get_server_cfg(self):
        try:
            server_cfg_fname = self._get_cfg_file_path()
            with open(server_cfg_fname, 'r') as server_cfg_file:
                return yaml.load(server_cfg_file)
        except FileNotFoundError:
            return {}


class MongozenDatabase(Database, metaclass=InheritableDocstrings):
    """A wrapper class for pymongo.database.Database that behaves
    identically to it, except it returns MongozenCollection objects instead of
    pymongo.collection.Collection objects. Original documentation below:

    """
    __doc__ = __doc__ + Database.__init__.__doc__

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        cfg = self._get_db_cfg()
        cols_to_init = [
            col for col in cfg
            if col not in _mongozen_cfg()[CfgKey.BAD_COL_NAMES.value]
        ]
        for col_name in cols_to_init:
            setattr(self, col_name, self[col_name])

    @copy_ancestor_docstring
    def get_collection(self, name, codec_options=None, read_preference=None,
                       write_concern=None, read_concern=None):
        return MongozenCollection(
            self, name, False, codec_options, read_preference,
            write_concern, read_concern)

    @copy_ancestor_docstring
    def create_collection(self, name, codec_options=None,
                          read_preference=None, write_concern=None,
                          read_concern=None, **kwargs):
        if name in self.collection_names():
            raise CollectionInvalid("collection %s already exists" % name)
        return MongozenCollection(
            self, name, True, codec_options, read_preference, write_concern,
            read_concern, **kwargs)

    @copy_ancestor_docstring
    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError(
                "Database has no attribute %r. To access the %s"
                " collection, use database[%r]." % (name, name, name))
        return self.__getitem__(name)

    @copy_ancestor_docstring
    def __getitem__(self, name):
        return MongozenCollection(self, name)

    def _get_cfg_folder_path(self):
        return os.path.abspath(
            os.path.join(
                super().client._get_cfg_folder_path(),
                super().name
            )
        )

    def _get_cfg_file_path(self):
        return os.path.abspath(
            os.path.join(
                super().client._get_cfg_folder_path(),
                super().name + '.py'
            )
        )

    def _get_db_cfg(self):
        try:
            db_cfg_fname = self._get_cfg_file_path()
            with open(db_cfg_fname, 'r') as db_cfg_file:
                return yaml.load(db_cfg_file)
        except FileNotFoundError:
            return {}


class MongozenCollection(Collection, metaclass=InheritableDocstrings):
    """A wrapper class for pymongo.collection.Collection that behaves
    identically to it, except it is also aware of some unique attributes of
    mongozn MongoDB collections. Original documentation below:

    """
    __doc__ = __doc__ + Collection.__init__.__doc__

    def _get_cfg_file_path(self):
        folder = super().database._get_cfg_folder_path()
        file_name = super().name + '.py'
        return os.path.abspath(os.path.join(folder, file_name))

    def _get_collection_cfg(self):
        try:
            col_cfg_fname = self._get_cfg_file_path()
            with open(col_cfg_fname, 'rb') as col_cfg_file:
                return pickle.load(col_cfg_file)
        except (FileNotFoundError, TypeError):
            return {}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        cfg = self._get_collection_cfg()
        for key in cfg:
            setattr(self, key, cfg[key])
