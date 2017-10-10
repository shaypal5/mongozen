"""Utility functions for the mongozen package."""

import subprocess  # for running terminal commands
import os  # for path handling
from shutil import rmtree  # for deleting directories
from ntpath import basename  # for OS-agnostic file-name extraction from path
import numbers  # to check for number types

import numpy as np
import bson
import utilitime

from ..core import get_collection
from ..shared import (
    CfgKey,
    _mongozen_cfg,
    _get_mongo_cred
)
from ..util_constants import HOMEDIR


# ======= module-specific constants ======

NUM_BYTES_IN_MB = 1048576
MONGODB_DOC_SIZE_LIMIT_IN_MEGABYTES = 16
MONGODB_DOC_SIZE_LIMIT_IN_BYTES = \
    (MONGODB_DOC_SIZE_LIMIT_IN_MEGABYTES * NUM_BYTES_IN_MB) - 1024
MONGODB_DOC_SAFE_SIZE_LIMIT_IN_BYTES = \
    MONGODB_DOC_SIZE_LIMIT_IN_BYTES - (50*1024)


# ======= utility methods ======

def bson_doc_bytesize(document):
    """Returns the size, in bytes, of the given document when encoded as bson.

    Arguments
    ---------
    document : dict
        A python dict object to be encoded as a BSON document.

    Returns
    -------
    int
        The size, in bytes, of the given document when encoded as bson.
    """
    return len(bson.BSON.encode(document))


def document_is_not_too_big(document):
    """Returns True if the given documents can be encoded to BSON and the
    result is under the MongoDB size limit of 16MB.

    Arguments
    ---------
    document : dict
        A python dict object to be encoded as a BSON document.

    Returns
    -------
    boolean
        True if the given documents can be encoded to BSON and the  result is
        under the MongoDB size limit of 16MB.
    """
    try:
        return bson_doc_bytesize(document) < MONGODB_DOC_SIZE_LIMIT_IN_BYTES
    except bson.errors.InvalidDocument:
        return False


def document_is_strictly_not_too_big(document):
    """Returns True if the given documents can be encoded to BSON and the
    result is significantly under the MongoDB size limit of 16MB.

    Arguments
    ---------
    document : dict
        A python dict object to be encoded as a BSON document.

    Returns
    -------
    boolean
        True if the given documents can be encoded to BSON and the result is
        under the MongoDB size limit of 16MB.
    """
    try:
        return bson_doc_bytesize(document) < MONGODB_DOC_SIZE_LIMIT_IN_BYTES
    except bson.errors.InvalidDocument:
        return False


def parse_value_for_mongo(value):
    """Parse the given value, which might also be a structure like a dict or a
    list, into a format that can be written to mongodb."""
    newval = 'DefaultValue'
    if isinstance(value, np.ndarray):
        newval = value.tolist()
    elif isinstance(value, dict):
        newval = {}
        for k, v in value.items():
            newval[parse_value_for_mongo(k)] = parse_value_for_mongo(v)
    elif isinstance(value, str):
        if '.' in value:
            newval = value.replace('.', '-')
        else:
            newval = value
    elif hasattr(value, '__iter__') and not isinstance(value, str):
        newval = []
        for element in value:
            newval.append(parse_value_for_mongo(element))
    elif isinstance(value, numbers.Number):
        if isinstance(value, numbers.Rational):
            newval = int(value)
        elif isinstance(value, numbers.Real):
            newval = float(value)
        elif isinstance(value, numbers.Complex):
            newval = complex(value)
        newval = int(value)
    else:
        newval = str(value)
    return newval


def dateint_to_objectid(dateint):
    """Converts the given dateint into a corresponding dummy MongoDB ObjectId.

    Arguments
    ---------
    dateint : int
        An integer object decipting a specific calendaric day; e.g. 20161225.

    Returns
    -------
    bson.objectid.ObjectId
        A dummy MongoDB ObjectId corresponding to the input dateint.
    """
    datetime_obj = utilitime.dateint.dateint_to_datetime(dateint)
    return bson.objectid.ObjectId.from_datetime(datetime_obj)


def dateint_range_to_objectid_range(from_dateint, to_dateint):
    """Converts the given dateint range into a corresponding ObjectId range.

    Arguments
    ---------
    from_dateint : int
        An integer object decipting a specific calendaric day; e.g. 20161225.
    to_dateint : int
        An integer object decipting a specific calendaric day; e.g. 20161225.

    Returns
    -------
    from_id, to_id : bson.objectid.ObjectId
        A pair of corresponding ObjectId objects, for which {'_id': {
            '$gte': from_id, '$lt': to_id
        }} corresponds to querying for obejcts whose insertion time is in the
        range from_dateint <= X <= to_dateint.
    """
    end_dateint = utilitime.dateint.shift_dateint(to_dateint, 1)
    return dateint_to_objectid(from_dateint), dateint_to_objectid(end_dateint)


def timestamp_to_objectid(timestamp):
    """Converts the given dateint into a corresponding dummy MongoDB ObjectId.

    Arguments
    ---------
    timestamp : int
        Seconds since the epoch.

    Returns
    -------
    bson.objectid.ObjectId
        A dummy MongoDB ObjectId corresponding to the input dateint.
    """
    datetime_obj = utilitime.timestamp.timestamp_to_datetime(timestamp)
    return bson.objectid.ObjectId.from_datetime(datetime_obj)


def timestamp_range_to_objectid_range(from_timestamp, to_timestgamp):
    """Converts the given timestamp range into a corresponding ObjectId range.

    Arguments
    ---------
    from_timestamp : int
        Seconds since the epoch.
    to_timestgamp : int
        Seconds since the epoch.

    Returns
    -------
    from_id, to_id : bson.objectid.ObjectId
        A pair of corresponding ObjectId objects, for which {'_id': {
            '$gte': from_id, '$lt': to_id
        }} corresponds to querying for obejcts whose insertion time is in the
        range from_timestamp <= X <= to_timestgamp.
    """
    return (
        timestamp_to_objectid(from_timestamp),
        timestamp_to_objectid(to_timestgamp)
    )

# ==== Collection dump and restore

MONGOTEMP_CMD = "{cmd} --host {{host}} --port {{port}} --username {{usr}} " \
                "--password {{pwd}} --authenticationDatabase admin " \
                "--db {{db}} {collection} " \
                " {dir_flag} {{dir_path}} {{verbosity}}"

CMD_MSG = "{action} {collection} in database={{db}}, server={{server}} and "\
        "environment={{env}}, {direction} directory {{dir}}"

def _mongo_cmd(command, msg, db_obj, dir_path, mode, verbose):
    db_name = db_obj.name
    server_name = db_obj.client.server
    env_name = db_obj.client.env
    if verbose:
        print(msg.format(db=db_name, server=server_name, env=env_name,
                         dir=dir_path))
        response = input("Please confirm by typing 'y': ")
        if response != 'y':
            return
    server_config = _mongozen_cfg()[CfgKey.ENVS.value][env_name][server_name]
    server_cred = _get_mongo_cred()[env_name][server_name][mode]
    host_str = server_config['host'][0]
    for host_name in server_config['host'][1:]:
        host_str = host_str + "," + host_name
    verbosity_flag = '--quiet'
    if verbose:
        verbosity_flag = '-vvvvv'
    mongo_cmd = command.format(
        host=host_str,
        port=server_config['port'],
        usr=server_cred['username'],
        pwd=server_cred['password'],
        db=db_name,
        dir_path=dir_path,
        verbosity=verbosity_flag
    )
    process = subprocess.Popen(mongo_cmd.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    if verbose:
        print('Command was ran.')
        print('Output: {}'.format(output))
        print('Error: {}'.format(error))


def dump_collection(source_collection, output_dir_path, verbose=True):
    """Dumps the contents of the given source collection to the directory in
    the given path.

    Arguments
    ---------
    source_collection : mongozen.mongozen_objs.MongozenCollection
        The collection whose contents will be dump.
    output_dir_path : str
        The full path to the desired output directory.
    verbose: bool
        Whether to print messages during the operation. Defaults to True.
    """
    if verbose:
        doc_count = source_collection.count()
        if doc_count > 100000:
            print("The collection is very large ({} documents)!".format(
                doc_count))
            response = input("Please confirm by typing 'y': ")
            if response != 'y':
                return
    partially_formatted_cmd = MONGOTEMP_CMD.format(
        cmd='mongodump',
        collection='--collection {}'.format(source_collection.name),
        dir_flag='--out'
    )
    partially_formatted_msg = CMD_MSG.format(
        action='Dumping', collection=source_collection.name, direction='to')
    _mongo_cmd(
        partially_formatted_cmd, partially_formatted_msg,
        source_collection.database, output_dir_path, 'reading', verbose)


def restore_collection(target_db, input_file_path, verbose=True):
    """Dumps the contents of the given source collection to the directory in
    the given path.

    Arguments
    ---------
    collection_name : str
        The namecollection of the collection to restore.
    input_dir_path : str
        The full path to the desired input directory.
    verbose: bool
        Whether to print messages during the operation. Defaults to True.
    """
    partially_formatted_cmd = MONGOTEMP_CMD.format(
        cmd='mongorestore',
        collection='',
        dir_flag='--dir'
    )
    collection_name = basename(input_file_path)
    partially_formatted_msg = CMD_MSG.format(
        action='Resgtoring', collection=collection_name,
        direction='from')
    _mongo_cmd(
        partially_formatted_cmd, partially_formatted_msg, target_db,
        input_file_path, 'writing', verbose)


DUMPS_DIR_NAME = '.mongozen_temp_dump'
DUMPS_DIR_PATH = os.path.join(HOMEDIR, DUMPS_DIR_NAME)

def copy_collection(source_collection, target_db, temp_dir_path=None,
                    verbose=True):
    """Copies the contents of the given source collection to that of the
    target one.

    Arguments
    ---------
    source_collection : mongozen.mongozen_objs.MongozenCollection
        The collection whose contents will be copied.
    target_collection : mongozen.mongozen_objs.MongozenCollection
        The collection to which all contents of the source collection will be
        copied.
    temp_dir_path: str, optional
        A path to the directory to use for temporary storage of collection
        contents while copying.
    verbose: bool, optional
        Whether to print messages during the operation. Defaults to True.
    """
    if verbose:
        print("Copying collection {}.{}.{}.{} to {}.{}.{}".format(
            source_collection.database.client.env,
            source_collection.database.client.server,
            source_collection.database.name,
            source_collection.name,
            target_db.client.env,
            target_db.client.server,
            target_db.name))
    temp_dir_name = "{}/{}".format(
        source_collection.database.client.env,
        source_collection.database.client.server
    )
    if temp_dir_path is None:
        dumps_dir_path = DUMPS_DIR_PATH
    temp_dump_dir_path = os.path.join(dumps_dir_path, temp_dir_name)
    os.mkdirs(temp_dump_dir_path, exist_ok=True)
    dump_collection(source_collection, temp_dump_dir_path, verbose)
    if verbose:
        print("Done dumping the source collection. Now copying to target...")
    temp_dump_subdir_path = os.path.join(
        temp_dump_dir_path, source_collection.database.name)
    temp_dump_file_path = os.path.join(
        temp_dump_subdir_path, source_collection.name+'.bson')
    restore_collection(target_db, temp_dump_file_path, verbose)
    if verbose:
        print("Done copying. Deleting dump from drive...")
    rmtree(temp_dump_dir_path)
    if verbose:
        print("Collection copy operation done.")
