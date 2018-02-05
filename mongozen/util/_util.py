"""Utility functions for the mongozen package."""

import subprocess  # for running terminal commands
import os  # for path handling
from shutil import rmtree  # for deleting directories
from ntpath import basename  # for OS-agnostic file-name extraction from path
import numbers  # to check for number types
import csv
from json import JSONDecodeError
import copy  # for deepcopy-ing dicts

import numpy as np
import bson
import utilitime
from strct.dicts import flatten_dict
from bson.json_util import (
    dumps,
    loads,
)

from ..shared import (
    CfgKey,
    _mongozen_cfg,
    _get_server_cfg,
    _get_mongo_cred
)
from ..util_constants import HOMEDIR

# ======= module-specific constants ======

NUM_BYTES_IN_MB = 1048576
MONGODB_DOC_SIZE_LIMIT_IN_MEGABYTES = 16
MONGODB_DOC_SIZE_LIMIT_IN_BYTES = \
    (MONGODB_DOC_SIZE_LIMIT_IN_MEGABYTES * NUM_BYTES_IN_MB) - 1024


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


def _strictify(some_object):
    if isinstance(some_object, dict):
        new_dict = copy.deepcopy(some_object)
        for key, value in some_object.items():
            new_dict[key] = _strictify(value)
        return new_dict
    if isinstance(some_object, (tuple, list)):
        new_list = []
        for item in some_object:
            new_list.append(_strictify(item))
        return new_list
    if isinstance(some_object, bson.objectid.ObjectId):
        return {"$oid": str(some_object)}
    return some_object


def strictify_query(query_dict):
    """Converts a query dict into the MongoDB defined strict mode JSON.

    See: https://docs.mongodb.com/manual/reference/mongodb-extended-json/

    Parameters
    ----------
    query_dict : dict
        A dict representing a MongoDB query.

    Returns
    -------
    dict
        A corresponding query dict with values converted to approriate strict
        mode JSON representations.
    """
    return _strictify(query_dict)


def parse_value_for_mongo(value):
    """Parse the given value, which might also be a structure like a dict or a
    list, into a format that can be written to mongodb.

    This method handles some common numpy data types and generic iterables. It
    also replaces dots in key strings with hyphens, as to prevent unintentional
    nesting.
    """
    newval = 'DefaultValue'
    if isinstance(value, np.ndarray):
        newval = value.tolist()
    elif isinstance(value, dict):
        newval = {}
        for k, v in value.items():
            key = parse_value_for_mongo(k)
            if isinstance(key, str) and '.' in key:
                key = key.replace('.', '-')
            newval[key] = parse_value_for_mongo(v)
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


def _exec_cmd(cmd):
    popen = subprocess.Popen(
        cmd.split(), stdout=subprocess.PIPE, universal_newlines=True)
    for stdout_line in iter(popen.stdout.readline, ""):
        print(stdout_line, end="")
    popen.stdout.close()
    return_code = popen.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)


MONGOTEMP_CMD = (
    '{cmd} --host="{host}" --username="{usr}" --password="{pwd}" --db="{db}"'
    ' {authdb} {readpreference} {verbosity}'
)


CMD_MSG = "{msg} for database={db}, server={server} and environment={env}"


def _mongo_cmd(cmd, msg, db_obj, mode, verbose=None, auto=None):
    """
    verbose : bool, default True
        If true, prints information to terminal and requests for confirmation.
    auto : bool, default False
        If true, does not ask for confirmation before running the command.
        Otherwise, confirmation is asked if verbose is set to True.
    """
    if verbose is None:
        verbose = True
    if auto is None:
        auto = False
    db_name = db_obj.name
    server_name = db_obj.client.server
    env_name = db_obj.client.env
    if verbose:
        print(CMD_MSG.format(msg=msg, db=db_name, server=server_name,
                             env=env_name))
        if not auto:
            response = input("Please confirm by typing 'y': ")
            if response != 'y':
                return
    server_cfg = _get_server_cfg(server_name, env_name, mode='reading')
    server_config = _mongozen_cfg()[CfgKey.ENVS.value][env_name][server_name]
    server_cred = _get_mongo_cred()[env_name][server_name][mode]
    try:
        repl_set = server_cfg['replicaSet']
    except KeyError:
        repl_set = ''
    host_str = repl_set + '/' + ','.join(server_config['host'])
    try:
        authdb = '--authenticationDatabase="{}"'.format(
            server_cfg['authSource'])
    except KeyError:
        authdb = ''
    try:
        readpref = '--readPreference="{}"'.format(server_cfg['readPreference'])
    except KeyError:
        readpref = ''
    verbosity_flag = '--quiet'
    if verbose:
        verbosity_flag = '-v'
    mongo_cmd = MONGOTEMP_CMD.format(
        cmd=cmd,
        host=host_str,
        usr=server_cred['username'],
        pwd=server_cred['password'],
        db=db_name,
        authdb=authdb,
        readpreference=readpref,
        verbosity=verbosity_flag
    )
    # process = subprocess.Popen(mongo_cmd.split(), stdout=subprocess.PIPE)
    try:
        # _exec_cmd(mongo_cmd)
        output = subprocess.check_output(
            mongo_cmd, stderr=subprocess.STDOUT, shell=True,
            universal_newlines=True)
    except subprocess.CalledProcessError as exc:
        print("Status : FAIL", exc.returncode, exc.output)
    else:
        if verbose:
            print("Output: \n{}\n".format(output))
    finally:
        if verbose:
            print('Command was ran.')
            print('Command:\n{}'.format(mongo_cmd))


def dump_collection(source_collection, output_dir_path, query=None,
                    verbose=True, auto=False):
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
    auto : bool, default False
        If true, does not ask for confirmation before running the command.
        Otherwise, confirmation is asked if verbose is set to True.
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


def restore_collection(target_db, input_file_path, verbose=True, auto=False):
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
    auto : bool, default False
        If true, does not ask for confirmation before running the command.
        Otherwise, confirmation is asked if verbose is set to True.
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


EXPORT_CMD = "mongoexport {fields} {query} {type}"


def export_collection(collection, output_fpath, fields=None, query=None,
                      ftype=None, escape_dollar=None, verbose=None, auto=None):
    """Exports the contents of the given collection to a file.

    Parameters
    ----------
    collection : mongozen.mongozen_objs.MongozenCollection
        The collection whose contents will be exported.
    output_fpath : str
        The full path to the desired output file.
    fields : list, optional
        Specifies fields to include in the export.
    query : dict, optional
        Provides a JSON document as a query that optionally limits the
        documents returned in the export. Specify JSON in strict format. Only
        single quotes can be used in this query document.
    ftype : string, optional
        Specifies the file type to export. Specify 'csv' for CSV format or
        'json' for JSON format. Defaults to 'json'.
    escape_dollar : bool, optional
        Whether to escape dollar sign in the query string (required in most
        common shells). Defaults to True.
    verbose: bool, optional
        Whether to print messages during the operation. Defaults to True.
    auto : bool, default False
        If true, does not ask for confirmation before running the command.
        Otherwise, confirmation is asked if verbose is set to True.
    """
    if ftype is None:
        ftype = 'csv'
    if escape_dollar is None:
        escape_dollar = True
    if '~' in output_fpath:
        output_fpath = os.path.expanduser(output_fpath)
    cmd = 'mongoexport --collection="{}" --out="{}"'.format(
        collection.name, output_fpath)
    msg = "Exporting collection {} to {}".format(collection.name, output_fpath)
    if fields:
        cmd += ' --fields="{}"'.format(','.join(fields))
        msg += ", limiting to fields {}".format(fields)
    if query:
        msg += ", with query {},".format(query)
        query = strictify_query(query)
        query = "{}".format(query)
        query = query.replace(" ", "")
        if escape_dollar:
            query = query.replace("$", "\$")
            assert isinstance(query, str)
        cmd += ' --query="{}"'.format(query)
    if ftype:
        cmd += ' --type="{}"'.format(ftype)
        msg += " with {} file type,".format(ftype)
    _mongo_cmd(cmd=cmd, msg=msg, db_obj=collection.database, mode='reading',
               verbose=verbose, auto=auto)


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


def dump_document_cursor_to_csv(doc_cursor, file_path, fieldnames=None,
                                missing_val=None, flatten=False):
    """Writes documents in a pymongo cursor into a csv file.

    Documents are dumped in the order they are returned from the cursor.

    Arguments
    ---------
    doc_cursor : pymongo.cursor.Cursor
        A pymongo document cursor returned by commands like find or aggregate.
    file_path : str
        The full path of the file into which cursor documents are dumped.
    fieldnames : sequence, optional
        The list of field names used as headers of the resulting csv file. If
        not given, the lexicographically-sorted field names of the first
        document in the cursor are used. Fields only found in subsequent
        documents will be ignored, while fields missing in subsequent documents
        will be filled with the given missing value string parameter.
    missing_val : str, optional
        The value used to fill missing fields in documents. Defaults to "NA".
    flatten : bool, optional
        If set to True, documents are flattened to dicts of depth one before
        writing them to file. Defaults to False.
    """
    def doc_trans(doc): return doc
    if missing_val is None:
        missing_val = "NA"
    if flatten:
        def doc_trans(doc): return flatten_dict(  # noqa: F811
            dict_obj=doc, separator='.', flatten_lists=True)
    first_doc = None
    if fieldnames is None:
        first_doc = doc_cursor.next()
        fieldnames = sorted(list(doc_trans(first_doc).keys()))
    with open(file_path, 'w+') as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames, restval=missing_val,
                                extrasaction='ignore', dialect='excel')
        writer.writeheader()
        if first_doc:
            writer.writerow(doc_trans(first_doc))
            print(doc_trans(first_doc))
        for document in doc_cursor:
            writer.writerow(doc_trans(document))


def dump_document_cursor_to_json(doc_cursor, file_path):
    """Writes documents in a pymongo cursor into a json file.

    Arguments
    ---------
    doc_cursor : pymongo.cursor.Cursor
        A pymongo document cursor returned by commands like find or aggregate.
    file_path : str
        The full path of the file into which cursor documents are dumped.
    """
    with open(file_path, 'w+') as dump_json:
        dump_json.write('[\n')
        dump_json.write(dumps(doc_cursor.next()))
        for doc in doc_cursor:
            dump_json.write(',\n')
            dump_json.write(dumps(doc))
        dump_json.write('\n]')


def load_document_iterator_from_json(file_path):
    """Creates a lazy iterator over documents from a json file.

    Arguments
    ---------
    file_path : str
        The full path of the file from which documents are read.
    """
    with open(file_path, 'r') as load_json:
        line = load_json.readline()
        while line:
            if line not in ('[\n', ']'):  # ignore start and end of array
                try:  # skip trailing , and one \n
                    yield loads(line[:-2])
                except JSONDecodeError:  # last line has no , so just \n
                    yield loads(line[:-1])
            line = load_json.readline()
        raise StopIteration
