"""Queries exploring distibution of data by timeframes."""

from datetime import datetime

from utilitime.datetime import epoch_datetime


def groupop_on_date_obj_by_timeframe(timefield_name, timeframe_len_in_sec):
    """Returns a Mongo group operator that group documents on date object time
    field by the timeframe the belong to."""
    timefield = '$' + timefield_name
    return {'$group': {
        '_id': {
            '$subtract': [
                {'$subtract': [timefield, epoch_datetime()]},
                {'$mod': [
                    {'$subtract': [timefield, epoch_datetime()]},
                    1000 * timeframe_len_in_sec
                ]}
            ]
        },
        'count': {'$sum': 1}
    }}


def group_by_timeframe_with_date(collection_obj, timeframe_len_in_sec,
                                 timefield_name, match_op=None):
    """Group entries by timeframe of the given length in seconds and return
    a cursor on the count per timeframe."""
    if match_op is None:
        match_op = {}
    return collection_obj.aggregate([
        {'$match': match_op},
        groupop_on_date_obj_by_timeframe(timefield_name, timeframe_len_in_sec),
    ])


def timeframe_size_dist(
        collection_obj, timefield, timefield_type, timeframe_len,
        matchop=None, second_group=None):
    """Computes timeframe size distribution for a given frame length.

    Timeframe size is measured in collection entries in that frame.

    Arguments
    ---------
    collection_obj : pymongo.collection.Collection
        The collection for which to compute the distribution.
    timefield : str
        The name of the time field for the given collection.
    timefield_type : object
        int or datetime.datetime. If int is given the time field is assumed to
        represent a UTC timestamp.
    timeframe_len : int
        The length of the timeframe, in seconds.
    matchop : dict, optional
        A dictionary representing a matchop to filter objects on which the
        distribution is computed. If not given, the distribution is computed
        over all documents in the given collection.
    second_group : str, optional
        A field name to do a second group on. This will yield a timeframe size
        distribution per group of items in this second froup operation. If not
        give, a single distribution is computed over all matching documents.

    Returns
    -------
    result : pymongo.cursor.Cursor
        A cursor over the resulting distributions, given in dicts of the
        following form:
            group_id: The id of the group the distribution belongs to.
            n: The number of timeframes in the distribution.
            Sx_i: The number of entries in the distribution.
            Sx_i: The sum of squraes of frame sizes in the distribution.
            mean: The mean of the distribution.
            for_all: True if this distribution was computed in a batch
                where all documents were included, False otherwise.
            timeframe_length: The length in seconds of a frame.
            collection_name: The name of the collection.
            variance: The variance of the distribution.
    """
    timefield_name = '$' + timefield
    collection_name = collection_obj.name
    if matchop is None:
        matchop = {}
    if second_group is not None:
        second_group = '$' + second_group

    if timefield_type is int:
        timestamp_exp = timefield_name
    elif timefield_type is datetime:
        timestamp_exp = {'$subtract': [
            timefield_name, epoch_datetime()]}
    else:
        raise TypeError('timeframe_size_dist() should recieve ' +\
            'timefield_type parameter either an int or datetime.datetime!')

    first_project_stage = {'$project': {
        'timeframe': {
            '$subtract': [
                timestamp_exp,
                {'$mod': [
                    timestamp_exp,
                    timeframe_len
                ]}
            ]
        }
    }}

    first_group_stage = {'$group': {
        '_id': {'timeframe': '$timeframe'}, 'x_i': {'$sum': 1}
    }}

    if second_group:
        first_group_stage['$group']['_id']['second_group'] = second_group

    second_project_stage = {'$project': {
        '_id': 1, 'x_i':1, 'x_i_sqr': {'$multiply': ['$x_i', '$x_i']}
    }}

    if second_group:
        second_project_stage['$project'][second_group] = 1

    second_group_stage = {'$group': {
        '_id': second_group,
        'mean': {'$avg': '$x_i'},
        'Sx_i': {'$sum': '$x_i'},
        'Sx_i_sqr': {'$sum': '$x_i_sqr'},
        'n': {'$sum': 1}
    }}

    third_project_stage = {'$project': {
        '_id': 0,
        'n': 1, 'Sx_i': 1, 'Sx_i_sqr': 1, 'mean': 1,
        'for_all': {'$literal': second_group is None},
        'timeframe_length': {'$literal': timeframe_len},
        'collection_name': {'$literal': collection_name},
        'type': {'$literal': 'timeframe_size_dist'},
        'variance': {'$cond': [
            {'$gte': ['$n', 2]},
            {'$divide': [
                {'$subtract': [
                    '$Sx_i_sqr',
                    {'$divide':[{'$multiply':['$Sx_i', '$Sx_i']}, '$n']}
                ]},
                {'$subtract': ['$n', 1]}
            ]},
            0
        ]},
    }}

    if second_group:
        agg_pipline = [
            {'$match': matchop},
            first_project_stage,
            first_group_stage,
            second_project_stage,
            second_group_stage,
            third_project_stage,
        ]
    else:
        agg_pipline = [
            {'$match': matchop},
            first_project_stage,
            first_group_stage,
            second_project_stage,
            third_project_stage,
        ]

    return collection_obj.aggregate(pipeline=agg_pipline, allowDiskUse=True)
