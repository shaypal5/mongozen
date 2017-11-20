
import operator  # for sorting result dicts
from collections import OrderedDict

from prettytable import PrettyTable  # printing results in pretty ASCII tables

import mongozen

def get_array_of_field_values_matching(field_names, matchop, collection_obj):
    """Returns an array with all values the given field takes in all documents
    matched by the given Matchop in the given collection."""
    push_dict = {field_name : "$" + field_name for field_name in field_names}
    agg_pipe = [
        {'$match' : matchop},
        {'$group': {
            '_id': None,
            'values': {
                # '$push':  {'value': "$" + field_name}
                '$push': push_dict
            }
        }}
    ]
    return collection_obj.aggregate(agg_pipe).next()['values']


def count_keys_in(keys_list, collection_obj, verbose=False):
    """Counts the keys in a mongo collection."""
    if verbose:
        print('\nCounting keys in collection %s.' % collection_obj.full_name)
    if '_id' in keys_list:
        keys_list.remove('_id')
    group_params = {'_id': None}
    for key in keys_list:
        group_params[key] = {'$sum': {
            '$cond': {
                'if': {
                    '$ifNull': ["$" + key, False]}, 'then': 1, 'else': 0}
        }}
    cursor = collection_obj.aggregate([
        {'$group': group_params},
    ])
    res = cursor.next()
    del res['_id']
    return res


def key_value_counts(key, collection_obj, pre_operators=(), verbose=False):
    """Counting all possible values of a given key in a collection."""
    if verbose:
        print('\nCounting values of key {} in collection {}.'.format(
            key, collection_obj.full_name))
    cur = collection_obj.aggregate(list(pre_operators) + [
        {'$group': {'_id': '$' + key, 'total': {'$sum': 1}}},
        {'$sort': {'total': -1}},
    ])
    res = {doc['_id']: doc['total'] for doc in cur}
    return OrderedDict(sorted(
        res.items(), key=lambda item: item[1], reverse=True))


def get_distinct_vals_for_key(key, collection_obj, matchop=None):
    """Returns a list of the unique values of the given key in all documents
    matching the given matchop in the given collection."""
    if matchop is None:
        matchop = mongozen.matchop.all_matchop()
    res = collection_obj.aggregate([
        {'$match': matchop},
        {'$project': {'value': '$'+key}},
        {'$group': {'_id': None, 'distinct_vals': {'$addToSet': '$value'}}}
    ])
    return res.next()['distinct_vals']


def get_distinct_vals_for_nested_key(key, subkey, collection, matchop=None):
    """Returns a list of the unique values of the given subkey, nested in a
    document mapped to by the given key, in all documents matching the given
    matchop in the given collection."""
    if matchop is None:
        matchop = mongozen.matchop.all_matchop()
    return get_distinct_vals_for_key(key+'.'+subkey, collection, matchop)


# ==== visualizations ====

def get_pretty_frequency_table(type_name, frequency_dict, total_count):
    """Returns a pretty frequency table."""
    pretty_table_headers = [
        type_name, 'frequency', 'frequency %']
    result_table = PrettyTable(pretty_table_headers)
    sorted_items = sorted(frequency_dict.items(), key=operator.itemgetter(1))
    sorted_items.reverse()
    for item in sorted_items:
        print(item)
        total_key_count = item[1]
        occurrence_percent = round(
            total_key_count * 100.0 / total_count, 2) if total_count else 0.0
        prettytable_row = [item[0], total_key_count, occurrence_percent]
        result_table.add_row(prettytable_row)
    result_table.add_row(['-----', '--------', '------'])
    result_table.add_row(['TOTAL', total_count, '//////'])
    return result_table


def display_keys_counts_in(keys_list, collection_obj):
    """Displays the key count in a mongo collection."""
    res = count_keys_in(keys_list, collection_obj)
    total = collection_obj.count()
    table = get_pretty_frequency_table('Key', res, total)
    print(table)


def display_key_values_dist_in(key, collection_obj, pre_operators=()):
    """Displays the values distribution for a specific key in a mongo
    collection."""
    res = key_value_counts(key, collection_obj, pre_operators)
    total = sum(res.values())
    table = get_pretty_frequency_table('Value', res, total)
    print(table)


    # db.getCollection('connected_bts').aggregate([
    #     {'$match': {
    #         'createdAt': {'$gte': ISODate("2017-03-29T10:20:00.000Z"), '$lte': ISODate("2017-03-29T11:40:00.000Z")}
    #     }},
    #     {'$project': {
    #         '_id': 1,
    #         'hour':  {'$substr': [{'$hour': '$createdAt'}, 0, 3]},
    #         'minute': {'$substr': [{'$minute': '$createdAt'}, 0, 3]}
    #     }},
    #     {'$project': {
    #         '_id': 1,
    #         'hour':  1,
    #         'minute': 1,
    #         'insertion_time': {'$concat': ['$hour', ':', '$minute']}
    #     }},
    #     {'$group': {
    #         '_id': '$insertion_time',
    #         'insertion_time': {'$first': '$insertion_time'},
    #         'hour': {'$first': '$hour'},
    #         'minute': {'$first': '$minute'},
    #         'count': {'$sum': 1}
    #     }},
    #     {'$sort': {'hour': 1, 'minute': 1}}
    # ])
