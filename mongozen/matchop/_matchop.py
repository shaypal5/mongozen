"""Defines a Matchop class representing a pymongo matching operator."""

import re
import warnings
from math import inf

from comath.segment import LineSegment


__LOGICAL_OPS = set(('$or', '$and', '$not', '$nor'))
__COMPAR_OPS = set(('$eq', '$gt', '$gte', '$lt', '$lte', '$ne', '$in', '$nin'))


def _contains_logical_op(matchop):
    return len(matchop.keys() & __LOGICAL_OPS) > 0


def _val_in_inter(val, intersection):
    if intersection:
        return val in intersection
    return True


def _build_op_kinds(compar_ops, compar_vals):
    equ = []
    gts = {}
    lts = {}
    nins = set()
    ins = []
    for op, val in zip(compar_ops, compar_vals):
        if op == '$eq':
            equ.append(val)
        if op in ['$gt', '$gte']:
            try:
                gts[val].add(op)
            except KeyError:
                gts[val] = set([op])
        if op in ['$lt', '$lte']:
            try:
                lts[val].add(op)
            except KeyError:
                lts[val] = set([op])
        if op == '$ne':
            nins.add(val)
        if op == '$nin':
            nins = nins.union(val)
        if op == '$in':
            ins.append(val)
    return equ, gts, lts, nins, ins


def _resolve_compar_ops(compar_ops, compar_vals):

    # we don't mess with nested stuff...
    if any([isinstance(val, dict) for val in compar_vals]):
        raise ValueError("Complex matchops! Doing a simple and!")

    # we sort the operators into kinds...
    equ, gts, lts, nins, ins = _build_op_kinds(compar_ops, compar_vals)

    # resolving stuff with $eq in them is easy... it overrides everything.
    if len(set(equ)) > 1:
        warnings.warn("More than one $eq for the same field in Matchops joined"
                      " by &, with different values. "
                      "Resulting Matchop matches the empty set.")
        raise ValueError("More than one $eq! Doing a simple and!")

    res = {}
    # now we generate the joined query using only implicit ands
    if len(gts) > 0:
        max_gt = max(gts.keys())
        if len(gts[max_gt]) == 1:
            gt_op = gts[max_gt].pop()
        else:
            gt_op = '$gt'
        res[gt_op] = max_gt
    else:
        gt_op = '$gte'
        max_gt = -inf
    if len(lts) > 0:
        min_lt = min(lts)
        if len(lts[min_lt]) == 1:
            lt_op = lts[min_lt].pop()
        else:
            lt_op = '$lt'
        res[lt_op] = min_lt
    else:
        lt_op = '$lte'
        max_gt = inf
    segment = LineSegment(max_gt, max_gt, gt_op == '$gte', lt_op == '$lte')
    if len(nins) > 0:
        res['$nin'] = list(nins)
    ins_intersection = None
    if len(ins) > 0:
        ins_intersection = set(ins[0])
        for i in range(1, len(ins)):
            ins_intersection = ins_intersection & ins[i]
        res['$in'] = ins_intersection
    if ins_intersection:
        if len(ins_intersection.intersection(nins)) > 1:
            warnings.warn(
                "Matchop and operation resulted in a Matchop demanding")
        ins_intersection = segment & ins_intersection
    if len(set(equ)) == 1:
        res['$eq'] = equ[0]
        if equ[0] in segment and _val_in_inter(equ[0], ins_intersection):
            return {'$eq': equ[0]}
        warnings.warn(
            "$eq value not in resulting range of Matchops joined by &. "
            "Resulting Matchop matches the empty set.")
    return res


def _optimized_and(first, second, intersection):
    new_op = {}
    non_compar_seen = set()
    for key in intersection:
        new_expr = {}
        compar_ops = []
        compar_vals = []
        for matchop in [first, second]:
            if isinstance(matchop[key], dict):
                for subkey in matchop[key]:
                    if subkey in __COMPAR_OPS:
                        compar_ops.append(subkey)
                        compar_vals.append(matchop[key][subkey])
                    else:
                        if subkey in non_compar_seen:
                            raise ValueError(
                                "Non-comparison operators in intersection. "
                                "Performing a simple and.")
                        non_compar_seen.add(subkey)
                        new_expr[subkey] = matchop[key][subkey]
            else:
                compar_ops.append('$eq')
                compar_vals.append(matchop[key])
        resolved_comp = _resolve_compar_ops(compar_ops, compar_vals)
        for subkey in resolved_comp:
            new_expr[subkey] = resolved_comp[subkey]
        new_op[key] = new_expr
    for matchop in [first, second]:
        for key in matchop.keys() - intersection:
            new_op[key] = matchop[key]
    return new_op


class Matchop(dict):
    """Defines a matching operator for mongodb operations."""

    def __and__(self, other):
        if not isinstance(other, dict):
            raise TypeError("unsupported operand type(s) for +: '{}' and '{}'"
                            .format(self.__class__, type(other)))
        if _contains_logical_op(self) or _contains_logical_op(other):
            return Matchop({'$and': [self, other]})
        intersection = self.keys() & other.keys()
        if len(intersection) == 0:
            return Matchop({**self, **other})  # flake8: noqa:
        try:
            return _optimized_and(self, other, intersection)
        except ValueError:
            return Matchop({'$and': [self, other]})

    def __or__(self, other):
        if isinstance(other, dict):
            return Matchop({'$or': [self, other]})
        else:
            raise TypeError("unsupported operand type(s) for +: '{}' and '{}'"
                            .format(self.__class__, type(other)))


def all_matchop():
    """Return a Matchop that matches all documents."""
    return Matchop({})


def regex_matchop(field_name, pattern_obj):
    """Returns a Matchop matching documents where the field with the given
    name matches the regex in the given Pattern object."""
    return Matchop({
        field_name: {'$regex': pattern_obj}
    })


def substring_matchop(field_name, substring, ignore_case=True):
    """Returns a Matchop matching documents where the field with the given
    name contains the given string."""
    if ignore_case:
        pattern_obj = re.compile(substring, re.I)
    else:
        pattern_obj = re.compile(substring)
    return regex_matchop(field_name, pattern_obj)
