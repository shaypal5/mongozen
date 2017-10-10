
from ._util import (
    bson_doc_bytesize,
    document_is_not_too_big,
    document_is_strictly_not_too_big,
    parse_value_for_mongo,
    dateint_to_objectid,
    dateint_range_to_objectid_range,
    timestamp_to_objectid,
    timestamp_range_to_objectid_range,
    dump_collection,
    restore_collection,
    copy_collection
)
try:
    del _util
except NameError:
    pass
