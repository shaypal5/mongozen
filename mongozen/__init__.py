"""Enhance MongoDB for Python dynamic shells and scripts."""
# flake8: noqa  # prevents 'imported but unused' erros
# pylint: disable=C0413,C0411

# ignore IPython's ShimWarning, if IPython is installed
import sys
try:
    import warnings
    from IPython.utils.shimmodule import ShimWarning
    warnings.simplefilter("ignore", ShimWarning)
except ImportError:
    pass
try:
    del ShimWarning
except NameError:
    pass

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

# === module imports

from .core import (
    get_mongo_uri,
    get_reading_client,
    get_writing_client,
    get_db,
    get_collection,
    free_unused_clients
)
import mongozen.util
import mongozen.matchop

# optionally import mongozen.queries
try:
    import mongozen.queries
except ImportError:
    pass
    # warnings.warn("mongozen's queries sub-package was not imported. "
    #               "To use it reinstall with 'pip install mongozen[queries]'.")


from .core import _add_env_attr
_add_env_attr(sys.modules[__name__])

for name in [
        '_add_env_attr', 'sys', 'warnings', 'constants', 'core', 'name',
        'param_inference', 'shared', 'mongozen'
]:
    try:
        globals().pop(name)
    except KeyError:
        pass
try:
    del name  # pylint: disable=W0631
except NameError:
    pass
