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
import mongozen.matchop
import mongozen.queries
import mongozen.util


for name in [
        'sys', 'warnings', 'constants', 'name', 'mongozen'
]:
    try:
        globals().pop(name)
    except KeyError:
        pass
try:
    del name  # pylint: disable=W0631
except NameError:
    pass
