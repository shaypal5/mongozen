mongozen
########
|PyPI-Status| |PyPI-Versions| |Build-Status| |Codecov| |LICENCE|

MongoDB utilities for Python.

.. code-block:: python

  from mongozen.util import export_collection
  export_collection(collection_obj, '~/dump.json')

.. contents::

.. section-numbering::


Installation
============

.. code-block:: bash

  pip install mongozen


Use
===

``mongozen`` is divided into three sub-packages, by functionality:

matchop
-------

Defines a ``Matchop`` class representing a pymongo matching operator. It extends the standard Python ``dict``, provides a smart representation of a MongoDB matching operator with well-defined and optimized ``&`` and ``|`` operators. For example:

.. code-block:: python

  from mongozen.matchop import Matchop
  match_dateint = Matchop({'dateInt': {'$gt': 20161203}})
  match_dateint_and_id = match_dateint & {'user_id': 12}
  print(match_dateint_and_id)

will output

.. code-block:: python

  {'user_id': 12, 'dateInt': {'$gt': 20161203}}

While

.. code-block:: python

  match_dateint = Matchop({'dateInt': {'$gt': 20161203}})
  match_dateint_updated = match_dateint & {'dateInt': {'$gt': 20161208}}
  print(match_dateint_updated)

will output

.. code-block:: python

  { {'dateInt': {'$gt': 20161208}} }


queries
-------

Contains some usefull queries.

util
----

Contains utility functions, like Python wrappers for MongoDB command-line tools.


Contributing
============

Package author and current maintainer is Shay Palachy (shay.palachy@gmail.com); You are more than welcome to approach him for help. Contributions are very welcomed.

Installing for development
----------------------------

Clone:

.. code-block:: bash

  git clone git@github.com:shaypal5/mongozen.git


Install in development mode:

.. code-block:: bash

  cd mongozen
  pip install -e .[test]
  # or, if you use pipenv
  pipenv install --dev


Running the tests
-----------------

To run the tests use:

.. code-block:: bash

  pytest
  # or, if you use pipenv
  pipenv run pytest


Adding documentation
--------------------

The project is documented using the `numpy docstring conventions`_, which were chosen as they are perhaps the most widely-spread conventions that are both supported by common tools such as Sphinx and result in human-readable docstrings. When documenting code you add to this project, follow `these conventions`_.

.. _`numpy docstring conventions`: https://github.com/numpy/numpy/blob/master/doc/HOWTO_DOCUMENT.rst.txt
.. _`these conventions`: https://github.com/numpy/numpy/blob/master/doc/HOWTO_DOCUMENT.rst.txt

Additionally, if you update this ``README.rst`` file,  use ``python setup.py checkdocs`` (or ``pipenv run`` the same command) to validate it compiles.


Credits
=======

Created by Shay Palachy (shay.palachy@gmail.com).


.. |PyPI-Status| image:: https://img.shields.io/pypi/v/mongozen.svg
  :target: https://pypi.python.org/pypi/mongozen

.. |PyPI-Versions| image:: https://img.shields.io/pypi/pyversions/mongozen.svg
   :target: https://pypi.python.org/pypi/mongozen

.. |Build-Status| image:: https://travis-ci.org/shaypal5/mongozen.svg?branch=master
  :target: https://travis-ci.org/shaypal5/mongozen

.. |LICENCE| image:: https://img.shields.io/github/license/shaypal5/mongozen.svg
  :target: https://github.com/shaypal5/mongozen/blob/master/LICENSE

.. |Codecov| image:: https://codecov.io/github/shaypal5/mongozen/coverage.svg?branch=master
   :target: https://codecov.io/github/shaypal5/mongozen?branch=master
