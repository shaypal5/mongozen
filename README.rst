mongozen
########
|PyPI-Status| |PyPI-Versions| |Build-Status| |Codecov| |LICENCE|

Enhance MongoDB for Python dynamic shells and scripts.

.. code-block:: python

  import mongozen
  users = mongozen.get_collection('users')

.. contents::

.. section-numbering::


Installation
============

.. code-block:: bash

  pip install mongozen


Setting up mongozen
===================

mongozen uses a couple of simple conventions to handle credentials and refer to MongoDB servers:

1. Many companys deploy corresponding MongoDB servers on several environments, using a largely similar (though not identical) architecture. Common environments include production, staging and performance.
2. On each of these environment a set of MongoDB servers are deployed.

To use mongozen you will need to set up a configuration file, detailing connection parameters, and a credentials file for mongozen to use.

The motivation behind this division is that the same group of developers (in a certain company, working on a certain project, etc.) might share a configuration file to both share connection details to a server (or a group of servers) and to enforce best practices (pool sizes, timeouts and read preferences), while credentials should be maintained per user.


Configuration file
------------------

To configure ``mongozen``, create a ``.mongozen/mongozen_cfg.yml`` file in your **home folder**, populating it with the desired parameters and connection details. Here is an example (explanation follows):

.. code-block:: yaml

  envs:
    production:
      mongozen_env_params:
        maxPoolSize: 10
      transactionl_server:
        host:
          - 'ourmongo.bestcompany.com'
        port: 28177
        mongozen_server_params:
          connectTimeoutMS: 2500
  global_params:
    readPreference: 'secondary'
    maxIdleTimeMS: 60000

* The ``envs`` parameter is the only mandatory parameter, and it is used to define which environments and servers mongozen "knows" about, and to define connection parameters for each of them. The only mandatory parameters are host (for host name) and port.

  * Each environment can contain many server mappings.
  * Each server mapping should include a ``host`` and ``port`` parameters, where ``host`` is a list of hostnames (which can be more than one in the case of a sharded cluster, for example) and ``port`` is an integer.

* The ``global_params`` parameter can be used to detail parameters used for all connections (they will be passed to all ``pymongo.MongoClient`` constructor calls, unless overriden in the following ways).

* The ``mongozen_env_params`` can be used in the same way inside a specific environment context, determining parameters used to initialize all clients connecting to that environment (also overiding corresponding values given at the global level).

* The ``mongo_server_params`` works the same way for a specific server, also overiding global and environment level values.

Any optional parameter of the ``pymongo.MongoClient`` constructor (of a type supported by the ``yaml`` format) can be given in the above three ways. Naturally, client objects are initialized without explictly stating the value of any optional optional parameter not given in the configuration file, thus delegating decisions regarding the appropriate default value to ``pymongo``.

You can print the current configuration your ``mongozen`` installation uses to terminal by running the following shell command:

.. code-block:: bash

  mongozen util printcfg


Credentials file
----------------

You must set up a credentials file for mongozen to use. Create a ``.mongozen/mongozen_credentials.yml`` file in your home folder, populating it with your MongoDB credentials, using an identical structure to the inner structure of the ``envs`` configuration parameters:

.. code-block:: python

  environment_name:
    server_name:
      reading:
        username: reading_username
        password: password1
      writing:
        username: writing_username
        password: password2

You can extend this to include any number of environments and servers.


Configuring advanced features
------------------------------

The following parameters control some of the more advanced features of ``mongozen``, detailed in the `Enhanced Python-based MongoDB shell`_ section. These too should be added to ``~/.mongozen/mongozen_cfg.yml``.

* Use ``infer_parameters`` to turn the parameter inference feature on.
* Use ``default_env`` to set which environment is used when the environment parameter is not supplied, and hints cannot be used (for example when directly getting a client object). Used only if ``infer_parameters`` is set to true.
* Use ``default_server`` to similarly set which server is used when the server parameter is not supplied and hints cannot be used. Used only if ``infer_parameters`` is set to true.
* Use ``env_priority`` and ``server_priority`` to give ordered lists detailing priorities when solving ambiguity for identically-named collections or databases present in several different environments and/or servers.
* Use ``bad_db_terms`` to detail terms that, if appear in a db name, will prevent it from being inferred as a missing parameter. Common examples are terms such as ``admin``, ``config``, ``mirror``, etc.
* Use ``bad_collection_names`` to prevent certain collections from being added as attributes of database objects (e.g. ``$cmd``).

For example:

.. code-block:: python

  env_priority:
    - staging
    - local
    - production
  server_priority:
    production:
      - user_data
      - system_data
    staging:
      - system_data
      - user_data
  infer_parameters: True


Basic Use
=========

Getting pymongo objects
-----------------------

To get a ``pymongo`` MongoClient object with reading permissions connected to a server use:

.. code-block:: python

  prod_tr = mongozen.get_reading_client(server_name='system', env_name='production')

``get_writing_clint`` works similarly to provide writing permissions.

To get a ``pymongo`` Database object use:

.. code-block:: python

  user_data = mongozen.get_db(db_name='user_data', server_name='system', env_name='staging')

Use ``mode='writing'`` to get a db connected to a writing client; otherwise, the default mode is reading.

Finally, to get a ``pymongo`` Collection object use:

.. code-block:: python

  users = mongozen.get_collection(collection_name='users', db_name='user_data', server_name='system', env_name='production')

You can of course omit keyword argument names for brevity:

.. code-block:: python

  users = mongozen.get_collection('users', 'user_data', 'system', 'production')

Like with DB objects, reading access is the default (again, use ``mode='writing'`` for writing permissions).


Smart pymongo objects
---------------------------

Environment and server attributes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To make things a little easier, ``mongozen`` also holds an attribute for each environment which can be used to access the servers of that environment using the following syntax:

.. code-block:: python

  sys_prod = mongozen.production.system


DB and collection attributes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

mongozen also enhances the client, database and collection pymongo objects it returns. Client objects have all the databases of the server they are connected to as **attributes**, and the same goes for database objects and the collections they contain. For example:

.. code-block:: python

  sys_prod = mongozen.production.system
  users = sys_prod.user_data.users
  contacts = mongozen.production.system.user_data.contacts

This is unlike the default pymongo objects, where the same syntax can be used but rather accesses an object property. Having these as attributes (or descriptors, in some cases) rather than properties means they pop up in suggestions and auto-completions when using a dynamic Python REPL.


Collection field types
~~~~~~~~~~~~~~~~~~~~~~

Additionally, each collection object returned by ``mongozen`` has an attribute named ``fields`` which is a dictionary mapping field names to their types. This again enables some collection-agnostic code, such as:

.. code-block:: python

  def get_docs_since_timestamp(collection, timestamp):
      if collection.fields['start'] == int:
        matchop = {'start': {'$gte': timestamp}}
      elif collection.fields['start'] == datetime.datetime:
        matchop = {'start': {'$gte': timestamp_to_datetime(timestamp)}}
      cursor = collection.find(filter=matchop)
      return cursor

This attribute files need to be built (or rebuilt, on changes) using:

.. code-block:: bash

  mongozen util rebuildattr


Matchop
-------

The utility class ``Matchop``, which extends the standard Python ``dict``, provides a smart representation of a MongoDB matching operator with well-defined and optimized ``&`` and ``|`` operators. For example:

.. code-block:: python

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


Usefull Queries
---------------

Additionally, mongozen contains quite a few useful MongoDB queries. They can be found in ``mongozen.queries``, divided into sub-modules by subject (such as common and time queries).


Enhanced Python-based MongoDB Shell
===================================

mongozen can be configured to enhance the use of a Python REPL (for example IPython_ or the wonderfull ptpython_, especially when it is wrapped around IPython by running ``ptiptyhon``) as a powerfull MongoDB shell.

.. _IPython: https://ipython.org/
.. _ptpython: https://github.com/jonathanslenders/ptpython

All features geared towards this use of ``mongozen`` are optional, so as to leave the default behavior of ``mongozen`` appropriate for a component meant to be used in other Python scripts.


Intelligent collection inference
--------------------------------

mongozen can be configured to intelligently infer parameters for the ``get_db`` and the ``get_collection`` methods. To enable parameter inference add the following line to your ``~/.mongozen/mongozen_cfg.yml`` file:

.. code-block:: python

  infer_parameters: True

Now, with the parameter inference, you can ommit database and server names when "getting" a colection or a db object. ``mongozen`` will intelligently infer the missing parameters; ambiguity for identically-named collections present in several different environments and/or servers is solved using the a config parameter named ``env_priority``:

.. code-block:: python

  env_priority:
    - production
    - staging
    - performance

The same can be done with per-environment server priority using:

.. code-block:: python

  server_priority:
    production:
      - system
      - data_dumps

For example, to get the Pymongo Collection object corresponding to the *users* collection on the *ystem production* server, simply use:

.. code-block:: python

  import mongozen
  users = mongozen.get_collection('users')

You can provide explicit hints using either `db_name` or `server_name`, but if configured correctly the ``get_collection`` method should intelligently infer these without needing any hints.

See the above `Configuration`_ section for further details on how to configure ``mongozen``.

``mongozen`` needs mapping files that enable this feature. To use the feature, you will have to build them using:

.. code-block:: bash

  mongozen util rebuildmaps

 If new databases and collections are added these maps become outdated, and might infere parameters incorrectly. If you encounter this problem, run the command again.

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
  pip install -e .


Running the tests
-----------------

To run the tests use:

.. code-block:: bash

  pip install pytest pytest-cov coverage
  cd mongozen
  pytest


Adding documentation
--------------------

The project is documented using the `numpy docstring conventions`_, which were chosen as they are perhaps the most widely-spread conventions that are both supported by common tools such as Sphinx and result in human-readable docstrings. When documenting code you add to this project, follow `these conventions`_.

.. _`numpy docstring conventions`: https://github.com/numpy/numpy/blob/master/doc/HOWTO_DOCUMENT.rst.txt
.. _`these conventions`: https://github.com/numpy/numpy/blob/master/doc/HOWTO_DOCUMENT.rst.txt


Credits
=======

Created by Shay Palachy (shay.palachy@gmail.com).


.. |PyPI-Status| image:: https://img.shields.io/pypi/v/mongozen.svg
  :target: https://pypi.python.org/pypi/mongozen

.. |PyPI-Versions| image:: https://img.shields.io/pypi/pyversions/mongozen.svg
   :target: https://pypi.python.org/pypi/mongozen

.. |Build-Status| image:: https://travis-ci.org/shaypal5/mongozen.svg?branch=master
  :target: https://travis-ci.org/shaypal5/mongozen

.. |LICENCE| image:: https://img.shields.io/pypi/l/mongozen.svg
  :target: https://pypi.python.org/pypi/mongozen

.. |Codecov| image:: https://codecov.io/github/shaypal5/mongozen/coverage.svg?branch=master
   :target: https://codecov.io/github/shaypal5/mongozen?branch=master