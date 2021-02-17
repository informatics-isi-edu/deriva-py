deriva.core package
===================

Subpackages
-----------

.. toctree::

    deriva.core.utils

Submodules
----------

deriva.core.annotation module
----------------------------

.. automodule:: deriva.core.annotation
    :members:
    :undoc-members:
    :show-inheritance:

deriva.core.base\_cli module
----------------------------

.. automodule:: deriva.core.base_cli
    :members:
    :undoc-members:
    :show-inheritance:

deriva.core.datapath module
---------------------------

.. automodule:: deriva.core.datapath
    :members:
    :undoc-members:
    :show-inheritance:

deriva.core.deriva\_binding module
----------------------------------

.. automodule:: deriva.core.deriva_binding
    :members:
    :undoc-members:
    :show-inheritance:
    :exclude-members: defaults

deriva.core.deriva\_server module
---------------------------------

.. automodule:: deriva.core.deriva_server
    :members:
    :undoc-members:
    :show-inheritance:

deriva.core.ermrest\_catalog module
-----------------------------------

.. automodule:: deriva.core.ermrest_catalog
    :members:
    :undoc-members:
    :show-inheritance:

deriva.core.ermrest\_config module
----------------------------------

.. automodule:: deriva.core.ermrest_config
    :members:
    :undoc-members:
    :show-inheritance:

deriva.core.ermrest\_model module
---------------------------------

.. automodule:: deriva.core.ermrest_model
    :members:
    :undoc-members:
    :show-inheritance:

deriva.core.hatrac\_cli module
------------------------------

.. automodule:: deriva.core.hatrac_cli
    :members:
    :undoc-members:
    :show-inheritance:

deriva.core.hatrac\_store module
--------------------------------

.. automodule:: deriva.core.hatrac_store
    :members:
    :undoc-members:
    :show-inheritance:

deriva.core.polling\_ermrest\_catalog module
--------------------------------------------

.. automodule:: deriva.core.polling_ermrest_catalog
    :members:
    :undoc-members:
    :show-inheritance:


Module contents
---------------

.. automodule:: deriva.core
    :members:
    :show-inheritance:

.. autofunction:: get_credential(host, credential_file=DEFAULT_CREDENTIAL_FILE, globus_credential_file=DEFAULT_GLOBUS_CREDENTIAL_FILE, config_file=DEFAULT_CONFIG_FILE, requested_scope=None, force_scope_lookup=False, match_scope_tag="deriva-all")
.. autofunction:: read_credential(credential_file=DEFAULT_CREDENTIAL_FILE, create_default=False, default=DEFAULT_CREDENTIAL)
.. autofunction:: write_credential(credential_file=DEFAULT_CREDENTIAL_FILE, credential=DEFAULT_CREDENTIAL)
.. autofunction:: read_config(config_file=DEFAULT_CONFIG_FILE, create_default=False, default=DEFAULT_CONFIG)
.. autofunction:: write_config(config_file=DEFAULT_CONFIG_FILE, config=DEFAULT_CONFIG)

.. autoattribute:: deriva.core.DEFAULT_CONFIG_PATH
    :annotation: = System dependent default path to the configuration directory.

.. autoattribute:: deriva.core.DEFAULT_CONFIG_FILE
    :annotation: = System dependent default path to the config file.

.. autoattribute:: deriva.core.DEFAULT_CREDENTIAL_FILE
    :annotation: = System dependent default path to the credential file.

.. autoattribute:: deriva.core.DEFAULT_GLOBUS_CREDENTIAL_FILE
    :annotation: = System dependent default path to the Globus Auth credential file.