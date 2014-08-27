PUPETDB-API
===========

Puppetdb-api is python API around the puppetDB REST API.

QUERIES
_______

*Nodes*, *facts* and *resources* can by queried by matching *nodes*, *facts* and *resources* with boolean operators *or*, *and* and *not*.

All the queries can be done as a *raw* queries, to obtain the raw JSON, that puppetDB returns.

NODES
~~~~~

.. code-block:: pycon

    >>> p = PuppetDB('https://puppetdb.example.com')
    >>> p.nodes(Or(Fact('role', 'example', '='), Resource('Class', 'Klass_name', '=')))
    [u'node1.example.com', u'node2.example.com']
    >>> p.nodes_raw(Node('node1.example.com', '='))
    [{u'deactivated': None, u'facts_timestamp': u'2014-08-25T19:45:00.707Z', u'name': u'node1.example.com', u'report_timestamp': u'2014-08-25T19:46:09.616Z', u'catalog_timestamp': u'2014-08-25T19:45:14.896Z'}]

FACTS
~~~~~

Values for multiple facts can be obtained by passing multiple facts names to *facts* argument.

.. code-block:: pycon

    >>> p = PuppetDB('https://puppetdb.example.com')
    >>> p.facts(query=And(Fact('role', 'example', '='), Not(Resource('Class', 'Klass_name', '='))), facts=['role', 'hostname'])
    {u'hostname': [u'hostname1'], u'role': [u'example']}
    >>> p.facts_raw(Node('node1.example.com', '~'))
    [{u'certname': u'node1.example.com', u'name': u'clientversion', u'value': u'3.8.7'}, {u'certname': u'node1.example.com', u'name': u'uptime_hours', u'value': u'656'}]

RESOURCES
~~~~~~~~~

Values for multiple resources can be obtained by passing multiple resources names to *resources* argument.

.. code-block:: pycon

    >>> p = PuppetDB('https://puppetdb.example.com')
    >>> p.resources(query=And(Fact('role', 'example', '='), Not(Resource('Class', 'Klass_name', '='))), resources=['Some_resource'])
    {u'Some_resource': [u'Resource_title']}
    >>> p.resources_raw(query=Node('node1.example.com', '~'), resources=['Some_resource'])
    [{u'certname': u'node1.example.com', u'resource': u'8df598f3923a05e543e884d247d74cac08087a45', u'parameters': {}, u'title': u'resource_title', u'tags': [u'tag1', u'tag2'], u'exported': False, u'file': u'/etc/puppet/environments/example/example.pp', u'line': 25, u'type': u'Some_resource'}]

INSTALL
_______

.. code-block:: bash

    $ pip install puppetdb_api
