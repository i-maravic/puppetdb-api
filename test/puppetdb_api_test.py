import json
import requests
import unittest2

from httmock import all_requests, response, HTTMock
from puppetdb_api import And, Or, Not, Fact, Resource, PuppetDB
from urllib2 import quote, unquote


class PuppetDBApiTest(unittest2.TestCase):
    PUPPETDB = 'https://puppetdb.example.com/'

    puppetDB = PuppetDB(PUPPETDB)

    def generate_puppetdb_nodes_mock(self, response_nodes=(), response_code=200, query=''):
        @all_requests
        def puppetdb_mock(url, requests):
            self.assertEqual(unquote(query), unquote(url.query))
            self.assertEqual('https', url.scheme)
            self.assertEqual('puppetdb.example.com', url.netloc)
            self.assertEqual('/v3/nodes', url.path)

            def generate_nodes_dict(name):
                return {
                    'name': name,
                    'deactivated': None,
                    'catalog_timestamp': '2014-08-08T14:14:12.153Z',
                    'facts_timestamp': '2014-08-08T14:13:53.981Z',
                    'report_timestamp': None
                }
            json_response_content = [generate_nodes_dict(name) for name in response_nodes]
            json_response_content = json.dumps(json_response_content if len(json_response_content) > 1
                                               else json_response_content[0])
            return response(response_code, json_response_content)
        return puppetdb_mock

    # Nodes
    def test_all_nodes_query(self):
        mocked_res = [
            'node1.example.net',
            'node2.example.net',
            'node3.example.net'
        ]

        with HTTMock(self.generate_puppetdb_nodes_mock(response_nodes=mocked_res)):
            res = self.puppetDB.nodes()

        self.assertEqual(mocked_res, res)

    def test_node_query_by_fact(self):
        mocked_res = [
            'node1.example.net',
            'node2.example.net',
            'node3.example.net'
        ]
        expected_query = 'query=' + quote('["=",["fact","domain"],"example.net"]')

        with HTTMock(self.generate_puppetdb_nodes_mock(response_nodes=mocked_res, query=expected_query)):
            res = self.puppetDB.nodes(Fact(operator='=', name='domain', value='example.net'))

        self.assertEqual(res, mocked_res)

    def test_node_query_by_combining_facts_with_or(self):
        mocked_res = [
            'node1.example.net',
            'node2.example.net',
            'node3.example.net'
        ]
        expected_query = 'query=' + quote('["or",'
                                          '["=",["fact","fqdn"],"node1.example.net"],'
                                          '["=",["fact","fqdn"],"node2.example.net"],'
                                          '["=",["fact","fqdn"],"node3.example.net"]]')

        with HTTMock(self.generate_puppetdb_nodes_mock(response_nodes=mocked_res, query=expected_query)):
            res = self.puppetDB.nodes(Or(
                Fact(operator='=', name='fqdn', value='node1.example.net'),
                Fact(operator='=', name='fqdn', value='node2.example.net'),
                Fact(operator='=', name='fqdn', value='node3.example.net')))

        self.assertEqual(res, mocked_res)

    def test_node_query_by_combining_facts_with_and(self):
        mocked_res = [
            'node1.example.net',
            'node2.example.net',
            'node3.example.net'
        ]
        expected_query = 'query=' + quote('["and",'
                                          '["=",["fact","domain"],"example.net"],'
                                          '["~",["fact","operatingsystem"],"Ubuntu"]]')

        with HTTMock(self.generate_puppetdb_nodes_mock(response_nodes=mocked_res, query=expected_query)):
            res = self.puppetDB.nodes(And(
                Fact(operator='=', name='domain', value='example.net'),
                Fact(operator='~', name='operatingsystem', value='Ubuntu')))

        self.assertEqual(res, mocked_res)

    def test_node_query_by_prefixing_facts_with_not(self):
        mocked_res = [
            'node1.example.net',
            'node3.example.net'
        ]
        expected_query = 'query=' + quote('["not",'
                                          '["=",["fact","fqdn"],"node2.example.net"]]')

        with HTTMock(self.generate_puppetdb_nodes_mock(response_nodes=mocked_res, query=expected_query)):
            res = self.puppetDB.nodes(Not(
                Fact(operator='=', name='fqdn', value='node2.example.net')))

        self.assertEqual(res, mocked_res)

    # Facts

    def generate_puppetdb_facts_mock(self, response_facts={}, response_code=200, query=''):
        @all_requests
        def puppetdb_mock(url, requests):
            self.assertEqual(unquote(url.query), unquote(query))
            self.assertEqual(url.scheme, 'https')
            self.assertEqual(url.netloc, 'puppetdb.example.com')
            self.assertEqual(url.path, '/v3/facts')

            def generate_facts(name, values):
                return [{
                    'name': name,
                    'value': value,
                    'certname': 'node1.example.net',
                } for value in values]

            json_response_content = [generate_facts(name, values) for name, values in response_facts.iteritems()]

            def flatten_list(l):
                return [item for sublist in l for item in sublist]

            json_response_content = flatten_list(json_response_content)
            json_response_content = json.dumps(json_response_content if len(json_response_content) > 1
                                               else json_response_content[0])
            return response(response_code, json_response_content)
        return puppetdb_mock

    def test_all_facts_query(self):
        mocked_res = {
            'fqdn': [
                'node1.example.net',
                'node2.example.net',
                'node3.example.net'
            ],
            'domain': [
                'example.net',
                'example.net',
                'example.net'
            ],
            'hostname': [
                'node1',
                'node2',
                'node3'
            ]
        }

        with HTTMock(self.generate_puppetdb_facts_mock(response_facts=mocked_res)):
            res = self.puppetDB.facts()

        self.assertEqual(mocked_res, res)

    def test_single_fact_query(self):
        mocked_res = {
            'fqdn': [
                'node1.example.net',
            ],
            'domain': [
                'example.net'
            ],
            'hostname': [
                'node1',
            ]
        }
        expected_query = 'query=' + quote('["in","certname",'
                                          '["extract","certname",'
                                          '["select-facts",'
                                          '["and",'
                                          '["=","name","fqdn"],'
                                          '["~","value","node1\.example\.net"]]]]]')

        with HTTMock(self.generate_puppetdb_facts_mock(response_facts=mocked_res, query=expected_query)):
            res = self.puppetDB.facts(query=Fact('fqdn', r'node1\.example\.net', '~'))

        self.assertEqual(mocked_res, res)

    def test_single_fact_query_with_selecting_facts(self):
        mocked_res = {
            'fqdn': [
                'node1.example.net',
            ],
            'domain': [
                'example.net'
            ],
        }
        expected_query = 'query=' + quote('["and",'
                                          '["or",'
                                          '["and",'
                                          '["=","name","fqdn"],'
                                          '["~","value",".*"]],'
                                          '["and",'
                                          '["=","name","domain"],'
                                          '["~","value",".*"]]],'
                                          '["in","certname",'
                                          '["extract","certname",'
                                          '["select-facts",'
                                          '["and",'
                                          '["=","name","fqdn"],'
                                          '["~","value","node1\.example\.net"]]]]]]')

        with HTTMock(self.generate_puppetdb_facts_mock(response_facts=mocked_res, query=expected_query)):
            res = self.puppetDB.facts(query=Fact('fqdn', r'node1\.example\.net', '~'),
                                      facts=['fqdn', 'domain'])

        self.assertEqual(mocked_res, res)

    # Resources
    def generate_puppetdb_resources_mock(self, response_resources={}, response_code=200, query=''):
        @all_requests
        def puppetdb_mock(url, requests):
            self.assertEqual(unquote(url.query), unquote(query))
            self.assertEqual(url.scheme, 'https')
            self.assertEqual(url.netloc, 'puppetdb.example.com')
            self.assertEqual(url.path, '/v3/resources')

            def generate_resources(res_type, res_titles):
                return [{
                    'type': res_type,
                    'title': res_title,
                    'exported': str(res_type.startswith('@@')).lower(),
                } for res_title in res_titles]

            json_response_content = [generate_resources(type, titles) for type, titles in response_resources.iteritems()]

            def flatten_list(l):
                return [item for sublist in l for item in sublist]

            json_response_content = flatten_list(json_response_content)
            json_response_content = json.dumps(json_response_content if len(json_response_content) > 1
                                               else json_response_content[0])
            return response(response_code, json_response_content)
        return puppetdb_mock

    def test_all_resources_query(self):
        mocked_res = {
            'class': [
                'klass1',
                'klass2',
                'klass3'
            ],
            '@@cert': [
                'example1.net',
                'example2.net',
                'example3.net'
            ],
            'file': [
                '/etc/example/script1',
                '/etc/example/script2',
            ]
        }

        with HTTMock(self.generate_puppetdb_resources_mock(response_resources=mocked_res)):
            res = self.puppetDB.resources()

        self.assertEqual(mocked_res, res)

    def test_single_resource_query(self):
        mocked_res = {
            'class': [
                'klass_name',
                'klass2',
            ],
            '@@cert': [
                'example1.net',
            ],
            'file': [
                '/etc/example/script1',
            ]
        }
        expected_query = 'query=' + quote('["in","certname",'
                                          '["extract","certname",'
                                          '["select-resources",'
                                          '["and",'
                                          '["=","type","cert"],'
                                          '["=","title","example1.net"],'
                                          '["=","exported",true]]]]]')

        with HTTMock(self.generate_puppetdb_resources_mock(response_resources=mocked_res, query=expected_query)):
            res = self.puppetDB.resources(query=Resource('@@cert', 'example1.net', '='))

        self.assertEqual(mocked_res, res)

    def test_single_fact_query_with_selecting_resources(self):
        mocked_res = {
            'class': [
                'klass1',
                'klass2',
            ],
            '@@cert': [
                'example1.net',
            ],
        }
        expected_query = 'query=' + quote('["and",'
                                          '["or",'
                                          '["and",'
                                          '["=","type","class"],'
                                          '["~","title",".*"],'
                                          '["=","exported",false]],'
                                          '["and",'
                                          '["=","type","cert"],'
                                          '["~","title",".*"],'
                                          '["=","exported",true]]],'
                                          '["in","certname",'
                                          '["extract","certname",'
                                          '["select-facts",'
                                          '["and",'
                                          '["=","name","fqdn"],'
                                          '["~","value","node1\.example1\.net"]]]]]]')

        with HTTMock(self.generate_puppetdb_resources_mock(response_resources=mocked_res, query=expected_query)):
            res = self.puppetDB.resources(query=Fact('fqdn', r'node1\.example1\.net', '~'),
                                          resources=['class', '@@cert'])

        self.assertEqual(mocked_res, res)

    # Invalid response
    def test_error_response_raises_HTTPError(self):
        def puppetdb_mock(url, request):
            return response(401)

        with HTTMock(puppetdb_mock):
            self.assertRaises(requests.HTTPError, self.puppetDB.nodes, ())