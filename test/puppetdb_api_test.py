import json
import unittest2

from httmock import all_requests, response, HTTMock
from puppetdb_api import And, Or, Not, Fact, PuppetDB
from urllib2 import quote, unquote

class PuppetDBApiTest(unittest2.TestCase):
    PUPPETDB = 'https://puppetdb.example.com/'

    def generate_puppetdb_nodes_mock(self, response_nodes=(), response_code=200, query=''):
        @all_requests
        def puppetdb_mock(url, requests):
            self.assertEqual(unquote(url.query), unquote(query))
            self.assertEqual(url.scheme, 'https')
            self.assertEqual(url.netloc, 'puppetdb.example.com')
            self.assertEqual(url.path, '/v3/nodes')

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

    def test_all_nodes_query(self):
        mocked_res = [
            'node1.example.net',
            'node2.example.net',
            'node3.example.net'
        ]
        puppetDB = PuppetDB(self.PUPPETDB)

        with HTTMock(self.generate_puppetdb_nodes_mock(response_nodes=mocked_res)):
            res = puppetDB.nodes()

        self.assertEqual(res, mocked_res)

    def test_node_query_by_fact(self):
        mocked_res = [
            'node1.example.net',
            'node2.example.net',
            'node3.example.net'
        ]
        expected_query = 'query=' + quote('["=",["fact","domain"],"example.net"]')

        puppetDB = PuppetDB(self.PUPPETDB)

        with HTTMock(self.generate_puppetdb_nodes_mock(response_nodes=mocked_res, query=expected_query)):
            res = puppetDB.nodes(Fact(operator='=', name='domain', value='example.net'))

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

        puppetDB = PuppetDB(self.PUPPETDB)

        with HTTMock(self.generate_puppetdb_nodes_mock(response_nodes=mocked_res, query=expected_query)):
            res = puppetDB.nodes(Or(
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

        puppetDB = PuppetDB(self.PUPPETDB)

        with HTTMock(self.generate_puppetdb_nodes_mock(response_nodes=mocked_res, query=expected_query)):
            res = puppetDB.nodes(And(
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

        puppetDB = PuppetDB(self.PUPPETDB)

        with HTTMock(self.generate_puppetdb_nodes_mock(response_nodes=mocked_res, query=expected_query)):
            res = puppetDB.nodes(Not(
                Fact(operator='=', name='fqdn', value='node2.example.net')))

        self.assertEqual(res, mocked_res)
