import requests

from urlparse import urljoin
from query import And, Or, FactSelector


class PuppetDB(object):
    API_VERSION = '/v3'

    def __init__(self, puppetdb_addr, cert=None, timeout=2):
        self.session = requests.Session()
        self.puppetdb_addr = puppetdb_addr
        self.cert = cert
        self.timeout = timeout

    @staticmethod
    def _get_nodes_query_dict(query):
        if query:
            return {'query': query.node_query()}
        return None

    def nodes(self, query=None):

        nodes_url = urljoin(self.puppetdb_addr, self.API_VERSION + '/nodes')

        def create_query_dict():
            if query:
                return {'query': query.node_query()}
            return None

        nodes_params = create_query_dict()
        response = self.session.get(nodes_url,
                                    params=nodes_params,
                                    cert=self.cert,
                                    timeout=self.timeout)

        def extract_names_from_json(json_res):
            return [i['name'] for i in json_res]

        return extract_names_from_json(response.json())

    def facts(self, facts=None, query=None):

        facts_url = urljoin(self.puppetdb_addr, self.API_VERSION + '/facts')

        facts_query = None
        if facts:
            facts_query = Or(*[FactSelector(fact) for fact in facts])

        if query:
            facts_query = And(facts_query, query) if facts_query else query

        def create_query_dict():
            if facts_query:
                return {'query': facts_query.fact_query()}
            return None

        facts_params = create_query_dict()
        response = self.session.get(facts_url,
                                    params=facts_params,
                                    cert=self.cert,
                                    timeout=self.timeout)

        def extract_fact_dict_from_json(json_res):
            facts = {}
            for fact in json_res:
                fact_dict = facts.get(fact['name'], {})
                fact_dict[fact['certname']] = fact['value']
                facts[fact['name']] = fact_dict

            return facts

        return extract_fact_dict_from_json(response.json())
