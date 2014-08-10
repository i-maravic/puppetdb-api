import requests

from urlparse import urljoin
from query import And, Or, FactSelector, ResourceSelector


class PuppetDB(object):
    API_VERSION = '/v3'

    def __init__(self,
                 puppetdb_addr,
                 timeout=None,
                 verify=None,
                 cert=None):
        self.session = requests.Session()
        self.puppetdb_addr = puppetdb_addr
        self.timeout = timeout
        self.verify = verify
        self.cert = cert

    @staticmethod
    def _construct_req_params(query, query_type):
        if query:
            return {'query': getattr(query, query_type + '_query')()}
        return None

    def _construct_req_url(self, query_type):
        return urljoin(self.puppetdb_addr, self.API_VERSION + '/' + query_type + 's')

    def _send_req(self, url, params):
        return self.session.get(url,
                                params=params,
                                timeout=self.timeout,
                                verify=self.verify,
                                cert=self.cert)

    def _raw_query(self, query_type, query):
        url = self._construct_req_url(query_type)
        params = self._construct_req_params(query, query_type)

        response = self._send_req(url, params)
        response.raise_for_status()
        return response.json()

    def nodes_raw(self, query=None):
        return self._raw_query('node', query)

    def nodes(self, query=None):
        json_res = self.nodes_raw(query)

        def extract_node_names_from_json():
            return [i['name'] for i in json_res]

        return extract_node_names_from_json()

    def facts_raw(self, query=None, facts=None):
        facts_query = None
        if facts:
            facts_query = Or(*[FactSelector(fact) for fact in facts])

        if query:
            facts_query = And(facts_query, query) if facts_query else query

        return self._raw_query('fact', facts_query)

    def facts(self, query=None, facts=None):
        json_res = self.facts_raw(query, facts)

        def extract_fact_values_from_json():
            fact_dict = {}
            for fact in json_res:
                fact_values = fact_dict.get(fact['name'], [])
                fact_values.append(fact['value'])
                fact_dict[fact['name']] = fact_values

            return fact_dict

        return extract_fact_values_from_json()

    def resources_raw(self, query=None, resources=None):
        resources_query = None
        if resources:
            resources_query = Or(*[ResourceSelector(resource) for resource in resources])

        if query:
            resources_query = And(resources_query, query) if resources_query else query

        return self._raw_query('resource', resources_query)

    def resources(self, query=None, resources=None):
        json_res = self.resources_raw(query, resources)

        def extract_resource_titles_from_json():
            resource_dict = {}
            for resource in json_res:
                resource_values = resource_dict.get(resource['type'], [])
                resource_values.append(resource['title'])
                resource_dict[resource['type']] = resource_values

            return resource_dict

        return extract_resource_titles_from_json()
