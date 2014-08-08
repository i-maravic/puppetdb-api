class Query(object):

    def generate_subquery(self,
                          subquery_statement,
                          extract_field,
                          in_field):
        return '["in","%s",' \
               '["extract","%s",' \
               '["%s",%s]]]' % (in_field,
                                extract_field,
                                subquery_statement,
                                self.name_value_query())

    def node_query(self):
        raise NotImplementedError(self.node_query.__name__)

    def fact_query(self):
        raise NotImplementedError(self.fact_query.__name__)

    def name_value_query(self):
        raise NotImplementedError(self.name_value_query.__name__)


class BooleanOperator(Query):
    def __init__(self, name, *args):
        self.name = name
        self.queries = args

    def _generic_query(self, method_name):
        query_str = [getattr(query, method_name)() for query in self.queries]
        return '["%s",%s]' % (self.name, ','.join(query_str))

    def node_query(self):
        return self._generic_query(self.node_query.__name__)

    def fact_query(self):
        return self._generic_query(self.node_query.__name__)


class Not(BooleanOperator):
    def __init__(self, single_arg):
        super(Not, self).__init__('not', single_arg)


class And(BooleanOperator):
    def __init__(self, *args):
        super(And, self).__init__('and', *args)


class Or(BooleanOperator):
    def __init__(self, *args):
        super(Or, self).__init__('or', *args)


class Node(Query):
    def __init__(self, name, operator='='):
        self.name = name
        self.operator = operator

    def node_query(self):
        return '["%s","name","%s"]' % (self.operator, self.name)

    def fact_query(self):
        return '["%s","certname","%s"]' % (self.operator, self.name)


class Fact(Query):
    def __init__(self, name, value, operator='='):
        self.name = name
        self.value = value
        self.operator = operator

    def node_query(self):
        return '["%s",["fact","%s"],"%s"]' % (self.operator, self.name, self.value)

    def fact_query(self):
        return self.generate_subquery('select_facts',
                                      'certname',
                                      'certname')

    def name_value_query(self):
        return '["and",' \
               '["=","name","%s"],' \
               '["%s","value","%s"]]' % (self.name, self.operator, self.value)


class Resources(Query):
    def __init__(self, resource_type, resource_title, exported=False, operator='='):
        self.type = resource_type
        self.title = resource_title
        self.exported = exported
        self.operator = operator

    def name_value_query(self):
        return '["and",' \
               '["=","type","%s"],' \
               '["%s","title","%s"],' \
               '["=","exported",%s]]' % (self.type,
                                         self.operator,
                                         self.title,
                                         str(self.exported).lower)

    def node_query(self):
        return self.generate_subquery('select_resources',
                                      'certname',
                                      'name')

    def fact_query(self):
        return self.generate_subquery('select_resources',
                                      'certname',
                                      'certname')
