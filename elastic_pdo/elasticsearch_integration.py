from typing import Dict, List, Optional, Tuple, Type, TYPE_CHECKING, Union

from .util import _is

if TYPE_CHECKING:
    from .elasticsearch_model import ElasticsearchModel
    from elasticsearch import Elasticsearch


# noinspection PyPep8Naming, PyUnresolvedReferences
class _Meta(type):
    @property
    def META_ID_FIELD(cls) -> str:
        return cls._META_ID_FIELD

    @property
    def client(cls) -> 'Elasticsearch':
        return cls._client


# noinspection GrazieInspection
class ElasticsearchIntegration(metaclass=_Meta):
    _META_ID_FIELD = '__meta_id__'

    _client: 'Elasticsearch' = None

    @classmethod
    def create_client(cls, elasticsearch_endpoint: str, elasticsearch_authorization: Tuple[str, str]):
        from elasticsearch import Elasticsearch
        cls._client = Elasticsearch(hosts=[f'{elasticsearch_endpoint}:443'],
                                    http_auth=elasticsearch_authorization, use_ssl=True)

    @classmethod
    def add(cls, *args: Union['ElasticsearchModel', List['ElasticsearchModel']]):
        """
        Commit varargs amount of models into elasticsearch

        :param args: The models to add, can be any amount and of and model type (mix and match allowed)
        """

        # group args by type
        by_type: Dict[Type['ElasticsearchModel'], List['ElasticsearchModel']] = {}

        def func(_by_type, _arg):
            _t = type(_arg)
            if _t not in _by_type:
                _by_type[_t] = []
            _by_type[_t].append(_arg)

        # if the arg is a list break iterate it, otherwise just keep iterating
        for arg in args:
            if _is(type(arg), list):
                for actual_arg in args:
                    func(by_type, actual_arg)
            else:
                func(by_type, arg)

        from elasticsearch import helpers
        for t, l in by_type.items():
            helpers.bulk(cls.client, [model.to_elastic_document() for model in l], index=l[0].index, doc_type='_doc')

        # fetch the meta ids, HIGH: there's gotta be a better way than querying them all
        from .elasticsearch_model import _META_ID
        for arg in args:
            primary_key = arg.primary_key
            while True:
                document = cls.get(index=arg.index, key=primary_key, value=object.__getattribute__(arg, primary_key))
                if document:
                    arg.__setattr__(_META_ID, document[0][cls.META_ID_FIELD])
                    break

    @classmethod
    def count(cls, index: str, query: Dict[str, any] = None, max_elements: int = None, offset: int = None) -> int:
        term = {'term': query} if query else {'match_all': {}}
        search_response = cls.client.search(index=index, body={'query': term}, track_total_hits=True,
                                            size=max_elements, from_=offset)
        return search_response['hits']['total']['value']

    @classmethod
    def distinct(cls, index: str, field: str) -> List[Tuple[str, int]]:
        """
        Fetch distinct values of the supplied field for a models based of their index

        :param index: The index of the models
        :param field: The field to fetch distinct values of
        :return: A list of all distinct values coupled with their counts
        """

        aggregate = {
            'size': 0,
            'aggs': {
                '*': {
                    'terms': {
                        'field': field
                    }
                }
            }
        }
        search_response = cls.client.search(index=index, body=aggregate)
        return [(bucket['key'], bucket['doc_count']) for bucket in search_response['aggregations']['*']['buckets']]

    @classmethod
    def get(cls, index: str, key: str, value: any) -> Optional[Tuple[Dict[str, any], int]]:
        """
        Fetch a single model from elasticsearch based off its index and [key]=value match

        :param index: The index of the model
        :param key: The key to use for searching
        :param value: The value to match against
        """

        request_body_search = {
            'query': {
                'bool': {
                    'must': [
                        {'match_phrase': {key: value}}
                    ]
                }
            }
        }
        search_response = cls.client.search(index=index, body=request_body_search)
        hits = search_response['hits']['hits']
        if not hits:
            return None

        res = hits[0]['_source']
        res[cls.META_ID_FIELD] = search_response['hits']['hits'][0]['_id']
        return res, search_response['hits']['total']['value']

    @classmethod
    def get_all(cls, index: str, sort: Union[Dict[str, any], List[Dict[str, any]]] = None) \
            -> Tuple[List[Dict[str, any]], int]:
        """
        Fetch all models from elasticsearch based off their index

        :param index: The index of the models
        :param sort: The order by which to sort the documents
        :return: The all models for the supplied index,<br>
        <b><u>This request can be slow if many items exist for the supplied index<u><b>
        """

        return cls.get_matching(index, sort=sort)

    @classmethod
    def get_matching(cls, index: str, query: Dict[str, any] = None,
                     sort: Union[Dict[str, any], List[Dict[str, any]]] = None,
                     max_elements: int = 10000, offset: int = 0) -> Tuple[List[Dict[str, any]], int]:
        """
        Fetch all models from elasticsearch based off their index that match the supplied query

        :param index: The index of the model
        :param query: The query to search and match against,
        if <span style="color:#0055aa">None</span> defaults to match all
        :param sort: The order by which to sort the documents
        :param max_elements: The maximum number of documents to return
        :param offset: The to start from during pagination
        """

        joint = {'query': query or {'match_all': {}}}
        if sort:
            joint['sort'] = sort if _is(type(sort), list) else [sort]

        search_response = cls.client.search(index=index, body=joint, size=max_elements,
                                            from_=offset)  # FIXME: see how to fetch more
        # FIXME: also disable the output for the above line
        res = []
        for hit in search_response['hits']['hits']:
            document = hit['_source']
            document[cls.META_ID_FIELD] = hit['_id']
            res.append(document)
        return res, search_response['hits']['total']['value']

    @classmethod
    def get_one(cls, index: str) -> Dict[str, any]:
        """
        Fetch a single model from elasticsearch based off its index

        :param index: The index of the model
        :return: The first model for the supplied index<br>
        <b><u>This may change based off model modifications DO NOT rely on consistent results<u><b>
        """

        request_body_search = {
            'query': {
                'match_all': {}
            },
            'size':  1
        }
        search_response = cls.client.search(index=index, body=request_body_search)
        res = search_response['hits']['hits'][0]['_source']
        res[cls.META_ID_FIELD] = search_response['hits']['hits'][0]['_id']
        return res

    @classmethod
    def remove(cls, index: str, key: str, value: any):
        """
        Remove models from elasticsearch based off their index and [key]=value match

        :param index: The index of the model
        :param key: The key to use for searching
        :param value: The value to match against
        """

        query = {
            'query': {
                'term': {
                    key: value
                }
            }
        }
        cls.client.delete_by_query(index=index, body=query)

    @classmethod
    def remove_by_meta_id(cls, index: str, meta_id: str):
        """
        Remove a model from elasticsearch based off its index and meta_id

        :param index: The index of the model
        :param meta_id: The meta id for removal
        """

        cls.client.delete(index=index, id=meta_id)

    @classmethod
    def update_model(cls, model: 'ElasticsearchModel', data: Dict[str, any]):
        return cls.client.update(index=model.index, body={'doc': data}, id=model.meta_id)
