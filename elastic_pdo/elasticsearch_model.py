from datetime import datetime
from types import TracebackType
from typing import Dict, Iterable, List, Optional, Tuple, Type, TypeVar, Union

from .util import _is, _is_builtin, _is_dunder, _is_swagger

_EXTENDS_ElasticsearchModel = TypeVar('_EXTENDS_ElasticsearchModel', bound='ElasticsearchModel')
_META_ID = '_ElasticsearchModel__meta_id'
_PATH = '__path__'
_OWNER = '__owner__'
_WRAPPED = '__wrapped__'


# noinspection PyProtectedMember
class _BaseElasticObject:
    _METHODS_TO_INTERCEPT = ['_notify_child_update', '_wrapped_type']

    def __init__(self, owner: '_BaseElasticObject' = None, path: any = None):
        object.__setattr__(self, _OWNER, owner)
        object.__setattr__(self, _PATH, path)

    def _notify_child_update(self, path: List[Tuple[any, type]], value: any):
        path.insert(0, (_get(self, _PATH), self._wrapped_type()))
        _get(self, _OWNER)._notify_child_update(path, value)

    def _wrapped_type(self) -> type:
        return dict


class _ElasticWrappedBase(_BaseElasticObject):
    def __init__(self, owner: _BaseElasticObject, path: str, wrapped: any):
        super().__init__(owner, path)
        object.__setattr__(self, _WRAPPED, wrapped)


def _get(caller: _BaseElasticObject, field: str):
    return object.__getattribute__(caller, field)


def _wrap_if_needed(owner: _BaseElasticObject, path: any, obj: any):
    klass = type(obj)

    if _is(klass, dict):
        return ElasticDictWrapper(owner, path, obj)
    if _is(klass, list):
        return ElasticListWrapper(owner, path, obj)
    if not _is_builtin(klass):
        return ElasticObjectWrapper(owner, path, obj)

    return obj


# a helper class for access to members via owner.key
class _AttrKey:
    def __init__(self, key):
        self.key = key

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return f'_AttrKey({self.key})'


# LOW: extend dict
# noinspection PyProtectedMember
class ElasticDictWrapper(_ElasticWrappedBase):
    def __init__(self, owner: _BaseElasticObject, path: any, wrapped: dict):
        super().__init__(owner, path, wrapped)

    def __getitem__(self, item):
        res = _get(self, _WRAPPED)[item]
        return res if _is_dunder(item) else _wrap_if_needed(self, item, res)

    def __repr__(self):
        return f'ElasticDictWrapper({_get(self, _WRAPPED)})'

    def __setitem__(self, key, value):
        _get(self, _WRAPPED)[key] = value
        if not _is_dunder(key):
            _get(self, _OWNER)._notify_child_update([(_get(self, _PATH), dict), key], value)

    def __str__(self):
        return str(_get(self, _WRAPPED))

    # FIXME: implement missing dict methods


# LOW: extend list
# noinspection PyProtectedMember
class ElasticListWrapper(_ElasticWrappedBase):
    def __init__(self, owner: _BaseElasticObject, path: any, wrapped: list):
        super().__init__(owner, path, wrapped)

    def __getitem__(self, item):
        res = _get(self, _WRAPPED)[item]
        return res if _is_dunder(item) else _wrap_if_needed(self, item, res)

    def __repr__(self):
        return f'ElasticListWrapper({_get(self, _WRAPPED)})'

    def __setitem__(self, key, value):
        _get(self, _WRAPPED)[key] = value
        if not _is_dunder(key):
            _get(self, _OWNER)._notify_child_update([(_get(self, _PATH), list), key], value)

    def __str__(self):
        return str(_get(self, _WRAPPED))

    def _wrapped_type(self) -> type:
        return list

    def append(self, value):
        _get(self, _WRAPPED).append(value)
        _get(self, _OWNER)._notify_child_update([(_get(self, _PATH), list), len(_get(self, _WRAPPED)) - 1], value)

    # LOW: make this more efficient by having some kinda bulk operation
    def extend(self, lst: Iterable):
        prev_len = len(_get(self, _WRAPPED))
        _get(self, _WRAPPED).extend(lst)
        for i, v in enumerate(lst):
            _get(self, _OWNER)._notify_child_update([(_get(self, _PATH), list), prev_len + i], v)

    def remove(self, value):
        _get(self, _WRAPPED).remove(value)

    # FIXME: implement missing list methods


# noinspection PyProtectedMember
class ElasticObjectWrapper(_ElasticWrappedBase):
    def __init__(self, owner: _BaseElasticObject, path: any, wrapped: list):
        super().__init__(owner, path, wrapped)

    def __getattribute__(self, attr):
        if attr in _BaseElasticObject._METHODS_TO_INTERCEPT:
            return object.__getattribute__(self, attr)
        res = _get(self, _WRAPPED).__getattribute__(attr)
        return res if _is_dunder(attr) else _wrap_if_needed(self, _AttrKey(attr), res)

    def __getitem__(self, item):
        res = _get(self, _WRAPPED)[item]
        return res if _is_dunder(item) else _wrap_if_needed(self, item, res)

    def __repr__(self):
        return f'ElasticObjectWrapper({_get(self, _WRAPPED)})'

    def __setattr__(self, attr, value):
        _get(self, _WRAPPED).__setattr__(attr, value)
        if not _is_dunder(attr):
            _get(self, _OWNER)._notify_child_update([(_get(self, _PATH), dict), _AttrKey(attr)], value)

    def __setitem__(self, key, value):
        _get(self, _WRAPPED)[key] = value
        if not _is_dunder(key):
            _get(self, _OWNER)._notify_child_update([(_get(self, _PATH), dict), key], value)

    def __str__(self):
        return str(_get(self, _WRAPPED))


# noinspection PyProtectedMember
class ElasticsearchTransaction:
    def __init__(self, owner: 'ElasticsearchModel'):
        self.__owner = owner

    def __enter__(self) -> 'ElasticsearchTransaction':
        self.__owner._start_transaction()
        return self

    def __exit__(self, ex_type: Optional[Type[BaseException]], ex_value: Optional[BaseException],
                 traceback: Optional[TracebackType]) -> bool:
        self.__owner._apply_transaction()
        return False

    def reset(self):
        """
        Removes all changes from this transaction preventing them from being committed

        Note that this doesn't undo them in the local model LOW: maybe fix that?
        """

        self.__owner._start_transaction()


class ElasticsearchQuery:
    """ ElasticsearchQuery contains useful query builders for elasticsearch """

    @staticmethod
    def exists(field: str):
        return {
            'exists': {
                'field': field
            }
        }

    @staticmethod
    def equals(field: str, value: any):
        return {
            'match_phrase': {
                field: value
            }
        }

    @staticmethod
    def or_(field: str, values: List[any]):
        if values and type(values[0]) == str:
            field = f'{field}.keyword'

        return {
            'terms': {
                field: values
            }
        }

    @staticmethod
    def range(field: str, *, gt: Union[float, int, datetime] = None, gte: Union[float, int, datetime] = None,
              lt: Union[float, int, datetime] = None, lte: Union[float, int, datetime] = None):

        if gt is not None and gte is not None:
            raise ValueError('gt and gte are mutually exclusive')
        if lt is not None and lte is not None:
            raise ValueError('lt and lte are mutually exclusive')

        _range = {}
        if gt is not None:
            _range['gt'] = gt
        elif gte is not None:
            _range['gte'] = gte
        if lt is not None:
            _range['lt'] = lt
        elif lte is not None:
            _range['lte'] = lte

        return {
            'range': {
                field: _range
            }
        }

    @staticmethod
    def text(value: str):
        return {
            'multi_match': {
                'type':    'phrase',
                'query':   value,
                'lenient': True,
                'slop':    100
            }
        }


# noinspection GrazieInspection
class ElasticsearchModel(_BaseElasticObject):
    """
    ElasticsearchModel is the base class for any model to function with elasticsearch

    To make your model work, simply extend ElasticsearchModel and add the following class fields:
    (__index: str) and (__primary_key: str)
    """

    _ATTRS_TO_INTERCEPT = ['_ElasticsearchModel__index', _META_ID, '_ElasticsearchModel__primary_key',
                           '_ElasticsearchModel__trans']

    @classmethod
    def _add_transaction_step(cls, base, path, value):
        # check if we don;t need to recurse
        if len(path) == 1:
            p = path[0]

            # these are left separate in case they need to be handled differently in the future
            if isinstance(p, _AttrKey):
                base[path[0].key] = value
            else:
                base[path[0]] = value

            return base

        # setup the path  so we can add our value
        else:
            # setup base for next iteration
            p = path[0][0]
            p = p.key if isinstance(p, _AttrKey) else p
            if p not in base:
                base[p] = path[0][1]()
            path.pop(0)

            # recursion
            cls._add_transaction_step(base[p], path, value)
            return base

    @classmethod
    def count(cls, query: Dict[str, any] = None) -> int:
        """
        Counts all models of this type matching the supplied query

        :param query: The query to match models against or <span style="color:#0055aa">True</span> if not supplied
        :return: The amount of models matching the supplied query or total amount if no query supplied
        """

        from .elasticsearch_integration import ElasticsearchIntegration
        index = object.__getattribute__(cls, f'_{cls.__name__}__index')
        return ElasticsearchIntegration.count(index, query)

    @classmethod
    def delete_static(cls, primary_key_value: any) -> None:
        """
        Removes a single model from elasticsearch based off primary_key_value

        :param primary_key_value: The value of the primary key to search for
        """

        from .elasticsearch_integration import ElasticsearchIntegration
        index = object.__getattribute__(cls, f'_{cls.__name__}__index')
        primary_key = object.__getattribute__(cls, f'_{cls.__name__}__primary_key')
        return ElasticsearchIntegration.remove(index, primary_key, primary_key_value)

    @classmethod
    def distinct(cls, field: str) -> List[Tuple[str, int]]:
        """
        Gets distinct values of the supplied field for this model from elasticsearch

        :param field: The field to fetch distinct values of
        :return: A list of all distinct values coupled with their counts
        """

        from .elasticsearch_integration import ElasticsearchIntegration
        index = object.__getattribute__(cls, f'_{cls.__name__}__index')
        return ElasticsearchIntegration.distinct(index, field)

    @classmethod
    def fetch(cls: Type[_EXTENDS_ElasticsearchModel], primary_key_value: any = None) \
            -> Optional[_EXTENDS_ElasticsearchModel]:
        """
        Fetches a single model from elasticsearch based off primary_key_value

        :param primary_key_value: The value of the primary key to search for
        :return: Either the first model matching <b><i>primary_key_value</i></b> or the first model if none supplied
        """

        from .elasticsearch_integration import ElasticsearchIntegration
        index = object.__getattribute__(cls, f'_{cls.__name__}__index')

        if primary_key_value is None:
            return cls().from_elastic_document(ElasticsearchIntegration.get_one(index))

        primary_key = object.__getattribute__(cls, f'_{cls.__name__}__primary_key')
        document = ElasticsearchIntegration.get(index, primary_key, primary_key_value)
        if document is not None:
            return cls().from_elastic_document(document[0])

    @classmethod
    def fetch_all(cls: Type[_EXTENDS_ElasticsearchModel], sort: Union[Dict[str, any], List[Dict[str, any]]] = None) \
            -> Tuple[List[_EXTENDS_ElasticsearchModel], int]:
        """
        Fetches all models of this type from elasticsearch

        :param sort: The order by which to sort the models
        :return: A list of all models belonging to this index
        """

        from .elasticsearch_integration import ElasticsearchIntegration
        index = object.__getattribute__(cls, f'_{cls.__name__}__index')
        documents, count = ElasticsearchIntegration.get_all(index, sort)
        return [cls().from_elastic_document(document) for document in documents], count

    @classmethod
    def fetch_matching(cls: Type[_EXTENDS_ElasticsearchModel], query: Dict[str, any] = None,
                       sort: Union[Dict[str, any], List[Dict[str, any]]] = None, max_elements: int = 10000,
                       offset: int = 0) -> Tuple[List[_EXTENDS_ElasticsearchModel], int]:
        """
        Fetches all models of this type from elasticsearch that match the supplied query

        :param query: The query to search and match against,
        if <span style="color:#0055aa">None</span> defaults to match all
        :param sort: The order by which to sort the models
        :param max_elements: The maximum number of documents to return
        :param offset: The to start from during pagination
        :return: A list of all models belonging to this index matching the supplied query
        """

        from .elasticsearch_integration import ElasticsearchIntegration
        index = object.__getattribute__(cls, f'_{cls.__name__}__index')
        documents, count = ElasticsearchIntegration.get_matching(index, query=query, sort=sort,
                                                                 max_elements=max_elements, offset=offset)
        return [cls().from_elastic_document(document) for document in documents], count

    def __init__(self):
        super().__init__()
        self.__index = self.__getattribute__(f'_{type(self).__name__}__index')
        self.__primary_key = self.__getattribute__(f'_{type(self).__name__}__primary_key')
        self.__trans = None
        self.__meta_id = None

    @property
    def index(self) -> str:
        """The distinct index of this model in elasticsearch"""
        return self.__index

    @property
    def meta_id(self):
        """The meta id mapping this model to a document in elasticsearch"""
        return self.__meta_id

    @property
    def primary_key(self):
        """The primary key of this model used for most searches"""
        return self.__primary_key

    def __getattribute__(self, attr):
        res = object.__getattribute__(self, attr)
        return res if _is_dunder(attr) or attr in ElasticsearchModel._ATTRS_TO_INTERCEPT \
            else _wrap_if_needed(self, _AttrKey(attr), res)

    def __setattr__(self, attr, value):
        object.__setattr__(self, attr, value)

        if not _is_dunder(attr) and attr not in ElasticsearchModel._ATTRS_TO_INTERCEPT and self.__meta_id is not None:
            trans = self.__trans
            if trans is not None:
                trans.append(([_AttrKey(attr)], value))
            else:
                from .elasticsearch_integration import ElasticsearchIntegration
                body = ElasticsearchModel._add_transaction_step({}, [_AttrKey(attr)], value)
                ElasticsearchIntegration.update_model(self, body)

    # TODO: look into maybe just overwriting existing one..?
    def commit(self) -> None:
        """
        Add the local model to elasticsearch

        To update a model already in elasticsearch simple use its fields
        """

        if self.__meta_id is not None:
            raise RuntimeError('Cannot commit a model to elasticsearch when it was fetched from there')

        from .elasticsearch_integration import ElasticsearchIntegration
        ElasticsearchIntegration.add(self)

    def delete(self) -> None:
        """
        Removes this model from elasticsearch

        If for some reason you would like to add it back later call the commit method
        """

        if self.__meta_id is None:
            raise RuntimeError("Cannot delete a model from elasticsearch when it wasn't fetched from there")

        from .elasticsearch_integration import ElasticsearchIntegration
        ElasticsearchIntegration.remove_by_meta_id(self.index, self.__meta_id)
        self.__meta_id = None

    def from_elastic_document(self, dikt: Dict[str, any]) -> 'ElasticsearchModel':
        """
        Load values from the supplied elasticsearch document into this model

        :param dikt: The elasticsearch document to load values from
        :return: Self for chaining
        """

        if self.__trans is not None:
            raise RuntimeError('Cannot load from elasticsearch during a transaction')

        # unset all previous values
        attrs_old = dict(vars(self))
        for k, v in attrs_old.items():
            if not _is_dunder(k) and k not in ElasticsearchModel._ATTRS_TO_INTERCEPT:
                object.__delattr__(self, k)

        # set meta id for elasticsearch syncing
        from .elasticsearch_integration import ElasticsearchIntegration
        self.__meta_id = dikt[ElasticsearchIntegration.META_ID_FIELD]
        del dikt[ElasticsearchIntegration.META_ID_FIELD]

        # set values based off the provided dictionary
        for k, v in dikt.items():
            if k in attrs_old and _is_swagger(type(attrs_old[k])):
                object.__setattr__(self, k, type(attrs_old[k]).from_dict(v))
            else:
                object.__setattr__(self, k, v)

        # any values that don't exist in elasticsearch will be set to None or empty version
        for k, v in attrs_old.items():
            if not hasattr(self, k):
                klass = type(v)
                value_empty = None
                if _is(klass, dict):
                    value_empty = {}
                elif _is(klass, list):
                    value_empty = []
                elif _is_swagger(klass):
                    value_empty = klass()
                object.__setattr__(self, k, value_empty)

        return self

    # FIXME: support attributes that are lists/dict/whatever containing swagger types (requires recursion)
    def to_elastic_document(self) -> Dict[str, any]:
        """
        Constructs an elasticsearch document from the values of this model
        This method can also be though of as .to_dict()

        :return: A dict representing an elasticsearch document
        """

        res = {}
        for k, v in vars(self).items():
            if not _is_dunder(k) and k not in ElasticsearchModel._ATTRS_TO_INTERCEPT:
                res[k] = v.to_dict() if _is_swagger(type(v)) else v
        return res

    def transaction(self) -> ElasticsearchTransaction:
        """
        Create a transaction to join multiple operations into a single request

        :return: The transaction for use within a <span style="color:#0055aa">with</span> block
        """

        if self.__meta_id is None:
            raise RuntimeError(
                f'Cannot start a transaction on {self.__class__.__name__} before connecting it to elastic')
        return ElasticsearchTransaction(self)

    def _apply_transaction(self):
        transaction: list = self.__trans
        self.__trans = None

        body = {}
        for (path, value) in transaction:
            ElasticsearchModel._add_transaction_step(body, path, value)

        from .elasticsearch_integration import ElasticsearchIntegration
        ElasticsearchIntegration.update_model(self, body)

    def _start_transaction(self):
        self.__trans = []

    def _notify_child_update(self, path: List[Tuple[any, type]], value: any):
        if self.__meta_id is not None:
            trans = self.__trans
            if trans is not None:
                trans.append((path, value))
            else:
                from .elasticsearch_integration import ElasticsearchIntegration
                body = ElasticsearchModel._add_transaction_step({}, path, value)
                ElasticsearchIntegration.update_model(self, body)
