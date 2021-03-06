import datetime

import six


# noinspection GrazieInspection
def _deserialize(data, klass):
    """Deserializes dict, list, str into an object.

    :param data: dict, list or str.
    :param klass: class literal, or string of class name.

    :return: object.
    """
    if data is None:
        return None

    if klass in six.integer_types or klass in (float, str, bool):
        return _deserialize_primitive(data, klass)
    elif klass == object:
        return _deserialize_object(data)
    elif klass == datetime.date:
        return deserialize_date(data)
    elif klass == datetime.datetime:
        return deserialize_datetime(data)
    elif hasattr(klass, '__origin__'):
        if klass.__origin__ == list:
            return _deserialize_list(data, klass.__args__[0])
        if klass.__origin__ == dict:
            return _deserialize_dict(data, klass.__args__[1])
    # elif type(klass) == typing.GenericMeta:
    #    if klass.__extra__ == list:
    #        return _deserialize_list(data, klass.__args__[0])
    #    if klass.__extra__ == dict:
    #        return _deserialize_dict(data, klass.__args__[1])
    else:
        return deserialize_model(data, klass)


# noinspection GrazieInspection
def _deserialize_primitive(data, klass):
    """Deserializes to primitive type.

    :param data: data to deserialize.
    :param klass: class literal.

    :return: int, long, float, str, bool.
    :rtype: int | long | float | str | bool
    """
    try:
        if klass == bool and data.__str__().lower() == 'false':
            return False
        value = klass(data)
    except UnicodeEncodeError:
        value = six.u(data)
    except TypeError:
        value = data
    return value


def _deserialize_object(value):
    """Return a original value.

    :return: object.
    """
    return value


def deserialize_date(string):
    """Deserializes string to date.

    :param string: str.
    :type string: str
    :return: date.
    :rtype: date
    """
    try:
        from dateutil.parser import parse
        return parse(string).date()
    except ImportError:
        return string


def deserialize_datetime(string):
    """Deserializes string to datetime.

    The string should be in iso8601 datetime format.

    :param string: str.
    :type string: str
    :return: datetime.
    :rtype: datetime
    """
    try:
        from dateutil.parser import parse
        return parse(string)
    except ImportError:
        return string


def deserialize_model(data, klass):
    """Deserializes list or dict to model.

    :param data: dict, list.
    :type data: dict | list
    :param klass: class literal.
    :return: model object.
    """
    instance = klass()

    if not instance.swagger_types:
        return data

    if data is not None and isinstance(data, (list, dict)):
        for attr, attr_type in six.iteritems(instance.swagger_types):
            mapped_attr = instance.attribute_map[attr]

            # it's supplied as a list get all values
            if attr_type == list or getattr(attr_type, '__origin__', None) == list and f'{mapped_attr}[]' in data:
                # noinspection PyUnresolvedReferences
                value = data.getlist(f'{mapped_attr}[]')

            # otherwise get value
            elif mapped_attr in data:
                value = data[mapped_attr]

            # no value is a skip
            else:
                continue

            setattr(instance, attr, _deserialize(value, attr_type))

    return instance


# noinspection GrazieInspection
def _deserialize_list(data, boxed_type):
    """Deserializes a list and its elements.

    :param data: list to deserialize.
    :type data: list
    :param boxed_type: class literal.

    :return: deserialized list.
    :rtype: list
    """
    return [_deserialize(sub_data, boxed_type) for sub_data in data]


def _deserialize_dict(data, boxed_type):
    """Deserializes a dict and its elements.

    :param data: dict to deserialize.
    :type data: dict
    :param boxed_type: class literal.

    :return: deserialized dict.
    :rtype: dict
    """
    return {k: _deserialize(v, boxed_type) for k, v in six.iteritems(data)}
