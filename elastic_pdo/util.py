def _is(klass: type, match: type):
    return klass == match or getattr(klass, '__origin__', None) == match


def _is_builtin(klass: type):
    return klass.__module__.__contains__('builtins')


def _is_dunder(name: str):
    return name.startswith('__') and name.endswith('__')


def _is_swagger(klass: type):
    return hasattr(klass, 'swagger_types')
