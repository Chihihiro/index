import inspect
import types


def get_kwargs_used(kwargs_used, **kwargs):
    return map(lambda x: kwargs.get(x), kwargs_used)


