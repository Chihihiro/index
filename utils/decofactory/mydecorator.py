import datetime as dt
from functools import wraps, partial
import functools
from inspect import signature
import hashlib
import pickle


def log(level="performance"):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if level == "exec":
                print("Executing Function: {func_name}".format(func_name=func.__name__))
                return func(*args, **kwargs)
            elif level == "performance":
                tn_s = dt.datetime.now().timestamp()
                res = func(*args, **kwargs)
                tn_e = dt.datetime.now().timestamp()
                print("{func_name}: {time} s".format(func_name=func.__name__, time=tn_e - tn_s))
                return res

        return wrapper

    return decorator


def cache(func):
    cached = {}

    @wraps(func)
    def wrapper(*args, **kwargs):
        k = hashlib.md5(pickle.dumps((func.__name__, args, kwargs))).hexdigest()
        if k not in cached:
            cached[k] = func(*args, **kwargs)
        return cached[k]

    return wrapper


def change_sig(func):
    print("catching exception")
    func.__name__ = "HACKED!"
    return func


def inscache(cacheattr, paramhash=False):
    """
        Cache decorator for class instance, with different level of cache strategy.

    Args:
        cacheattr: str
            instance attribute for storing cache;
        paramhash: bool
            cache strategy

    Returns:

    """

    def _cache(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if paramhash:
                hash_key = hashlib.md5(pickle.dumps((func.__name__, args, kwargs))).hexdigest()
            else:
                hash_key = hashlib.md5(pickle.dumps(func.__name__)).hexdigest()

            if hasattr(self, cacheattr):
                if hash_key not in self.__getattribute__(cacheattr):
                    self.__getattribute__(cacheattr)[hash_key] = func(self, *args, **kwargs)
            else:
                self.__setattr__(cacheattr, {})
                self.__getattribute__(cacheattr)[hash_key] = func(self, *args, **kwargs)

            return self.__getattribute__(cacheattr)[hash_key]

        return wrapper

    return _cache
