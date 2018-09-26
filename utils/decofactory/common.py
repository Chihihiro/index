import hashlib
import pickle
from functools import wraps


def hash_cache(cacheattr=None, paramhash=False, selfhash=False, maxcache=-1):
    """
        Cache decorator for class instance, with different level of cache strategy.

    Args:
        cacheattr: str
            instance attribute for storing cache;
        paramhash: bool, default False
            cache strategy. If True, when `func` is called with different parameters, all result will be cached;
        selfhash: bool, default False
            whether to use self as a parameter to generate cache key. If True, the instance should support __hash__
            method;
        maxcache: int, default -1
            max cache number of keys. Cache dict will clear if length of cache exceed this number.
            default -1, means no limit;

    """

    def _cache(func):
        cachewhere = cacheattr or "_" + func.__name__

        @wraps(func)
        def wrapper(this, *args, **kwargs):
            to_pickle = [func.__name__]
            if paramhash:
                to_pickle.extend([args, kwargs])
            if selfhash:
                to_pickle.append(hash(this))
            hash_key = hashlib.md5(pickle.dumps(tuple(to_pickle))).hexdigest()

            if hasattr(this, cachewhere):
                if hash_key not in getattr(this, cachewhere):
                    if 0 < maxcache <= len(getattr(this, cachewhere)):
                        getattr(this, cachewhere).clear()
                    getattr(this, cachewhere)[hash_key] = func(this, *args, **kwargs)
            else:
                setattr(this, cachewhere, {hash_key: func(this, *args, **kwargs)})

            return getattr(this, cachewhere).get(hash_key)
        return wrapper
    return _cache


def unhash_cache(prefix="_", suffix=""):
    def _cache(func):
        @wraps(func)
        def wrapper(this, *args, **kwargs):
            fn = prefix + func.__name__ + suffix
            if not hasattr(this, fn):
                setattr(this, fn, func(this, *args, **kwargs))

            return getattr(this, fn)
        return wrapper
    return _cache


def tolerant_exec(except_val=None):
    """

    Args:
        except_val: Any
            Value to return when exception occurred.

    Returns:

    """

    def _tolerant(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except:
                return except_val
        return wrapper
    return _tolerant


def auto_retry(max_retry, latency=5):
    """
    处理读写超时错误;

    Args:
        max_retry:
        latency:

    Returns:

    """
    from time import sleep
    from pymysql import err
    from sqlalchemy import exc

    def _auto_retry(func):
        cnt = [1]

        def wrapper(*args, **kwargs):
            while cnt[0] <= max_retry:
                try:
                    func(*args, **kwargs)

                except (exc.TimeoutError, exc.OperationalError, err.OperationalError, TimeoutError) as e:
                    if cnt[0] <= max_retry:
                        print("Encounter error: ", e, "\nRetry[{cnt}] after {lat}s ".format(lat=latency, cnt=cnt[0]))
                        sleep(latency)
                        cnt[0] += 1
                        wrapper(*args, **kwargs)

            raise RuntimeError("Reached MAX RETRY: ", max_retry)

        return wrapper
    return _auto_retry


# compatible to previous ver
inscache = hash_cache
hash_inscache = hash_cache
unhash_inscache = unhash_cache
