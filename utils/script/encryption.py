import sys
import base64
import hashlib as h
import pandas as pd


def sum_ord(acc):
    return sum([ord(c) for c in acc])


def salted(content, salt):
    """
    :param content: string to be salted by b64encode
    :param salt: string used as salt
    :return: salted string
    """
    content += str(sum_ord(salt))
    return base64.b64encode(content.encode("utf8")).decode("utf8")


def unsalted(content, salt):
    return base64.b64decode(content.encode("utf8")).decode("utf8")[:-len(str(sum_ord(salt)))]


def gen_pwd(acc):
    acc_shift = "{acc}_{shift}".format(acc=acc, shift=str(sum_ord(acc)))
    return h.sha1(acc_shift.encode("utf8")).hexdigest()

