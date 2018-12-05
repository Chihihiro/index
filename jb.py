#!usr/bin/python3
from threading import Timer
import pdf_TorF as E
import datetime


def now_num(a=0):
    now = datetime.datetime.now()
    delta = datetime.timedelta(minutes=a)
    n_days = now + delta
    print(n_days.strftime('%Y-%m-%d %H:%M:%S'))
    f = n_days.strftime('%H')
    i = int(f)
    return i


def say_work():
    time = now_num()
    num = len(E.len_pdf())
    if 6 > time >= 22 and num > 50:
        E.main()
        t = Timer(1*60*60, say_work)
        t.start()
    elif time == 23:
        if num == 0:
            t = Timer(1 * 60 * 60, say_work)
            t.start()
        else:
            E.main()
            t = Timer(1*60*60, say_work)
            t.start()
    else:
        print("Hello World")
        t = Timer(1*60*60, say_work)
        t.start()


def printHello():
    print("Hello World")
    t = Timer(2, printHello)
    t.start()


def work():
    time = now_num()
    num = len(E.len_pdf())
    print(num)
    if 22 > time >= 6 and num > 50:
        E.main()
        print('开始任务')
        t = Timer(1*60*60, work)
        t.start()
    elif time == 23:
        if num == 0:
            t = Timer(1 * 60 * 60, work)
            t.start()
            pass
        else:
            E.main()
            t = Timer(1*60*60, work)
            t.start()
    else:
        print("Hello World")
        t = Timer(1*60*60, work)
        t.start()


if __name__ == "__main__":
    work()

# def work():
#     time = now_num()
#     num = len(E.len_pdf())
#     print(num)
#     if 22 > time >= 6 and num > 50:
#         # E.main()
#         print('开始任务')
#         t = Timer(1*6, work)
#         t.start()
#     elif time == 23:
#         if num == 0:
#             t = Timer(1 * 6, work)
#             t.start()
#             pass
#         else:
#             # E.main()
#             t = Timer(1*6, work)
#             t.start()
#     else:
#         print("Hello World")
#         t = Timer(1*6, work)
#         t.start()

