import sys
import datetime as dt
from dateutil.relativedelta import relativedelta


def parse_argv_time():
    """
    Receive and parse args pass from cli.

    e.g.
        1) python "some_script.py" "20180101120000" "20180101235959"
        -> dt.datetime(2018, 1, 1, 12, 0, 0), dt.datetime(2018, 1, 1, 23, 59, 59)

        2) python "some_script.py" "20180101120000" "20180101235959" 0 1 5 0
        -> dt.datetime(2018, 1, 1, 12, 0, 0), dt.datetime(2018, 1, 1, 23, 59, 59), (0, 1, 5, 0)

    Returns:
        tuple[
            Optilnal[dt.datetime, None],
            Optilnal[dt.datetime, None],
            Optilnal[tuple[int, int, int, int], None]
            ]

    """

    if len(sys.argv) == 1:
        t0 = t1 = None
        time_chunk = None
    elif len(sys.argv) > 1:
        try:
            t0, t1 = [dt.datetime.strptime(x, "%Y%m%d%H%M%S") for x in sys.argv[1:3]]
        except ValueError:
            t0 = t1 = None
        if len(sys.argv) == 3:
            time_chunk = None
        elif len(sys.argv) == 7:
            time_chunk = [int(x) for x in sys.argv[3:7]]  # days, hours, minutes, seconds
    else:
        raise NotImplementedError("not supported cli args")

    return t0, t1, time_chunk


def _slice_timechunk(start, end, days=0, hours=1, minutes=5, seconds=0):
    """

    Args:
        start: dt.datetime
        end: dt.datetime
        days: int
        hours: int
        minutes: int
        seconds: int

    Returns:

    """

    res = [end]
    i = 1
    chunk_size = relativedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)

    if start - chunk_size >= start:
        raise ValueError("Incorrect Chunk Size", chunk_size)

    while True:
        tmp = end - chunk_size * i
        if tmp <= start:
            res.append(start)
            break
        res.append(tmp)
        i += 1
    return res


def generate_task(start, end, days=0, hours=1, minutes=5, seconds=0):
    res = _slice_timechunk(start, end, days=days, hours=hours, minutes=minutes, seconds=seconds)
    return [(res[i+1], res[i]) for i in range(len(res) - 1)]


def main():
    # start, end = dt.datetime(2018, 1, 1), dt.datetime(2018, 1, 1, 13)
    t0, t1, time_chunk = parse_argv_time()
    res = generate_task(t0, t1, *time_chunk)
    print(t0, t1, res)


if __name__ == "__main__":
    main()
