# Utilities for constructing SQL string


def sqlfmt(it, usage="v"):
    """
    Format iterable into SQL string;

    Args:
        it: Iterable[int, str]
        usage: str, optional {"v", "c"}
            if "v"(or "value), format `it` into form of "('it_1', 'it_2', ...,)";
            if "c"(or "column"), format `it` into form of "`it_1`, `it_2`, ...";

    Returns:
        str

    """

    if usage == "v" or usage == "value":
        return ",".join([("'" + str(x) + "'") for x in it])
    elif usage == "c" or usage == "column":
        return ",".join([("`" + str(x) + "`") for x in it])
    else:
        NotImplementedError("Not implemented usage")


def main():
    a = ["1", "2"]
    b = ["3"]
    [sqlfmt(x) for x in [a, b]]


if __name__ == "__main__":
    main()
