
def get_difference(df_src, df_tgt, main="source"):
    """
        Return records of the main frame which not contained in the sub-frame;
    Args:
        df_src: pd.DataFrame
        df_tgt: pd.DataFrame
        main: str, optional {"source", "target"}, default "source"
            To specify which Dataframe to be considered as the main frame.

    Returns:

    """
    if main == "source":
        df_main, df_sub = df_src, df_tgt
    elif main == "target":
        df_main, df_sub = df_tgt, df_src

    df_main["__flag"] = 1
    df_sub["__flag"] = 1

    data = df_main.merge(df_sub, "outer", on=[x for x in df_main.columns if x != "__flag"], suffixes=["_src", "_tgt"])
    data_diff = data.loc[data["__flag_src"] != 1]
    data_diff = data_diff[[x for x in data_diff.columns if x not in ("__flag_src", "__flag_tgt")]]
    data_diff.index = range(len(data_diff))
    return data_diff


def merge_dataframe(ls):
    for idx, df in enumerate(ls):
        result = result.append(df) if idx > 0 else df
    result.index = range(len(result))
    return result
