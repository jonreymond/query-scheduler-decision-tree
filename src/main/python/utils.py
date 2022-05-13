import numpy as np
import pandas as pd
import sys
from scipy.interpolate import interp1d

sys.path.append(".")
sys.path.append("../resources/results")
path = "../resources/results/"


def load_col(filename, num_partitions, other_path=""):
    if other_path == "":
        df_raw = pd.read_csv(path + filename)
    else:
        df_raw = pd.read_csv(other_path + filename)
    partitions = np.array(df_raw.columns[1:])
    df = df_raw.copy()
    df[partitions] = df_raw[partitions] / 1000
    df["num_cores"].astype('string') + " cores"
    return df[num_partitions], df['num_cores']


def load(queries: str, num_partitions: str, other_path=""):
    query_names = pd.Series(sorted(list(set(queries))))
    date = ""
    filenames = query_names + date + ".csv"
    df = pd.DataFrame()
    for i in range(len(filenames)):
        res, index = load_col(filenames[i], num_partitions)
        if i == 0 :
            df['num_cores'] = index
        df[query_names[i]] = res
    return df

    return df[num_partitions]


def interpolate(df):
    x = df['num_cores']
    names = list(df.columns.values)[1:]
    res = {}
    for n in names:
        f = interp1d(x, df[n], kind="linear")
        res.update({n: f})
    return res

