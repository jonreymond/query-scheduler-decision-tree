import numpy as np
import sys
from scipy.interpolate import interp1d
import math

sys.path.append(".")
sys.path.append("../resources/results")
path = "../resources/results/"


def load_col(filename, num_partitions, other_path=""):
    if other_path == "":
        res = np.genfromtxt(path + filename, delimiter=',', dtype=str)
    else:
        res = np.genfromtxt(other_path + filename, delimiter=',', dtype=str)

    bool_idx = np.tile(res[0,:] == num_partitions, res.shape[0]).reshape(res.shape)
    time_col = res[bool_idx][1:].astype(float) / 1000
    cores = res[1:, 0].astype(int)
    return dict(zip(cores, time_col))


def load(queries: str, num_partitions: str, other_path=""):
    query_names = np.array(sorted(list(set(queries))), dtype=str)
    date = ""
    dict_q_res = {}
    for i in range(len(query_names)):
        time= load_col(query_names[i] + date + ".csv", num_partitions)
        dict_q_res.update({query_names[i] : time})
    return dict_q_res


def interpolate(dict_q_res):
    #cores
    x = list(list(dict_q_res.values())[0].keys())

    res = {}
    for q_key in dict_q_res:
        time_q = list(dict_q_res[q_key].values())
        f = interp1d(x, time_q, kind="linear")
        res.update({q_key: f})
    return res


def get_path_sets(q_list):
    results = [set([q_list[0]])]
    height = int(math.log2(len(q_list)))

    for i in range(height):
        for j in range(2**i):
            idx_parent = int(j + 2**i - 1)
            parent_set = results[idx_parent]

            idx_left = 2**(i + 1) + j*2 - 1
            left_set = parent_set.copy()
            left_set.add(q_list[idx_left])
            results.append(left_set)

            right_set = parent_set.copy()
            right_set.add(q_list[idx_left + 1])
            results.append(right_set)

    return results[2**height - 1:]
