import cp_optimizer
import numpy as np
import utils
MAX_LEN_QUERIES = 9


def redefine_proba_variables(list_p):
    num_paths = len(list_p)
    probas, path_sets_idx = zip(*list_p)
    return list(probas), num_paths, list(path_sets_idx)


def rearrange_queries_probas(q_list, q_left, q_right, proba_variables):
    probas, num_paths, path_sets_idx = proba_variables
    left_queries = set(q_left)
    right_queries = set(q_right)

    # 1. split paths : determine which belong only to left
    left_p = []
    right_p_raw = []
    for (proba, path) in zip(probas, path_sets_idx):
        intersect = path.intersection(right_queries)
        if len(intersect) == 0:
            left_p.append((proba, path))
        else:
            right_p_raw.append((proba, path))

    # 2. select queries appearing only in right paths
    _, left_paths = zip(*left_p)
    remain_queries = left_queries.difference(set.union(*left_paths))
    left_queries = left_queries.difference(remain_queries)
    right_queries = right_queries.union(remain_queries)

    # 3. redefine paths right containing queries in left
    right_p = []
    for (proba, path) in right_p_raw:
        new_path = right_queries.intersection(path)
        if len(new_path) > 0:
            right_p.append((proba, new_path))

    # 4. redefine proba_variables
    pr_left = redefine_proba_variables(left_p)
    pr_right = redefine_proba_variables(right_p)

    return (list(left_queries), pr_left), (list(right_queries), pr_right)


def redefine_list(q_list_tot, q_index, proba_variables):
    q_list = [q_list_tot[i] for i in q_index]
    if proba_variables is None:
        return q_list, None
    else:
        probas, num_paths, path_sets_idx = proba_variables
        dict_path = dict(zip(q_index, range(len(q_index))))
        new_path_sets = []
        for path_set in path_sets_idx:
            new_path_set = set([dict_path[i] for i in path_set])
            new_path_sets.append(new_path_set)

        proba_variables = (probas, num_paths, new_path_sets)
        return q_list, proba_variables


def split(q_list_tot, q_index, res, C, precision, proba_variables=None, normal_exec=True, reg_factor=None):
    if len(q_index) > MAX_LEN_QUERIES:
        # need to map indices to a range
        q_list, new_proba_variables = redefine_list(q_list_tot, q_index, proba_variables)
        R = 2
        Q = len(q_list)
        C_ = 2 * Q
        process_time, (runtime, res_schedule) = cp_optimizer.compute_result(q_list, res, C, R, precision,
                                                                            name_queries=False,
                                                                            C_=C_, split=True,
                                                                            proba_variables=new_proba_variables,
                                                                            normal_exec=normal_exec,
                                                                            reg_factor=reg_factor)

        # need to come back to the true indices
        q_left_index = [q_index[q[0]] for q in res_schedule[0]]
        q_right_index = [q_index[q[0]] for q in res_schedule[1]]

        if proba_variables is None:
            process_time_left, q_left = split(q_list_tot, q_left_index, res, C, precision, None, normal_exec,
                                              reg_factor)
            process_time_right, q_right = split(q_list_tot, q_right_index, res, C, precision, None, normal_exec,
                                                reg_factor)
            return process_time + process_time_left + process_time_right, q_left + q_right

        else:

            (left_q, pr_left), (right_q, pr_right) = rearrange_queries_probas(q_list, q_left_index, q_right_index,
                                                                              proba_variables)
            process_l, l = split(q_list_tot, left_q, res, C, precision, pr_left, normal_exec, reg_factor)
            process_r, r = split(q_list_tot, right_q, res, C, precision, pr_right, normal_exec, reg_factor)
            return process_time + process_l + process_r, l + r

    else:
        if proba_variables is None:
            return 0, [q_index]
        else:
            return 0, [(q_index, proba_variables)]


def compute_new_schedule(q_index, res_schedule):
    new_schedule = []
    for batch in res_schedule:
        res_queries, cores = zip(*batch)
        new_res_queries = [q_index[q] for q in res_queries]
        new_batch = list(zip(new_res_queries, cores))
        new_schedule.append(new_batch)
    return new_schedule


def compute_new_schedule_proba(q_index, res_schedule):
    new_schedule = []
    for time_batch, batch in res_schedule:
        res_queries, cores = zip(*batch)
        new_res_queries = [q_index[q] for q in res_queries]
        new_batch = list(zip(new_res_queries, cores))
        new_schedule.append((time_batch, new_batch))
    return new_schedule


def compute_new_path(q_index, path_time):
    new_path_time = []
    for path_set, t in path_time:
        new_path_set = set([q_index[i] for i in path_set])
        new_path_time.append((new_path_set, t))
    return new_path_time


def compute_one_split(q_list_tot, q_index, res, C, R, precision, proba_variables=None, normal_exec=True,
                      reg_factor=None):
    q_list, new_proba_variables = redefine_list(q_list_tot, q_index, proba_variables)
    process_time, x = cp_optimizer.compute_result(q_list, res, C, R, precision, name_queries=False,
                                                  split=False, proba_variables=new_proba_variables,
                                                  normal_exec=normal_exec, reg_factor=reg_factor)

    if proba_variables is None:
        (runtime, res_schedule) = x
        new_schedule = compute_new_schedule(q_index, res_schedule)
        return process_time, (runtime, new_schedule)

    else:
        runtime, res_schedule, path_time, run_time, query_time = x
        new_schedule = compute_new_schedule_proba(q_index, res_schedule)
        new_path_time = compute_new_path(q_index, path_time)
        return process_time, (runtime, new_schedule, new_path_time, run_time, query_time)


def combine_split_results(left, right, proba_variables=None):
    if left is None:
        return right
    process_time_l, x_l = left
    process_time_r, x_r = right
    if proba_variables is None:
        runtime_l, schedule_l = x_l
        runtime_r, schedule_r = x_r

    else:
        runtime_l, schedule_l, path_time_l, run_time_l, query_time_l = x_l
        runtime_r, schedule_r, path_time_r, run_time_r, query_time_r = x_r

    process_time = process_time_l + process_time_r
    runtime = runtime_l + runtime_r
    schedule = schedule_l + schedule_r
    if proba_variables is None:
        return process_time, (runtime, schedule)
    else:
        # redefine path time right
        path_set_r, t_path_r = zip(*path_time_r)
        new_t_path_r = [runtime_l + t for t in t_path_r]
        new_path_time_r = list(zip(path_set_r, new_t_path_r))
        # redefine run_time/query_time right
        new_run_time_r = [runtime_l + r for r in run_time_r]
        new_query_time_r = [runtime_l + q for q in query_time_r]
        # combine
        path_time = path_time_l + new_path_time_r
        run_time = run_time_l + new_run_time_r
        query_time = query_time_l + new_query_time_r
        return process_time, (runtime, schedule, path_time, run_time, query_time)


def process_all(q_list, res, C, R, precision, proba_variables=None, reg_factor=None,
                normal_exec=True, normal_exec_split=False, query_name=True):

    res_split = split(q_list, range(len(q_list)), res, C, precision,
                      proba_variables=proba_variables, reg_factor=reg_factor, normal_exec=normal_exec_split)
    _, res_split_var = res_split
    final_res = None
    for i, sp in enumerate(res_split_var):
        if proba_variables is None:
            q_index = sp
            res_opti = compute_one_split(q_list, q_index, res, C, R, precision,
                                         normal_exec=normal_exec, reg_factor=reg_factor)
        else :
            q_index, split_proba_variables = sp
            res_opti = compute_one_split(q_list, q_index, res, C, R, precision,
                                         proba_variables=split_proba_variables,
                                         normal_exec=normal_exec, reg_factor=reg_factor)

        final_res = combine_split_results(final_res, res_opti, proba_variables=proba_variables)

    # See if time to change
    if proba_variables is None :
        return final_res
    else:
        _, _, path_sets_idx = proba_variables
        process_time, final = final_res
        runtime, res_schedule, path_time, run_time, query_time = final
        path_sets, path_time_res = zip(*path_time)
        if query_name:
            new_path_time = utils.recover_init_paths(path_sets_idx, path_time, q_list)
            new_res_schedule = utils.schedule_to_string(res_schedule, q_list)
            new_final = runtime, new_res_schedule, new_path_time, run_time, query_time
            return new_final
        else :
            new_path_time = utils.recover_init_paths(path_sets_idx, path_time)
            new_final = runtime, res_schedule, new_path_time, run_time, query_time
            return new_final


