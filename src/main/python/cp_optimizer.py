from ortools.sat.python import cp_model
import numpy as np
import time
import utils

# maximal length algorithm can scale
MAX_LEN_QUERIES = 9
reg_factor = 0.01

'''
Initialize matrix T of runtime foreach query :
T_ij = runtime of query i with j cores
'''


def init_matrix(q_list, res, C, precision):
    Q = len(q_list)
    T = np.zeros((Q, C))
    for q in range(Q):
        for c in range(C):
            T[q, c] = res[q_list[q]](c + 1)
    T_int = np.rint(T * precision).astype(int)
    return np.c_[np.zeros(T_int.shape[0]), T_int].astype(int)


def init_variables(model, T, Q, C, R, num_paths=None):
    V = {(q, r): model.NewIntVar(0, C, f'V_{q},{r}') for q in range(Q) for r in range(R)}
    I = {(q, r): model.NewIntVar(0, R - 1, f'I_{q},{r}') for q in range(Q) for r in range(R)}
    X = {q: model.NewIntVar(1, C, f'X_{q}') for q in range(Q)}
    k = {r: model.NewIntVar(0, T.max(), f'k_{r}') for r in range(R)}
    t_ind = {(q, r): model.NewIntVar(0, T.max(), f't_ind_{q},{r}') for q in range(Q) for r in range(R)}

    A = np.zeros((Q, R)).astype(int).astype(object)
    for q in range(Q):
        A[q, -1] = X[q]

    if num_paths is None:
        return V, I, X, k, t_ind, A, None
    else :
        V_bool = {(q, r): model.NewBoolVar(f'V_bool_{q},{r}') for q in range(Q) for r in range(R)}
        index_run = {q: model.NewIntVar(0, R - 1, f'index_run_{q}') for q in range(Q)}
        #TODO :check if can reduce range
        runtime_runs = {r: model.NewIntVar(T.min() * Q, T.max() * Q, f'run_r_{r}') for r in range(R)}

        runtime_queries = {q: model.NewIntVar(T.min() * Q, T.max() * Q, f'run_q_{q}') for q in range(Q)}
        runtime_paths = {p: model.NewIntVar(T.min() * Q, T.max() * Q, f'run_path_{q}') for p in range(num_paths)}
        return V, I, X, k, t_ind, A, (V_bool, index_run, runtime_runs, runtime_queries, runtime_paths)

def define_program(model, variables, T, Q, C, R, C_=None, proba_variables=None):
    (V, I, X, k, t_ind, A, remain) = variables
    # 1
    for q in range(Q):
        for r in range(R):
            model.AddElement(I[q, r], list(A[q, :]), V[q, r])
        model.AddAllDifferent([I[q, r] for r in range(R)])
    # 2
    for r in range(R):
        if C_ is None:
            model.Add(sum(V[q, r] for q in range(Q)) <= C)
        else:
            model.Add(sum(V[q, r] for q in range(Q)) == C_)
    # 3
    for q in range(Q):
        for r in range(R):
            model.AddElement(V[q, r], list(T[q, :]), t_ind[q, r])
    for r in range(R):
        model.AddMaxEquality(k[r], [t_ind[q, r] for q in range(Q)])

    obj = sum(k[r] for r in range(R))
    if proba_variables is not None :
        obj = define_proba_program(model, variables, Q, R, proba_variables)

    model.Minimize(obj)


def define_proba_program(model, variables, Q, R, proba_variables):
    #Get index of which run each query is
    (V, I, X, k, t_ind, A, remain) = variables
    V_bool, index_run, runtime_runs, runtime_queries, runtime_paths = remain
    probas, num_paths, path_sets_idx = proba_variables
    for q in range(Q):
        for r in range(R):
            model.Add(V[q,r] > 0).OnlyEnforceIf(V_bool[q,r])
            model.Add(V[q,r] == 0).OnlyEnforceIf(V_bool[q,r].Not())

    for q in range(Q):
        model.Add(index_run[q]==0).OnlyEnforceIf([V_bool[q,r].Not() for r in range(R)])
        for r in range(R):
            model.Add(index_run[q]== r).OnlyEnforceIf(V_bool[q,r])

    #Get runtime of each run
    for r in range(R):
        model.Add(runtime_runs[r] == sum(k[rr] for rr in range(r + 1)))

    #Get runtime of each query
    for q in range(Q):
        model.AddElement(index_run[q], runtime_runs, runtime_queries[q])
    #Set path runtime
    for (id_p, path_set) in enumerate(path_sets_idx):
        model.AddMaxEquality(runtime_paths[id_p], [runtime_queries[q] for q in path_set])

    obj = sum(probas[p] * runtime_paths[p] for p in range(num_paths)) +  reg_factor *sum(k[r] for r in range(R))
    return obj


def model_to_solution(solver, R, V, k, q_list, precision):
    Q = len(q_list)
    res_schedule = []
    for r in range(R):
        run_r = []
        for q in range(Q):
            q_r_val = round(solver.Value(V[(q, r)]))
            if q_r_val > 0:
                run_r.append((q_list[q], q_r_val))
        if run_r != []:
            res_schedule.append(run_r)
    runtime = 0
    for r in range(R):
        runtime += solver.Value(k[r])

    return runtime / precision, res_schedule


def optimize(q_list, res, C, R, precision, probas=None, C_=None):
    Q = len(q_list)
    T = init_matrix(q_list, res, C, precision)
    model = cp_model.CpModel()
    proba_variables = None
    if probas is None:
        variables = init_variables(model, T, Q, C, R)
    else :
        path_sets_idx = utils.get_path_sets(range(len(q_list)))
        num_paths = len(path_sets_idx)
        variables = init_variables(model, T, Q, C, R, num_paths)
        proba_variables = (probas, num_paths, path_sets_idx)

    define_program(model, variables, T, Q, C, R, C_, proba_variables)

    start_time = time.time()
    solver = cp_model.CpSolver()
    status = solver.Solve(model)
    process_time = time.time() - start_time

    if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
        (V, _, _, k, _, _) = variables
        runtime, res_schedule = model_to_solution(solver, R, V, k, q_list, precision)
    else:
        runtime, res_schedule = -1, []

    return process_time, runtime, res_schedule


def split(q_list, res, C, precision):
    if len(q_list) > MAX_LEN_QUERIES:
        R = 2
        Q = len(q_list)
        C_ = 2 * Q
        process_time, _, res_schedule = optimize(q_list, res, C, R, precision, C_)
        q_left, runtime_left = split([x[0] for x in res_schedule[0]], res, C, precision)
        q_right, runtime_right = split([x[0] for x in res_schedule[1]], res, C, precision)

        return q_left + q_right, process_time + runtime_right + runtime_left
    else:
        return [q_list], 0


def combine_results(results):
    process_time = sum(x[0] for x in results)
    runtime = sum(x[1] for x in results)
    res_schedule = []
    for r_s in [x[2] for x in results]:
        res_schedule += r_s
    return process_time, runtime, res_schedule
