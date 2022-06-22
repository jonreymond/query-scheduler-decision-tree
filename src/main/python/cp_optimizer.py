from ortools.sat.python import cp_model
import numpy as np
import time
import utils

# maximal length algorithm can scale
MAX_LEN_QUERIES = 9
MODEL_STATUS = ["UNKNOWN", "MODEL_INVALID", "FEASIBLE", "IN-FEASIBLE", "OPTIMAL"]
# normal_exec = False

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


def init_variables(model, T, Q, C, R, proba_variables=None):
    V = {(q, r): model.NewIntVar(0, C, f'V_{q},{r}') for q in range(Q) for r in range(R)}
    I = {(q, r): model.NewIntVar(0, R - 1, f'I_{q},{r}') for q in range(Q) for r in range(R)}
    X = {q: model.NewIntVar(1, C, f'X_{q}') for q in range(Q)}
    k = {r: model.NewIntVar(0, int(T.max()), f'k_{r}') for r in range(R)}
    t_ind = {(q, r): model.NewIntVar(0, int(T.max()), f't_ind_{q},{r}') for q in range(Q) for r in range(R)}

    A = np.zeros((Q, R)).astype(int).astype(object)
    for q in range(Q):
        A[q, -1] = X[q]

    if proba_variables is None:
        return V, I, X, k, t_ind, A, None
    else:
        probas, num_paths, _ = proba_variables
        max_proba = max(utils.probas_to_int(probas))
        V_bool = {(q, r): model.NewBoolVar(f'V_bool_{q},{r}') for q in range(Q) for r in range(R)}
        index_run = {q: model.NewIntVar(0, int(R - 1), f'index_run_{q}') for q in range(Q)}

        # TODO :check if can reduce range : T.min()= 0 now
        min_time = int(0)
        max_time = int(T.max() * R)
        runtime_runs = {r: model.NewIntVar(min_time, max_time, f'run_r_{r}') for r in range(R)}

        runtime_queries = {q: model.NewIntVar(min_time, max_time, f'run_q_{q}') for q in range(Q)}
        runtime_paths = {p: model.NewIntVar(min_time, max_time, f'run_path_{q}') for p in range(num_paths)}

        max_runtime_path = model.NewIntVar(min_time, int(max_time * max_proba), f'max_run_path')

        return V, I, X, k, t_ind, A, (V_bool, index_run, runtime_runs, runtime_queries, runtime_paths, max_runtime_path)


def define_program(model, variables, T, Q, C, R, C_=None, proba_variables=None, normal_exec=True, reg_factor=None):
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
    if proba_variables is not None:
        obj = define_proba_program(model, variables, Q, R, proba_variables, normal_exec, reg_factor)

    model.Minimize(obj)


def define_proba_program(model, variables, Q, R, proba_variables, normal_exec=True, reg_factor=None):
    # Get index of which run each query is
    (V, I, X, k, t_ind, A, remain) = variables
    #CP variables for probability case
    V_bool, index_run, runtime_runs, runtime_queries, runtime_paths, max_runtime_path = remain
    #fixed variables
    probas, num_paths, path_sets_idx = proba_variables
    probas_int = utils.probas_to_int(probas)
    #Get runtime of each run
    for r in range(R):
        model.Add(runtime_runs[r] == sum(k[rr] for rr in range(r + 1)))

    #Get index of which run each query is
    for q in range(Q):
        for r in range(R):
            model.Add(V[q,r] > 0).OnlyEnforceIf(V_bool[q,r])
            model.Add(V[q,r] == 0).OnlyEnforceIf(V_bool[q,r].Not())

    for q in range(Q):
        # model.Add(index_run[q]==0).OnlyEnforceIf([V_bool[q,r].Not() for r in range(R)])
        model.Add(runtime_queries[q]==0).OnlyEnforceIf([V_bool[q,r].Not() for r in range(R)])
        for r in range(R):
            # model.Add(index_run[q]== r).OnlyEnforceIf(V_bool[q,r])
            model.Add(runtime_queries[q]== runtime_runs[r]).OnlyEnforceIf(V_bool[q,r])

    #Set path runtime
    for (id_p, path_set) in enumerate(path_sets_idx):
        model.AddMaxEquality(runtime_paths[id_p], [runtime_queries[q] for q in path_set])

    if normal_exec:
        # print("normal exec")
        obj = sum(probas[p] * runtime_paths[p] for p in range(num_paths))
    else :
        # print("alternative exec")
        model.AddMaxEquality(max_runtime_path, [probas_int[p] * runtime_paths[p] for p in range(num_paths)])
        obj = max_runtime_path

    #using reg_factor
    # obj = obj + reg_factor * sum(k[r] for r in range(R))
    return obj


def optimize(q_list, res, C, R, precision, C_=None, proba_variables=None, normal_exec=True, reg_factor=None):
    Q = len(q_list)
    T = init_matrix(q_list, res, C, precision)
    model = cp_model.CpModel()
    if proba_variables is None:
        variables = init_variables(model, T, Q, C, R)
    else:
        variables = init_variables(model, T, Q, C, R, proba_variables=proba_variables)

    define_program(model, variables, T, Q, C, R, C_, proba_variables, normal_exec, reg_factor)

    start_time = time.time()
    solver = cp_model.CpSolver()
    status = solver.Solve(model)
    process_time = time.time() - start_time
    assert status == cp_model.OPTIMAL or status == cp_model.FEASIBLE, "status is " + MODEL_STATUS[status]
    return process_time, (solver, R, variables, q_list, precision)




def model_to_solution(solver, R, variables, q_list, precision, name_queries=True, proba_variables=None, split=False):
    (V, I, X, k, t_ind, A, remain) = variables
    if split:
        name_queries = False
        proba_variables = None
    if proba_variables is not None and not split:
        V_bool, index_run, runtime_runs, runtime_queries, runtime_paths, max_runtime_path = remain
        probas, num_paths, path_sets_idx = proba_variables

    Q = len(q_list)
    res_schedule = []
    for r in range(R):
        run_r = []
        for q in range(Q):
            q_r_val = round(solver.Value(V[(q, r)]))
            if q_r_val > 0:
                if name_queries:
                    run_r.append((q_list[q], q_r_val))
                else:
                    run_r.append((q, q_r_val))
        if run_r:
            if proba_variables is not None and not split:
                res_schedule.append((solver.Value(k[r])/ precision, run_r))
            else:
                res_schedule.append(run_r)
    runtime = sum(solver.Value(k[r]) for r in range(R))

    if proba_variables is not None and not split:
        if name_queries :
            path_time = [([q_list[i] for i in list(path_sets_idx[p])], solver.Value(runtime_paths[p])/precision) for p in range(num_paths)]
        else :
            path_time = [(path_sets_idx[p], solver.Value(runtime_paths[p])/precision) for p in range(num_paths)]
        run_time = [solver.Value(runtime_runs[r])/precision for r in range(R)]
        query_time = [solver.Value(runtime_queries[q])/precision for q in range(Q)]
        return runtime / precision, res_schedule, path_time, run_time, query_time
    else:
        return runtime / precision, res_schedule


def compute_result(q_list, res, C, R, precision, name_queries=True, C_=None, split=False, proba_variables=None, normal_exec=True,reg_factor=None):
    process_time, x = optimize(q_list, res, C, R, precision, C_, proba_variables,normal_exec, reg_factor)
    return process_time, model_to_solution(*x, name_queries=name_queries, proba_variables=proba_variables, split=split)


