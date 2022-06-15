from ortools.sat.python import cp_model
import numpy as np
import time

# maximal length algorithm can scale
MAX_LEN_QUERIES = 9
epsilon = 0.1

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


def init_variables(model, T, Q, C, R):
    V = {(q, r): model.NewIntVar(0, C, f'V_{q},{r}') for q in range(Q) for r in range(R)}
    I = {(q, r): model.NewIntVar(0, R - 1, f'I_{q},{r}') for q in range(Q) for r in range(R)}
    X = {q: model.NewIntVar(1, C, f'X_{q}') for q in range(Q)}
    k = {r: model.NewIntVar(0, T.max(), f'k_{r}') for r in range(R)}
    t_ind = {(q, r): model.NewIntVar(0, T.max(), f't_ind_{q},{r}') for q in range(Q) for r in range(R)}

    A = np.zeros((Q, R)).astype(int).astype(object)
    for q in range(Q):
        A[q, -1] = X[q]
    return V, I, X, k, t_ind, A


def define_program(model, variables, T, Q, C, R, probas, C_=None):
    (V, I, X, k, t_ind, A) = variables
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

    # 4 probabilities
    V_norm = V / sum(V)
        
    reg = sum([int((1 - epsilon)**r * 10000) * sum([probas[q] * V_norm[q, r] for q in range(Q)]) for r in range(R)])

    obj = sum(k[r] for r in range(R)) + reg
    model.Minimize(obj)


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


def optimize(q_list, res, C, R, precision, probas, C_=None):
    Q = len(q_list)
    T = init_matrix(q_list, res, C, precision)
    model = cp_model.CpModel()
    variables = init_variables(model, T, Q, C, R)
    probas_int = int(probas * 10000)
    define_program(model, variables, T, Q, C, R, probas_int, C_)

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
