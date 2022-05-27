from ortools.sat.python import cp_model
import numpy as np
import time

'''
Initialize matrix T of runtime foreach query :
T_ij = runtime of query i with j cores
'''
def init_matrix(q_list, res, Q, C, precision):
    T = np.zeros((Q, C))
    for q in range(Q):
        for c in range(C):
            T[q, c] = res[q_list[q]](c + 1)
    T_int = np.rint(T * precision).astype(int)
    return np.c_[np.zeros(T_int.shape[0]),  T_int].astype(int)

def init_variables(model, T, Q, C, R):
    V = {(q,r): model.NewIntVar(0, C, f'V_{q},{r}') for q in range(Q) for r in range(R)}
    I = {(q,r): model.NewIntVar(0, R -1, f'I_{q},{r}') for q in range(Q) for r in range(R)}
    X = {q: model.NewIntVar(1, C, f'X_{q}') for q in range(Q)}
    k = {r: model.NewIntVar(0, T.max(), f'k_{r}') for r in range(R)}
    t_ind = {(q,r): model.NewIntVar(0, T.max(), f't_ind_{q},{r}') for q in range(Q) for r in range(R)}

    A = np.zeros((Q, R)).astype(int).astype(object)
    for q in range(Q):
        A[q, -1] = X[q]
    return V, I, X, k, t_ind, A


def define_program(model, variables,T, Q, C, R, C_ = None):
    (V, I, X, k, t_ind, A) = variables
    #1
    for q in range(Q):
        for r in range(R):
            model.AddElement(I[q,r], list(A[q,:]), V[q,r])
        model.AddAllDifferent([I[q,r] for r in range(R)])
    #2
    for r in range(R):
        if C_ is None:
            model.Add(sum(V[q,r] for q in range(Q)) <= C)
        else :
            model.Add(sum(V[q,r] for q in range(Q)) == C_)
    #3
    for q in range(Q):
        for r in range(R):
            model.AddElement(V[q,r] , list(T[q,:]), t_ind[q,r])
    for r in range(R):
        model.AddMaxEquality(k[r], [t_ind[q,r] for q in range(Q)])

    obj = sum(k[r] for r in range(R))
    model.Minimize(obj)


def model_to_solution(solver, Q, R, V, k, q_list, precision):
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

    return runtime/ precision, res_schedule


def optimize(q_list, res, Q, C, R, precision):
    T = init_matrix(q_list, res, Q, C, precision)
    model = cp_model.CpModel()
    variables = init_variables(model, T, Q, C, R)
    define_program(model, variables, T, Q, C, R)

    start_time = time.time()
    solver = cp_model.CpSolver()
    status = solver.Solve(model)
    process_time = time.time() - start_time

    if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
        runtime, res_schedule = model_to_solution(solver, Q, R, V, k, q_list, precision)
    else:
        runtime, res_schedule = -1, []

    process_time, runtime, res_schedule




