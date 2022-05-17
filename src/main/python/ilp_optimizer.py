import cvxpy as cp
import numpy as np
import mosek
import numpy as np
import pandas as pd

#rel_gap - 0.25
def optimize(q_list, res, C=16, R=None, rel_gap = 0.01):
    Q = len(q_list)
    if R is None:
        R = int(round(Q / 2))

    (T, X, k, constr) = init(q_list, res, Q, C, R)

    prob = cp.Problem(cp.Minimize(k @ np.ones(R)),
                      constr)

    prob.solve(solver=cp.MOSEK,
           mosek_params = {mosek.dparam.mio_tol_rel_gap : rel_gap},
           verbose=False)

    return get_results(X, q_list, Q, C, R)


def get_results(X, q_list, Q, C, R):
    res_schedule = []
    for r in range(R):
        run_r = []
        for q in range(Q):
            for c in range(C):
                if round(X[q].value[c,r]) == 1:
                    run_r.append((q_list[q], c + 1))
        if run_r != []:
            res_schedule.append(run_r)
    return res_schedule


def init(q_list, res, Q, C, R):
    T = np.zeros((Q, C))
    for q in range(Q):
        for c in range(C):
            T[q, c] = res[q_list[q]](c + 1)

    X = []
    for q in range(Q):
        X.append(cp.Variable((C, R), boolean = True))

    k = cp.Variable(R)

    constr = []
    #1
    for q in range(Q):
        constr.append(np.ones(C) @ X[q] @ np.ones(R) == 1)
    #2
    temp = []
    cores = np.arange(1, C +1)
    for q in range(Q):
        temp.append(cores @ X[q])
    constr.append(sum(temp) <= C)
    #3
    for i in range(Q):
        constr.append(T[i, :].T @ X[i] <= k)
    return T, X, k, constr




