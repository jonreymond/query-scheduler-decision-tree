import os
import pathlib as plib
import time
from datetime import datetime
import click
import matplotlib.pyplot as plt
import numpy as np
import utils
import cp_optimizer
import ast
import splitter


class PythonLiteralOption(click.Option):

    def type_cast_value(self, ctx, value):
        try:
            return ast.literal_eval(value)
        except:
            raise click.BadParameter(value)


@click.command()
@click.option(
    "--queries",
    cls=PythonLiteralOption,
    help="list of queries to be scheduled",
)
@click.option(
    "--num_partitions",
    default=16,
    type=int,
    help="number of partitions",
)
@click.option(
    "--probas",
    cls=PythonLiteralOption,
    help="probabilities list",
)
def optimize_schedule(queries, num_partitions, probas):
    # print("success")
    print(queries)
    print(num_partitions)
    # print(probas)
    print([float(i) for i in probas])
    print("done")


if __name__ == "__main__":
    # optimize_schedule()

    # runtime example without split#
    # q_list = ["q1", "q3", "q5", "q4", "q2", "q5", "q1"]
    # res = utils.interpolate(utils.load(q_list, num_partitions='16'))
    # precision = 1000
    # C = 16
    # C_ = None  # for split
    # Q = len(q_list)
    # R = int(round(Q / 2))
    # # reg_factor = 0.001
    #
    # # with probas : make sure that length q_list = 2**x - 1
    # path_sets_idx = utils.get_path_sets(range(len(q_list)))
    # num_paths = len(path_sets_idx)
    # probas = np.random.random(size=num_paths)
    # probas = list(probas / sum(probas))
    # probas_variables = None
    # proba_variables = cp_optimizer.get_proba_variables(q_list, probas)
    #
    # process_time, r = cp_optimizer.compute_result(q_list, res, C, R, precision,
    #                                               name_queries=True, C_=None, proba_variables=proba_variables)
    # print("process time : ", process_time)
    # if proba_variables is None:
    #     runtime, res_schedule = r
    #     print(runtime, res_schedule)
    # else:
    #     cp_optimizer.print_proba_results(r)
    #     runtime, res_schedule, path_time, run_time, query_time = r


    # Split example #
    seed = 2
    q_list = ["q1", "q3", "q5", "q4", "q2", "q5", "q1",
              "q1", "q3", "q5", "q4", "q2", "q5", "q1", "q1"]
    #  ,
    # "q1", "q3", "q5", "q4","q2", "q5", "q1",
    # "q1", "q3", "q5", "q4","q2", "q5", "q1", "q1", "q5"]
    res = utils.interpolate(utils.load(q_list, num_partitions='16'))
    precision = 1000
    C = 16
    C_ = None  # for split
    Q = len(q_list)
    R = int(round(Q / 2))

    path_sets_idx = utils.get_path_sets(range(len(q_list)))
    num_paths = len(path_sets_idx)
    np.random.seed(seed)
    probas = np.random.random(size=num_paths)
    probas = list(probas / sum(probas))
    proba_variables = utils.get_proba_variables(q_list, probas)

    reg_factor = None  # 0.001
    normal_exec = True
    normal_exec_split = False

    new_final = splitter.process_all(q_list, res, C, R, precision, proba_variables,
                                     reg_factor, normal_exec, normal_exec_split, query_name=True)
    utils.print_proba_results(new_final)
