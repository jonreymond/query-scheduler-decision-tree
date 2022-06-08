import os
import pathlib as plib
import time
from datetime import datetime
import click
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import cvxpy as cp
import utils
import ilp_optimizer
import ast


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
    optimize_schedule()
#     queries = ["q1", "q2", "q3", "q4"]
#     num_partitions = '16'
#     df_queries = utils.load(queries, num_partitions)
#     q_interp = utils.interpolate(df_queries)
#     result = ilp_optimizer.optimize(queries, q_interp)
#     print(result)
#
#     print("done")

