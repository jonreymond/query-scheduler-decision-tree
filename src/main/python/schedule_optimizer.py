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

@click.command()
@click.option(
    "--queries",
    default=(),
    type=list,
    help="list of queries to be scheduled",
)
@click.option(
    "--num_partitions",
    default=16,
    type=int,
    help="number of partitions",
)
def optimize_schedule(queries, num_partitions):
    # print("success")
    print("q1,q2;q3,q4;q3,q4,q5")

if __name__ == "__main__":
    # optimize_schedule()
    queries = ["q1", "q2", "q3", "q4"]
    num_partitions = '16'
    df_queries = utils.load(queries, num_partitions)
    q_interp = utils.interpolate(df_queries)

    print("done")

