#!/usr/bin/env python3
# encoding: utf-8

import sys
import argparse
from tqdm import tqdm

from model import Dataset, Model, tools

if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser()
    # data import
    parser.add_argument("--day_index", type=list, default=[0], help="Data day index, subset of [0, .. , 11], default: day0")
    # serverless compute node parameters
    parser.add_argument("--num_nodes", type=int, default=1, help="Number of compute nodes, default: 1")
    parser.add_argument("--node_mem_mb", type=int, default=8192, help="Memory capacity per node, default: 8GB")
    args = parser.parse_args()

    # load dataset
    data = Dataset()
    data.data_import(args.day_index)

    # create backend
    model = Model()
    model.add_compute_nodes(num_nodes=args.num_nodes, node_mem_mb=args.node_mem_mb)

    # run model
    for day in range(1, 2):  # 1..12 TODO: extend to full range
        for time in range(1, 5):  # 1..1440 TODO: extend to full range
            invocations = data.get_function_invocations(day, time)

            # iterate over all invocations of the minute time bin
            invocations_num = invocations.shape[0]  # calculate each minute's invocated number
            for i, invocation in tqdm(invocations.iterrows(),total=invocations_num):
                model.schedule(i, invocation, invocations_num, method='earliest_app')

            # update the duration after one minute
            rest_ms = invocations_num - i
            model.compute_nodes[0].update_minute_fun_duration(rest_ms)


            model.df_mem_available[0][time] = model.compute_nodes[0].mem_available()
            model.df_mem_usage[0][time] = args.node_mem_mb - model.compute_nodes[0].mem_available()

    # TODO: analysis
    tools.analysis_mem(model.df_mem_available,type='available')
    tools.analysis_mem(model.df_mem_usage,type='usage')

    # exit
    sys.exit(0)
