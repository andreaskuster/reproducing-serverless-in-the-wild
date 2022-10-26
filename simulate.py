#!/usr/bin/env python3
# encoding: utf-8

from ast import parse
import sys
import argparse
from xmlrpc.client import boolean
from tqdm import tqdm
from Controller.controller import Controller
import numpy as np

from model import Dataset, Model, tools
from utility import *

if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser()
    # data import
    parser.add_argument("--day_index", type=list, default=[2], help="Data day index, subset of [0, .. , 11], default: day0")
    parser.add_argument("--max_time", type=int, default=60, help="Time period for simuation, maximum = 1440")
    # serverless compute node parameters
    parser.add_argument("--num_nodes", type=int, default=1, help="Number of compute nodes, default: 1")
    parser.add_argument("--node_mem_mb", type=int, default=1024 * 2048, help="Memory capacity per node, default: 2T")
    parser.add_argument("--method", type=str, default='hybrid', choices=['keep_alive', 'hybrid', 'reinfored'], help="Controller stragety for pre-warming window and keep-alive window")
    parser.add_argument("--fast_read", type=boolean, default=True, help="read data saved in 'app_xxx' ")
    parser.add_argument("--dir_name", type=str, default="test", help="dir to save result")
    args = parser.parse_args()

    assert args.max_time <= 1440

    args.dir_result = "./result/" + args.dir_name
    # performance evaluation array
    cold_start_percentage = np.zeros(args.max_time)
    wasted_memory = np.zeros(args.max_time)

    # load dataset
    data = Dataset()
    if args.fast_read:
        data.read_data_from_parse_data()
    else:
        data.data_import(args.day_index)
    
    # create backend
    model = Model()
    model.add_compute_nodes(num_nodes=args.num_nodes, node_mem_mb=args.node_mem_mb)

    controller = Controller(args.method)

    # run model
    for day in args.day_index:  # 1..12 TODO: extend to full range
        for time in range(1, args.max_time + 1):  # 1..1440 TODO: extend to full range
            model.release_app(time)  # delete pre-warm app
            model.load_app(time)    # load keep-alive app
            
            model = fetch_app_wise_wasted_memory_time(model)  # data analysis

            invocations = data.get_function_invocations(day, time)

            # iterate over all invocations of the minute time bin
            invocations_num = invocations.shape[0]  # calculate each minute's invocated number``
            i_record = 0
            for i, invocation in tqdm(invocations.iterrows(),total=invocations_num):
                invocation = controller.set_window(invocation, time)
                model.schedule(i, invocation, invocations_num, method='earliest_app')
                i_record = i
                # if i > 1000:
                #     break

            # update the duration after one minute
            rest_ms = invocations_num - i_record
            model.compute_nodes[0].update_minute_fun_duration(rest_ms)

            model.df_mem_available[0][time] = model.compute_nodes[0].mem_available()
            model.df_mem_usage[0][time] = args.node_mem_mb - model.compute_nodes[0].mem_available()


    model = fetch_app_wise_wasted_memory_time(model)
    record_app_wise_inforamtion(model, args.dir_result)

    # TODO: analysis
    tools.analysis_mem(model.df_mem_available,type='available')
    tools.analysis_mem(model.df_mem_usage,type='usage')

    # print(f"Cold start percentage change with time {cold_start_percentage}")
    # print(f"Total cold start percentage is {cold_start_percentage[-1]}")
    # print(f"Waste Memory change along with time {wasted_memory}")
    # exit
    sys.exit(0)
