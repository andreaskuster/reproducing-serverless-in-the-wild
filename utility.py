#!/usr/bin/env python3
# encoding: utf-8

from matplotlib.colors import Normalize
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt

def fetch_cold_start_percentage(model):
    if model.InvocationCount == 0:
        return 0
    return model.ColdStartCount/model.InvocationCount * 100


def fetch_wasted_memory(function_store):
    waste_memory = 0
    for i in range(len(function_store)):
        function = function_store.iloc[i]
        function_from_same_app = function_store[function_store["HashApp"] == function["HashApp"]]
        if len(function_from_same_app[function_from_same_app["ExecuteDuration"] < function_from_same_app["AverageDuration"]]) == 0 :
            waste_memory += function["AverageMem"]
    return waste_memory


def fetch_app_wise_wasted_memory_time(model):
    function_store = model.compute_nodes[0].function_store
    for i in range(len(model.app_record)):
        function_from_same_app = function_store[function_store["HashApp"] == model.app_record.iloc[i]["HashApp"]]
        if len(function_from_same_app) == 0:
            continue
        if len(function_from_same_app[function_from_same_app["ExecuteDuration"] < function_from_same_app["AverageDuration"]]) == 0 :
            model.app_record.iloc[i, -1] = model.app_record.iloc[i, -1] + 1
    return model


def record_app_wise_inforamtion(model, dir_result):
    app_len = len(model.app_record)
    cold_start = np.zeros(app_len)
    waste_memory_time = np.zeros(app_len)
    for i in range(app_len):
        cold_start[i] = model.app_record.iloc[i]["ColdStartCount"] / model.app_record.iloc[i]["InvocationCount"] * 100
        waste_memory_time[i] = model.app_record.iloc[i]["WasteMemoryTime"]
    
    np.savez(dir_result, cold_start=cold_start, waste_memory_time=waste_memory_time)
    print(cold_start)
    print(waste_memory_time)
    return


def result_analysis():
    dir_hybrid = "./result/hybrid_1h_result.npz"
    dir_keep_alive = "./result/keep_alive_1h_result.npz"

    hybrid = np.load(dir_hybrid)
    keep_alive = np.load(dir_keep_alive)

    cold_hybrid, cold_keep = hybrid["cold_start"], keep_alive["cold_start"]
    waste_hybrid, waste_keep = hybrid["waste_memory_time"], keep_alive["waste_memory_time"]

    plt.figure()
    plt.hist(cold_hybrid, cumulative=True, histtype='step', density=True, color='b')
    plt.hist(cold_keep, cumulative=True, histtype='step', density=True, color='r')
    plt.xlabel("App Cold Start (%)")
    plt.ylabel("CDF")
    plt.legend(["Hybrid", "10-min Fixed"])
    plt.savefig('./cold_hybrid.png')

    plt.figure()
    plt.hist(waste_hybrid, cumulative=True, histtype='step', density=True, color='b')
    plt.hist(waste_keep, cumulative=True, histtype='step', density=True, color='r')
    plt.xlabel("Waste Memory Time")
    plt.ylabel("CDF")
    plt.legend(["Hybrid", "10-min Fixed"])
    plt.savefig('./waste_hybrid.png')


if __name__ == "__main__":
    result_analysis()