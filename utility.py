#!/usr/bin/env python3
# encoding: utf-8

import pandas as pd
import numpy as np

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
    for i in range(len(function_store)):
        function = function_store.iloc[i]
        function_from_same_app = function_store[function_store["HashApp"] == function["HashApp"]]
        if len(function_from_same_app[function_from_same_app["ExecuteDuration"] < function_from_same_app["AverageDuration"]]) == 0 :
            model.app_record.loc[model.app_record["HashApp"] == function["HashApp"], "WasteMemoryTime"][0] += 1
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