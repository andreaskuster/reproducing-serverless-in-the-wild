from re import L
from xml.dom import InvalidModificationErr




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