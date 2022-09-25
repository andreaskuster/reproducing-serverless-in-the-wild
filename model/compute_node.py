#!/usr/bin/env python3
# encoding: utf-8

import pandas as pd


class ComputeNode:

    def __init__(self, index, mem_mb=8192):
        self.index = index
        self.total_mem = mem_mb
        self.mem_avail = self.total_mem
        self.function_store = pd.DataFrame(
            columns=["HashApp", "HashFunction", "AverageMem", "AverageDuration", "InvocationCount", "LastUsed",
                     "ColdStartCount"])

    def mem_available(self):
        return self.mem_avail

    def function_invocation(self):
        # This should be called on every function invocation to update internal metrics (e.g. #function calls,
        # last time of function call (to check if the function is still in execution, and thus cannot get evicted yet)
        # #cold starts / #warm starts..
        raise NotImplementedError()

    def add_function(self, invocation):
        # cold start -> add data to memory
        if not self.app_exists(invocation):  # TODO: this is a cold-start -> we should add it as a metric
            self.mem_avail -= invocation["AverageMem"]

        # prep data frame
        df = pd.DataFrame({
            "HashApp": [invocation["HashApp"]],
            "HashFunction": [invocation["HashFunction"]],
            "AverageMem": [invocation["AverageMem"]],
            "AverageDuration": [invocation["AverageDuration"]],
            "InvocationCount": [0],  # TODO: add performance metrics
            "LastUsed": [0],  # TODO: add performance metrics
            "ColdStartCount": [0]  # TODO: add performance metrics
        })
        self.function_store = pd.concat([self.function_store, df], axis=0)

    def remove_app(self, invocation):
        self.function_store = self.function_store[self.function_store["HashApp"] != invocation["HashApp"]]
        self.mem_avail += invocation["AverageMem"]

    def function_exists(self, invocation):
        return invocation["HashFunction"] in self.function_store["HashFunction"].values

    def app_exists(self, invocation):
        return invocation["HashApp"] in self.function_store["HashApp"].values

    def get_random_app(self):
        return self.function_store.iloc[0]
