#!/usr/bin/env python3
# encoding: utf-8

import random
import pandas as pd


class ComputeNode:

    def __init__(self, index, mem_mb=8192):
        self.index = index
        self.total_mem = mem_mb
        self.mem_avail = self.total_mem
        self.total_wait_min = 0
        self.function_store = pd.DataFrame(
            columns=["HashApp", "HashFunction", "AverageMem", "AverageDuration", "ExecuteDuration", "InvocationCount", "LastUsed",
                     "ColdStartCount"]) # TODO: maybe call AverageMem AverageAppMemory?

    def mem_available(self):
        return self.mem_avail

    def mem_total(self):
        return self.total_mem

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
            "ExecuteDuration": [0],  # see how long the app has already execute, metrics is millisecond
            "InvocationCount": [0],  # TODO: add performance metrics
            "LastUsed": [0],  # TODO: add performance metrics
            "ColdStartCount": [0]  # TODO: add performance metrics
        })
        self.function_store = pd.concat([self.function_store, df], axis=0)

    def add_duration(self):
        # add 1ms to the execute duration
        self.function_store.loc[:,'ExecuteDuration'] += 1

    def update_minute_fun_duration(self, rest_ms):
        # add the rest milliseconds in one minute after execute all the invocations in this time
        rest_ms = rest_ms-self.total_wait_min
        if rest_ms<0:
            # avoid smaller than zero
            rest_ms=0
        self.function_store['ExecuteDuration'] += rest_ms

    def reset_fun_duration(self, invocation):
        self.function_store.loc[invocation['HashFunction'] == self.function_store['HashFunction'],'ExecuteDuration'] = 0

    def remove_app(self, invocation):
        """release the selected invocation memory, 
           at the same time remove the app contains this invocation

        Args:
            invocation (Series): from the function_store
        """
        self.function_store = self.function_store[self.function_store["HashApp"] != invocation["HashApp"]]
        self.mem_avail += invocation["AverageMem"]

    def function_exists(self, invocation):
        return invocation["HashFunction"] in self.function_store["HashFunction"].values

    def app_exists(self, invocation):
        return invocation["HashApp"] in self.function_store["HashApp"].values

    def get_finished_fun_df(self):
        if self.function_store[self.function_store['ExecuteDuration']>=self.function_store['AverageDuration']]['ExecuteDuration'].isnull().all():
            tmp = self.function_store['AverageDuration'] - self.function_store['ExecuteDuration']
            wait_min = min(tmp)
            self.total_wait_min += wait_min
            self.function_store.loc[:,'ExecuteDuration'] += wait_min
        return self.function_store[self.function_store['ExecuteDuration']>=self.function_store['AverageDuration']]

    def get_random_app(self):
        # TODO: if the len(finished_app_df) equals to 0?
        finished_app_df = self.get_finished_fun_df()
        return finished_app_df.iloc[random.randrange(0,len(finished_app_df))]

    def get_earliest_app(self):
        finished_app_df = self.get_finished_fun_df()
        return finished_app_df.iloc[0]

    def get_largest_mem_app(self):
        finished_app_df = self.get_finished_fun_df()
        tmp = finished_app_df['AverageMem'].copy()
        return finished_app_df.iloc[tmp.astype('int64').argmax(),:]
