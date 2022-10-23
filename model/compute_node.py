#!/usr/bin/env python3
# encoding: utf-8

import random
from numpy import arange, empty
import pandas as pd


class ComputeNode:

    def __init__(self, index, mem_mb=8192):
        self.index = index
        self.total_mem = mem_mb
        self.mem_avail = self.total_mem
        self.total_wait_min = 0
        self.function_store = pd.DataFrame(
            columns=["HashApp", "HashFunction", "AverageMem", "AverageDuration", "pre_warm_window", "keep_alive_window", 
                     "ArrivalTime", "ExecuteDuration", "InvocationCount", "LastUsed", "ColdStartCount"]) # TODO: maybe call AverageMem AverageAppMemory?

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
            "pre_warm_window": [invocation["pre_warm_window"]],  # based on schedule
            "keep_alive_window": [invocation["keep_alive_window"]],  # based on schedule
            "ArrivalTime": [invocation["arrival_time"]],  # based on arrival time
            "ExecuteDuration": [0],  # see how long the app has already execute
            "InvocationCount": [0],  # TODO: add performance metrics
            "LastUsed": [0],  # TODO: add performance metrics
            "ColdStartCount": [1]  # The first start is always cold start.
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

    def update_app_property(self, invocation):
        pos = invocation['HashApp'] == self.function_store['HashApp']
        self.function_store.loc[pos,'pre_warm_window'] = invocation["pre_warm_window"]
        self.function_store.loc[pos,'keep_alive_window'] = invocation["keep_alive_window"]
        self.function_store.loc[pos,'ArrivalTime'] = invocation["arrival_time"]

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
        if len(self.function_store) == 0:
            return None
        if self.function_store[self.function_store['ExecuteDuration']>=self.function_store['AverageDuration']]['ExecuteDuration'].isnull().all():
            tmp = self.function_store['AverageDuration'] - self.function_store['ExecuteDuration']
            wait_min = min(tmp)
            wait_min = min([wait_min, 1000])  # to avoid the wait_min larger than 1000 ms, which would lead to time recording error.
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
    
    def get_earlist_with_pre_warm_app(self):
        finished_app_df = self.get_finished_fun_df()
        for i in arange(len(finished_app_df)):
            app = finished_app_df.iloc[i]
            if app['pre_warm_window'] > 0:
                app_to_kill = pd.concat([app_to_kill, app], axis=0)
        return app_to_kill
    