#!/usr/bin/env python3
# encoding: utf-8

import atexit
import io
from xmlrpc.client import Error
import numpy as np
from numpy._typing import _ArrayLikeStr_co
import pandas as pd

import model


class Model:

    def __init__(self):
        self.compute_nodes = list()
        # TODO: add performance counters e.g. how many cold starts, how many hot starts
        self.df_mem_available = pd.DataFrame(columns=range(0,11),index=range(1,1441))
        self.df_mem_usage = pd.DataFrame(columns=range(0,11),index=range(1,1441))
        self.app_to_reload = pd.DataFrame(columns=["HashApp", "HashFunction", "AverageMem", "AverageDuration", "pre_warm_window", 
                                        "keep_alive_window", "ArrivalTime", "ExecuteDuration", "InvocationCount", "LastUsed", "ColdStartCount"])
        self.ColdStartCount = 0  # recore total cold start number
        self.InvocationCount = 0  # record total invocation number
        # record app-wise cold start, invocation and wasted memory time
        self.app_record = pd.DataFrame(columns=["HashApp", "ColdStartCount", "InvocationCount", "WasteMemoryTime"])  
        

    def add_compute_nodes(self, num_nodes=1, node_mem_mb=8192):
        # create compute node instances
        for i in range(num_nodes):
            self.compute_nodes.append(model.ComputeNode(i, mem_mb=node_mem_mb))
            self.df_mem_available[i].fillna(self.compute_nodes[i].mem_total(), inplace=True)
            self.df_mem_usage[i].fillna(0, inplace=True)

    def max_mem_avail_node_idx(self):
        return np.argmax([x.mem_available() for x in self.compute_nodes])

    def schedule(self, i, invocation, invocations_num, method='random'):
        """
        Args:
            invocation (Series): the invocated function needed to be loaded
            method (str): how existing app will remove in order to give enough memory space.
                          Option: 'earliest_app', 'largest_mem', 'random', 'earlist_with_pre_warm_app'

        The current implementation:
         - always schedules on node0
         - remove random function until enough memory is available
        TODO:
         (1) implement the scheduler from the paper
         (2) Improve e.g. different policy, reinforcement learning, ..
        """

        self.record_start(invocation)

         # make space if necessary (if the app already exists in memory -> we just need to update the metrics)
        if not self.compute_nodes[0].app_exists(invocation):  # to judge whether exist in memory
            # kick off the finished
            while invocation["AverageMem"] > self.compute_nodes[0].mem_available():
                if method == 'random':
                    remove_store = self.compute_nodes[0].get_random_app()
                elif method == 'earliest_app':
                    remove_store = self.compute_nodes[0].get_earliest_app()
                elif method == 'largest_mem':
                    remove_store = self.compute_nodes[0].get_largest_mem_app()
                elif method == 'earlist_with_pre_warm_app':
                    remove_store = self.compute_nodes[0].get_earlist_with_pre_warm_app()
                    ## if we remove here, we should also modify related information
                self.compute_nodes[0].remove_app(remove_store)
                print("use up the memory")
            # record the app-wise cold start
            self.record_cold_start(invocation)

        # load app & function
        if not self.compute_nodes[0].function_exists(invocation):
            self.compute_nodes[0].add_function(invocation)
        else:
            self.compute_nodes[0].reset_fun_duration(invocation)  # reset the function duration
            self.compute_nodes[0].update_app_property(invocation) # reset the app properties, including the pre-warm window, keep-alive window.
        # let the clock increase 1ms
        self.compute_nodes[0].add_duration()
    
    def release_app(self, time):
        """
        release 2 kinds of app:
        1. finished app and their pre-warm window > 0
        2. the app whose keep-warm window time is finished.
        """ 
        finished_app = self.compute_nodes[0].get_finished_fun_df()
        if finished_app is None:
            return
        app_to_kill = pd.DataFrame(columns=["HashApp", "HashFunction", "AverageMem", "AverageDuration", "pre_warm_window", "keep_alive_window",
                     "ArrivalTime", "ExecuteDuration", "InvocationCount", "LastUsed", "ColdStartCount"])
        
        for i in np.arange(len(finished_app)):
            app = finished_app.iloc[i].to_frame().T
            if app['pre_warm_window'][0] > 0:
                app_to_kill = pd.concat([app_to_kill, app], axis=0)
                app['release_time'][0] = time
                app_to_reload = pd.concat([app_to_reload, app], axis=0)
            elif app['pre_warm_window'][0] == 0 and time >= app['ArrivalTime'][0] + app['keep_alive_window'][0]:
                app_to_kill = pd.concat([app_to_kill, app], axis=0)
            elif app['pre_warm_window'][0] == 0 and time < app['ArrivalTime'][0] + app['keep_alive_window'][0]:
                pass
            else:
                raise ValueError
        for i in np.arange(len(app_to_kill)):
            app = app_to_kill.iloc[i]
            self.compute_nodes[0].remove_app(app)
        return
    
    def load_app(self, time):
        """
        reload the apps based on the pre_warm_window and the time
        reset the ArrivalTime
        set the pre_warm_time to 0, it means when the app is finished, it would be released directly, given no new related invocation changes its property.
        """ 
        for i in np.arange(len(self.app_to_reload)):
            app = self.app_to_reload.iloc[i]
            if app['release_time'] + app['pre_warm_window'] >= time:
                app_clean = app.drop('release_time', axis=1)
                app_clean['ArrivalTime'] = time
                app_clean['pre_warm_window'] = 0
                self.compute_nodes[0].add_function(app_clean)
        return
    
    def record_cold_start(self, invocation):
        self.ColdStartCount += 1
        pos = invocation["HashApp"] == self.app_record["HashApp"]
        self.app_record.loc[pos, "ColdStartCount"] = [self.app_record.loc[pos, "ColdStartCount"][0] + 1]
        return
    
    def record_start(self, invocation):
        self.InvocationCount += 1
        if self.app_meet_before(invocation):
            pos = invocation["HashApp"] == self.app_record["HashApp"]
            self.app_record.loc[pos, "InvocationCount"] = [self.app_record.loc[pos, "InvocationCount"][0] + 1]
        
        else: # record this app information.
            df = pd.DataFrame({
                        "HashApp": [invocation["HashApp"]],
                        "ColdStartCount": [0],
                        "InvocationCount": [1],
                        "WasteMemoryTime": [0],
                    })
            self.app_record = pd.concat([self.app_record, df], axis=0)
        return

    def app_meet_before(self, invocation):
        if len(self.app_record) == 0:
            return False
        return invocation["HashApp"] in self.app_record["HashApp"].to_list()