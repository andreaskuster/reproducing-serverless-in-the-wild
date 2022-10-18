#!/usr/bin/env python3
# encoding: utf-8

import numpy as np
import pandas as pd

import model


class Model:

    def __init__(self):
        self.compute_nodes = list()
        # TODO: add performance counters e.g. how many cold starts, how many hot starts
        self.df_mem_available = pd.DataFrame(columns=range(0,11),index=range(1,1441))
        self.df_mem_usage = pd.DataFrame(columns=range(0,11),index=range(1,1441))
        

    def add_compute_nodes(self, num_nodes=1, node_mem_mb=8192):
        # create compute node instances
        for i in range(num_nodes):
            self.compute_nodes.append(model.ComputeNode(i, mem_mb=node_mem_mb))
            self.df_mem_available[i].fillna(self.compute_nodes[i].mem_total(), inplace=True)
            self.df_mem_usage[i].fillna(0, inplace=True)

    def max_mem_avail_node_idx(self):
        return np.argmax([x.mem_available() for x in self.compute_nodes])

    def schedule(self,i, invocation, invocations_num, method='random'):
        """
        Args:
            invocation (Series): the invocated function needed to be loaded
            method (str): how existing app will remove in order to give enough memory space.
                          Option: 'earliest_app', 'largest_mem', 'random'

        The current implementation:
         - always schedules on node0
         - remove random function until enough memory is available
        TODO:
         (1) implement the scheduler from the paper
         (2) Improve e.g. different policy, reinforcement learning, ..
        """
        # make space if necessary (if the app already exists in memory -> we just need to update the metrics)
        if not self.compute_nodes[0].app_exists(invocation):
            # kick off the finished
            while invocation["AverageMem"] > self.compute_nodes[0].mem_available():
                if method == 'random':
                    remove_store = self.compute_nodes[0].get_random_app()
                elif method == 'earliest_app':
                    remove_store = self.compute_nodes[0].get_earliest_app()
                elif method == 'largest_mem':
                    remove_store = self.compute_nodes[0].get_largest_mem_app()
                self.compute_nodes[0].remove_app(remove_store)
        # load app & function
        if not self.compute_nodes[0].function_exists(invocation):
            self.compute_nodes[0].add_function(invocation)
        else:
            self.compute_nodes[0].reset_fun_duration(invocation)
        # let the clock increase 1ms
        self.compute_nodes[0].add_duration()