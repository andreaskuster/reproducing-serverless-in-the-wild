#!/usr/bin/env python3
# encoding: utf-8

import numpy as np
import pandas as pd


class ComputeNode:

    def __init__(self, index, mem_mb=8192):
        self.index = index
        self.total_mem = mem_mb
        self.mem_avail = self.total_mem
        self.function_store = pd.DataFrame(
            columns=["HashApp", "HashFunction", "AverageMem", "AverageDuration", "InvocationCount", "LastUsed"])

    def mem_available(self):
        return self.mem_avail

    def add_function(self, invocation):
        # cold start -> add data to memory
        if not self.app_exists(invocation):
            self.mem_avail -= invocation["AverageMem"]

        # prep data frame
        df = pd.DataFrame({
            "HashApp": [invocation["HashApp"]],
            "HashFunction": [invocation["HashFunction"]],
            "AverageMem": [invocation["AverageMem"]],
            "AverageDuration": [invocation["AverageDuration"]],
            "InvocationCount": [0],  # TODO
            "LastUsed": [0]  # TODO
        })
        self.function_store = pd.concat([self.function_store, df], axis=0)

    def remove_app(self, invocation):
        self.function_store = self.function_store[self.function_store["HashApp"] != invocation["HashApp"]]
        self.mem_avail += invocation["AverageMem"][0]

    def function_exists(self, invocation):
        return invocation["HashFunction"] in self.function_store["HashFunction"]

    def app_exists(self, invocation):
        return invocation["HashApp"] in self.function_store["HashApp"]

    def get_lru_function(self):  # lru = least recently used
        raise NotImplementedError()

    def get_random_function(self):
        return self.function_store.head(1)

    def get_random_app(self):
        return self.function_store.head(1)


class Model:

    def __init__(self):
        self.compute_nodes = list()
        # TODO: add performance counters e.g. how many cold starts, how many hot starts

    def add_compute_nodes(self, num_nodes=1, node_mem_mb=8192):
        # create compute node instances
        for i in range(num_nodes):
            self.compute_nodes.append(ComputeNode(i, mem_mb=node_mem_mb))

    def max_mem_avail_node_idx(self):
        return np.argmax([x.mem_available() for x in self.compute_nodes])

    def schedule(self, invocation):
        # The current implementation:
        # - always schedules on node0
        # - remove random function until enough memory is available
        # make space if necessary
        if not self.compute_nodes[0].app_exists(
                invocation):  # if function already exists in memory -> we just need to update the metrics (e.g. lru)
            while invocation["AverageMem"] > self.compute_nodes[0].mem_available():
                self.compute_nodes[0].remove_app(self.compute_nodes[0].get_random_app())
        # load app & function
        if not self.compute_nodes[0].function_exists(invocation):
            self.compute_nodes[0].add_function(invocation)
        # TODO:
        # (1) implement the scheduler from the paper
        # (2) Improve e.g. different policy, reinforcement learning, ..
