#!/usr/bin/env python3
# encoding: utf-8

import numpy as np
import pandas as pd


class ComputeNode:

    def __init__(self, index, mem_mb=8 * 1024):
        self.index = index
        self.total_mem = mem_mb
        self.mem_avail = self.total_mem
        self.function_store = pd.DataFrame(columns=["HashFunction", "Mem", "InvocationCount", "LastUsed"])

    def mem_available(self):
        return self.mem_avail

    def add_function(self, invocation):
        df = pd.DataFrame({
            "HashFunction": [invocation[1]["HashFunction"]],
            "Mem": [10],  # TODO
            "InvocationCount": [0],  # TODO
            "LastUsed": [0]  # TODO
        })
        self.function_store = pd.concat([self.function_store, df], axis=0)
        # TODO: add to memory: self.mem_avail -= MEM

    def remove_function(self, invocation):
        self.function_store.drop()
        # TODO: remove from memory: self.mem_avail += MEM

    def function_exists(self, invocation):
        return invocation[1]["HashFunction"] in self.function_store["HashFunction"]

    def get_lru_function(self):  # lru = least recently used
        raise NotImplementedError()

    def get_random_function(self):
        return self.function_store.head(1)


class Model:

    def __init__(self):
        self.compute_nodes = list()
        # TODO: add performance counters e.g. how many cold starts, how many hot starts

    def add_compute_nodes(self, num_nodes=1, node_mem_mb=8 * 1024):
        # create compute node instances
        for i in range(num_nodes):
            self.compute_nodes.append(ComputeNode(i, mem_mb=node_mem_mb))

    def max_mem_avail_node_idx(self):
        return np.argmax([x.mem_available() for x in self.compute_nodes])

    def schedule(self, invocation):
        # The current implementation:
        # - always schedules on node0
        # - remove random function until enough memory is available
        if not self.compute_nodes[0].function_exists(invocation):  # if function already exists in memory -> we just need to update the metrics (e.g. lru)
            while invocation[1]["Mem"] > self.compute_nodes[0].mem_available():
                self.compute_nodes[0].remove_function(self.compute_nodes[0].get_random_function())
            self.compute_nodes[0].add_function(invocation)
        # TODO:
        # (1) implement the scheduler from the paper
        # (2) Improve e.g. different policy, reinforcement learning, ..
